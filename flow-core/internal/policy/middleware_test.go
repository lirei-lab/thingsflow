package policy

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
)

// The middleware adversarial tests exercise the full
// middleware → Store.Resolve → Authorize path. The entity resolver is faked
// (returns the owning tenant + a policyId), while the policy catalog is real
// (DSN-backed), so cross-tenant / revoke-override / path-granular behavior is
// proven against an actual resolved policy document.

func policyAdminJWT(t *testing.T, tenantID string) string {
	t.Helper()
	token, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID: "99999999-9999-9999-9999-999999999999", Email: "admin@test.local",
		Authority: "TENANT_ADMIN", TenantID: tenantID, Enabled: true,
	}, "policy-middleware-test")
	if err != nil {
		t.Fatalf("generate JWT: %v", err)
	}
	return token
}

func policySysAdminJWT(t *testing.T) string {
	t.Helper()
	token, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID: "99999999-9999-9999-9999-999999999999", Email: "sysadmin@test.local",
		Authority: "SYS_ADMIN", TenantID: "", Enabled: true,
	}, "policy-middleware-test")
	if err != nil {
		t.Fatalf("generate JWT: %v", err)
	}
	return token
}

// staticResolver returns the given tenant + policyId for any entity.
func staticResolver(tenantID, policyID string) Resolver {
	return func(_ context.Context, _, _ string) (string, string, error) {
		return tenantID, policyID, nil
	}
}

// setTwinPathValues populates the {entityType}/{entityId} path values the
// enforcement middleware reads via r.PathValue. In production the ServeMux
// pattern does this; the unit tests call the wrapped handlers directly, so the
// values are set explicitly here.
func setTwinPathValues(request *http.Request) {
	const prefix = "/api/twins/"
	rest := request.URL.Path
	if len(rest) >= len(prefix) && rest[:len(prefix)] == prefix {
		parts := strings.Split(strings.TrimPrefix(rest, prefix), "/")
		if len(parts) >= 2 {
			request.SetPathValue("entityType", parts[0])
			request.SetPathValue("entityId", parts[1])
		}
	}
}

func doTwinRead(handler http.HandlerFunc, method, path, token string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, nil)
	setTwinPathValues(request)
	if token != "" {
		request.Header.Set("X-Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func doTwinWrite(handler http.HandlerFunc, method, path string, body []byte, token string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewReader(body))
	setTwinPathValues(request)
	if token != "" {
		request.Header.Set("X-Authorization", "Bearer "+token)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func twinOK(w http.ResponseWriter, _ *http.Request, _, _ string) {
	httputilWriteJSONOK(w)
}

// twinOKHandler satisfies the plain http.HandlerFunc variant (list/model).
func twinOKHandler(w http.ResponseWriter, _ *http.Request) {
	httputilWriteJSONOK(w)
}

func httputilWriteJSONOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func TestMiddlewareCrossTenantDenied(t *testing.T) {
	db := newPolicyTestDB(t)
	setupPolicySchema(t, db)
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() { dbpkg.SetPoolForTest(t, nil) })
	authpkg.InitConfig()
	t.Setenv("POLICY_ENFORCEMENT_ENABLED", "true")

	// Tenant A owns the twin and its default policy; the owning tenant subject	// is bound to tenant:A.
	tenantA := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	tenantB := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	path := "/api/twins/DEVICE/11111111-1111-1111-1111-111111111111"

	readHandler := EnforceRead(staticResolver(tenantA, "tenant:"+tenantA+":default"), twinOK)
	t.Run("owning tenant passes", func(t *testing.T) {
		if response := doTwinRead(readHandler, http.MethodGet, path, policyAdminJWT(t, tenantA)); response.Code != http.StatusOK {
			t.Fatalf("owning tenant status=%d body=%s", response.Code, response.Body.String())
		}
	})
	t.Run("foreign tenant denied 403", func(t *testing.T) {
		response := doTwinRead(readHandler, http.MethodGet, path, policyAdminJWT(t, tenantB))
		if response.Code != http.StatusForbidden {
			t.Fatalf("cross-tenant status=%d body=%s, want 403", response.Code, response.Body.String())
		}
	})
	t.Run("SYS_ADMIN passes", func(t *testing.T) {
		if response := doTwinRead(readHandler, http.MethodGet, path, policySysAdminJWT(t)); response.Code != http.StatusOK {
			t.Fatalf("SYS_ADMIN status=%d body=%s", response.Code, response.Body.String())
		}
	})
	t.Run("unauthenticated 401", func(t *testing.T) {
		if response := doTwinRead(readHandler, http.MethodGet, path, ""); response.Code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated status=%d, want 401", response.Code)
		}
	})
}

func TestMiddlewareRevokeOverrideEndToEnd(t *testing.T) {
	db := newPolicyTestDB(t)
	setupPolicySchema(t, db)
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() { dbpkg.SetPoolForTest(t, nil) })
	authpkg.InitConfig()
	t.Setenv("POLICY_ENFORCEMENT_ENABLED", "true")

	tenantA := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	root := "thing:/" + tenantA + "/DEVICE/11111111-1111-1111-1111-111111111111"

	// An explicit maintenance policy: grants WRITE on the feature root,
	// revokes WRITE on the setpoint sub-path (deep revoke wins).
	doc := json.RawMessage(`{"policyId":"maintenance","version":"1.0.0","subjects":["tenant:` + tenantA + `"],"resources":["` + root + `"],"grants":[{"resource":"` + root + `","actions":["READ","WRITE"]}],"revokes":[{"resource":"` + root + `/features/setpoint","actions":["WRITE"]}]}`)
	if _, err := NewStore(db).Create(context.Background(), tenantA, doc); err != nil {
		t.Fatalf("create maintenance policy: %v", err)
	}

	// Twin is pinned to the maintenance policy.
	writeHandler := EnforceWrite(staticResolver(tenantA, "maintenance"), twinOK)
	path := "/api/twins/DEVICE/11111111-1111-1111-1111-111111111111/features"
	token := policyAdminJWT(t, tenantA)

	t.Run("granted feature write allowed", func(t *testing.T) {
		body := []byte(`{"features":{"temp":{"properties":{"value":22}}}}`)
		if response := doTwinWrite(writeHandler, http.MethodPut, path, body, token); response.Code != http.StatusOK {
			t.Fatalf("granted feature write status=%d body=%s", response.Code, response.Body.String())
		}
	})
	t.Run("revoked feature write denied 403", func(t *testing.T) {
		body := []byte(`{"features":{"setpoint":{"properties":{"value":200}}}}`)
		response := doTwinWrite(writeHandler, http.MethodPut, path, body, token)
		if response.Code != http.StatusForbidden {
			t.Fatalf("revoked feature write status=%d body=%s, want 403", response.Code, response.Body.String())
		}
	})
	t.Run("mixed write with a revoked feature denied 403", func(t *testing.T) {
		body := []byte(`{"features":{"temp":{"properties":{"value":22}},"setpoint":{"properties":{"value":200}}}}`)
		response := doTwinWrite(writeHandler, http.MethodPut, path, body, token)
		if response.Code != http.StatusForbidden {
			t.Fatalf("mixed write status=%d body=%s, want 403", response.Code, response.Body.String())
		}
	})
}

func TestMiddlewarePathGranularReadWrite(t *testing.T) {
	db := newPolicyTestDB(t)
	setupPolicySchema(t, db)
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() { dbpkg.SetPoolForTest(t, nil) })
	authpkg.InitConfig()
	t.Setenv("POLICY_ENFORCEMENT_ENABLED", "true")

	tenantA := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	root := "thing:/" + tenantA + "/DEVICE/11111111-1111-1111-1111-111111111111"
	doc := json.RawMessage(`{"policyId":"viewer","version":"1.0.0","subjects":["tenant:` + tenantA + `"],"resources":["` + root + `"],"grants":[{"resource":"` + root + `","actions":["READ"]},{"resource":"` + root + `/features/temp","actions":["READ"]},{"resource":"` + root + `/attributes/name","actions":["READ"]}],"revokes":[]}`)
	if _, err := NewStore(db).Create(context.Background(), tenantA, doc); err != nil {
		t.Fatalf("create viewer policy: %v", err)
	}

	readHandler := EnforceRead(staticResolver(tenantA, "viewer"), twinOK)
	writeHandler := EnforceWrite(staticResolver(tenantA, "viewer"), twinOK)
	token := policyAdminJWT(t, tenantA)

	// Read on the granted feature path is allowed (READ grant).
	if response := doTwinRead(readHandler, http.MethodGet, "/api/twins/DEVICE/11111111-1111-1111-1111-111111111111", token); response.Code != http.StatusOK {
		t.Fatalf("base read status=%d body=%s", response.Code, response.Body.String())
	}
	// Write to a READ-only feature is denied.
	response := doTwinWrite(writeHandler, http.MethodPut, "/api/twins/DEVICE/11111111-1111-1111-1111-111111111111/features", []byte(`{"features":{"temp":{"properties":{"value":1}}}}`), token)
	if response.Code != http.StatusForbidden {
		t.Fatalf("READ-only feature write status=%d body=%s, want 403", response.Code, response.Body.String())
	}
}

func TestMiddlewareEnforcementGate(t *testing.T) {
	db := newPolicyTestDB(t)
	setupPolicySchema(t, db)
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() { dbpkg.SetPoolForTest(t, nil) })
	authpkg.InitConfig()
	t.Setenv("POLICY_ENFORCEMENT_ENABLED", "false")

	// With enforcement disabled, a foreign tenant passes through to the handler
	// (the existing tenant isolation inside the handler still applies — the
	// middleware just does not add policy checks).
	tenantA := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	tenantB := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	path := "/api/twins/DEVICE/11111111-1111-1111-1111-111111111111"
	readHandler := EnforceRead(staticResolver(tenantA, "tenant:"+tenantA+":default"), twinOK)
	if response := doTwinRead(readHandler, http.MethodGet, path, policyAdminJWT(t, tenantB)); response.Code != http.StatusOK {
		t.Fatalf("enforcement disabled status=%d body=%s, want pass-through", response.Code, response.Body.String())
	}
}

func TestMiddlewareModelWriteAndList(t *testing.T) {
	db := newPolicyTestDB(t)
	setupPolicySchema(t, db)
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() { dbpkg.SetPoolForTest(t, nil) })
	authpkg.InitConfig()
	t.Setenv("POLICY_ENFORCEMENT_ENABLED", "true")

	tenantA := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	tenantB := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	path := "/api/twins/DEVICE/11111111-1111-1111-1111-111111111111/model"

	modelHandler := EnforceModelWrite(staticResolver(tenantA, "tenant:"+tenantA+":default"), twinOKHandler)
	t.Run("owning tenant model write passes", func(t *testing.T) {
		if response := doTwinWrite(modelHandler, http.MethodPut, path, []byte(`{}`), policyAdminJWT(t, tenantA)); response.Code != http.StatusOK {
			t.Fatalf("owning tenant model write status=%d body=%s", response.Code, response.Body.String())
		}
	})
	t.Run("foreign tenant model write denied 403", func(t *testing.T) {
		response := doTwinWrite(modelHandler, http.MethodPut, path, []byte(`{}`), policyAdminJWT(t, tenantB))
		if response.Code != http.StatusForbidden {
			t.Fatalf("cross-tenant model write status=%d body=%s, want 403", response.Code, response.Body.String())
		}
	})

	listHandler := EnforceList(twinOKHandler)
	t.Run("owning tenant list passes", func(t *testing.T) {
		if response := doTwinRead(listHandler, http.MethodGet, "/api/twins", policyAdminJWT(t, tenantA)); response.Code != http.StatusOK {
			t.Fatalf("owning tenant list status=%d body=%s", response.Code, response.Body.String())
		}
	})
	t.Run("SYS_ADMIN list passes", func(t *testing.T) {
		if response := doTwinRead(listHandler, http.MethodGet, "/api/twins", policySysAdminJWT(t)); response.Code != http.StatusOK {
			t.Fatalf("SYS_ADMIN list status=%d body=%s", response.Code, response.Body.String())
		}
	})
}

// TestMiddlewareWriteFailsClosedOnMalformedBody guards the EnforceWrite gate:
// a body that is not valid JSON at all must be rejected with 400 and must NOT
// reach the handler — otherwise a crafted body could fail the middleware's
// path parse and pass an unenforced write through while the handler still
// writes.
func TestMiddlewareWriteFailsClosedOnMalformedBody(t *testing.T) {
	db := newPolicyTestDB(t)
	setupPolicySchema(t, db)
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() { dbpkg.SetPoolForTest(t, nil) })
	authpkg.InitConfig()
	t.Setenv("POLICY_ENFORCEMENT_ENABLED", "true")

	tenantA := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	writeHandler := EnforceWrite(staticResolver(tenantA, "tenant:"+tenantA+":default"), twinOK)
	token := policyAdminJWT(t, tenantA)

	for _, body := range []string{`{`, `not json`, `{"features":}`} {
		response := doTwinWrite(writeHandler, http.MethodPut, "/api/twins/DEVICE/11111111-1111-1111-1111-111111111111/features", []byte(body), token)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("unparseable body %q status=%d body=%s, want 400 (fail closed)", body, response.Code, response.Body.String())
		}
	}
}

// TestMiddlewareWriteToleratesExtraneousKey guards the route-scoped path
// parse: a valid write carrying an extraneous key for the other route (whether
// malformed or a well-formed object) must NOT be rejected — the handler's
// single-key struct ignores it too — but the route's own valid key's paths
// must still be enforced, so a policy denying them still 403s.
func TestMiddlewareWriteToleratesExtraneousKey(t *testing.T) {
	db := newPolicyTestDB(t)
	setupPolicySchema(t, db)
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() { dbpkg.SetPoolForTest(t, nil) })
	authpkg.InitConfig()
	t.Setenv("POLICY_ENFORCEMENT_ENABLED", "true")

	tenantA := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	root := "thing:/" + tenantA + "/DEVICE/11111111-1111-1111-1111-111111111111"
	// viewer grants WRITE on attributes/name only — an extraneous features
	// object (even one not granted) must not turn the write into a 403, and a
	// READ-only attribute must still deny.
	doc := json.RawMessage(`{"policyId":"viewer","version":"1.0.0","subjects":["tenant:` + tenantA + `"],"resources":["` + root + `"],"grants":[{"resource":"` + root + `/attributes/name","actions":["WRITE"]}],"revokes":[]}`)
	if _, err := NewStore(db).Create(context.Background(), tenantA, doc); err != nil {
		t.Fatalf("create viewer policy: %v", err)
	}

	viewerHandler := EnforceWrite(staticResolver(tenantA, "viewer"), twinOK)
	ownerHandler := EnforceWrite(staticResolver(tenantA, "tenant:"+tenantA+":default"), twinOK)
	token := policyAdminJWT(t, tenantA)
	path := "/api/twins/DEVICE/11111111-1111-1111-1111-111111111111/attributes"

	// Well-formed extraneous features object on an attributes write: only the
	// attributes key is enforced, so the granted write passes.
	withObject := []byte(`{"attributes":{"name":"x"},"features":{"temp":{"properties":{"value":1}}}}`)
	if response := doTwinWrite(viewerHandler, http.MethodPut, path, withObject, token); response.Code != http.StatusOK {
		t.Fatalf("granted attributes write + extraneous object status=%d body=%s, want 200", response.Code, response.Body.String())
	}
	// Malformed extraneous features value on an attributes write: tolerated.
	withMalformed := []byte(`{"attributes":{"name":"x"},"features":0}`)
	if response := doTwinWrite(viewerHandler, http.MethodPut, path, withMalformed, token); response.Code != http.StatusOK {
		t.Fatalf("granted attributes write + malformed extraneous status=%d body=%s, want 200", response.Code, response.Body.String())
	}
	// The route's own key is still enforced: a READ-only attribute denies.
	readOnly := json.RawMessage(`{"policyId":"reader","version":"1.0.0","subjects":["tenant:` + tenantA + `"],"resources":["` + root + `"],"grants":[{"resource":"` + root + `/attributes/name","actions":["READ"]}],"revokes":[]}`)
	if _, err := NewStore(db).Create(context.Background(), tenantA, readOnly); err != nil {
		t.Fatalf("create reader policy: %v", err)
	}
	readerHandler := EnforceWrite(staticResolver(tenantA, "reader"), twinOK)
	response := doTwinWrite(readerHandler, http.MethodPut, path, withObject, token)
	if response.Code != http.StatusForbidden {
		t.Fatalf("READ-only attribute write status=%d body=%s, want 403 (route key still enforced)", response.Code, response.Body.String())
	}
	// Owner default still passes.
	if response := doTwinWrite(ownerHandler, http.MethodPut, path, withObject, token); response.Code != http.StatusOK {
		t.Fatalf("owner write status=%d body=%s, want 200", response.Code, response.Body.String())
	}
}

// TestMiddlewareResolverErrorFailsClosed guards the resolver leg: only a
// missing entity (ErrNoRows) passes through for the handler's 404; any other
// resolver failure must fail closed (500) rather than silently disabling
// enforcement for an existing protected twin.
func TestMiddlewareResolverErrorFailsClosed(t *testing.T) {
	db := newPolicyTestDB(t)
	setupPolicySchema(t, db)
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() { dbpkg.SetPoolForTest(t, nil) })
	authpkg.InitConfig()
	t.Setenv("POLICY_ENFORCEMENT_ENABLED", "true")

	tenantA := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	path := "/api/twins/DEVICE/11111111-1111-1111-1111-111111111111"
	token := policyAdminJWT(t, tenantA)

	t.Run("transient resolver error fails closed 500", func(t *testing.T) {
		failing := func(_ context.Context, _, _ string) (string, string, error) {
			return "", "", errors.New("registry read failed")
		}
		handler := EnforceRead(failing, twinOK)
		response := doTwinRead(handler, http.MethodGet, path, token)
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("resolver error status=%d body=%s, want 500 (fail closed)", response.Code, response.Body.String())
		}
	})
	t.Run("missing entity passes through for handler 404", func(t *testing.T) {
		notFound := func(_ context.Context, _, _ string) (string, string, error) {
			return "", "", sql.ErrNoRows
		}
		handler := EnforceRead(notFound, twinOK)
		response := doTwinRead(handler, http.MethodGet, path, token)
		if response.Code != http.StatusOK {
			t.Fatalf("missing entity status=%d body=%s, want pass-through", response.Code, response.Body.String())
		}
	})
}
