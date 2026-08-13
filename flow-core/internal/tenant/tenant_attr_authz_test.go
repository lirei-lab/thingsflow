package tenant

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dbpkg "flow-core/internal/db"
)

// Cross-tenant coverage for the attribute REST surface (the BLOCKER of the
// phase-1 review): attribute_kv has NO tenant_id column and the handlers used
// to trust the raw path UUID, so any authenticated user could read or write
// any tenant's attributes. These tests pin the ownership gate on both the GET
// and POST paths, the SYS_ADMIN crossing, and the scope validation.

const (
	attrDeviceA = "55555555-5555-5555-5555-555555555551"
	attrDeviceB = "55555555-5555-5555-5555-555555555552"
)

// newAttrAuthzDB extends the shared authz harness with the attribute-surface
// slice: device + attribute_kv + key_dictionary, one device per tenant, each
// holding one SERVER_SCOPE attribute.
func newAttrAuthzDB(t *testing.T) *sql.DB {
	t.Helper()
	db := newAuthzDB(t)
	stmts := []string{
		`DROP TABLE IF EXISTS attribute_kv CASCADE`,
		`DROP TABLE IF EXISTS twin_registry CASCADE`,
		`DROP TABLE IF EXISTS twin_model CASCADE`,
		`DROP TABLE IF EXISTS device CASCADE`,
		`DROP TABLE IF EXISTS key_dictionary CASCADE`,
		`CREATE TABLE device (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, name text, type text)`,
		`CREATE TABLE twin_model (
			tenant_id uuid NOT NULL, model_id varchar(255) NOT NULL, version varchar(64) NOT NULL,
			kind varchar(64) NOT NULL, schema jsonb NOT NULL,
			PRIMARY KEY (tenant_id, model_id, version))`,
		`CREATE TABLE twin_registry (
			tenant_id uuid NOT NULL, entity_type varchar(255) NOT NULL, entity_id uuid NOT NULL,
			model_id varchar(255), model_version varchar(64),
			UNIQUE (tenant_id, entity_type, entity_id))`,
		`CREATE TABLE key_dictionary (key_id serial PRIMARY KEY, key text UNIQUE)`,
		`CREATE TABLE attribute_kv (
			entity_id uuid, attribute_type int, attribute_key int,
			bool_v boolean, str_v text, long_v bigint, dbl_v double precision, json_v text,
			last_update_ts bigint,
			CONSTRAINT attr_authz_pkey PRIMARY KEY (entity_id, attribute_type, attribute_key))`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("attr schema: %v", err)
		}
	}
	now := time.Now().UnixMilli()
	for _, seed := range []struct{ id, tenant string }{
		{attrDeviceA, tenantA}, {attrDeviceB, tenantB},
	} {
		if _, err := db.Exec(`INSERT INTO device (id, created_time, tenant_id, name, type)
			VALUES ($1, $2, $3, 'dev', 'default')`, seed.id, now, seed.tenant); err != nil {
			t.Fatalf("seed device: %v", err)
		}
	}
	if _, err := db.Exec(`INSERT INTO twin_registry (tenant_id, entity_type, entity_id) VALUES
		($1, 'DEVICE', $2), ($3, 'DEVICE', $4)`, tenantA, attrDeviceA, tenantB, attrDeviceB); err != nil {
		t.Fatalf("seed no-model registry rows: %v", err)
	}
	// Materialise every key this fixture writes because the process-wide cache
	// survives table recreation (see internal/ws's harness).
	keyIDs := map[string]int{}
	for _, key := range []string{"site", "cfgkey"} {
		keyID := dbpkg.GetOrInsertKeyID(key)
		if keyID <= 0 {
			t.Fatalf("key_dictionary seed failed (%s=%d)", key, keyID)
		}
		if _, err := db.Exec(`INSERT INTO key_dictionary (key_id, key) VALUES ($1,$2) ON CONFLICT DO NOTHING`, keyID, key); err != nil {
			t.Fatalf("seed key_dictionary %s: %v", key, err)
		}
		keyIDs[key] = keyID
	}
	for _, seed := range []struct {
		id, val string
	}{{attrDeviceA, "siteA"}, {attrDeviceB, "siteB"}} {
		if _, err := db.Exec(`INSERT INTO attribute_kv (entity_id, attribute_type, attribute_key, str_v, last_update_ts)
			VALUES ($1, 2, $2, $3, $4)`, seed.id, keyIDs["site"], seed.val, now); err != nil {
			t.Fatalf("seed attribute_kv: %v", err)
		}
	}
	return db
}

func doAttrReq(t *testing.T, method, path, body, tok string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if tok != "" {
		req.Header.Set("X-Authorization", "Bearer "+tok)
	}
	w := httptest.NewRecorder()
	HandleAttributeRest(w, req)
	return w
}

func TestHandleAttributeRest_TenantIsolation(t *testing.T) {
	db := newAttrAuthzDB(t)
	tenantAdminB := authzJWT(t, tenantB, "TENANT_ADMIN")
	sysAdmin := authzJWT(t, "", "SYS_ADMIN")

	valuesPath := func(id string) string {
		return "/api/plugins/telemetry/DEVICE/" + id + "/values/attributes/SERVER_SCOPE"
	}
	keysPath := func(id string) string {
		return "/api/plugins/telemetry/DEVICE/" + id + "/keys/attributes"
	}

	// Tenant B reading/writing tenant A's device → 403, nothing leaks.
	if w := doAttrReq(t, "GET", valuesPath(attrDeviceA), "", tenantAdminB); w.Code != http.StatusForbidden {
		t.Errorf("cross-tenant GET values: status = %d, want 403 (body=%s)", w.Code, w.Body.String())
	} else if strings.Contains(w.Body.String(), "siteA") {
		t.Errorf("cross-tenant GET leaked the value: %s", w.Body.String())
	}
	if w := doAttrReq(t, "GET", keysPath(attrDeviceA), "", tenantAdminB); w.Code != http.StatusForbidden {
		t.Errorf("cross-tenant GET keys: status = %d, want 403", w.Code)
	}
	if w := doAttrReq(t, "POST", "/api/plugins/telemetry/DEVICE/"+attrDeviceA+"/SERVER_SCOPE",
		`{"site":"pwned"}`, tenantAdminB); w.Code != http.StatusForbidden {
		t.Errorf("cross-tenant POST: status = %d, want 403", w.Code)
	}
	var val string
	if err := db.QueryRow(`SELECT str_v FROM attribute_kv WHERE entity_id = $1`, attrDeviceA).Scan(&val); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if val != "siteA" {
		t.Errorf("cross-tenant POST overwrote the attribute: %q", val)
	}

	// Tenant B on its OWN device still works.
	if w := doAttrReq(t, "GET", valuesPath(attrDeviceB), "", tenantAdminB); w.Code != http.StatusOK ||
		!strings.Contains(w.Body.String(), "siteB") {
		t.Errorf("own GET values: status = %d body=%s, want 200 with siteB", w.Code, w.Body.String())
	}
	if w := doAttrReq(t, "POST", "/api/plugins/telemetry/DEVICE/"+attrDeviceB+"/SERVER_SCOPE",
		`{"site":"siteB2"}`, tenantAdminB); w.Code != http.StatusOK {
		t.Errorf("own POST: status = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}

	// SYS_ADMIN crosses tenants (platform admin console).
	if w := doAttrReq(t, "GET", valuesPath(attrDeviceA), "", sysAdmin); w.Code != http.StatusOK ||
		!strings.Contains(w.Body.String(), "siteA") {
		t.Errorf("sysadmin GET values: status = %d body=%s, want 200 with siteA", w.Code, w.Body.String())
	}
	if w := doAttrReq(t, "POST", "/api/plugins/telemetry/DEVICE/"+attrDeviceA+"/SERVER_SCOPE",
		`{"site":"sys"}`, sysAdmin); w.Code != http.StatusOK {
		t.Errorf("sysadmin POST: status = %d, want 200", w.Code)
	}

	// No token → 401; unknown entity type fails closed → 403.
	if w := doAttrReq(t, "GET", valuesPath(attrDeviceB), "", ""); w.Code != http.StatusUnauthorized {
		t.Errorf("no token: status = %d, want 401", w.Code)
	}
	if w := doAttrReq(t, "GET", "/api/plugins/telemetry/WIDGET_TYPE/"+attrDeviceB+"/values/attributes/SERVER_SCOPE",
		"", tenantAdminB); w.Code != http.StatusForbidden {
		t.Errorf("unknown entity type: status = %d, want 403 (fail closed)", w.Code)
	}
}

// TestHandleAttributeRest_ScopeValidation — a typo'd scope must be rejected,
// not silently rewritten into SERVER_SCOPE (the old fallback persisted data in
// a scope the caller never asked for).
func TestHandleAttributeRest_ScopeValidation(t *testing.T) {
	newAttrAuthzDB(t)
	tenantAdminB := authzJWT(t, tenantB, "TENANT_ADMIN")

	if w := doAttrReq(t, "POST", "/api/plugins/telemetry/DEVICE/"+attrDeviceB+"/SHAERD_SCOPE",
		`{"site":"typo"}`, tenantAdminB); w.Code != http.StatusBadRequest ||
		!strings.Contains(w.Body.String(), "Unknown attribute scope") {
		t.Errorf("typo scope POST: status = %d body=%s, want 400 Unknown attribute scope", w.Code, w.Body.String())
	}
	if w := doAttrReq(t, "GET", "/api/plugins/telemetry/DEVICE/"+attrDeviceB+"/values/attributes/SHAERD_SCOPE",
		"", tenantAdminB); w.Code != http.StatusBadRequest {
		t.Errorf("typo scope GET: status = %d, want 400", w.Code)
	}
	// Lower-case / padded spellings normalize instead of failing.
	if w := doAttrReq(t, "POST", "/api/plugins/telemetry/DEVICE/"+attrDeviceB+"/shared_scope",
		`{"cfgkey":"v"}`, tenantAdminB); w.Code != http.StatusOK {
		t.Errorf("lower-case scope POST: status = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}
}
