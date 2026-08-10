package policy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
)

func TestPolicyHTTPStatusTenantAndEnvelopeMatrix(t *testing.T) {
	db := newPolicyTestDB(t)
	setupPolicySchema(t, db)
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() { dbpkg.SetPoolForTest(t, nil) })
	authpkg.InitConfig()
	tenantA := policyJWT(t, "TENANT_ADMIN", policyTenantA)
	tenantB := policyJWT(t, "TENANT_ADMIN", policyTenantB)
	mux := policyTestMux()

	assertPolicyHTTPStatusAndEnvelope(t, mux, http.MethodGet, "/api/policies", nil, "", http.StatusUnauthorized)
	assertPolicyHTTPStatusAndEnvelope(t, mux, http.MethodPatch, "/api/policies", nil, tenantA, http.StatusMethodNotAllowed)
	assertPolicyHTTPStatusAndEnvelope(t, mux, http.MethodPost, "/api/policies", []byte(`{"policyId":"owner","version":"1.0"}`), tenantA, http.StatusBadRequest)

	created := doPolicyRequest(t, mux, http.MethodPost, "/api/policies", ownerDoc(policyTenantA, "1.0.0"), tenantA)
	if created.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	var createdBody map[string]interface{}
	decodePolicyResponse(t, created, &createdBody)
	if createdBody["policyId"] != "owner" || createdBody["version"] != "1.0.0" || createdBody["deprecated"] != false {
		t.Fatalf("create response=%v", createdBody)
	}
	assertPolicyHTTPStatusAndEnvelope(t, mux, http.MethodPost, "/api/policies", ownerDoc(policyTenantA, "1.0.0"), tenantA, http.StatusConflict)

	otherTenant := doPolicyRequest(t, mux, http.MethodPost, "/api/policies", ownerDoc(policyTenantB, "1.0.0"), tenantB)
	if otherTenant.Code != http.StatusOK {
		t.Fatalf("tenant B create status=%d body=%s", otherTenant.Code, otherTenant.Body.String())
	}
	// A policy only tenant A owns.
	maintenance := json.RawMessage(fmt.Sprintf(`{
		"policyId": "maintenance", "version": "1.0.0",
		"subjects": ["tenant:%s"], "resources": ["thing:/%s/#"],
		"grants": [{"resource": "thing:/%s/#", "actions": ["READ"]}], "revokes": []
	}`, policyTenantA, policyTenantA, policyTenantA))
	if response := doPolicyRequest(t, mux, http.MethodPost, "/api/policies", maintenance, tenantA); response.Code != http.StatusOK {
		t.Fatalf("create maintenance status=%d body=%s", response.Code, response.Body.String())
	}

	list := doPolicyRequest(t, mux, http.MethodGet, "/api/policies?page=0&pageSize=5000&latest=true&includeDeprecated=false", nil, tenantA)
	if list.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", list.Code, list.Body.String())
	}
	var page struct {
		Data          []map[string]interface{} `json:"data"`
		TotalElements int                      `json:"totalElements"`
		TotalPages    int                      `json:"totalPages"`
		HasNext       bool                     `json:"hasNext"`
		Page          int                      `json:"page"`
	}
	decodePolicyResponse(t, list, &page)
	if page.TotalElements != 2 || len(page.Data) != 2 {
		t.Fatalf("tenant-scoped page=%+v", page)
	}
	policyIDs := []string{page.Data[0]["policyId"].(string), page.Data[1]["policyId"].(string)}
	if !contains(policyIDs, "owner") || !contains(policyIDs, "maintenance") {
		t.Fatalf("expected owner + maintenance in page, got %v", policyIDs)
	}
	for _, path := range []string{
		"/api/policies?page=-1", "/api/policies?page=abc", "/api/policies?pageSize=nope",
		"/api/policies?latest=perhaps", "/api/policies?includeDeprecated=perhaps",
	} {
		assertPolicyHTTPStatusAndEnvelope(t, mux, http.MethodGet, path, nil, tenantA, http.StatusBadRequest)
	}

	get := doPolicyRequest(t, mux, http.MethodGet, "/api/policies/owner/1.0.0", nil, tenantA)
	if get.Code != http.StatusOK {
		t.Fatalf("get status=%d body=%s", get.Code, get.Body.String())
	}
	assertPolicyHTTPStatusAndEnvelope(t, mux, http.MethodGet, "/api/policies/Owner/1.0.0", nil, tenantA, http.StatusBadRequest)
	assertPolicyHTTPStatusAndEnvelope(t, mux, http.MethodGet, "/api/policies/owner/2147483648.0.0", nil, tenantA, http.StatusBadRequest)
	assertPolicyHTTPStatusAndEnvelope(t, mux, http.MethodGet, "/api/policies/missing/1.0.0", nil, tenantA, http.StatusNotFound)
	// Cross-tenant read of tenant A's maintenance policy from tenant B is 404 (fail closed).
	assertPolicyHTTPStatusAndEnvelope(t, mux, http.MethodGet, "/api/policies/maintenance/1.0.0", nil, tenantB, http.StatusNotFound)

	if response := doPolicyRequest(t, mux, http.MethodDelete, "/api/policies/owner/1.0.0", nil, tenantA); response.Code != http.StatusOK {
		t.Fatalf("deprecate status=%d body=%s", response.Code, response.Body.String())
	}
	if response := doPolicyRequest(t, mux, http.MethodDelete, "/api/policies/owner/1.0.0", nil, tenantA); response.Code != http.StatusOK {
		t.Fatalf("idempotent deprecate status=%d body=%s", response.Code, response.Body.String())
	}

	dbpkg.SetPoolForTest(t, nil)
	assertPolicyHTTPStatusAndEnvelope(t, mux, http.MethodGet, "/api/policies", nil, tenantA, http.StatusInternalServerError)
}

func TestPolicyHTTPRejectsInvalidBodies(t *testing.T) {
	db := newPolicyTestDB(t)
	setupPolicySchema(t, db)
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() { dbpkg.SetPoolForTest(t, nil) })
	authpkg.InitConfig()
	tenantA := policyJWT(t, "TENANT_ADMIN", policyTenantA)
	mux := policyTestMux()

	rejects := []string{
		``,
		`not json`,
		`{"policyId":"owner","version":"1.0.0","resources":["http://nope"]}`,
		`{"policyId":"owner","version":"1.0.0","subjects":["group:x"]}`,
		`{"policyId":"owner","version":"1.0.0","grants":[{"resource":"thing:/a","actions":[]}]}`,
	}
	for _, body := range rejects {
		assertPolicyHTTPStatusAndEnvelope(t, mux, http.MethodPost, "/api/policies", []byte(body), tenantA, http.StatusBadRequest)
	}
}

func policyTestMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/policies", HandleCollection)
	mux.HandleFunc("/api/policies/{policyId}/{version}", HandleVersion)
	return mux
}

func policyJWT(t *testing.T, authority, tenantID string) string {
	t.Helper()
	token, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID: "99999999-9999-9999-9999-999999999999", Email: fmt.Sprintf("%s@test.local", authority),
		Authority: authority, TenantID: tenantID, Enabled: true,
	}, "policy-test")
	if err != nil {
		t.Fatalf("generate JWT: %v", err)
	}
	return token
}

func doPolicyRequest(t *testing.T, handler http.Handler, method, path string, body []byte, token string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, bytes.NewReader(body))
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

func assertPolicyHTTPStatusAndEnvelope(t *testing.T, handler http.Handler, method, path string, body []byte, token string, status int) {
	t.Helper()
	response := doPolicyRequest(t, handler, method, path, body, token)
	if response.Code != status {
		t.Fatalf("%s %s status=%d body=%s, want %d", method, path, response.Code, response.Body.String(), status)
	}
	var envelope struct {
		Status    int    `json:"status"`
		Message   string `json:"message"`
		ErrorCode int    `json:"errorCode"`
		Timestamp int64  `json:"timestamp"`
	}
	decodePolicyResponse(t, response, &envelope)
	if envelope.Status != status || envelope.Message == "" || envelope.ErrorCode == 0 || envelope.Timestamp == 0 {
		t.Fatalf("invalid error envelope for %s %s: %+v body=%s", method, path, envelope, response.Body.String())
	}
}

func decodePolicyResponse(t *testing.T, response *httptest.ResponseRecorder, target interface{}) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response status=%d body=%s: %v", response.Code, response.Body.String(), err)
	}
}
