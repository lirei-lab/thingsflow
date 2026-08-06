package twinmodel

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
)

func TestTwinModelHTTPStatusTenantAndEnvelopeMatrix(t *testing.T) {
	db := newCatalogTestDB(t)
	setupCatalogTestSchema(t, db)
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() { dbpkg.SetPoolForTest(t, nil) })
	authpkg.InitConfig()
	tenantA := catalogJWT(t, "TENANT_ADMIN", catalogTenantA)
	tenantB := catalogJWT(t, "TENANT_ADMIN", catalogTenantB)
	sysAdmin := catalogJWT(t, "SYS_ADMIN", "")
	mux := catalogTestMux()

	assertHTTPStatusAndEnvelope(t, mux, http.MethodGet, "/api/twin-models", nil, "", http.StatusUnauthorized)
	assertHTTPStatusAndEnvelope(t, mux, http.MethodPatch, "/api/twin-models", nil, tenantA, http.StatusMethodNotAllowed)
	assertHTTPStatusAndEnvelope(t, mux, http.MethodPost, "/api/twin-models", []byte(`{"modelId":"bad","version":"1.0.0","kind":"DEVICE","oneOf":[]}`), tenantA, http.StatusBadRequest)

	created := doCatalogRequest(t, mux, http.MethodPost, "/api/twin-models", authoredModel("Energy Meter", "1.0.9", "DEVICE"), tenantA)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	var createdBody map[string]interface{}
	decodeCatalogResponse(t, created, &createdBody)
	if createdBody["modelId"] != "energy_meter" || createdBody["x-owner"] != "ops" || createdBody["deprecated"] != false {
		t.Fatalf("create response=%v", createdBody)
	}
	assertHTTPStatusAndEnvelope(t, mux, http.MethodPost, "/api/twin-models", authoredModel("Energy Meter", "1.0.9", "DEVICE"), tenantA, http.StatusConflict)

	otherTenant := doCatalogRequest(t, mux, http.MethodPost, "/api/twin-models", authoredModel("Energy Meter", "1.0.9", "DEVICE"), tenantB)
	if otherTenant.Code != http.StatusCreated {
		t.Fatalf("tenant B same model status=%d body=%s", otherTenant.Code, otherTenant.Body.String())
	}
	list := doCatalogRequest(t, mux, http.MethodGet, "/api/twin-models?page=0&pageSize=5000&kind=DEVICE&latest=true&includeDeprecated=false", nil, tenantA)
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
	decodeCatalogResponse(t, list, &page)
	if page.TotalElements != 1 || len(page.Data) != 1 || page.Data[0]["modelId"] != "energy_meter" {
		t.Fatalf("tenant-scoped page=%+v", page)
	}
	for _, path := range []string{
		"/api/twin-models?page=-1", "/api/twin-models?page=abc", "/api/twin-models?pageSize=nope",
		"/api/twin-models?kind=GATEWAY", "/api/twin-models?latest=perhaps", "/api/twin-models?includeDeprecated=perhaps",
	} {
		assertHTTPStatusAndEnvelope(t, mux, http.MethodGet, path, nil, tenantA, http.StatusBadRequest)
	}

	get := doCatalogRequest(t, mux, http.MethodGet, "/api/twin-models/energy_meter/1.0.9", nil, tenantA)
	if get.Code != http.StatusOK {
		t.Fatalf("get status=%d body=%s", get.Code, get.Body.String())
	}
	assertHTTPStatusAndEnvelope(t, mux, http.MethodGet, "/api/twin-models/Energy_Meter/1.0.9", nil, tenantA, http.StatusBadRequest)
	assertHTTPStatusAndEnvelope(t, mux, http.MethodGet, "/api/twin-models/energy_meter/2147483648.0.0", nil, tenantA, http.StatusBadRequest)
	assertHTTPStatusAndEnvelope(t, mux, http.MethodGet, "/api/twin-models/missing/1.0.0", nil, tenantA, http.StatusNotFound)

	if response := doCatalogRequest(t, mux, http.MethodDelete, "/api/twin-models/energy_meter/1.0.9", nil, tenantA); response.Code != http.StatusOK {
		t.Fatalf("deprecate status=%d body=%s", response.Code, response.Body.String())
	}
	if response := doCatalogRequest(t, mux, http.MethodDelete, "/api/twin-models/energy_meter/1.0.9", nil, tenantA); response.Code != http.StatusOK {
		t.Fatalf("idempotent deprecate status=%d body=%s", response.Code, response.Body.String())
	}
	assertHTTPStatusAndEnvelope(t, mux, http.MethodPut, "/api/twins/DEVICE/"+catalogDeviceA+"/model", []byte(`{"modelId":"energy_meter","version":"1.0.9"}`), tenantA, http.StatusConflict)

	for _, model := range []json.RawMessage{
		authoredModel("Energy Meter", "2.0.0", "DEVICE"),
		authoredModel("Building", "1.0.0", "ASSET"),
	} {
		response := doCatalogRequest(t, mux, http.MethodPost, "/api/twin-models", model, tenantA)
		if response.Code != http.StatusCreated {
			t.Fatalf("create repoint model status=%d body=%s", response.Code, response.Body.String())
		}
	}
	assertHTTPStatusAndEnvelope(t, mux, http.MethodPut, "/api/twins/DEVICE/"+catalogDeviceA+"/model", []byte(`{"modelId":"building","version":"1.0.0"}`), tenantA, http.StatusBadRequest)
	assertHTTPStatusAndEnvelope(t, mux, http.MethodPut, "/api/twins/DEVICE/"+catalogDeviceA+"/model", []byte(`{"modelId":"energy_meter","version":"2.0.0","extra":true}`), tenantA, http.StatusBadRequest)
	assertHTTPStatusAndEnvelope(t, mux, http.MethodPut, "/api/twins/DEVICE/"+catalogDeviceA+"/model", []byte(`{"modelId":"energy_meter","version":"2.0.0"}`), tenantB, http.StatusForbidden)
	assertHTTPStatusAndEnvelope(t, mux, http.MethodPut, "/api/twins/DEVICE/99999999-9999-9999-9999-999999999999/model", []byte(`{"modelId":"energy_meter","version":"2.0.0"}`), tenantA, http.StatusNotFound)

	repoint := doCatalogRequest(t, mux, http.MethodPut, "/api/twins/DEVICE/"+catalogDeviceA+"/model", []byte(`{"modelId":"energy_meter","version":"2.0.0"}`), sysAdmin)
	if repoint.Code != http.StatusOK {
		t.Fatalf("SYS_ADMIN repoint status=%d body=%s", repoint.Code, repoint.Body.String())
	}
	repointAgain := doCatalogRequest(t, mux, http.MethodPut, "/api/twins/DEVICE/"+catalogDeviceA+"/model", []byte(`{"modelId":"energy_meter","version":"2.0.0"}`), sysAdmin)
	if repointAgain.Code != http.StatusOK || repointAgain.Body.String() != repoint.Body.String() {
		t.Fatalf("idempotent repoint first=%s second=%s", repoint.Body.String(), repointAgain.Body.String())
	}

	dbpkg.SetPoolForTest(t, nil)
	assertHTTPStatusAndEnvelope(t, mux, http.MethodGet, "/api/twin-models", nil, tenantA, http.StatusInternalServerError)
}

func catalogTestMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/twin-models", HandleCollection)
	mux.HandleFunc("/api/twin-models/{modelId}/{version}", HandleVersion)
	mux.HandleFunc("/api/twins/{entityType}/{entityId}/model", HandleRepoint)
	return mux
}

func catalogJWT(t *testing.T, authority, tenantID string) string {
	t.Helper()
	token, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID: "99999999-9999-9999-9999-999999999999", Email: authority + "@test.local",
		Authority: authority, TenantID: tenantID, Enabled: true,
	}, "catalog-test")
	if err != nil {
		t.Fatalf("generate JWT: %v", err)
	}
	return token
}

func doCatalogRequest(t *testing.T, handler http.Handler, method, path string, body []byte, token string) *httptest.ResponseRecorder {
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

func assertHTTPStatusAndEnvelope(t *testing.T, handler http.Handler, method, path string, body []byte, token string, status int) {
	t.Helper()
	response := doCatalogRequest(t, handler, method, path, body, token)
	if response.Code != status {
		t.Fatalf("%s %s status=%d body=%s, want %d", method, path, response.Code, response.Body.String(), status)
	}
	var envelope struct {
		Status    int    `json:"status"`
		Message   string `json:"message"`
		ErrorCode int    `json:"errorCode"`
		Timestamp int64  `json:"timestamp"`
	}
	decodeCatalogResponse(t, response, &envelope)
	if envelope.Status != status || envelope.Message == "" || envelope.ErrorCode == 0 || envelope.Timestamp == 0 {
		t.Fatalf("invalid error envelope for %s %s: %+v body=%s", method, path, envelope, response.Body.String())
	}
}

func decodeCatalogResponse(t *testing.T, response *httptest.ResponseRecorder, target interface{}) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response status=%d body=%s: %v", response.Code, response.Body.String(), err)
	}
}
