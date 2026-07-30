package telemetry

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	_ "github.com/lib/pq"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
)

// These tests close the non-DEVICE cross-tenant read gap (adversarial review
// ISSUE #2): ASSET / API_USAGE_STATE / ENTITY_VIEW telemetry routes to
// ts_kv / ts_kv_latest (keyed by entity_id, no tenant column). Without an
// ownership check a TENANT_ADMIN of tenant A could read tenant B's entity
// telemetry by UUID. The fix verifies the entity belongs to the caller's
// tenant before any read; SYS_ADMIN may cross tenants; empty tenant → 401.

const (
	assetB = "22222222-2222-2222-2222-222222222222"
	usageB = "33333333-3333-3333-3333-333333333333"
	// sysTenant is ThingsBoard's system-tenant sentinel: a real SYS_ADMIN token
	// carries this (non-empty) tenantId, so the deny-empty-tenant 401 gate never
	// fires for it, while its SYS_ADMIN scope still bypasses the ownership check.
	sysTenant = "13814000-1dd2-11b2-8080-808080808080"
)

func newEntityTenantTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	pool, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := pool.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	// Default read backend (postgres ts_kv) — the DEFAULT path this gap lives on.
	t.Setenv("USAGE_READ_BACKEND", "postgres")

	stmts := []string{
		`DROP TABLE IF EXISTS asset CASCADE`,
		`DROP TABLE IF EXISTS api_usage_state CASCADE`,
		`DROP TABLE IF EXISTS ts_kv CASCADE`,
		`DROP TABLE IF EXISTS ts_kv_latest CASCADE`,
		`DROP TABLE IF EXISTS key_dictionary CASCADE`,
		`CREATE TABLE asset (id uuid PRIMARY KEY, tenant_id uuid, name text)`,
		`CREATE TABLE api_usage_state (id uuid PRIMARY KEY, tenant_id uuid, entity_id uuid)`,
		`CREATE TABLE key_dictionary (key_id serial PRIMARY KEY, key text UNIQUE)`,
		`CREATE TABLE ts_kv (entity_id uuid, key int, ts bigint,
			bool_v boolean, str_v text, long_v bigint, dbl_v double precision, json_v text)`,
		`CREATE TABLE ts_kv_latest (entity_id uuid, key int, ts bigint,
			bool_v boolean, str_v text, long_v bigint, dbl_v double precision, json_v text)`,
	}
	for _, s := range stmts {
		if _, err := pool.Exec(s); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
	// Entities owned by tenant B only.
	if _, err := pool.Exec(`INSERT INTO asset (id, tenant_id, name) VALUES ($1,$2,'assetB')`, assetB, tenantB); err != nil {
		t.Fatalf("seed asset: %v", err)
	}
	if _, err := pool.Exec(`INSERT INTO api_usage_state (id, tenant_id, entity_id) VALUES ($1,$2,$1)`, usageB, tenantB); err != nil {
		t.Fatalf("seed usage: %v", err)
	}
	// One ts_kv point under each entity so an owner/SYS_ADMIN read is non-empty.
	dbpkg.SetPoolForTest(t, pool)
	keyID := dbpkg.GetOrInsertKeyID("power")
	if keyID <= 0 {
		t.Fatalf("key_dictionary seed failed")
	}
	ts := time.Now().UnixMilli()
	for _, ent := range []string{assetB, usageB} {
		if _, err := pool.Exec(`INSERT INTO ts_kv (entity_id, key, ts, dbl_v) VALUES ($1,$2,$3,42.0)`, ent, keyID, ts); err != nil {
			t.Fatalf("seed ts_kv: %v", err)
		}
	}

	t.Cleanup(func() {
		dbpkg.SetPoolForTest(t, nil)
		pool.Close()
	})
	return pool
}

func entityJWT(t *testing.T, tenantID, authority string) string {
	t.Helper()
	authpkg.InitConfig()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    "00000000-0000-0000-0000-00000000000a",
		Email:     "u@x.org",
		Authority: authority,
		TenantID:  tenantID,
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	return tok
}

// TestHandleTelemetryValues_NonDeviceTenantEnforcement — an ASSET of tenant B
// is NOT readable by tenant A (403), IS readable by its owner (200+data) and by
// SYS_ADMIN (200+data); a missing token is 401.
func TestHandleTelemetryValues_NonDeviceTenantEnforcement(t *testing.T) {
	newEntityTenantTestDB(t)
	start := time.Now().Add(-time.Hour).UnixMilli()
	end := time.Now().Add(time.Hour).UnixMilli()
	path := "/api/plugins/telemetry/ASSET/" + assetB + "/values/timeseries"

	call := func(tenantID, authority string, withToken bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", path+"?key=power"+
			"&startTs="+strconv.FormatInt(start, 10)+"&endTs="+strconv.FormatInt(end, 10), nil)
		if withToken {
			req.Header.Set("X-Authorization", "Bearer "+entityJWT(t, tenantID, authority))
		}
		w := httptest.NewRecorder()
		HandleTelemetryValues(w, req)
		return w
	}

	// Foreign tenant A: hard 403, no read.
	if w := call(tenantA, "TENANT_ADMIN", true); w.Code != http.StatusForbidden {
		t.Fatalf("cross-tenant status = %d, want 403 (body=%s)", w.Code, w.Body.String())
	}

	// Owner tenant B: 200 with the point.
	w := call(tenantB, "TENANT_ADMIN", true)
	if w.Code != http.StatusOK {
		t.Fatalf("owner status = %d, body=%s", w.Code, w.Body.String())
	}
	var owned map[string][]map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &owned)
	if len(owned["power"]) != 1 {
		t.Fatalf("owner got %d points, want 1 (body=%s)", len(owned["power"]), w.Body.String())
	}

	// SYS_ADMIN (system tenant): may cross tenants — 200 with the point.
	w = call(sysTenant, "SYS_ADMIN", true)
	if w.Code != http.StatusOK {
		t.Fatalf("sysadmin status = %d, body=%s", w.Code, w.Body.String())
	}
	var sysRead map[string][]map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &sysRead)
	if len(sysRead["power"]) != 1 {
		t.Fatalf("sysadmin got %d points, want 1 (body=%s)", len(sysRead["power"]), w.Body.String())
	}

	// No token: 401 before any read.
	if w := call("", "", false); w.Code != http.StatusUnauthorized {
		t.Fatalf("no-token status = %d, want 401", w.Code)
	}
}

// TestHandleTelemetryKeys_NonDeviceTenantEnforcement — the keys endpoint applies
// the same ownership gate: tenant A is denied, the owner is allowed.
func TestHandleTelemetryKeys_NonDeviceTenantEnforcement(t *testing.T) {
	newEntityTenantTestDB(t)
	path := "/api/plugins/telemetry/ASSET/" + assetB + "/keys/timeseries"

	call := func(tenantID, authority string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("X-Authorization", "Bearer "+entityJWT(t, tenantID, authority))
		w := httptest.NewRecorder()
		HandleTelemetryKeys(w, req)
		return w
	}

	if w := call(tenantA, "TENANT_ADMIN"); w.Code != http.StatusForbidden {
		t.Fatalf("cross-tenant keys status = %d, want 403 (body=%s)", w.Code, w.Body.String())
	}
	if w := call(tenantB, "TENANT_ADMIN"); w.Code != http.StatusOK {
		t.Fatalf("owner keys status = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}
}

// TestHandleTelemetryValues_ApiUsageStateTenantEnforcement — API_USAGE_STATE
// resolves its tenant from the api_usage_state row; tenant A cannot read
// tenant B's usage counters.
func TestHandleTelemetryValues_ApiUsageStateTenantEnforcement(t *testing.T) {
	newEntityTenantTestDB(t)
	path := "/api/plugins/telemetry/API_USAGE_STATE/" + usageB + "/values/timeseries"

	call := func(tenantID string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", path+"?key=power", nil)
		req.Header.Set("X-Authorization", "Bearer "+entityJWT(t, tenantID, "TENANT_ADMIN"))
		w := httptest.NewRecorder()
		HandleTelemetryValues(w, req)
		return w
	}

	if w := call(tenantA); w.Code != http.StatusForbidden {
		t.Fatalf("cross-tenant usage status = %d, want 403 (body=%s)", w.Code, w.Body.String())
	}
	if w := call(tenantB); w.Code != http.StatusOK {
		t.Fatalf("owner usage status = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}
}
