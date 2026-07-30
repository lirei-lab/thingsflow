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
)

// These tests verify the tenant predicate added to the device telemetry read
// path. device_telemetry_kv carries a tenant_id column; every device read must
// be scoped to the requesting session's tenant, or an authenticated tenant A
// can read tenant B's device telemetry by its UUID (IDOR).

const (
	tenantA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	tenantB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	deviceA = "11111111-1111-1111-1111-111111111111"
)

func newTelemetryTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	// Force the plain-timestamp column name so the reader's query runs against a
	// vanilla Postgres test table (not the greptime_timestamp default).
	t.Setenv("TELEMETRY_KV_TS_COLUMN", "timestamp")
	t.Setenv("TELEMETRY_HISTORY_STORE", "questdb")

	stmts := []string{
		`DROP TABLE IF EXISTS device_telemetry_kv`,
		`CREATE TABLE device_telemetry_kv (
			tenant_id text, device_id text, telemetry_key text,
			value_string text, value_kind text, "timestamp" timestamptz)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
	// Same device_id UUID is (re)used under tenant A only — a row must never
	// surface for tenant B even when it guesses deviceA.
	ts := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO device_telemetry_kv VALUES ($1,$2,'power',$3,'number',$4)`,
		tenantA, deviceA, "42.0", ts); err != nil {
		t.Fatalf("seed A: %v", err)
	}

	prev := PG
	PG = db
	t.Cleanup(func() {
		PG = prev
		db.Close()
	})
	return db
}

func telemetryJWT(t *testing.T, tenantID string) string {
	t.Helper()
	authpkg.InitConfig()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    "00000000-0000-0000-0000-000000000009",
		Email:     "u@x.org",
		Authority: "TENANT_ADMIN",
		TenantID:  tenantID,
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	return tok
}

// TestQueryQuestDBKVTimeseries_TenantScoped is the query-builder level check:
// the owning tenant sees the point, a different tenant sees nothing.
func TestQueryQuestDBKVTimeseries_TenantScoped(t *testing.T) {
	newTelemetryTestDB(t)
	start := time.Now().Add(-time.Hour).UnixMilli()
	end := time.Now().Add(time.Hour).UnixMilli()

	own, ok := QueryQuestDBKVTimeseries(tenantA, deviceA, []string{"power"}, start, end, 100, "ASC", "", "", false)
	if !ok {
		t.Fatalf("owning-tenant query returned ok=false")
	}
	if len(own["power"]) != 1 {
		t.Fatalf("owning tenant got %d points, want 1", len(own["power"]))
	}

	cross, ok := QueryQuestDBKVTimeseries(tenantB, deviceA, []string{"power"}, start, end, 100, "ASC", "", "", false)
	if !ok {
		t.Fatalf("cross-tenant query returned ok=false")
	}
	if len(cross["power"]) != 0 {
		t.Fatalf("cross-tenant leak: got %d points for tenant B, want 0", len(cross["power"]))
	}
}

func TestQueryQuestDBKVKeys_TenantScoped(t *testing.T) {
	newTelemetryTestDB(t)
	if keys, ok := QueryQuestDBKVKeys(tenantA, deviceA); !ok || len(keys) != 1 {
		t.Fatalf("owning tenant keys = %v ok=%v, want 1 key", keys, ok)
	}
	if keys, _ := QueryQuestDBKVKeys(tenantB, deviceA); len(keys) != 0 {
		t.Fatalf("cross-tenant keys leak: %v, want none", keys)
	}
}

// TestHandleTelemetryValues_TenantEnforcement drives the HTTP handler end to
// end: owner 200+data, cross-tenant 200+empty (no leak), no token 401.
func TestHandleTelemetryValues_TenantEnforcement(t *testing.T) {
	newTelemetryTestDB(t)
	start := time.Now().Add(-time.Hour).UnixMilli()
	end := time.Now().Add(time.Hour).UnixMilli()
	path := "/api/plugins/telemetry/DEVICE/" + deviceA + "/values/timeseries"

	call := func(tenantID string, withToken bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", path+"?key=power"+
			"&startTs="+strconv.FormatInt(start, 10)+"&endTs="+strconv.FormatInt(end, 10), nil)
		if withToken {
			req.Header.Set("X-Authorization", "Bearer "+telemetryJWT(t, tenantID))
		}
		w := httptest.NewRecorder()
		HandleTelemetryValues(w, req)
		return w
	}

	// Owner: 200 with the point.
	w := call(tenantA, true)
	if w.Code != http.StatusOK {
		t.Fatalf("owner status = %d, body=%s", w.Code, w.Body.String())
	}
	var owned map[string][]map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &owned)
	if len(owned["power"]) != 1 {
		t.Fatalf("owner got %d points, want 1 (body=%s)", len(owned["power"]), w.Body.String())
	}

	// Cross-tenant: must not leak tenant A's data.
	w = call(tenantB, true)
	if w.Code != http.StatusOK {
		t.Fatalf("cross-tenant status = %d", w.Code)
	}
	var crossed map[string][]map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &crossed)
	if len(crossed["power"]) != 0 {
		t.Fatalf("cross-tenant leak via handler: %v", crossed["power"])
	}

	// No token: hard 401, never an unscoped query.
	w = call("", false)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("no-token status = %d, want 401", w.Code)
	}
}
