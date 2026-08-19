package system

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	_ "github.com/lib/pq"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/quotas"
)

// Regression coverage for the ui-contract data-fidelity audit
// (docs/adr/0002, docs/UI_CONTRACT_DATA_FIDELITY.md P2): GET /api/usage
// hardcoded every max* field to 0 regardless of the tenant's real configured
// quota. quotas.LimitsFor already computed the real numbers elsewhere in the
// codebase; HandleUsage now calls it.
//
// transportMessages is not exercised here: its real source
// (telemetry.DeviceKVLatest, reading the ts_kv snapshot internal/usage
// writes once a minute) needs a live GreptimeDB connection
// (internal/telemetry.PG), which this unit test doesn't stand up. With
// telemetry.PG nil, DeviceKVLatest returns ok=false and the handler
// correctly falls back to 0 — this test only proves that fallback doesn't
// panic, not that the live counter round-trips. See
// docs/UI_CONTRACT_DATA_FIDELITY.md for how to verify that end to end on a
// live cluster (the same pattern used for the twin-state Phase 3 work).
func TestHandleUsage_ReadsRealQuotaLimits(t *testing.T) {
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

	const tenantID = "22222222-2222-2222-2222-222222222222"
	const stateID = "33333333-3333-3333-3333-333333333333"

	stmts := []string{
		`DROP TABLE IF EXISTS device, asset, customer, dashboard, tb_user, alarm, api_usage_state CASCADE`,
		`CREATE TABLE device (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE asset (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE customer (id uuid PRIMARY KEY, tenant_id uuid, title text)`,
		`CREATE TABLE dashboard (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE tb_user (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE alarm (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE api_usage_state (id uuid PRIMARY KEY, tenant_id uuid)`,
		`INSERT INTO device (id, tenant_id) VALUES (gen_random_uuid(), $1), (gen_random_uuid(), $1)`,
		`INSERT INTO api_usage_state (id, tenant_id) VALUES ($2, $1)`,
	}
	for i, s := range stmts {
		var execErr error
		if i == len(stmts)-2 {
			_, execErr = db.Exec(s, tenantID)
		} else if i == len(stmts)-1 {
			_, execErr = db.Exec(s, tenantID, stateID)
		} else {
			_, execErr = db.Exec(s)
		}
		if execErr != nil {
			t.Fatalf("schema/seed %d: %v", i, execErr)
		}
	}
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() {
		dbpkg.SetPoolForTest(t, nil)
		db.Close()
	})

	quotas.SeedForTest(tenantID, quotas.Limits{
		MaxDevices:           50,
		MaxAssets:            20,
		MaxUsers:             10,
		MaxCustomers:         5,
		MaxDashboards:        15,
		MaxTransportMessages: 100000,
	})
	t.Cleanup(func() { quotas.InvalidateCache(tenantID) })

	req := httptest.NewRequest(http.MethodGet, "/api/usage", nil)
	req.Header.Set("X-Authorization", "Bearer "+fakeSystemJWT(t, tenantID))
	rec := httptest.NewRecorder()

	HandleUsage(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	want := map[string]float64{
		"maxDevices":           50,
		"maxAssets":            20,
		"maxUsers":             10,
		"maxCustomers":         5,
		"maxDashboards":        15,
		"maxTransportMessages": 100000,
	}
	for key, wantVal := range want {
		got, ok := body[key].(float64)
		if !ok || got != wantVal {
			t.Errorf("%s: got %v, want %v (a prior version hardcoded every max* field to 0)", key, body[key], wantVal)
		}
	}
	if got, _ := body["devices"].(float64); got != 2 {
		t.Errorf("devices: got %v, want 2 (real count query, unrelated to this fix, should still work)", body["devices"])
	}
}
