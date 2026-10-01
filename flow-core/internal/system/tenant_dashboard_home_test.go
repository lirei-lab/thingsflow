package system

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	_ "github.com/lib/pq"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/testdb"
)

// Regression coverage for the ui-contract data-fidelity audit (docs/adr/0002,
// docs/UI_CONTRACT_DATA_FIDELITY.md P2): GET/POST /api/tenant/dashboard/home/info
// always answered {dashboardId: nil, hideDashboardToolbar: true} with no DB
// read at all, and POST had no method branch — it silently discarded the
// posted selection. Both now round-trip through tenant.additional_info, the
// same JSON column internal/tenant.HandleTenantSave already reads/writes,
// merging in rather than overwriting.
func TestTenantDashboardHomeInfo_RoundTrips(t *testing.T) {
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	db, err := sql.Open("postgres", testdb.Scoped(t, dsn))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}

	const tenantID = "44444444-4444-4444-4444-444444444444"
	stmts := []string{
		`DROP TABLE IF EXISTS tenant CASCADE`,
		`CREATE TABLE tenant (id uuid PRIMARY KEY, additional_info text)`,
		`INSERT INTO tenant (id, additional_info) VALUES ($1, '{"description":"keep me"}')`,
	}
	for i, s := range stmts {
		var execErr error
		if i == len(stmts)-1 {
			_, execErr = db.Exec(s, tenantID)
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
	tok := fakeSystemJWT(t, tenantID)

	// GET before any save: no home dashboard configured yet.
	getReq := httptest.NewRequest(http.MethodGet, "/api/tenant/dashboard/home/info", nil)
	getReq.Header.Set("X-Authorization", "Bearer "+tok)
	getRec := httptest.NewRecorder()
	HandleTenantDashboardHomeInfo(getRec, getReq)
	var before map[string]interface{}
	if err := json.Unmarshal(getRec.Body.Bytes(), &before); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if before["dashboardId"] != nil {
		t.Fatalf("before save: dashboardId = %v, want nil", before["dashboardId"])
	}

	// POST a selection.
	const dashboardID = "55555555-5555-5555-5555-555555555555"
	body, _ := json.Marshal(map[string]interface{}{"dashboardId": dashboardID, "hideDashboardToolbar": false})
	postReq := httptest.NewRequest(http.MethodPost, "/api/tenant/dashboard/home/info", bytes.NewReader(body))
	postReq.Header.Set("X-Authorization", "Bearer "+tok)
	postRec := httptest.NewRecorder()
	HandleTenantDashboardHomeInfo(postRec, postReq)
	if postRec.Code != http.StatusOK {
		t.Fatalf("POST status: got %d, body=%s", postRec.Code, postRec.Body.String())
	}

	// GET again: the selection must round-trip (a prior version would still
	// answer the same hardcoded {nil, true} here).
	getReq2 := httptest.NewRequest(http.MethodGet, "/api/tenant/dashboard/home/info", nil)
	getReq2.Header.Set("X-Authorization", "Bearer "+tok)
	getRec2 := httptest.NewRecorder()
	HandleTenantDashboardHomeInfo(getRec2, getReq2)
	var after map[string]interface{}
	if err := json.Unmarshal(getRec2.Body.Bytes(), &after); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if after["dashboardId"] != dashboardID {
		t.Errorf("dashboardId: got %v, want %q", after["dashboardId"], dashboardID)
	}
	if after["hideDashboardToolbar"] != false {
		t.Errorf("hideDashboardToolbar: got %v, want false", after["hideDashboardToolbar"])
	}

	// The unrelated additionalInfo key set before the POST must survive the merge.
	var raw string
	if err := db.QueryRow("SELECT additional_info FROM tenant WHERE id = $1", tenantID).Scan(&raw); err != nil {
		t.Fatalf("verify: %v", err)
	}
	var stored map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		t.Fatalf("verify decode: %v", err)
	}
	if stored["description"] != "keep me" {
		t.Errorf("POST clobbered an unrelated additionalInfo key: got %v, want %q", stored["description"], "keep me")
	}
}
