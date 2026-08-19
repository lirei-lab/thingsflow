package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Regression coverage for the ui-contract data-fidelity audit (docs/adr/0002,
// docs/UI_CONTRACT_DATA_FIDELITY.md P2): GET /api/dashboard/home wrote an
// empty 200 unconditionally, never checking whether the caller actually has
// a home dashboard configured. TB-classic stores that selection in
// tb_user.additional_info.homeDashboardId — the same column internal/user.Save
// already persists — so a real selection existed but this endpoint could
// never see it. It now forwards to ByID when one is set, and still answers
// empty when none is (the deliberate "no home dashboard" shape, unchanged).
func TestHome_ForwardsToConfiguredDashboard(t *testing.T) {
	db := newTestDB(t)
	setupTables(t, db)
	if _, err := db.Exec(`DROP TABLE IF EXISTS tb_user CASCADE;
		CREATE TABLE tb_user (id uuid PRIMARY KEY, tenant_id uuid, additional_info text)`); err != nil {
		t.Fatalf("schema: %v", err)
	}

	const userID = "00000000-0000-0000-0000-000000000001" // matches fakeJWT's UserID
	const dashboardID = "66666666-6666-6666-6666-666666666666"

	if _, err := db.Exec(
		`INSERT INTO dashboard (id, created_time, tenant_id, title, version) VALUES ($1, 1000, $2, 'My Home', 1)`,
		dashboardID, tenantA,
	); err != nil {
		t.Fatalf("seed dashboard: %v", err)
	}

	tok := fakeJWT(t, tenantA)

	// No home dashboard configured yet: still the deliberate empty 200.
	reqBefore := httptest.NewRequest(http.MethodGet, "/api/dashboard/home", nil)
	reqBefore.Header.Set("X-Authorization", "Bearer "+tok)
	recBefore := httptest.NewRecorder()
	Home(recBefore, reqBefore)
	if recBefore.Code != http.StatusOK || recBefore.Body.Len() != 0 {
		t.Fatalf("before config: got status=%d body=%q, want 200 with an empty body", recBefore.Code, recBefore.Body.String())
	}

	if _, err := db.Exec(
		`INSERT INTO tb_user (id, tenant_id, additional_info) VALUES ($1, $2, $3)`,
		userID, tenantA, `{"homeDashboardId":"`+dashboardID+`"}`,
	); err != nil {
		t.Fatalf("seed tb_user: %v", err)
	}

	reqAfter := httptest.NewRequest(http.MethodGet, "/api/dashboard/home", nil)
	reqAfter.Header.Set("X-Authorization", "Bearer "+tok)
	recAfter := httptest.NewRecorder()
	Home(recAfter, reqAfter)
	if recAfter.Code != http.StatusOK {
		t.Fatalf("after config: status=%d body=%s", recAfter.Code, recAfter.Body.String())
	}
	var got map[string]interface{}
	if err := json.Unmarshal(recAfter.Body.Bytes(), &got); err != nil {
		t.Fatalf("after config: expected the real dashboard body, got empty/invalid JSON (%v) — a prior version always wrote nothing", err)
	}
	if got["title"] != "My Home" {
		t.Errorf("title: got %v, want %q", got["title"], "My Home")
	}
}
