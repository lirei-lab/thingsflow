package twin

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"flow-core/internal/topology"
)

// setupExpandTestDB builds the schema + seed data for the expand tests: the
// depth-2 chain assetA Contains deviceA Contains deviceC (all tenant A) with
// twin_registry pins. The legacy relation table must exist because the
// ExpandWithCTE edge-source union references it, even though no legacy edge is
// seeded here.
func setupExpandTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)

	if _, err := db.Exec(`DROP TABLE IF EXISTS relation CASCADE`); err != nil {
		t.Fatalf("drop relation: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE relation (
		from_id uuid NOT NULL, from_type text NOT NULL,
		to_id uuid NOT NULL, to_type text NOT NULL,
		relation_type_group text, relation_type text,
		additional_info text, version bigint default 0,
		PRIMARY KEY (from_id, from_type, relation_type_group, relation_type, to_id, to_type))`); err != nil {
		t.Fatalf("create relation: %v", err)
	}

	now := time.Now().UnixMilli()
	if _, err := db.Exec(`INSERT INTO device (id, created_time, tenant_id, name, type, label, additional_info)
		VALUES ($1, $2, $3, 'Meter C', 'meter', '', '{}')`, testDeviceC, now, testTenantA); err != nil {
		t.Fatalf("seed device C: %v", err)
	}
	for _, ent := range []struct{ tenant, etype, id string }{
		{testTenantA, "ASSET", testAssetA},
		{testTenantA, "DEVICE", testDeviceA},
		{testTenantA, "DEVICE", testDeviceC},
	} {
		if err := SyncRegistryRow(context.Background(), db, ent.tenant, ent.etype, ent.id); err != nil {
			t.Fatalf("sync registry %s/%s: %v", ent.etype, ent.id, err)
		}
	}
	// Chain: assetA Contains deviceA (seeded by setupTwinTables), then
	// deviceA Contains deviceC.
	if _, err := db.Exec(`INSERT INTO topology_edge
		(tenant_id, from_id, from_type, to_id, to_type, relation_type, relation_type_group,
		 direction, metadata, created_time, updated_time)
		VALUES ($1, $2, 'DEVICE', $3, 'DEVICE', 'Contains', 'COMMON', 'DIRECTED', '{}', $4, $4)`,
		testTenantA, testDeviceA, testDeviceC, now); err != nil {
		t.Fatalf("seed deviceA->deviceC edge: %v", err)
	}
	return db
}

func doExpandRequest(t *testing.T, entityType, entityID, query, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/twins/"+entityType+"/"+entityID+query, nil)
	req.Header.Set("X-Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	GetByEntity(w, req, entityType, entityID)
	return w
}

func TestParseExpandSpec(t *testing.T) {
	cases := []struct {
		raw     string
		want    int
		wantErr bool
	}{
		{"relations(1)", 1, false},
		{"relations(10)", 10, false},
		{"relations(0)", 0, true},
		{"relations(11)", 0, true},
		{"relations(abc)", 0, true},
		{"relations", 0, true},
		{"relations()", 0, true},
		{"foo", 0, true},
		{"", 0, true},
	}
	for _, tc := range cases {
		got, err := parseExpandSpec(tc.raw)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("parseExpandSpec(%q): expected error, got depth=%d", tc.raw, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseExpandSpec(%q): %v", tc.raw, err)
		}
		if got != tc.want {
			t.Fatalf("parseExpandSpec(%q)=%d want %d", tc.raw, got, tc.want)
		}
	}
}

func TestExpandErrorStatusMapsBudgetTo422(t *testing.T) {
	if status := expandErrorStatus(topology.ErrTraversalBudget); status != http.StatusUnprocessableEntity {
		t.Fatalf("budget status=%d want 422", status)
	}
	if status := expandErrorStatus(topology.ErrUnknownEntity); status != http.StatusInternalServerError {
		t.Fatalf("other error status=%d want 500", status)
	}
}

func TestExpandRelationsMalformed400(t *testing.T) {
	setupExpandTestDB(t)
	for _, q := range []string{"?expand=relations(abc)", "?expand=relations(0)", "?expand=relations", "?expand=foo"} {
		w := doExpandRequest(t, "ASSET", testAssetA, q, twinJWT(t, testTenantA))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("query %s status=%d body=%s want 400", q, w.Code, w.Body.String())
		}
	}
}

func TestExpandRelationsDepthAboveMax400(t *testing.T) {
	setupExpandTestDB(t)
	w := doExpandRequest(t, "ASSET", testAssetA, "?expand=relations(11)", twinJWT(t, testTenantA))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s want 400", w.Code, w.Body.String())
	}
}

func TestExpandRelationsReturnsDepthAnnotatedRelationsWithState(t *testing.T) {
	setupExpandTestDB(t)
	w := doExpandRequest(t, "ASSET", testAssetA, "?expand=relations(2)", twinJWT(t, testTenantA))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var got map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("json: %v", err)
	}
	relations := got["relations"].([]interface{})
	if len(relations) != 2 {
		t.Fatalf("relations=%v want 2 (immediate + depth-2 node)", relations)
	}
	first := relations[0].(map[string]interface{})
	if first["type"] != "Contains" {
		t.Fatalf("first relation type=%v", first["type"])
	}
	if first["depth"].(float64) != 1 {
		t.Fatalf("first depth=%v want 1", first["depth"])
	}
	if state, ok := first["state"].(map[string]interface{}); !ok || state["name"] != "Meter A" {
		t.Fatalf("first state=%v want Meter A", first["state"])
	}
	second := relations[1].(map[string]interface{})
	if second["depth"].(float64) != 2 {
		t.Fatalf("second depth=%v want 2", second["depth"])
	}
	if second["target"] != testTenantA+":device:"+testDeviceC {
		t.Fatalf("second target=%v", second["target"])
	}
	if state, ok := second["state"].(map[string]interface{}); !ok || state["name"] != "Meter C" {
		t.Fatalf("second state=%v want Meter C", second["state"])
	}
}

func TestExpandOnDeviceKeepsIncomingEdgesImmediate(t *testing.T) {
	setupExpandTestDB(t)
	// deviceA as root: it has an incoming edge (assetA->deviceA, IN) and an
	// outgoing edge (deviceA->deviceC, OUT). A depth-1 FROM expansion must
	// keep both as immediate depth-1 relations and must not re-emit deviceC
	// (already immediate) as a transitive node.
	w := doExpandRequest(t, "DEVICE", testDeviceA, "?expand=relations(1)", twinJWT(t, testTenantA))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var got map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("json: %v", err)
	}
	relations := got["relations"].([]interface{})
	if len(relations) != 2 {
		t.Fatalf("relations=%v want 2 immediate edges", relations)
	}
	for _, r := range relations {
		rel := r.(map[string]interface{})
		if rel["depth"].(float64) != 1 {
			t.Fatalf("relation depth=%v want 1: %v", rel["depth"], rel)
		}
		if _, ok := rel["state"].(map[string]interface{}); !ok {
			t.Fatalf("relation missing state: %v", rel)
		}
	}
}

func TestExpandForeignTenantNeighborStateNotLeaked(t *testing.T) {
	db := setupExpandTestDB(t)
	// Defense-in-depth: plant a cross-tenant edge directly (SaveEdge would
	// refuse it — ErrCrossTenant — but the expand hydration must not trust
	// that enforcement). deviceA (tenant A) -> deviceB (tenant B). The
	// traversal is scoped to tenant A, so deviceB's governed state must NOT
	// leak: its embedded state must degrade to a bare ref (entityType + id
	// only, no name/type/label/attributes).
	if _, err := db.Exec(`INSERT INTO topology_edge
		(tenant_id, from_id, from_type, to_id, to_type, relation_type, relation_type_group,
		 direction, metadata, created_time, updated_time)
		VALUES ($1, $2, 'DEVICE', $3, 'DEVICE', 'Contains', 'COMMON', 'DIRECTED', '{}', $4, $4)`,
		testTenantA, testDeviceA, testDeviceB, time.Now().UnixMilli()); err != nil {
		t.Fatalf("seed cross-tenant edge: %v", err)
	}

	w := doExpandRequest(t, "DEVICE", testDeviceA, "?expand=relations(1)", twinJWT(t, testTenantA))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var got map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("json: %v", err)
	}
	relations := got["relations"].([]interface{})
	for _, r := range relations {
		rel := r.(map[string]interface{})
		state, _ := rel["state"].(map[string]interface{})
		if state == nil {
			continue
		}
		// The only foreign neighbor reachable is deviceB (tenant B). Its
		// embedded state must be a bare ref — entityType + id only.
		if state["id"] == testDeviceB {
			if _, hasName := state["name"]; hasName {
				t.Fatalf("foreign tenant state leaked name=%v: %v", state["name"], state)
			}
			if _, hasType := state["type"]; hasType {
				t.Fatalf("foreign tenant state leaked type=%v: %v", state["type"], state)
			}
			if _, hasLabel := state["label"]; hasLabel {
				t.Fatalf("foreign tenant state leaked label=%v: %v", state["label"], state)
			}
			if state["entityType"] != "DEVICE" {
				t.Fatalf("bare ref entityType=%v", state["entityType"])
			}
		}
	}
}

func TestExpandCrossTenantRejected(t *testing.T) {
	setupExpandTestDB(t)
	// Tenant A reading tenant B's device with expand — GetByEntity's tenant	// check fires before any traversal runs.
	w := doExpandRequest(t, "DEVICE", testDeviceB, "?expand=relations(2)", twinJWT(t, testTenantA))
	if w.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s want 403", w.Code, w.Body.String())
	}
}
