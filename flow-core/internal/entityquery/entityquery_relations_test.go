package entityquery

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
)

const (
	rqTenantA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	rqTenantB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	rqAssetA  = "11111111-1111-1111-1111-111111111111" // tenant A root
	rqDeviceA = "33333333-3333-3333-3333-333333333333" // tenant A child
	rqDeviceB = "44444444-4444-4444-4444-444444444444" // tenant B (foreign)
)

func newRelationsQueryDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}
	schema := fmt.Sprintf("relations_query_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec(`SET search_path TO ` + schema); err != nil {
		t.Fatalf("set search path: %v", err)
	}
	for _, statement := range []string{
		`CREATE TABLE asset (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, name text, type text, label text)`,
		`CREATE TABLE device (id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, name text, type text, label text)`,
		`CREATE TABLE customer (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE entity_view (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE dashboard (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE device_profile (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE asset_profile (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE relation (
			from_id uuid, from_type text, to_id uuid, to_type text,
			relation_type_group text, relation_type text,
			additional_info text, version bigint default 0,
			PRIMARY KEY (from_id, from_type, relation_type_group, relation_type, to_id, to_type))`,
		`CREATE TABLE topology_edge (
			tenant_id uuid not null, from_id uuid not null, from_type text not null,
			to_id uuid not null, to_type text not null, relation_type text not null,
			relation_type_group text not null default 'COMMON', direction text not null default 'DIRECTED',
			metadata jsonb not null default '{}'::jsonb, created_time bigint not null,
			updated_time bigint not null, version bigint not null default 1,
			PRIMARY KEY (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type, direction),
			CHECK (direction IN ('DIRECTED', 'BIDIRECTIONAL')))`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("schema statement: %v\n%s", err, statement)
		}
	}
	if _, err := db.Exec(`INSERT INTO asset (id,created_time,tenant_id,name,type,label) VALUES
		($1,1000,$2,'Building A','building','HQ')`, rqAssetA, rqTenantA); err != nil {
		t.Fatalf("seed asset: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO device (id,created_time,tenant_id,name,type,label) VALUES
		($1,2000,$3,'Meter A','meter',''),($2,3000,$4,'Meter B','meter','')`,
		rqDeviceA, rqDeviceB, rqTenantA, rqTenantB); err != nil {
		t.Fatalf("seed devices: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO topology_edge
		(tenant_id,from_id,from_type,to_id,to_type,relation_type,relation_type_group,
		 direction,metadata,created_time,updated_time)
		VALUES ($1,$2,'ASSET',$3,'DEVICE','Contains','COMMON','DIRECTED','{}',1,1)`,
		rqTenantA, rqAssetA, rqDeviceA); err != nil {
		t.Fatalf("seed edge A: %v", err)
	}
	// A cross-tenant edge from asset A (tenant A) to device B (tenant B) — must
	// never surface for a tenant-A caller, and the foreign child must not be
	// traversed through.
	if _, err := db.Exec(`INSERT INTO topology_edge
		(tenant_id,from_id,from_type,to_id,to_type,relation_type,relation_type_group,
		 direction,metadata,created_time,updated_time)
		VALUES ($1,$2,'ASSET',$3,'DEVICE','ConnectedTo','COMMON','DIRECTED','{}',1,1)`,
		rqTenantA, rqAssetA, rqDeviceB); err != nil {
		t.Fatalf("seed cross-tenant edge: %v", err)
	}

	dbpkg.SetPoolForTest(t, db)
	t.Setenv("JWT_TOKEN_SIGNING_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	authpkg.InitConfig()
	t.Cleanup(func() {
		dbpkg.SetPoolForTest(t, nil)
		_, _ = db.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`)
		db.Close()
	})
	return db
}

func rqJWT(t *testing.T, tenantID string, sysAdmin bool) string {
	t.Helper()
	authpkg.InitConfig()
	authority := "TENANT_ADMIN"
	if sysAdmin {
		authority = "SYS_ADMIN"
	}
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    "00000000-0000-0000-0000-000000000001",
		Email:     "rq@test.org",
		Authority: authority,
		TenantID:  tenantID,
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	return tok
}

func doRelationsQuery(t *testing.T, tenantID string, sysAdmin bool, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/api/entitiesQuery/find", strings.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+rqJWT(t, tenantID, sysAdmin))
	w := httptest.NewRecorder()
	Find(w, req)
	return w
}

func decodeRelationsData(t *testing.T, w *httptest.ResponseRecorder) []map[string]interface{} {
	t.Helper()
	var resp struct {
		Data []map[string]interface{} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v\nbody=%s", err, w.Body.String())
	}
	return resp.Data
}

func TestRelationsQueryHappyPathLevelsAndEntityFields(t *testing.T) {
	_ = newRelationsQueryDB(t)
	body := `{"entityFilter":{"type":"relationsQuery",
		"rootEntity":{"entityType":"ASSET","id":"` + rqAssetA + `"},
		"direction":"FROM","maxLevel":1,"relationTypes":["Contains"]},
		"entityFields":[{"type":"ENTITY_FIELD","key":"name"},{"type":"ENTITY_FIELD","key":"type"}]}`
	w := doRelationsQuery(t, rqTenantA, false, body)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	data := decodeRelationsData(t, w)
	if len(data) != 1 {
		t.Fatalf("data len=%d want 1 (%s)", len(data), w.Body.String())
	}
	item := data[0]
	eid := item["entityId"].(map[string]interface{})
	if eid["entityType"] != "DEVICE" || eid["id"] != rqDeviceA {
		t.Fatalf("entityId=%v", eid)
	}
	if item["level"] != float64(1) {
		t.Fatalf("level=%v want 1", item["level"])
	}
	latest := item["latest"].(map[string]interface{})
	ef := latest["ENTITY_FIELD"].(map[string]interface{})
	if ef["name"].(map[string]interface{})["value"] != "Meter A" {
		t.Fatalf("ENTITY_FIELD name=%v", ef["name"])
	}
}

func TestRelationsQueryEntityTypesFilter(t *testing.T) {
	_ = newRelationsQueryDB(t)
	body := `{"entityFilter":{"type":"relationsQuery",
		"rootEntity":{"entityType":"ASSET","id":"` + rqAssetA + `"},
		"maxLevel":1,"relationTypes":["Contains"],
		"entityTypes":["ASSET"]}}`
	w := doRelationsQuery(t, rqTenantA, false, body)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	data := decodeRelationsData(t, w)
	if len(data) != 0 {
		t.Fatalf("data len=%d want 0 (only DEVICE children exist)", len(data))
	}
}

func TestRelationsQueryForeignTenantRootDenied(t *testing.T) {
	_ = newRelationsQueryDB(t)
	// Root is device B (tenant B); caller is tenant A → 403.
	body := `{"entityFilter":{"type":"relationsQuery",
		"rootEntity":{"entityType":"DEVICE","id":"` + rqDeviceB + `"},
		"maxLevel":1}}`
	w := doRelationsQuery(t, rqTenantA, false, body)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s want 403", w.Code, w.Body.String())
	}
}

func TestRelationsQuerySysAdminCrossesTenants(t *testing.T) {
	_ = newRelationsQueryDB(t)
	body := `{"entityFilter":{"type":"relationsQuery",
		"rootEntity":{"entityType":"DEVICE","id":"` + rqDeviceB + `"},
		"maxLevel":1}}`
	w := doRelationsQuery(t, "", true, body)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s want 200", w.Code, w.Body.String())
	}
}

func TestRelationsQueryMissingRoot400(t *testing.T) {
	_ = newRelationsQueryDB(t)
	body := `{"entityFilter":{"type":"relationsQuery","maxLevel":1}}`
	w := doRelationsQuery(t, rqTenantA, false, body)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s want 400", w.Code, w.Body.String())
	}
}

func TestRelationsQueryDepthAboveCeiling400(t *testing.T) {
	_ = newRelationsQueryDB(t)
	body := `{"entityFilter":{"type":"relationsQuery",
		"rootEntity":{"entityType":"ASSET","id":"` + rqAssetA + `"},
		"maxLevel":99999}}`
	w := doRelationsQuery(t, rqTenantA, false, body)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s want 400", w.Code, w.Body.String())
	}
}

func TestRelationsQueryInvalidDirection400(t *testing.T) {
	_ = newRelationsQueryDB(t)
	body := `{"entityFilter":{"type":"relationsQuery",
		"rootEntity":{"entityType":"ASSET","id":"` + rqAssetA + `"},
		"direction":"SIDEWAYS","maxLevel":1}}`
	w := doRelationsQuery(t, rqTenantA, false, body)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s want 400", w.Code, w.Body.String())
	}
}

func TestRelationsQueryCrossTenantEdgeExcludedFromTenantAWalk(t *testing.T) {
	_ = newRelationsQueryDB(t)
	// Asset A (tenant A) has a Contains edge to device A (tenant A) AND a
	// ConnectedTo edge to device B (tenant B). A tenant-A relationsQuery with
	// both relation types must return only device A; device B must never leak.
	body := `{"entityFilter":{"type":"relationsQuery",
		"rootEntity":{"entityType":"ASSET","id":"` + rqAssetA + `"},
		"direction":"FROM","maxLevel":1,"relationTypes":["Contains","ConnectedTo"]}}`
	w := doRelationsQuery(t, rqTenantA, false, body)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	data := decodeRelationsData(t, w)
	if len(data) != 1 {
		t.Fatalf("data len=%d want 1 (foreign child must be excluded)", len(data))
	}
	eid := data[0]["entityId"].(map[string]interface{})
	if eid["id"] != rqDeviceA {
		t.Fatalf("returned foreign/other id=%v", eid["id"])
	}
}

func TestParseEntityFieldSpecsAndAttrKeys(t *testing.T) {
	specs := parseEntityFieldSpecs([]interface{}{
		map[string]interface{}{"type": "ENTITY_FIELD", "key": "name"},
		map[string]interface{}{"type": "TIME_SERIES", "key": "x"},
		map[string]interface{}{"type": "ENTITY_FIELD", "key": "type"},
	})
	if len(specs) != 2 || specs[0].Key != "name" || specs[1].Key != "type" {
		t.Fatalf("specs=%+v", specs)
	}
	keys := parseAttrKeys([]interface{}{
		map[string]interface{}{"type": "ATTRIBUTE", "key": "location"},
		map[string]interface{}{"type": "TIME_SERIES", "key": "temperature"},
		map[string]interface{}{"type": "ATTRIBUTE", "key": "status"},
	})
	if len(keys) != 2 || keys[0] != "location" || keys[1] != "status" {
		t.Fatalf("keys=%v", keys)
	}
}
