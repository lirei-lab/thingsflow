package twin

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/twinstore"

	_ "github.com/lib/pq"
)

const (
	testTenantA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	testTenantB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	testAssetA  = "11111111-1111-1111-1111-111111111111"
	testDeviceA = "33333333-3333-3333-3333-333333333333"
	testDeviceB = "44444444-4444-4444-4444-444444444444"
)

func newTwinTestDB(t *testing.T) *sql.DB {
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
	dbpkg.SetPoolForTest(t, pool)
	t.Cleanup(func() {
		dbpkg.SetPoolForTest(t, nil)
		pool.Close()
	})
	return pool
}

func setupTwinTables(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`DROP TABLE IF EXISTS twin_registry CASCADE`,
		`DROP TABLE IF EXISTS twin_model CASCADE`,
		`DROP TABLE IF EXISTS topology_edge CASCADE`,
		`DROP TABLE IF EXISTS asset CASCADE`,
		`DROP TABLE IF EXISTS device CASCADE`,
		`DROP TABLE IF EXISTS ts_kv_latest CASCADE`,
		`DROP TABLE IF EXISTS key_dictionary CASCADE`,
		`CREATE TABLE asset (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			customer_id uuid, name text, type text, label text,
			additional_info text, version bigint default 1)`,
		`CREATE TABLE device (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			customer_id uuid, name text, type text, label text,
			additional_info text, version bigint default 1)`,
		`CREATE TABLE topology_edge (
			tenant_id uuid not null, from_id uuid not null, from_type text not null,
			to_id uuid not null, to_type text not null, relation_type text not null,
			relation_type_group text not null default 'COMMON',
			direction text not null default 'DIRECTED',
			metadata jsonb not null default '{}'::jsonb,
			created_time bigint not null, updated_time bigint not null,
			version bigint not null default 1,
			PRIMARY KEY (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type))`,
		`CREATE TABLE key_dictionary (
			key text PRIMARY KEY,
			key_id serial UNIQUE)`,
		`CREATE TABLE ts_kv_latest (
			entity_id uuid not null,
			key int not null,
			ts bigint not null,
			bool_v boolean,
			str_v text,
			long_v bigint,
			dbl_v double precision,
			json_v json,
			version bigint default 0,
			PRIMARY KEY (entity_id, key))`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
	now := time.Now().UnixMilli()
	if _, err := db.Exec(`INSERT INTO asset (id, created_time, tenant_id, name, type, label, additional_info)
		VALUES ($1, $2, $3, 'Building A', 'building', 'HQ', '{"floor":3}')`, testAssetA, now, testTenantA); err != nil {
		t.Fatalf("seed asset: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO device (id, created_time, tenant_id, name, type, label, additional_info)
		VALUES ($1, $2, $3, 'Meter A', 'meter', 'Main meter', '{"serial":"M-1"}'),
		       ($4, $2, $5, 'Meter B', 'meter', '', '{}')`,
		testDeviceA, now, testTenantA, testDeviceB, testTenantB); err != nil {
		t.Fatalf("seed device: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO topology_edge
		(tenant_id, from_id, from_type, to_id, to_type, relation_type, relation_type_group,
		 direction, metadata, created_time, updated_time)
		VALUES ($1, $2, 'ASSET', $3, 'DEVICE', 'Contains', 'COMMON', 'DIRECTED', '{}', $4, $4)`,
		testTenantA, testAssetA, testDeviceA, now); err != nil {
		t.Fatalf("seed edge: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO key_dictionary (key, key_id)
		VALUES ('temperature', 10), ('active', 11)`); err != nil {
		t.Fatalf("seed keys: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO ts_kv_latest (entity_id, key, ts, dbl_v)
		VALUES ($1, 10, $2, 22.5)`, testDeviceA, now); err != nil {
		t.Fatalf("seed latest temperature: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO ts_kv_latest (entity_id, key, ts, bool_v)
		VALUES ($1, 11, $2, true)`, testDeviceA, now); err != nil {
		t.Fatalf("seed latest active: %v", err)
	}
}

func twinJWT(t *testing.T, tenantID string) string {
	t.Helper()
	authpkg.InitConfig()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    "00000000-0000-0000-0000-000000000001",
		Email:     "twin@test.org",
		Authority: "TENANT_ADMIN",
		TenantID:  tenantID,
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	return tok
}

func TestGetDeviceTwinProjectsAttributesFeaturesAndRelations(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)

	req := httptest.NewRequest("GET", "/api/twins/DEVICE/"+testDeviceA, nil)
	req.Header.Set("X-Authorization", "Bearer "+twinJWT(t, testTenantA))
	w := httptest.NewRecorder()

	GetByEntity(w, req, "DEVICE", testDeviceA)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var got map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("json: %v", err)
	}
	if got["thingId"] != testTenantA+":device:"+testDeviceA {
		t.Fatalf("thingId=%v", got["thingId"])
	}
	if got["definition"] != "thingsflow:device:meter:1.0.0" {
		t.Fatalf("definition=%v", got["definition"])
	}
	attrs := got["attributes"].(map[string]interface{})
	if attrs["name"] != "Meter A" || attrs["type"] != "meter" || attrs["serial"] != "M-1" {
		t.Fatalf("attributes=%v", attrs)
	}
	features := got["features"].(map[string]interface{})
	telemetry := features["telemetry"].(map[string]interface{})
	props := telemetry["properties"].(map[string]interface{})
	if props["temperature"].(map[string]interface{})["value"] != 22.5 {
		t.Fatalf("temperature property=%v", props["temperature"])
	}
	if props["active"].(map[string]interface{})["value"] != true {
		t.Fatalf("active property=%v", props["active"])
	}
	relations := got["relations"].([]interface{})
	if len(relations) != 1 {
		t.Fatalf("relations=%v", relations)
	}
	rel := relations[0].(map[string]interface{})
	if rel["type"] != "Contains" || rel["direction"] != "IN" {
		t.Fatalf("relation=%v", rel)
	}
	if rel["source"] != testTenantA+":asset:"+testAssetA {
		t.Fatalf("relation source=%v", rel["source"])
	}
}

func TestLoadFeaturesUsesTwinStateStoreBeforePostgresLatest(t *testing.T) {
	store := twinstore.NewMemoryStore()
	twinstore.SetGlobal(store)
	t.Cleanup(func() { twinstore.SetGlobal(nil) })
	if err := store.MergeTelemetry(nil, testTenantA, "DEVICE", testDeviceA, 1234, map[string]interface{}{
		"temperature": 18.75,
	}); err != nil {
		t.Fatalf("merge telemetry: %v", err)
	}

	features, err := loadFeatures(testTenantA, "DEVICE", testDeviceA)
	if err != nil {
		t.Fatalf("loadFeatures: %v", err)
	}
	props := features["telemetry"].(map[string]interface{})["properties"].(map[string]interface{})
	temp := props["temperature"].(telemetryProperty)
	if temp.Value != 18.75 || temp.TS != int64(1234) || temp.Source != "nats_kv" {
		t.Fatalf("temperature feature = %+v", temp)
	}
}

func TestGetTwinReturnsNotFoundForMissingEntity(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)

	missingID := "55555555-5555-5555-5555-555555555555"
	req := httptest.NewRequest("GET", "/api/twins/DEVICE/"+missingID, nil)
	req.Header.Set("X-Authorization", "Bearer "+twinJWT(t, testTenantA))
	w := httptest.NewRecorder()

	GetByEntity(w, req, "DEVICE", missingID)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestGetTwinRejectsCrossTenant(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)

	req := httptest.NewRequest("GET", "/api/twins/DEVICE/"+testDeviceB, nil)
	req.Header.Set("X-Authorization", "Bearer "+twinJWT(t, testTenantA))
	w := httptest.NewRecorder()

	GetByEntity(w, req, "DEVICE", testDeviceB)

	if w.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestGetTwinRejectsUnknownEntityType(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)

	req := httptest.NewRequest("GET", "/api/twins/GATEWAY/"+testDeviceA, nil)
	req.Header.Set("X-Authorization", "Bearer "+twinJWT(t, testTenantA))
	w := httptest.NewRecorder()

	GetByEntity(w, req, "GATEWAY", testDeviceA)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}
