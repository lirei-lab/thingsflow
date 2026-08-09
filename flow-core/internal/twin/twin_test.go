package twin

import (
	"context"
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
		`DROP TABLE IF EXISTS attribute_kv CASCADE`,
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
			PRIMARY KEY (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type, direction),
			CHECK (direction IN ('DIRECTED', 'BIDIRECTIONAL')))`,
		`CREATE TABLE key_dictionary (
			key text PRIMARY KEY,
			key_id serial UNIQUE)`,
		`CREATE TABLE attribute_kv (
			entity_id uuid not null,
			attribute_type int not null,
			attribute_key int not null,
			bool_v boolean, str_v text, long_v bigint, dbl_v double precision, json_v json,
			last_update_ts bigint,
			PRIMARY KEY (entity_id, attribute_type, attribute_key))`,
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
	const now int64 = 1778692040123
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

func TestLoadFeaturesMergesPinnedModelDeclarationSkeletons(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)
	now := time.Now().UnixMilli()
	schema := `{
		"modelId":"meter","version":"1.2.3","kind":"DEVICE",
		"unknownKeys":"allow","enforcementMode":"warn","attributes":{},
		"features":{
			"electrical":{"definition":"thingsflow:feature:electrical:1.0.0","properties":{"voltage":{"type":"number"}},"desiredProperties":{"sample_interval":{"type":"integer"}}},
			"telemetry":{"definition":"thingsflow:feature:modeled_telemetry:1.0.0","properties":{},"desiredProperties":{}}
		},"relationships":{}
	}`
	if _, err := db.Exec(`INSERT INTO twin_model
		(tenant_id, model_id, version, kind, definition, schema, created_time, updated_time)
		VALUES ($1,'meter','1.2.3','DEVICE','{}',$2::jsonb,$3,$3)`, testTenantA, schema, now); err != nil {
		t.Fatalf("seed model: %v", err)
	}
	if err := SyncRegistryRow(context.Background(), db, testTenantA, "DEVICE", testDeviceA); err != nil {
		t.Fatalf("pin registry: %v", err)
	}

	features, err := loadFeatures(testTenantA, "DEVICE", testDeviceA)
	if err != nil {
		t.Fatalf("loadFeatures: %v", err)
	}
	electrical, ok := features["electrical"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing electrical skeleton: %#v", features)
	}
	if electrical["definition"] != "thingsflow:feature:electrical:1.0.0" {
		t.Fatalf("electrical definition=%v", electrical["definition"])
	}
	if properties, ok := electrical["properties"].(map[string]interface{}); !ok || len(properties) != 0 {
		t.Fatalf("skeleton properties=%#v, want an empty declaration map", electrical["properties"])
	}
	if desired, ok := electrical["desiredProperties"].(map[string]interface{}); !ok || len(desired) != 0 {
		t.Fatalf("skeleton desiredProperties=%#v", electrical["desiredProperties"])
	}
	telemetry := features["telemetry"].(map[string]interface{})
	if telemetry["definition"] != "thingsflow:feature:telemetry:1.0.0" {
		t.Fatalf("observed telemetry declaration was overwritten: %#v", telemetry)
	}
	if len(telemetry["properties"].(map[string]interface{})) != 2 {
		t.Fatalf("observed telemetry properties were overwritten: %#v", telemetry)
	}
}

// TestLoadFeaturesSurfacesPersistedDesiredProperties — R5 read surface: a
// feature whose desiredProperties were persisted as feature.<name>.desired.<property>
// SERVER_SCOPE keys appears WITH those values in loadFeatures, not as an empty
// skeleton placeholder.
func TestLoadFeaturesSurfacesPersistedDesiredProperties(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)
	now := time.Now().UnixMilli()
	schema := `{
		"modelId":"meter","version":"1.2.3","kind":"DEVICE",
		"unknownKeys":"allow","enforcementMode":"warn","attributes":{},
		"features":{
			"electrical":{"definition":"thingsflow:feature:electrical:1.0.0","properties":{"voltage":{"type":"number"}},"desiredProperties":{"sample_interval":{"type":"integer"}}}
		},"relationships":{}
	}`
	if _, err := db.Exec(`INSERT INTO twin_model
		(tenant_id, model_id, version, kind, definition, schema, created_time, updated_time)
		VALUES ($1,'meter','1.2.3','DEVICE','{}',$2::jsonb,$3,$3)`, testTenantA, schema, now); err != nil {
		t.Fatalf("seed model: %v", err)
	}
	if err := SyncRegistryRow(context.Background(), db, testTenantA, "DEVICE", testDeviceA); err != nil {
		t.Fatalf("pin registry: %v", err)
	}
	// Insert a persisted desired property exactly as HandleSaveFeatures would:
	// feature.electrical.desired.sample_interval in SERVER_SCOPE (attr_type 2).
	// Insert the key directly (RETURNING key_id): the in-memory GetOrInsertKeyID
	// cache persists across test schemas and can return a stale id in this schema.
	var keyID int
	if err := db.QueryRow(
		`INSERT INTO key_dictionary (key) VALUES ('feature.electrical.desired.sample_interval') RETURNING key_id`).Scan(&keyID); err != nil {
		t.Fatalf("seed desired key: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO attribute_kv (entity_id, attribute_type, attribute_key, long_v, last_update_ts)
		VALUES ($1, 2, $2, 300, $3)`, testDeviceA, keyID, now); err != nil {
		t.Fatalf("seed desired attribute: %v", err)
	}

	features, err := loadFeatures(testTenantA, "DEVICE", testDeviceA)
	if err != nil {
		t.Fatalf("loadFeatures: %v", err)
	}
	electrical, ok := features["electrical"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing electrical feature: %#v", features)
	}
	desired, ok := electrical["desiredProperties"].(map[string]interface{})
	if !ok {
		t.Fatalf("electrical.desiredProperties missing: %#v", electrical)
	}
	if got, ok := desired["sample_interval"].(int64); !ok || got != 300 {
		t.Fatalf("desired.sample_interval = %#v (%T), want int64(300)", desired["sample_interval"], desired["sample_interval"])
	}
}

// TestGetTwinDeltaSurfacesDesiredVsReported — R5 delta: a feature with
// persisted desiredProperties (SERVER_SCOPE) and a device-reported CLIENT_SCOPE
// value produces a delta block with the desired value and the reported value
// per key; a not-yet-reported desired key reports nil.
func TestGetTwinDeltaSurfacesDesiredVsReported(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)
	now := time.Now().UnixMilli()
	schema := `{
		"modelId":"meter","version":"1.2.3","kind":"DEVICE",
		"unknownKeys":"allow","enforcementMode":"warn","attributes":{},
		"features":{
			"electrical":{"definition":"thingsflow:feature:electrical:1.0.0","properties":{"voltage":{"type":"number"}},"desiredProperties":{"sample_interval":{"type":"integer"}}}
		},"relationships":{}
	}`
	if _, err := db.Exec(`INSERT INTO twin_model
		(tenant_id, model_id, version, kind, definition, schema, created_time, updated_time)
		VALUES ($1,'meter','1.2.3','DEVICE','{}',$2::jsonb,$3,$3)`, testTenantA, schema, now); err != nil {
		t.Fatalf("seed model: %v", err)
	}
	if err := SyncRegistryRow(context.Background(), db, testTenantA, "DEVICE", testDeviceA); err != nil {
		t.Fatalf("pin registry: %v", err)
	}
	// Desired persisted (SERVER_SCOPE attr_type 2) + reported (CLIENT attr_type 0).
	// Reported uses the FLAT device-reported key (e.g. "sample_interval" — the
	// shape a device publishes on the attributes topic and internal/desiredstate
	// merges as CLIENT_SCOPE), NOT the feature-prefixed form. This matches how a
	// real device reports and pins the delta's bare-key matching (a prefixed
	// reported key must still match via the fallback).
	// Insert the keys directly and capture the real key_ids: the in-memory
	// GetOrInsertKeyID cache persists across test schemas (each test recreates
	// key_dictionary with a fresh sequence), so a cached id can point at the
	// wrong row in THIS schema.
	var desiredKey, reportedKey int
	if err := db.QueryRow(
		`INSERT INTO key_dictionary (key) VALUES ('feature.electrical.desired.sample_interval') RETURNING key_id`).Scan(&desiredKey); err != nil {
		t.Fatalf("seed desired key: %v", err)
	}
	if err := db.QueryRow(
		`INSERT INTO key_dictionary (key) VALUES ('sample_interval') RETURNING key_id`).Scan(&reportedKey); err != nil {
		t.Fatalf("seed reported key: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO attribute_kv (entity_id, attribute_type, attribute_key, long_v, last_update_ts)
		VALUES ($1, 2, $2, 300, $4), ($1, 0, $3, 120, $4)`, testDeviceA, desiredKey, reportedKey, now); err != nil {
		t.Fatalf("seed desired+reported: %v", err)
	}

	features, err := loadFeatures(testTenantA, "DEVICE", testDeviceA)
	if err != nil {
		t.Fatalf("loadFeatures: %v", err)
	}
	reported, err := loadReportedAttributes(testDeviceA)
	if err != nil {
		t.Fatalf("loadReportedAttributes: %v", err)
	}
	delta := computeDelta(features, reported)

	electrical, ok := delta["electrical"].(map[string]interface{})
	if !ok {
		t.Fatalf("delta missing electrical feature: %#v", delta)
	}
	desired := electrical["desired"].(map[string]interface{})
	rep := electrical["reported"].(map[string]interface{})
	if got, ok := desired["sample_interval"].(int64); !ok || got != 300 {
		t.Fatalf("delta desired.sample_interval = %#v, want 300", desired["sample_interval"])
	}
	if got, ok := rep["sample_interval"].(int64); !ok || got != 120 {
		t.Fatalf("delta reported.sample_interval = %#v, want 120", rep["sample_interval"])
	}
}

// TestComputeDeltaMatchesBareAndPrefixedReportedKeys — the delta must match a
// device-reported value by the BARE property name first (the flat CLIENT key a
// device actually publishes), and fall back to the feature.<name>.<property>
// form so feature-prefixed reports also match. A missing reported value is nil.
func TestComputeDeltaMatchesBareAndPrefixedReportedKeys(t *testing.T) {
	features := map[string]interface{}{
		"electrical": map[string]interface{}{
			"definition":        "thingsflow:feature:electrical:1.0.0",
			"properties":        map[string]interface{}{},
			"desiredProperties": map[string]interface{}{"sample_interval": int64(300), "voltage": int64(230)},
		},
	}
	// Device reported a flat "sample_interval" (bare CLIENT key) and a
	// prefixed "feature.thermal.target" (for a second feature's report).
	reported := map[string]interface{}{
		"sample_interval":        int64(120),
		"feature.thermal.target": int64(40),
	}
	delta := computeDelta(features, reported)

	electrical := delta["electrical"].(map[string]interface{})
	rep := electrical["reported"].(map[string]interface{})
	if got, ok := rep["sample_interval"].(int64); !ok || got != 120 {
		t.Fatalf("bare reported.sample_interval = %#v, want 120", rep["sample_interval"])
	}
	if got, ok := rep["voltage"].(int64); ok {
		t.Fatalf("unreported voltage = %#v, want nil", got)
	}
	if rep["voltage"] != nil {
		t.Fatalf("unreported voltage must be nil, got %#v", rep["voltage"])
	}
}

func TestPinnedModelNullJSONBDefaultsToEmptySkeleton(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)
	for _, statement := range []string{
		`ALTER TABLE twin_model ALTER COLUMN definition DROP NOT NULL`,
		`ALTER TABLE twin_model ALTER COLUMN schema DROP NOT NULL`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("make legacy JSONB nullable: %v", err)
		}
	}
	now := time.Now().UnixMilli()
	if _, err := db.Exec(`INSERT INTO twin_model
		(tenant_id,model_id,version,kind,definition,schema,created_time,updated_time)
		VALUES ($1,'meter','1.0.0','DEVICE',NULL,NULL,$2,$2)`, testTenantA, now); err != nil {
		t.Fatalf("seed null JSONB model: %v", err)
	}
	if err := SyncRegistryRow(context.Background(), db, testTenantA, "DEVICE", testDeviceA); err != nil {
		t.Fatalf("pin null JSONB model: %v", err)
	}
	features, err := loadFeatures(testTenantA, "DEVICE", testDeviceA)
	if err != nil {
		t.Fatalf("loadFeatures with null JSONB defaults: %v", err)
	}
	if len(features) != 1 || features["telemetry"] == nil {
		t.Fatalf("null schema invented model features or removed telemetry: %#v", features)
	}
}

func TestLoadRelationsProjectsBidirectionalAsBoth(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	now := time.Now().UnixMilli()
	if _, err := db.Exec(`UPDATE topology_edge SET direction='BIDIRECTIONAL', updated_time=$1`, now); err != nil {
		t.Fatalf("make seeded edge bidirectional: %v", err)
	}

	for _, endpoint := range []struct {
		typeName, id string
	}{{"ASSET", testAssetA}, {"DEVICE", testDeviceA}} {
		relations, err := loadRelations(testTenantA, endpoint.typeName, endpoint.id)
		if err != nil {
			t.Fatalf("loadRelations %s: %v", endpoint.typeName, err)
		}
		if len(relations) != 1 || relations[0].Direction != "BOTH" {
			t.Fatalf("%s relations=%+v, want one BOTH projection", endpoint.typeName, relations)
		}
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

// TestGetTwinNonExpandPathByteIdentical pins the R3 compatibility contract:
// without ?expand= the relationProjection must not gain the expansion-only
// `depth`/`state` keys, and a repeated identical request must produce the
// byte-identical body. This is the regression guard for the expand wiring.
func TestGetTwinNonExpandPathByteIdentical(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)

	request := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/api/twins/DEVICE/"+testDeviceA, nil)
		req.Header.Set("X-Authorization", "Bearer "+twinJWT(t, testTenantA))
		w := httptest.NewRecorder()
		GetByEntity(w, req, "DEVICE", testDeviceA)
		return w
	}

	w := request()
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var got map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("json: %v", err)
	}
	relations, ok := got["relations"].([]interface{})
	if !ok || len(relations) != 1 {
		t.Fatalf("relations=%v want 1", got["relations"])
	}
	rel := relations[0].(map[string]interface{})
	for _, expansionKey := range []string{"depth", "state"} {
		if _, present := rel[expansionKey]; present {
			t.Fatalf("non-expand relation must not carry %q key: %v", expansionKey, rel)
		}
	}

	w2 := request()
	if w2.Body.String() != w.Body.String() {
		t.Fatalf("non-expand body not deterministic:\n%s\nvs\n%s", w.Body.String(), w2.Body.String())
	}
}
