package twin

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func setupTwinRegistryTables(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`DROP TABLE IF EXISTS twin_registry CASCADE`,
		`DROP TABLE IF EXISTS twin_model CASCADE`,
		`CREATE TABLE twin_model (
			tenant_id uuid NOT NULL,
			model_id varchar(255) NOT NULL,
			version varchar(64) NOT NULL DEFAULT '1.0.0',
			kind varchar(64) NOT NULL,
			definition jsonb NOT NULL DEFAULT '{}'::jsonb,
			schema jsonb NOT NULL DEFAULT '{}'::jsonb,
			deprecated boolean NOT NULL DEFAULT false,
			created_time bigint NOT NULL,
			updated_time bigint NOT NULL,
			PRIMARY KEY (tenant_id, model_id, version))`,
		`CREATE TABLE twin_registry (
			tenant_id uuid NOT NULL,
			thing_id varchar(512) NOT NULL,
			entity_type varchar(255) NOT NULL,
			entity_id uuid NOT NULL,
			policy_id varchar(512) NOT NULL,
			definition varchar(512) NOT NULL,
			attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
			model_id varchar(255),
			model_version varchar(64),
			created_time bigint NOT NULL,
			updated_time bigint NOT NULL,
			version bigint NOT NULL DEFAULT 1,
			UNIQUE (tenant_id, thing_id),
			UNIQUE (tenant_id, entity_type, entity_id),
			CHECK (entity_type IN ('DEVICE', 'ASSET')))`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("registry schema: %v", err)
		}
	}
}

// These guards intentionally landed before the registry SQL changed. The
// no-model corpus passed against the old projection, while the pin assertions
// failed because the old SQL never populated model_id/model_version. Together
// they pin both sides of the compatibility contract.
func TestRegistryModelResolutionPinsExactTenantKindTypeAndVersion(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)
	now := time.Now().UnixMilli()

	models := []struct {
		tenant, modelID, version, kind string
	}{
		{testTenantA, "meter", "1.0.9", "DEVICE"},
		{testTenantA, "meter", "1.0.10", "DEVICE"},
		{testTenantA, "energy_meter", "99.0.0", "DEVICE"},
		{testTenantA, "building", "1.0.0", "ASSET"},
		{testTenantB, "meter", "2147483647.0.0", "DEVICE"},
	}
	for _, model := range models {
		if _, err := db.Exec(`INSERT INTO twin_model
			(tenant_id, model_id, version, kind, definition, schema, created_time, updated_time)
			VALUES ($1,$2,$3,$4,'{}','{}',$5,$5)`,
			model.tenant, model.modelID, model.version, model.kind, now); err != nil {
			t.Fatalf("seed model %s/%s/%s: %v", model.tenant, model.modelID, model.version, err)
		}
	}

	if err := SyncRegistryRow(context.Background(), db, testTenantA, "DEVICE", testDeviceA); err != nil {
		t.Fatalf("sync device with competing catalog rows: %v", err)
	}
	var modelID, version, definition string
	if err := db.QueryRow(`SELECT model_id, model_version, definition FROM twin_registry
		WHERE tenant_id=$1 AND entity_type='DEVICE' AND entity_id=$2`, testTenantA, testDeviceA).
		Scan(&modelID, &version, &definition); err != nil {
		t.Fatalf("read device pin: %v", err)
	}
	if modelID != "meter" || version != "1.0.10" || definition != "thingsflow:device:meter:1.0.10" {
		t.Fatalf("device pin=(%q,%q,%q), want exact type model at numeric latest", modelID, version, definition)
	}

	if err := SyncRegistryRow(context.Background(), db, testTenantA, "ASSET", testAssetA); err != nil {
		t.Fatalf("sync asset model: %v", err)
	}
	if err := db.QueryRow(`SELECT model_id, model_version, definition FROM twin_registry
		WHERE tenant_id=$1 AND entity_type='ASSET' AND entity_id=$2`, testTenantA, testAssetA).
		Scan(&modelID, &version, &definition); err != nil {
		t.Fatalf("read asset pin: %v", err)
	}
	if modelID != "building" || version != "1.0.0" || definition != "thingsflow:asset:building:1.0.0" {
		t.Fatalf("asset pin=(%q,%q,%q)", modelID, version, definition)
	}

	const defaultDevice = "77777777-7777-7777-7777-777777777777"
	if _, err := db.Exec(`INSERT INTO device (id,created_time,tenant_id,name,type,label,additional_info)
		VALUES ($1,$2,$3,'Untyped',NULL,'','{}')`, defaultDevice, now, testTenantA); err != nil {
		t.Fatalf("seed empty-type device: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO twin_model
		(tenant_id,model_id,version,kind,definition,schema,created_time,updated_time)
		VALUES ($1,'default','1.0.0','DEVICE','{}','{}',$2,$2)`, testTenantA, now); err != nil {
		t.Fatalf("seed default model: %v", err)
	}
	if err := SyncRegistryRow(context.Background(), db, testTenantA, "DEVICE", defaultDevice); err != nil {
		t.Fatalf("sync empty-type device: %v", err)
	}
	if err := db.QueryRow(`SELECT model_id,model_version FROM twin_registry
		WHERE tenant_id=$1 AND entity_id=$2`, testTenantA, defaultDevice).Scan(&modelID, &version); err != nil {
		t.Fatalf("read default pin: %v", err)
	}
	if modelID != "default" || version != "1.0.0" {
		t.Fatalf("empty entity type pin=(%q,%q), want default/1.0.0", modelID, version)
	}
}

func TestRegistryPinNeverSilentlyAdvancesAndBackfillConverges(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)
	now := time.Now().UnixMilli()
	if _, err := db.Exec(`INSERT INTO twin_model
		(tenant_id, model_id, version, kind, definition, schema, created_time, updated_time)
		VALUES ($1,'meter','1.0.0','DEVICE','{}','{}',$2,$2)`, testTenantA, now); err != nil {
		t.Fatalf("seed first model: %v", err)
	}
	if _, err := BackfillRegistryContext(context.Background(), db); err != nil {
		t.Fatalf("first backfill: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO twin_model
		(tenant_id, model_id, version, kind, definition, schema, created_time, updated_time)
		VALUES ($1,'meter','2.0.0','DEVICE','{}','{}',$2,$2)`, testTenantA, now+1); err != nil {
		t.Fatalf("seed newer model: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := BackfillRegistryContext(context.Background(), db); err != nil {
			t.Fatalf("convergence pass %d: %v", i+1, err)
		}
	}
	var modelID, version string
	if err := db.QueryRow(`SELECT model_id, model_version FROM twin_registry
		WHERE tenant_id=$1 AND entity_type='DEVICE' AND entity_id=$2`, testTenantA, testDeviceA).
		Scan(&modelID, &version); err != nil {
		t.Fatalf("read immutable pin: %v", err)
	}
	if modelID != "meter" || version != "1.0.0" {
		t.Fatalf("pin silently advanced to %s/%s", modelID, version)
	}
}

func TestNoModelTwinJSONIsByteIdenticalWithUnrelatedCatalogRows(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)
	if _, err := BackfillRegistry(db); err != nil {
		t.Fatalf("initial backfill: %v", err)
	}

	deviceBefore := renderTwinBytes(t, "DEVICE", testDeviceA)
	assetBefore := renderTwinBytes(t, "ASSET", testAssetA)
	now := time.Now().UnixMilli()
	if _, err := db.Exec(`INSERT INTO twin_model
		(tenant_id, model_id, version, kind, definition, schema, created_time, updated_time)
		VALUES
		($1,'unrelated','9.9.9','DEVICE','{}','{}',$3,$3),
		($2,'meter','99.0.0','DEVICE','{}','{}',$3,$3)`, testTenantA, testTenantB, now); err != nil {
		t.Fatalf("seed unrelated/cross-tenant catalog: %v", err)
	}
	if _, err := BackfillRegistry(db); err != nil {
		t.Fatalf("backfill with unrelated catalog: %v", err)
	}
	deviceAfter := renderTwinBytes(t, "DEVICE", testDeviceA)
	assetAfter := renderTwinBytes(t, "ASSET", testAssetA)
	if string(deviceAfter) != string(deviceBefore) {
		t.Fatalf("no-model DEVICE bytes changed:\nbefore=%s\nafter =%s", deviceBefore, deviceAfter)
	}
	if string(assetAfter) != string(assetBefore) {
		t.Fatalf("no-model ASSET bytes changed:\nbefore=%s\nafter =%s", assetBefore, assetAfter)
	}
	t.Logf("no-model byte corpus DEVICE sha256=%x bytes=%d ASSET sha256=%x bytes=%d",
		sha256.Sum256(deviceBefore), len(deviceBefore), sha256.Sum256(assetBefore), len(assetBefore))
}

func renderTwinBytes(t *testing.T, entityType, entityID string) []byte {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/twins/"+entityType+"/"+entityID, nil)
	req.Header.Set("X-Authorization", "Bearer "+twinJWT(t, testTenantA))
	w := httptest.NewRecorder()
	GetByEntity(w, req, entityType, entityID)
	if w.Code != http.StatusOK {
		t.Fatalf("render %s/%s status=%d body=%s", entityType, entityID, w.Code, w.Body.String())
	}
	return append([]byte(nil), w.Body.Bytes()...)
}

func TestBackfillRegistryCreatesIdempotentDeviceAndAssetTwins(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)

	inserted, err := BackfillRegistry(db)
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if inserted != 3 {
		t.Fatalf("inserted=%d", inserted)
	}

	second, err := BackfillRegistry(db)
	if err != nil {
		t.Fatalf("second backfill: %v", err)
	}
	if second != 0 {
		t.Fatalf("second inserted=%d", second)
	}

	row, err := GetRegistryByEntity(db, testTenantA, "DEVICE", testDeviceA)
	if err != nil {
		t.Fatalf("registry row: %v", err)
	}
	if row.ThingID != testTenantA+":device:"+testDeviceA {
		t.Fatalf("thingID=%s", row.ThingID)
	}
	if row.PolicyID != "tenant:"+testTenantA+":default" {
		t.Fatalf("policyID=%s", row.PolicyID)
	}
	if row.Definition != "thingsflow:device:meter:1.0.0" {
		t.Fatalf("definition=%s", row.Definition)
	}
	if row.Attributes["name"] != "Meter A" || row.Attributes["serial"] != "M-1" {
		t.Fatalf("attributes=%v", row.Attributes)
	}
}

func TestSyncRegistryRowUpsertsSingleEntity(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)
	ctx := context.Background()

	if err := SyncRegistryRow(ctx, db, testTenantA, "DEVICE", testDeviceA); err != nil {
		t.Fatalf("sync device: %v", err)
	}
	row, err := GetRegistryByEntity(db, testTenantA, "DEVICE", testDeviceA)
	if err != nil {
		t.Fatalf("registry row after sync: %v", err)
	}
	if row.ThingID != testTenantA+":device:"+testDeviceA {
		t.Fatalf("thingID=%s", row.ThingID)
	}
	if row.Definition != "thingsflow:device:meter:1.0.0" {
		t.Fatalf("definition=%s", row.Definition)
	}

	// The sync must be scoped to ONE entity: the other seeded rows stay absent.
	var total int
	if err := db.QueryRow(`SELECT count(*) FROM twin_registry`).Scan(&total); err != nil {
		t.Fatalf("count: %v", err)
	}
	if total != 1 {
		t.Fatalf("registry rows=%d, want 1 (single-entity sync leaked)", total)
	}

	// Idempotent re-sync.
	if err := SyncRegistryRow(ctx, db, testTenantA, "DEVICE", testDeviceA); err != nil {
		t.Fatalf("re-sync device: %v", err)
	}

	if err := SyncRegistryRow(ctx, db, testTenantA, "ASSET", testAssetA); err != nil {
		t.Fatalf("sync asset: %v", err)
	}
	assetRow, err := GetRegistryByEntity(db, testTenantA, "ASSET", testAssetA)
	if err != nil {
		t.Fatalf("asset registry row: %v", err)
	}
	if assetRow.ThingID != testTenantA+":asset:"+testAssetA {
		t.Fatalf("asset thingID=%s", assetRow.ThingID)
	}

	if err := SyncRegistryRow(ctx, db, testTenantA, "GATEWAY", testDeviceA); err == nil {
		t.Fatal("unsupported entity type accepted")
	}
}

func TestDeleteRegistryRowRemovesRow(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)
	ctx := context.Background()

	if _, err := BackfillRegistry(db); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if err := DeleteRegistryRow(ctx, db, testTenantA, "DEVICE", testDeviceA); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := GetRegistryByEntity(db, testTenantA, "DEVICE", testDeviceA); err != sql.ErrNoRows {
		t.Fatalf("row after delete: err=%v, want sql.ErrNoRows", err)
	}
	// Other rows are untouched.
	if _, err := GetRegistryByEntity(db, testTenantA, "ASSET", testAssetA); err != nil {
		t.Fatalf("asset row must survive device delete: %v", err)
	}
}

// twin_registry has no FK/cascade, so a row whose entity disappeared (delete
// hook missed, direct SQL) can only be reclaimed by the boot backfill sweep.
func TestBackfillRegistryContextSweepsOrphans(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)
	ctx := context.Background()

	if _, err := BackfillRegistryContext(ctx, db); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	// Simulate a delete that bypassed the runtime hook.
	if _, err := db.Exec(`DELETE FROM device WHERE id = $1`, testDeviceA); err != nil {
		t.Fatalf("delete device row: %v", err)
	}

	converged, err := BackfillRegistryContext(ctx, db)
	if err != nil {
		t.Fatalf("second backfill: %v", err)
	}
	if converged != 1 {
		t.Fatalf("converged=%d, want exactly the 1 swept orphan", converged)
	}
	if _, err := GetRegistryByEntity(db, testTenantA, "DEVICE", testDeviceA); err != sql.ErrNoRows {
		t.Fatalf("orphan row after sweep: err=%v, want sql.ErrNoRows", err)
	}
	// Live entities keep their rows.
	if _, err := GetRegistryByEntity(db, testTenantA, "ASSET", testAssetA); err != nil {
		t.Fatalf("live asset row swept: %v", err)
	}
}

func TestGetTwinPrefersRegistryIdentityAndAttributes(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)

	now := time.Now().UnixMilli()
	attrs, err := json.Marshal(map[string]interface{}{
		"id":         testDeviceA,
		"entityType": "DEVICE",
		"tenantId":   testTenantA,
		"name":       "Meter A governed",
		"type":       "meter",
		"source":     "twin_registry",
	})
	if err != nil {
		t.Fatalf("attrs: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO twin_registry
		(tenant_id, thing_id, entity_type, entity_id, policy_id, definition, attributes, created_time, updated_time)
		VALUES ($1, $2, 'DEVICE', $3, $4, $5, $6::jsonb, $7, $7)`,
		testTenantA,
		"factory-a:line-1:meter-a",
		testDeviceA,
		"policy:factory-a:default",
		"thingsflow:device:industrial-meter:2.0.0",
		string(attrs),
		now,
	); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

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
	if got["thingId"] != "factory-a:line-1:meter-a" {
		t.Fatalf("thingId=%v", got["thingId"])
	}
	if got["policyId"] != "policy:factory-a:default" {
		t.Fatalf("policyId=%v", got["policyId"])
	}
	if got["definition"] != "thingsflow:device:industrial-meter:2.0.0" {
		t.Fatalf("definition=%v", got["definition"])
	}
	attrsOut := got["attributes"].(map[string]interface{})
	if attrsOut["source"] != "twin_registry" || attrsOut["name"] != "Meter A governed" {
		t.Fatalf("attributes=%v", attrsOut)
	}
}

// A scalar (non-object) additional_info must not poison the projection: the
// jsonb concatenation only accepts objects, and one legacy row with e.g.
// `"just a note"` used to make the WHOLE backfill statement throw — freezing
// registry convergence for every entity. Non-objects degrade to '{}'.
func TestSyncRegistryRowToleratesScalarAdditionalInfo(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)
	ctx := context.Background()

	const scalarDevice = "66666666-6666-6666-6666-666666666666"
	if _, err := db.Exec(`INSERT INTO device (id, created_time, tenant_id, name, type, label, additional_info)
		VALUES ($1, $2, $3, 'Scalar Meter', 'meter', '', '"just a note"')`,
		scalarDevice, time.Now().UnixMilli(), testTenantA); err != nil {
		t.Fatalf("seed scalar device: %v", err)
	}

	if err := SyncRegistryRow(ctx, db, testTenantA, "DEVICE", scalarDevice); err != nil {
		t.Fatalf("sync with scalar additional_info: %v", err)
	}
	row, err := GetRegistryByEntity(db, testTenantA, "DEVICE", scalarDevice)
	if err != nil {
		t.Fatalf("registry row: %v", err)
	}
	// Attributes stay a valid object carrying the projected identity fields.
	if row.Attributes["name"] != "Scalar Meter" || row.Attributes["entityType"] != "DEVICE" {
		t.Fatalf("attributes = %#v, want projected identity despite scalar additional_info", row.Attributes)
	}

	// The whole-table backfill also survives the row.
	if _, err := BackfillRegistryContext(ctx, db); err != nil {
		t.Fatalf("backfill with scalar additional_info present: %v", err)
	}
}

// One failing pass must not stop the others: dropping the device table fails
// the device pass (and the sweep guard), but the asset pass still converges
// and the aggregated error reports what broke.
func TestBackfillRegistryContextAggregatesErrors(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)
	ctx := context.Background()

	if _, err := db.Exec(`DROP TABLE device`); err != nil {
		t.Fatalf("drop device: %v", err)
	}
	_, err := BackfillRegistryContext(ctx, db)
	if err == nil {
		t.Fatal("want an aggregated error when the device pass fails")
	}
	// The asset pass ran despite the device failure.
	if _, err := GetRegistryByEntity(db, testTenantA, "ASSET", testAssetA); err != nil {
		t.Fatalf("asset pass did not run after device-pass failure: %v", err)
	}
}

// Sweep guard: with device AND asset both empty, the sweep must refuse to
// delete anything — that shape means a mis-wired database, not a real mass
// deletion.
func TestSweepAbortsWhenEntityTablesEmpty(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)
	ctx := context.Background()

	if _, err := BackfillRegistryContext(ctx, db); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM device`); err != nil {
		t.Fatalf("empty device: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM asset`); err != nil {
		t.Fatalf("empty asset: %v", err)
	}

	swept, err := sweepRegistryOrphans(ctx, db)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if swept != 0 {
		t.Fatalf("swept=%d, want 0 (guard must refuse)", swept)
	}
	var remaining int
	if err := db.QueryRow(`SELECT count(*) FROM twin_registry`).Scan(&remaining); err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining != 3 {
		t.Fatalf("registry rows=%d, want all 3 preserved", remaining)
	}
}

// Sweep guard: more candidates than TWIN_REGISTRY_SWEEP_MAX aborts the pass.
func TestSweepAbortsAboveCeiling(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)
	ctx := context.Background()
	t.Setenv("TWIN_REGISTRY_SWEEP_MAX", "1")

	if _, err := BackfillRegistryContext(ctx, db); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	// Orphan BOTH devices (the asset table stays populated, so the
	// empty-tables guard does not mask the ceiling).
	if _, err := db.Exec(`DELETE FROM device`); err != nil {
		t.Fatalf("delete devices: %v", err)
	}
	swept, err := sweepRegistryOrphans(ctx, db)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if swept != 0 {
		t.Fatalf("swept=%d, want 0 (2 candidates over ceiling 1)", swept)
	}
	var remaining int
	if err := db.QueryRow(`SELECT count(*) FROM twin_registry WHERE entity_type = 'DEVICE'`).Scan(&remaining); err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining != 2 {
		t.Fatalf("device registry rows=%d, want both preserved", remaining)
	}
}

// Tenant-aware sweep: a registry row whose entity id exists but under a
// DIFFERENT tenant is an orphan of its own tenant and must be reclaimed.
func TestSweepReclaimsTenantMismatchedRow(t *testing.T) {
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)
	ctx := context.Background()

	// testDeviceB belongs to testTenantB; file a registry row for it under
	// testTenantA (hook bug / manual insert shape).
	now := time.Now().UnixMilli()
	if _, err := db.Exec(`INSERT INTO twin_registry
		(tenant_id, thing_id, entity_type, entity_id, policy_id, definition, attributes, created_time, updated_time)
		VALUES ($1, $2, 'DEVICE', $3, 'p', 'd', '{}'::jsonb, $4, $4)`,
		testTenantA, testTenantA+":device:"+testDeviceB, testDeviceB, now); err != nil {
		t.Fatalf("seed mismatched row: %v", err)
	}

	swept, err := sweepRegistryOrphans(ctx, db)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if swept != 1 {
		t.Fatalf("swept=%d, want exactly the tenant-mismatched row", swept)
	}
	if _, err := GetRegistryByEntity(db, testTenantA, "DEVICE", testDeviceB); err != sql.ErrNoRows {
		t.Fatalf("mismatched row after sweep: err=%v, want sql.ErrNoRows", err)
	}
}
