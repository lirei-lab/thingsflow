package twinmodel

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"flow-core/internal/migrations"

	_ "github.com/lib/pq"
)

const (
	catalogTenantA      = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	catalogTenantB      = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	catalogDeviceA      = "11111111-1111-1111-1111-111111111111"
	catalogDevicePinned = "22222222-2222-2222-2222-222222222222"
	catalogDeviceB      = "33333333-3333-3333-3333-333333333333"
)

func newCatalogTestDB(t *testing.T) *sql.DB {
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
	schema := fmt.Sprintf("twinmodel_test_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create isolated test schema: %v", err)
	}
	if _, err := db.Exec(`SET search_path TO ` + schema); err != nil {
		t.Fatalf("set isolated test schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`)
		db.Close()
	})
	return db
}

func setupCatalogTestSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	statements := []string{
		`DROP TABLE IF EXISTS topology_edge CASCADE`,
		`DROP TABLE IF EXISTS twin_registry CASCADE`,
		`DROP TABLE IF EXISTS twin_model CASCADE`,
		`DROP TABLE IF EXISTS topology_relation_type CASCADE`,
		`DROP TABLE IF EXISTS device CASCADE`,
		`DROP TABLE IF EXISTS asset CASCADE`,
		`DROP FUNCTION IF EXISTS thingsflow_lock_bidirectional_edge() CASCADE`,
		`CREATE TABLE device (id uuid PRIMARY KEY, tenant_id uuid NOT NULL, type text, created_time bigint, name text)`,
		`CREATE TABLE asset (id uuid PRIMARY KEY, tenant_id uuid NOT NULL, type text, created_time bigint, name text)`,
		`CREATE TABLE twin_model (
			tenant_id uuid NOT NULL, model_id varchar(255) NOT NULL,
			version varchar(64) NOT NULL DEFAULT '1.0.0', kind varchar(64) NOT NULL,
			definition jsonb NOT NULL DEFAULT '{}', schema jsonb NOT NULL DEFAULT '{}',
			created_time bigint NOT NULL, updated_time bigint NOT NULL,
			PRIMARY KEY (tenant_id, model_id, version))`,
		`CREATE TABLE twin_registry (
			tenant_id uuid NOT NULL, thing_id varchar(512) NOT NULL,
			entity_type varchar(255) NOT NULL, entity_id uuid NOT NULL,
			policy_id varchar(512) NOT NULL, definition varchar(512) NOT NULL,
			attributes jsonb NOT NULL DEFAULT '{}', created_time bigint NOT NULL,
			updated_time bigint NOT NULL, version bigint NOT NULL DEFAULT 1,
			UNIQUE (tenant_id, thing_id), UNIQUE (tenant_id, entity_type, entity_id))`,
		`CREATE TABLE topology_edge (
			tenant_id uuid NOT NULL, from_id uuid NOT NULL, from_type varchar(255) NOT NULL,
			to_id uuid NOT NULL, to_type varchar(255) NOT NULL,
			relation_type_group varchar(255) NOT NULL DEFAULT 'COMMON', relation_type varchar(255) NOT NULL,
			direction varchar(32) NOT NULL DEFAULT 'DIRECTED', metadata jsonb NOT NULL DEFAULT '{}',
			created_time bigint NOT NULL, updated_time bigint NOT NULL, version bigint NOT NULL DEFAULT 1,
			PRIMARY KEY (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type),
			CHECK (direction IN ('DIRECTED')))`,
		`CREATE TABLE topology_relation_type (name varchar(255) PRIMARY KEY)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("catalog schema: %v\n%s", err, statement)
		}
	}
	up, err := migrations.FS.ReadFile("migrations/0014_twin_model_catalog.up.sql")
	if err != nil {
		t.Fatalf("read 0014 up: %v", err)
	}
	if _, err := db.Exec(string(up)); err != nil {
		t.Fatalf("apply 0014 up: %v", err)
	}
	seedCatalogEntities(t, db)
}

func TestStoreCreateRequiresKnownRelationType(t *testing.T) {
	db := newCatalogTestDB(t)
	setupCatalogTestSchema(t, db)
	store := NewStore(db)
	model := json.RawMessage(`{"modelId":"Energy Meter","version":"1.0.0","kind":"DEVICE","relationships":{"Unknown":{"target":["building"],"targetEntityTypes":["ASSET"],"maxCardinality":1,"bidirectional":false}}}`)
	if _, err := store.Create(context.Background(), catalogTenantA, model); !errors.Is(err, ErrInvalidModel) {
		t.Fatalf("unknown relation error=%v, want ErrInvalidModel", err)
	}
	if _, err := db.Exec(`INSERT INTO topology_relation_type (name) VALUES ('Contains')`); err != nil {
		t.Fatalf("seed relation vocabulary: %v", err)
	}
	model = json.RawMessage(`{"modelId":"Energy Meter","version":"1.0.0","kind":"DEVICE","relationships":{"Contains":{"target":["building"],"targetEntityTypes":["ASSET"],"maxCardinality":1,"bidirectional":false}}}`)
	if _, err := store.Create(context.Background(), catalogTenantA, model); err != nil {
		t.Fatalf("known relation create: %v", err)
	}
}

func seedCatalogEntities(t *testing.T, db *sql.DB) {
	t.Helper()
	statements := []string{
		`INSERT INTO device (id, tenant_id, type, created_time, name) VALUES
		 ('11111111-1111-1111-1111-111111111111','aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa','Energy Meter',1,'A'),
		 ('22222222-2222-2222-2222-222222222222','aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa','Energy Meter',1,'Pinned'),
		 ('33333333-3333-3333-3333-333333333333','bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb','Energy Meter',1,'B')`,
		`INSERT INTO twin_registry
		 (tenant_id, thing_id, entity_type, entity_id, policy_id, definition, attributes, created_time, updated_time, version)
		 VALUES
		 ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa','a:device:1','DEVICE','11111111-1111-1111-1111-111111111111','p','fallback','{}',1,1,1),
		 ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa','a:device:2','DEVICE','22222222-2222-2222-2222-222222222222','p','thingsflow:device:legacy:1.0.0','{}',1,1,1),
		 ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb','b:device:1','DEVICE','33333333-3333-3333-3333-333333333333','p','fallback','{}',1,1,1)`,
		`UPDATE twin_registry SET model_id='legacy', model_version='1.0.0'
		 WHERE entity_id='22222222-2222-2222-2222-222222222222'`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("seed catalog: %v", err)
		}
	}
}

func authoredModel(modelID, version, kind string) json.RawMessage {
	return json.RawMessage(`{"modelId":"` + modelID + `","version":"` + version + `","kind":"` + kind + `","x-owner":"ops"}`)
}

func TestStoreImmutableTenantCatalogActivationAndSemanticLatest(t *testing.T) {
	db := newCatalogTestDB(t)
	setupCatalogTestSchema(t, db)
	store := NewStore(db)
	ctx := context.Background()

	first, err := store.Create(ctx, catalogTenantA, authoredModel("Energy Meter", "1.0.9", "DEVICE"))
	if err != nil {
		t.Fatalf("create 1.0.9: %v", err)
	}
	if first.Model.ModelID != "energy_meter" || !json.Valid(first.Schema) {
		t.Fatalf("normalized first=%+v schema=%s", first.Model, first.Schema)
	}
	if _, err := store.Create(ctx, catalogTenantA, authoredModel("Energy Meter", "1.0.10", "DEVICE")); err != nil {
		t.Fatalf("create 1.0.10: %v", err)
	}
	if _, err := store.Create(ctx, catalogTenantB, authoredModel("Energy Meter", "1.0.9", "DEVICE")); err != nil {
		t.Fatalf("same identity in tenant B: %v", err)
	}
	if _, err := store.Create(ctx, catalogTenantA, authoredModel("Energy Meter", "1.0.9", "DEVICE")); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate err=%v, want conflict", err)
	}

	var modelID, version, definition string
	if err := db.QueryRow(`SELECT model_id, model_version, definition FROM twin_registry WHERE tenant_id=$1 AND entity_id=$2`, catalogTenantA, catalogDeviceA).Scan(&modelID, &version, &definition); err != nil {
		t.Fatalf("read activated registry: %v", err)
	}
	if modelID != "energy_meter" || version != "1.0.9" || definition != "thingsflow:device:energy_meter:1.0.9" {
		t.Fatalf("activation=(%s,%s,%s)", modelID, version, definition)
	}
	if err := db.QueryRow(`SELECT model_id, model_version FROM twin_registry WHERE entity_id=$1`, catalogDevicePinned).Scan(&modelID, &version); err != nil || modelID != "legacy" || version != "1.0.0" {
		t.Fatalf("existing pin advanced: (%s,%s) err=%v", modelID, version, err)
	}
	var tenantBPin sql.NullString
	if err := db.QueryRow(`SELECT model_id FROM twin_registry WHERE tenant_id=$1 AND entity_id=$2`, catalogTenantB, catalogDeviceB).Scan(&tenantBPin); err != nil || tenantBPin.String != "energy_meter" {
		t.Fatalf("tenant B activation=%v err=%v", tenantBPin, err)
	}

	latest, err := store.List(ctx, catalogTenantA, ListOptions{PageSize: 100, Latest: true})
	if err != nil {
		t.Fatalf("list latest: %v", err)
	}
	if latest.TotalElements != 1 || len(latest.Data) != 1 || latest.Data[0].Model.Version != "1.0.10" {
		t.Fatalf("semantic latest=%+v", latest)
	}
	all, err := store.List(ctx, catalogTenantA, ListOptions{PageSize: 1, Latest: false})
	if err != nil || all.TotalElements != 2 || all.TotalPages != 2 || !all.HasNext {
		t.Fatalf("all page=%+v err=%v", all, err)
	}

	got, err := store.Get(ctx, catalogTenantA, "energy_meter", "1.0.9")
	if err != nil || string(got.Model.Extra["x-owner"]) != `"ops"` {
		t.Fatalf("authored metadata round-trip record=%+v err=%v", got, err)
	}
	if _, err := store.Get(ctx, catalogTenantA, "energy_meter", "2147483648.0.0"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("overflow lookup err=%v", err)
	}

	deprecated, err := store.Deprecate(ctx, catalogTenantA, "energy_meter", "1.0.10")
	if err != nil || !deprecated.Deprecated {
		t.Fatalf("deprecate=%+v err=%v", deprecated, err)
	}
	again, err := store.Deprecate(ctx, catalogTenantA, "energy_meter", "1.0.10")
	if err != nil || again.UpdatedTime != deprecated.UpdatedTime {
		t.Fatalf("idempotent deprecate=%+v first=%+v err=%v", again, deprecated, err)
	}
	activeLatest, err := store.List(ctx, catalogTenantA, ListOptions{PageSize: 100, Latest: true})
	if err != nil || activeLatest.Data[0].Model.Version != "1.0.9" {
		t.Fatalf("active latest=%+v err=%v", activeLatest, err)
	}
	includingDeprecated, err := store.List(ctx, catalogTenantA, ListOptions{PageSize: 100, Latest: true, IncludeDeprecated: true})
	if err != nil || includingDeprecated.Data[0].Model.Version != "1.0.10" {
		t.Fatalf("latest including deprecated=%+v err=%v", includingDeprecated, err)
	}
}

func TestStoreRepointTenantKindDeprecatedAndIdempotence(t *testing.T) {
	db := newCatalogTestDB(t)
	setupCatalogTestSchema(t, db)
	store := NewStore(db)
	ctx := context.Background()

	for _, model := range []json.RawMessage{
		authoredModel("Energy Meter", "2.0.0", "DEVICE"),
		authoredModel("Building", "1.0.0", "ASSET"),
	} {
		if _, err := store.Create(ctx, catalogTenantA, model); err != nil {
			t.Fatalf("create repoint model: %v", err)
		}
	}
	if _, err := store.Create(ctx, catalogTenantB, authoredModel("Tenant B Only", "1.0.0", "DEVICE")); err != nil {
		t.Fatalf("create tenant B target: %v", err)
	}

	pin, err := store.Repoint(ctx, catalogTenantA, "DEVICE", catalogDeviceA, "energy_meter", "2.0.0", false)
	if err != nil || pin.Definition != "thingsflow:device:energy_meter:2.0.0" {
		t.Fatalf("repoint=%+v err=%v", pin, err)
	}
	var registryVersion int64
	if err := db.QueryRow(`SELECT version FROM twin_registry WHERE tenant_id=$1 AND entity_id=$2`, catalogTenantA, catalogDeviceA).Scan(&registryVersion); err != nil {
		t.Fatalf("registry version: %v", err)
	}
	if _, err := store.Repoint(ctx, catalogTenantA, "DEVICE", catalogDeviceA, "energy_meter", "2.0.0", false); err != nil {
		t.Fatalf("idempotent repoint: %v", err)
	}
	var after int64
	_ = db.QueryRow(`SELECT version FROM twin_registry WHERE tenant_id=$1 AND entity_id=$2`, catalogTenantA, catalogDeviceA).Scan(&after)
	if after != registryVersion {
		t.Fatalf("idempotent repoint advanced registry version %d -> %d", registryVersion, after)
	}
	if _, err := store.Repoint(ctx, catalogTenantB, "DEVICE", catalogDeviceA, "energy_meter", "2.0.0", false); !errors.Is(err, ErrForbidden) {
		t.Fatalf("cross-tenant err=%v", err)
	}
	if _, err := store.Repoint(ctx, "", "DEVICE", catalogDeviceA, "energy_meter", "2.0.0", true); err != nil {
		t.Fatalf("SYS_ADMIN actual-tenant repoint: %v", err)
	}
	if _, err := store.Repoint(ctx, catalogTenantA, "DEVICE", catalogDeviceA, "tenant_b_only", "1.0.0", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant model joined err=%v", err)
	}
	if _, err := store.Repoint(ctx, catalogTenantA, "DEVICE", catalogDeviceA, "building", "1.0.0", false); !errors.Is(err, ErrKindMismatch) {
		t.Fatalf("kind mismatch err=%v", err)
	}
	if _, err := store.Deprecate(ctx, catalogTenantA, "energy_meter", "2.0.0"); err != nil {
		t.Fatalf("deprecate target: %v", err)
	}
	if _, err := store.Repoint(ctx, catalogTenantA, "DEVICE", catalogDeviceA, "energy_meter", "2.0.0", false); !errors.Is(err, ErrDeprecated) {
		t.Fatalf("deprecated repoint err=%v", err)
	}
}
