package topology

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	dbpkg "flow-core/internal/db"
)

const (
	tenantA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	tenantB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	assetA  = "11111111-1111-1111-1111-111111111111"
	assetB  = "22222222-2222-2222-2222-222222222222"
	deviceA = "33333333-3333-3333-3333-333333333333"
	deviceB = "44444444-4444-4444-4444-444444444444"
	deviceC = "55555555-5555-5555-5555-555555555555"
	deviceD = "66666666-6666-6666-6666-666666666666"
)

func newTestDB(t *testing.T) *sql.DB {
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
		time.Sleep(100 * time.Millisecond)
		dbpkg.SetPoolForTest(t, nil)
		pool.Close()
	})
	return pool
}

func setupTopologyTables(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`DROP TABLE IF EXISTS topology_edge CASCADE`,
		`DROP TABLE IF EXISTS topology_relation_type CASCADE`,
		`DROP TABLE IF EXISTS relation CASCADE`,
		`DROP TABLE IF EXISTS twin_registry CASCADE`,
		`DROP TABLE IF EXISTS twin_model CASCADE`,
		`DROP TABLE IF EXISTS asset CASCADE`,
		`DROP TABLE IF EXISTS device CASCADE`,
		`DROP TABLE IF EXISTS customer CASCADE`,
		`DROP TABLE IF EXISTS entity_view CASCADE`,
		`DROP TABLE IF EXISTS dashboard CASCADE`,
		`DROP TABLE IF EXISTS device_profile CASCADE`,
		`DROP TABLE IF EXISTS asset_profile CASCADE`,
		`CREATE TABLE asset (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			customer_id uuid, name text, type text, label text)`,
		`CREATE TABLE device (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
			customer_id uuid, name text, type text, label text)`,
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
		`CREATE TABLE topology_relation_type (
			name text PRIMARY KEY, description text, allowed_from_types text[],
			allowed_to_types text[], is_directed boolean not null default true,
			created_time bigint not null)`,
		`CREATE TABLE twin_model (
			tenant_id uuid NOT NULL, model_id varchar(255) NOT NULL,
			version varchar(64) NOT NULL, kind varchar(64) NOT NULL,
			definition jsonb NOT NULL DEFAULT '{}'::jsonb,
			schema jsonb NOT NULL DEFAULT '{}'::jsonb,
			deprecated boolean NOT NULL DEFAULT false,
			created_time bigint NOT NULL, updated_time bigint NOT NULL,
			PRIMARY KEY (tenant_id, model_id, version))`,
		`CREATE TABLE twin_registry (
			tenant_id uuid NOT NULL, thing_id varchar(512) NOT NULL,
			entity_type varchar(255) NOT NULL, entity_id uuid NOT NULL,
			policy_id varchar(512) NOT NULL, definition varchar(512) NOT NULL,
			attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
			model_id varchar(255), model_version varchar(64),
			created_time bigint NOT NULL, updated_time bigint NOT NULL,
			version bigint NOT NULL DEFAULT 1,
			UNIQUE (tenant_id, thing_id), UNIQUE (tenant_id, entity_type, entity_id))`,
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
		`CREATE UNIQUE INDEX topology_edge_bidirectional_unq
			ON topology_edge (
				tenant_id, relation_type_group, relation_type,
				LEAST(from_type||':'||from_id::text,to_type||':'||to_id::text),
				GREATEST(from_type||':'||from_id::text,to_type||':'||to_id::text))
			WHERE direction='BIDIRECTIONAL'`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
	if err := SeedRelationTypes(db); err != nil {
		t.Fatalf("seed relation types: %v", err)
	}
	now := time.Now().UnixMilli()
	seeds := []struct {
		table, id, tenant, name, typ string
	}{
		{"asset", assetA, tenantA, "Building A", "building"},
		{"asset", assetB, tenantB, "Building B", "building"},
		{"device", deviceA, tenantA, "Meter A", "meter"},
		{"device", deviceB, tenantB, "Meter B", "meter"},
		{"device", deviceC, tenantA, "Main Total", "main_total"},
		{"device", deviceD, tenantA, "Meter D", "meter"},
	}
	for _, s := range seeds {
		if _, err := db.Exec(`INSERT INTO `+s.table+` (id, created_time, tenant_id, name, type)
			VALUES ($1, $2, $3, $4, $5)`, s.id, now, s.tenant, s.name, s.typ); err != nil {
			t.Fatalf("seed %s: %v", s.table, err)
		}
	}
}

func pinTopologyModel(t *testing.T, db *sql.DB, tenant string, ref EntityRef, modelID string, relationships map[string]map[string]interface{}) {
	t.Helper()
	schema := map[string]interface{}{
		"modelId": modelID, "version": "1.0.0", "kind": ref.Type,
		"unknownKeys": "allow", "enforcementMode": "warn",
		"attributes": map[string]interface{}{}, "features": map[string]interface{}{},
		"relationships": relationships,
	}
	raw, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("marshal model schema: %v", err)
	}
	now := time.Now().UnixMilli()
	if _, err := db.Exec(`INSERT INTO twin_model
		(tenant_id, model_id, version, kind, definition, schema, created_time, updated_time)
		VALUES ($1,$2,'1.0.0',$3,$4::jsonb,$4::jsonb,$5,$5)
		ON CONFLICT (tenant_id, model_id, version) DO UPDATE SET schema=EXCLUDED.schema`,
		tenant, modelID, ref.Type, string(raw), now); err != nil {
		t.Fatalf("seed model %s: %v", modelID, err)
	}
	if _, err := db.Exec(`INSERT INTO twin_registry
		(tenant_id, thing_id, entity_type, entity_id, policy_id, definition, attributes,
		 model_id, model_version, created_time, updated_time)
		VALUES ($1::uuid,$1::uuid::text||':'||lower($2::text)||':'||$3::uuid::text,$2::varchar,$3::uuid,'p',
		 'thingsflow:'||lower($2::text)||':'||$4::text||':1.0.0','{}',$4::varchar,'1.0.0',$5,$5)
		ON CONFLICT (tenant_id, entity_type, entity_id) DO UPDATE
		SET model_id=EXCLUDED.model_id, model_version=EXCLUDED.model_version,
		    definition=EXCLUDED.definition`, tenant, ref.Type, ref.ID, modelID, now); err != nil {
		t.Fatalf("pin %s/%s to %s: %v", ref.Type, ref.ID, modelID, err)
	}
}

func relationship(target []string, targetTypes []string, bidirectional bool) map[string]interface{} {
	return map[string]interface{}{
		"target": target, "targetEntityTypes": targetTypes,
		"maxCardinality": 100, "bidirectional": bidirectional,
	}
}

func TestSaveEdgeWritesTopologyAndLegacyRelation(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)

	metadata := json.RawMessage(`{"zone":"north"}`)
	edge := Edge{
		TenantID:          tenantA,
		From:              EntityRef{Type: "ASSET", ID: assetA},
		To:                EntityRef{Type: "DEVICE", ID: deviceA},
		RelationType:      "Contains",
		RelationTypeGroup: "COMMON",
		Metadata:          metadata,
	}
	if err := SaveEdge(db, edge); err != nil {
		t.Fatalf("SaveEdge: %v", err)
	}

	var topologyCount, legacyCount int
	if err := db.QueryRow(`SELECT count(*) FROM topology_edge WHERE tenant_id = $1 AND metadata->>'zone' = 'north'`, tenantA).Scan(&topologyCount); err != nil {
		t.Fatalf("count topology: %v", err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM relation WHERE from_id = $1 AND to_id = $2 AND relation_type = 'Contains'`, assetA, deviceA).Scan(&legacyCount); err != nil {
		t.Fatalf("count relation: %v", err)
	}
	if topologyCount != 1 || legacyCount != 1 {
		t.Fatalf("rows topology=%d legacy=%d, want 1/1", topologyCount, legacyCount)
	}
}

func TestSaveEdgeRejectsCrossTenantRelation(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)

	err := SaveEdge(db, Edge{
		TenantID:          tenantA,
		From:              EntityRef{Type: "ASSET", ID: assetA},
		To:                EntityRef{Type: "DEVICE", ID: deviceB},
		RelationType:      "Contains",
		RelationTypeGroup: "COMMON",
	})
	if err == nil {
		t.Fatal("SaveEdge cross-tenant returned nil, want error")
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM topology_edge`).Scan(&n)
	if n != 0 {
		t.Fatalf("topology_edge rows = %d, want 0", n)
	}
}

func TestListEdgesReturnsClassicRelationShape(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)
	if err := SaveEdge(db, Edge{
		TenantID:          tenantA,
		From:              EntityRef{Type: "ASSET", ID: assetA},
		To:                EntityRef{Type: "DEVICE", ID: deviceA},
		RelationType:      "Contains",
		RelationTypeGroup: "COMMON",
		Metadata:          json.RawMessage(`{"source":"test"}`),
	}); err != nil {
		t.Fatalf("SaveEdge: %v", err)
	}

	edges, err := ListEdges(db, EdgeFilter{
		TenantID: tenantA,
		From:     EntityRef{Type: "ASSET", ID: assetA},
	})
	if err != nil {
		t.Fatalf("ListEdges: %v", err)
	}
	if len(edges) != 1 {
		t.Fatalf("edges len = %d, want 1", len(edges))
	}
	if edges[0].To.ID != deviceA || edges[0].RelationType != "Contains" {
		t.Fatalf("edge = %+v", edges[0])
	}
}

func TestBackfillFromLegacyRelationsIsIdempotent(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)
	if _, err := db.Exec(`INSERT INTO relation
		(from_id, from_type, to_id, to_type, relation_type_group, relation_type, additional_info, version)
		VALUES ($1, 'ASSET', $2, 'DEVICE', 'COMMON', 'Contains', '{"legacy":true}', 1)`,
		assetA, deviceA); err != nil {
		t.Fatalf("seed legacy relation: %v", err)
	}

	first, err := BackfillFromLegacyRelations(db)
	if err != nil {
		t.Fatalf("first backfill: %v", err)
	}
	second, err := BackfillFromLegacyRelations(db)
	if err != nil {
		t.Fatalf("second backfill: %v", err)
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM topology_edge WHERE tenant_id = $1`, tenantA).Scan(&n)
	if first != 1 || second != 0 || n != 1 {
		t.Fatalf("backfill first=%d second=%d rows=%d, want 1/0/1", first, second, n)
	}
}

func TestNeighborsUsesTopologyAndFallsBackToLegacyRelation(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)
	if err := SaveEdge(db, Edge{
		TenantID:          tenantA,
		From:              EntityRef{Type: "ASSET", ID: assetA},
		To:                EntityRef{Type: "DEVICE", ID: deviceA},
		RelationType:      "Contains",
		RelationTypeGroup: "COMMON",
	}); err != nil {
		t.Fatalf("SaveEdge: %v", err)
	}

	neighbors, err := Neighbors(db, EntityRef{Type: "ASSET", ID: assetA}, "FROM", []string{"Contains"})
	if err != nil {
		t.Fatalf("Neighbors topology: %v", err)
	}
	if len(neighbors) != 1 || neighbors[0].ID != deviceA || neighbors[0].Type != "DEVICE" {
		t.Fatalf("topology neighbors = %+v", neighbors)
	}

	if _, err := db.Exec(`DELETE FROM topology_edge`); err != nil {
		t.Fatalf("clear topology: %v", err)
	}
	neighbors, err = Neighbors(db, EntityRef{Type: "ASSET", ID: assetA}, "FROM", []string{"Contains"})
	if err != nil {
		t.Fatalf("Neighbors legacy fallback: %v", err)
	}
	if len(neighbors) != 1 || neighbors[0].ID != deviceA || neighbors[0].Type != "DEVICE" {
		t.Fatalf("legacy neighbors = %+v", neighbors)
	}
}

func TestCheckConsistencyReportsHealthySyncedTopology(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)
	if err := SaveEdge(db, Edge{
		TenantID:          tenantA,
		From:              EntityRef{Type: "ASSET", ID: assetA},
		To:                EntityRef{Type: "DEVICE", ID: deviceA},
		RelationType:      "Contains",
		RelationTypeGroup: "COMMON",
	}); err != nil {
		t.Fatalf("SaveEdge: %v", err)
	}

	report, err := CheckConsistency(db)
	if err != nil {
		t.Fatalf("CheckConsistency: %v", err)
	}
	if report.Status != "OK" {
		t.Fatalf("status = %s, want OK; report=%+v", report.Status, report)
	}
	if report.TopologyEdges != 1 || report.LegacyRelations != 1 || report.GovernedLegacyRelations != 1 {
		t.Fatalf("unexpected counts: %+v", report)
	}
	if report.LegacyMissingTopology != 0 || report.TopologyMissingLegacy != 0 || report.CrossTenantLegacyRelations != 0 {
		t.Fatalf("unexpected drift: %+v", report)
	}
}

func TestCheckConsistencyReportsDriftAndCrossTenantLegacy(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)
	if _, err := db.Exec(`INSERT INTO relation
		(from_id, from_type, to_id, to_type, relation_type_group, relation_type, additional_info, version)
		VALUES
		($1, 'ASSET', $2, 'DEVICE', 'COMMON', 'Contains', '{}', 1),
		($1, 'ASSET', $3, 'DEVICE', 'COMMON', 'Contains', '{}', 1)`,
		assetA, deviceA, deviceB); err != nil {
		t.Fatalf("seed legacy relation: %v", err)
	}

	report, err := CheckConsistency(db)
	if err != nil {
		t.Fatalf("CheckConsistency: %v", err)
	}
	if report.Status != "WARN" {
		t.Fatalf("status = %s, want WARN; report=%+v", report.Status, report)
	}
	if report.GovernedLegacyRelations != 1 || report.LegacyMissingTopology != 1 {
		t.Fatalf("governed/missing = %d/%d, want 1/1; report=%+v",
			report.GovernedLegacyRelations, report.LegacyMissingTopology, report)
	}
	if report.CrossTenantLegacyRelations != 1 {
		t.Fatalf("cross tenant = %d, want 1; report=%+v", report.CrossTenantLegacyRelations, report)
	}
}

func TestRepairBackfillDryRunDoesNotWrite(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)
	if _, err := db.Exec(`INSERT INTO relation
		(from_id, from_type, to_id, to_type, relation_type_group, relation_type, additional_info, version)
		VALUES ($1, 'ASSET', $2, 'DEVICE', 'COMMON', 'Contains', '{"dryRun":true}', 1)`,
		assetA, deviceA); err != nil {
		t.Fatalf("seed legacy relation: %v", err)
	}

	result, err := RepairBackfill(db, true)
	if err != nil {
		t.Fatalf("RepairBackfill dry-run: %v", err)
	}
	if !result.DryRun || result.WouldInsert != 1 || result.Inserted != 0 {
		t.Fatalf("dry-run result = %+v, want dryRun=true wouldInsert=1 inserted=0", result)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM topology_edge`).Scan(&n); err != nil {
		t.Fatalf("count topology: %v", err)
	}
	if n != 0 {
		t.Fatalf("topology rows after dry-run = %d, want 0", n)
	}
}

func TestRepairBackfillApplyIsIdempotentAndSkipsCrossTenant(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)
	if _, err := db.Exec(`INSERT INTO relation
		(from_id, from_type, to_id, to_type, relation_type_group, relation_type, additional_info, version)
		VALUES
		($1, 'ASSET', $2, 'DEVICE', 'COMMON', 'Contains', '{"apply":true}', 1),
		($1, 'ASSET', $3, 'DEVICE', 'COMMON', 'Contains', '{"crossTenant":true}', 1)`,
		assetA, deviceA, deviceB); err != nil {
		t.Fatalf("seed legacy relation: %v", err)
	}

	first, err := RepairBackfill(db, false)
	if err != nil {
		t.Fatalf("first RepairBackfill apply: %v", err)
	}
	second, err := RepairBackfill(db, false)
	if err != nil {
		t.Fatalf("second RepairBackfill apply: %v", err)
	}
	if first.DryRun || first.WouldInsert != 1 || first.Inserted != 1 {
		t.Fatalf("first result = %+v, want dryRun=false wouldInsert=1 inserted=1", first)
	}
	if second.WouldInsert != 0 || second.Inserted != 0 {
		t.Fatalf("second result = %+v, want wouldInsert=0 inserted=0", second)
	}
	report, err := CheckConsistency(db)
	if err != nil {
		t.Fatalf("CheckConsistency: %v", err)
	}
	if report.LegacyMissingTopology != 0 || report.CrossTenantLegacyRelations != 1 {
		t.Fatalf("report after repair = %+v, want no governed missing and one cross-tenant legacy", report)
	}
}

func TestSaveEdgeModelNarrowingTypeFirstWarnRejectAndAbsentPins(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)
	pinTopologyModel(t, db, tenantA, EntityRef{Type: "ASSET", ID: assetA}, "building", map[string]map[string]interface{}{
		"Contains": relationship([]string{"meter"}, []string{"DEVICE"}, false),
	})
	pinTopologyModel(t, db, tenantA, EntityRef{Type: "DEVICE", ID: deviceA}, "meter", nil)

	// Global relation validation is the first semantic gate. A deliberately
	// undecodable model schema must not mask an unknown global relation type.
	if _, err := db.Exec(`UPDATE twin_model SET schema='[]'::jsonb WHERE tenant_id=$1 AND model_id='building'`, tenantA); err != nil {
		t.Fatalf("corrupt source schema: %v", err)
	}
	err := SaveEdge(db, Edge{TenantID: tenantA, From: EntityRef{Type: "ASSET", ID: assetA}, To: EntityRef{Type: "DEVICE", ID: deviceA}, RelationType: "NotGlobal"})
	if !errors.Is(err, ErrInvalidRelationType) {
		t.Fatalf("validation order err=%v, want ErrInvalidRelationType", err)
	}

	// Restore a valid source schema. Invalid/empty enforcement values are warn.
	pinTopologyModel(t, db, tenantA, EntityRef{Type: "ASSET", ID: assetA}, "building", map[string]map[string]interface{}{
		"Contains": relationship([]string{"meter"}, []string{"DEVICE"}, false),
	})
	t.Setenv("TWIN_MODEL_RELATION_ENFORCE", "invalid-value")
	var logs bytes.Buffer
	previousWriter := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previousWriter) })
	metricBefore := modelRelationViolationMetric.Load()
	warnEdge := Edge{TenantID: tenantA, From: EntityRef{Type: "ASSET", ID: assetA}, To: EntityRef{Type: "DEVICE", ID: deviceA}, RelationType: "Manages"}
	if err := SaveEdge(db, warnEdge); err != nil {
		t.Fatalf("warn-mode narrowing must continue: %v", err)
	}
	if got := modelRelationViolationMetric.Load() - metricBefore; got != 1 {
		t.Fatalf("warn metric delta=%d, want exactly 1", got)
	}
	for _, field := range []string{
		"tenant_id=" + tenantA, "relation_type=Manages", "from_type=ASSET", "from_id=" + assetA,
		"to_type=DEVICE", "to_id=" + deviceA, "from_model_id=building", "from_model_version=1.0.0",
		"to_model_id=meter", "to_model_version=1.0.0", "violation_count=1", "enforcement_mode=warn",
	} {
		if !strings.Contains(logs.String(), field) {
			t.Fatalf("warn log missing %q: %s", field, logs.String())
		}
	}

	t.Setenv("TWIN_MODEL_RELATION_ENFORCE", "reject")
	rejectEdge := warnEdge
	rejectEdge.RelationType = "Feeds"
	if err := SaveEdge(db, rejectEdge); !errors.Is(err, ErrRelationNotAllowedByModel) {
		t.Fatalf("reject err=%v, want ErrRelationNotAllowedByModel", err)
	}

	// Both absent and one absent endpoint pins preserve legacy permissiveness.
	if _, err := db.Exec(`DELETE FROM twin_registry`); err != nil {
		t.Fatalf("clear pins: %v", err)
	}
	if err := SaveEdge(db, Edge{TenantID: tenantA, From: EntityRef{Type: "ASSET", ID: assetA}, To: EntityRef{Type: "DEVICE", ID: deviceA}, RelationType: "Contains"}); err != nil {
		t.Fatalf("both absent pins must pass: %v", err)
	}
	pinTopologyModel(t, db, tenantA, EntityRef{Type: "ASSET", ID: assetA}, "building", nil)
	if err := SaveEdge(db, Edge{TenantID: tenantA, From: EntityRef{Type: "ASSET", ID: assetA}, To: EntityRef{Type: "DEVICE", ID: deviceD}, RelationType: "Contains"}); err != nil {
		t.Fatalf("one absent endpoint pin must pass: %v", err)
	}
}

func TestSaveEdgeFailsOnPinnedModelDatabaseAndSchemaErrors(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)
	pinTopologyModel(t, db, tenantA, EntityRef{Type: "ASSET", ID: assetA}, "building", nil)
	pinTopologyModel(t, db, tenantA, EntityRef{Type: "DEVICE", ID: deviceA}, "meter", nil)
	if _, err := db.Exec(`UPDATE twin_model SET schema='[]'::jsonb WHERE tenant_id=$1 AND model_id='building'`, tenantA); err != nil {
		t.Fatalf("corrupt schema: %v", err)
	}
	err := SaveEdge(db, Edge{TenantID: tenantA, From: EntityRef{Type: "ASSET", ID: assetA}, To: EntityRef{Type: "DEVICE", ID: deviceA}, RelationType: "Contains"})
	if err == nil || errors.Is(err, ErrRelationNotAllowedByModel) {
		t.Fatalf("schema decode err=%v, want hard persistence/decode failure", err)
	}
	if _, err := db.Exec(`DROP TABLE twin_model`); err != nil {
		t.Fatalf("drop twin_model: %v", err)
	}
	err = SaveEdge(db, Edge{TenantID: tenantA, From: EntityRef{Type: "ASSET", ID: assetA}, To: EntityRef{Type: "DEVICE", ID: deviceA}, RelationType: "Contains"})
	if err == nil {
		t.Fatal("catalog database failure must fail SaveEdge")
	}
}

func TestBidirectionalRequiresGlobalAndMutualModelPermission(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)
	t.Setenv("TWIN_MODEL_RELATION_ENFORCE", "warn")

	// Contains is globally asymmetric, so the reverse global type gate rejects
	// before model narrowing regardless of warn mode.
	err := SaveEdge(db, Edge{TenantID: tenantA, From: EntityRef{Type: "ASSET", ID: assetA}, To: EntityRef{Type: "DEVICE", ID: deviceA}, RelationType: "Contains", Direction: "BIDIRECTIONAL"})
	if !errors.Is(err, ErrInvalidRelationType) {
		t.Fatalf("asymmetric global relation err=%v, want ErrInvalidRelationType", err)
	}

	pinTopologyModel(t, db, tenantA, EntityRef{Type: "DEVICE", ID: deviceA}, "meter", map[string]map[string]interface{}{
		"ConnectedTo": relationship([]string{"main_total"}, []string{"DEVICE"}, true),
	})
	pinTopologyModel(t, db, tenantA, EntityRef{Type: "DEVICE", ID: deviceC}, "main_total", nil)
	err = SaveEdge(db, Edge{TenantID: tenantA, From: EntityRef{Type: "DEVICE", ID: deviceA}, To: EntityRef{Type: "DEVICE", ID: deviceC}, RelationType: "ConnectedTo", Direction: "BIDIRECTIONAL"})
	if !errors.Is(err, ErrRelationNotAllowedByModel) {
		t.Fatalf("one-sided declaration err=%v, want typed rejection even in warn mode", err)
	}

	pinTopologyModel(t, db, tenantA, EntityRef{Type: "DEVICE", ID: deviceC}, "main_total", map[string]map[string]interface{}{
		"ConnectedTo": relationship([]string{"wrong_model"}, []string{"DEVICE"}, true),
	})
	err = SaveEdge(db, Edge{TenantID: tenantA, From: EntityRef{Type: "DEVICE", ID: deviceA}, To: EntityRef{Type: "DEVICE", ID: deviceC}, RelationType: "ConnectedTo", Direction: "BIDIRECTIONAL"})
	if !errors.Is(err, ErrRelationNotAllowedByModel) {
		t.Fatalf("target-model mismatch err=%v, want typed rejection", err)
	}
}

func TestBidirectionalReverseSaveCollapsesPreservesOrientationMirrorsAndDelete(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)
	pinConnectedModels(t, db)

	forward := Edge{TenantID: tenantA, From: EntityRef{Type: "DEVICE", ID: deviceA}, To: EntityRef{Type: "DEVICE", ID: deviceC}, RelationType: "ConnectedTo", Direction: "BIDIRECTIONAL", Metadata: json.RawMessage(`{"orientation":"first"}`)}
	if err := SaveEdge(db, forward); err != nil {
		t.Fatalf("save forward bidirectional: %v", err)
	}
	reverse := Edge{TenantID: tenantA, From: forward.To, To: forward.From, RelationType: forward.RelationType, Direction: "BIDIRECTIONAL", Metadata: json.RawMessage(`{"orientation":"reverse-update"}`)}
	if err := SaveEdge(db, reverse); err != nil {
		t.Fatalf("save reverse bidirectional: %v", err)
	}

	edges, err := ListEdges(db, EdgeFilter{TenantID: tenantA, RelationType: "ConnectedTo"})
	if err != nil {
		t.Fatalf("ListEdges: %v", err)
	}
	if len(edges) != 1 || edges[0].From != forward.From || edges[0].To != forward.To || edges[0].Direction != "BIDIRECTIONAL" || edges[0].Version != 2 {
		t.Fatalf("collapsed edge=%+v, want first user orientation and version 2", edges)
	}
	var legacy int
	if err := db.QueryRow(`SELECT count(*) FROM relation WHERE relation_type='ConnectedTo'`).Scan(&legacy); err != nil || legacy != 2 {
		t.Fatalf("legacy mirrors=%d err=%v, want two orientations", legacy, err)
	}
	for _, root := range []EntityRef{forward.From, forward.To} {
		for _, direction := range []string{"FROM", "TO"} {
			neighbors, err := Neighbors(db, root, direction, []string{"ConnectedTo"})
			if err != nil || len(neighbors) != 1 || neighbors[0] == root {
				t.Fatalf("Neighbors root=%+v direction=%s got=%+v err=%v", root, direction, neighbors, err)
			}
		}
	}
	report, err := CheckConsistency(db)
	if err != nil || report.Status != "OK" || report.TopologyEdges != 1 || report.LegacyRelations != 2 || report.LegacyMissingTopology != 0 || report.TopologyMissingLegacy != 0 {
		t.Fatalf("bidirectional consistency=%+v err=%v", report, err)
	}
	if err := DeleteEdge(db, EdgeFilter{TenantID: tenantA, From: reverse.From, To: reverse.To, RelationType: "ConnectedTo"}); err != nil {
		t.Fatalf("delete by reverse orientation: %v", err)
	}
	var topologyRows int
	_ = db.QueryRow(`SELECT count(*) FROM topology_edge`).Scan(&topologyRows)
	_ = db.QueryRow(`SELECT count(*) FROM relation`).Scan(&legacy)
	if topologyRows != 0 || legacy != 0 {
		t.Fatalf("delete left topology=%d legacy=%d", topologyRows, legacy)
	}
}

func TestConcurrentReverseBidirectionalSavesCollapseAndIdentitiesStayIndependent(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)
	pinConnectedModels(t, db)
	base := Edge{TenantID: tenantA, From: EntityRef{Type: "DEVICE", ID: deviceA}, To: EntityRef{Type: "DEVICE", ID: deviceC}, RelationType: "ConnectedTo", Direction: "BIDIRECTIONAL"}

	const workers = 12
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			edge := base
			if i%2 == 1 {
				edge.From, edge.To = edge.To, edge.From
			}
			errs <- SaveEdge(db, edge)
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent SaveEdge: %v", err)
		}
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM topology_edge WHERE direction='BIDIRECTIONAL'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("concurrent logical edges=%d err=%v", count, err)
	}

	// Group, relation type, tenant, and directed orientation are distinct
	// identities even when the typed endpoint pair is the same.
	grouped := base
	grouped.RelationTypeGroup = "CUSTOM"
	if err := SaveEdge(db, grouped); err != nil {
		t.Fatalf("independent group: %v", err)
	}
	directed := base
	directed.Direction = "DIRECTED"
	if err := SaveEdge(db, directed); err != nil {
		t.Fatalf("coexisting directed forward: %v", err)
	}
	directed.From, directed.To = directed.To, directed.From
	if err := SaveEdge(db, directed); err != nil {
		t.Fatalf("coexisting directed reverse: %v", err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM topology_edge WHERE tenant_id=$1`, tenantA).Scan(&count); err != nil || count != 4 {
		t.Fatalf("independent identities count=%d err=%v, want 4", count, err)
	}
}

func TestBidirectionalLockKeyMatchesMigrationEncoding(t *testing.T) {
	edge := normalizeEdge(Edge{TenantID: tenantA, From: EntityRef{Type: "DEVICE", ID: deviceC}, To: EntityRef{Type: "ASSET", ID: assetA}, RelationType: "ConnectedTo", RelationTypeGroup: "COMMON", Direction: "BIDIRECTIONAL"})
	from := fmt.Sprintf("%d:%s%d:%s", utf8.RuneCountInString(edge.From.Type), edge.From.Type, utf8.RuneCountInString(edge.From.ID), edge.From.ID)
	to := fmt.Sprintf("%d:%s%d:%s", utf8.RuneCountInString(edge.To.Type), edge.To.Type, utf8.RuneCountInString(edge.To.ID), edge.To.ID)
	if from > to {
		from, to = to, from
	}
	want := fmt.Sprintf("%d:%s%d:%s%d:%s%d:%s%d:%s", utf8.RuneCountInString(edge.TenantID), edge.TenantID, utf8.RuneCountInString(edge.RelationTypeGroup), edge.RelationTypeGroup, utf8.RuneCountInString(edge.RelationType), edge.RelationType, utf8.RuneCountInString(from), from, utf8.RuneCountInString(to), to)
	if got := bidirectionalLockKey(edge); got != want {
		t.Fatalf("lock key mismatch:\ngot  %q\nwant %q", got, want)
	}
	edge.RelationTypeGroup = "GRÜP"
	edge.RelationType = "Conexión"
	unicodeKey := bidirectionalLockKey(edge)
	if !strings.Contains(unicodeKey, "4:GRÜP") || !strings.Contains(unicodeKey, "8:Conexión") {
		t.Fatalf("lock key used byte length instead of PostgreSQL character length: %q", unicodeKey)
	}
}

func pinConnectedModels(t *testing.T, db *sql.DB) {
	t.Helper()
	pinTopologyModel(t, db, tenantA, EntityRef{Type: "DEVICE", ID: deviceA}, "meter", map[string]map[string]interface{}{
		"ConnectedTo": relationship([]string{"main_total"}, []string{"DEVICE"}, true),
	})
	pinTopologyModel(t, db, tenantA, EntityRef{Type: "DEVICE", ID: deviceC}, "main_total", map[string]map[string]interface{}{
		"ConnectedTo": relationship([]string{"meter"}, []string{"DEVICE"}, true),
	})
}

func TestLegacyMixedTopologyFallbackHidesLegacyOnlyChild(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)
	if err := SaveEdge(db, Edge{
		TenantID: tenantA, From: EntityRef{Type: "ASSET", ID: assetA},
		To: EntityRef{Type: "DEVICE", ID: deviceA}, RelationType: "Contains",
	}); err != nil {
		t.Fatalf("seed topology child: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO relation
		(from_id,from_type,to_id,to_type,relation_type_group,relation_type,additional_info,version)
		VALUES ($1,'ASSET',$2,'DEVICE','COMMON','Contains','{}',1)`, assetA, deviceD); err != nil {
		t.Fatalf("seed legacy-only child: %v", err)
	}
	neighbors, err := Neighbors(db, EntityRef{Type: "ASSET", ID: assetA}, "FROM", []string{"Contains"})
	if err != nil {
		t.Fatalf("Neighbors: %v", err)
	}
	if len(neighbors) != 1 || neighbors[0].ID != deviceA {
		t.Fatalf("mixed fallback behavior changed: neighbors=%+v, want topology-only child", neighbors)
	}
	for _, neighbor := range neighbors {
		if neighbor.ID == deviceD {
			t.Fatalf("legacy-only child unexpectedly visible before Phase 3 UNION decision: %+v", neighbors)
		}
	}
}

func TestBidirectionalWalkVisitedSetDoesNotLoop(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)
	edges := []Edge{
		{TenantID: tenantA, From: EntityRef{Type: "DEVICE", ID: deviceA}, To: EntityRef{Type: "DEVICE", ID: deviceC}, RelationType: "ConnectedTo", Direction: "BIDIRECTIONAL"},
		{TenantID: tenantA, From: EntityRef{Type: "DEVICE", ID: deviceC}, To: EntityRef{Type: "DEVICE", ID: deviceD}, RelationType: "ConnectedTo", Direction: "BIDIRECTIONAL"},
		{TenantID: tenantA, From: EntityRef{Type: "DEVICE", ID: deviceD}, To: EntityRef{Type: "DEVICE", ID: deviceA}, RelationType: "ConnectedTo", Direction: "BIDIRECTIONAL"},
	}
	for _, edge := range edges {
		if err := SaveEdge(db, edge); err != nil {
			t.Fatalf("SaveEdge %+v: %v", edge, err)
		}
	}
	visited, err := walkNeighbors(db, edges[0].From, "FROM", 5, []string{"ConnectedTo"})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(visited) != 3 {
		t.Fatalf("visited=%v, want exactly the three-node cycle once", visited)
	}
}

func walkNeighbors(db *sql.DB, root EntityRef, direction string, depth int, relationTypes []string) (map[EntityRef]struct{}, error) {
	visited := map[EntityRef]struct{}{root: {}}
	frontier := []EntityRef{root}
	for level := 0; level < depth && len(frontier) > 0; level++ {
		next := make([]EntityRef, 0)
		for _, current := range frontier {
			neighbors, err := Neighbors(db, current, direction, relationTypes)
			if err != nil {
				return nil, err
			}
			for _, neighbor := range neighbors {
				if _, seen := visited[neighbor]; seen {
					continue
				}
				visited[neighbor] = struct{}{}
				next = append(next, neighbor)
			}
		}
		frontier = next
	}
	return visited, nil
}
