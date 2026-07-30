package topology

import (
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	dbpkg "flow-core/internal/db"
)

const (
	tenantA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	tenantB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	assetA  = "11111111-1111-1111-1111-111111111111"
	assetB  = "22222222-2222-2222-2222-222222222222"
	deviceA = "33333333-3333-3333-3333-333333333333"
	deviceB = "44444444-4444-4444-4444-444444444444"
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
		`CREATE TABLE topology_edge (
			tenant_id uuid not null, from_id uuid not null, from_type text not null,
			to_id uuid not null, to_type text not null, relation_type text not null,
			relation_type_group text not null default 'COMMON',
			direction text not null default 'DIRECTED',
			metadata jsonb not null default '{}'::jsonb,
			created_time bigint not null, updated_time bigint not null,
			version bigint not null default 1,
			PRIMARY KEY (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type))`,
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
	}
	for _, s := range seeds {
		if _, err := db.Exec(`INSERT INTO `+s.table+` (id, created_time, tenant_id, name, type)
			VALUES ($1, $2, $3, $4, $5)`, s.id, now, s.tenant, s.name, s.typ); err != nil {
			t.Fatalf("seed %s: %v", s.table, err)
		}
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
