package migrations

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

const (
	migrationTenantA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	migrationTenantB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	migrationFromID  = "11111111-1111-1111-1111-111111111111"
	migrationToID    = "22222222-2222-2222-2222-222222222222"
)

func TestTwinModelCatalog0014(t *testing.T) {
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
	schema := fmt.Sprintf("migration_0014_test_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create isolated migration schema: %v", err)
	}
	if _, err := db.Exec(`SET search_path TO ` + schema); err != nil {
		t.Fatalf("set isolated migration schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`)
		db.Close()
	})

	setup0014Prerequisites(t, db)
	up := readMigration0014(t, "migrations/0014_twin_model_catalog.up.sql")
	down := readMigration0014(t, "migrations/0014_twin_model_catalog.down.sql")

	if _, err := db.Exec(`INSERT INTO twin_model
		(tenant_id, model_id, version, kind, definition, schema, created_time, updated_time)
		VALUES ($1, 'overflow_existing', '2147483648.0.0', 'DEVICE', '{}', '{}', 1, 1)`, migrationTenantA); err != nil {
		t.Fatalf("seed pre-migration overflow: %v", err)
	}
	if _, err := db.Exec(up); err == nil {
		t.Fatal("migration accepted a pre-existing int32-overflow version")
	}
	if _, err := db.Exec(`DELETE FROM twin_model WHERE model_id='overflow_existing'`); err != nil {
		t.Fatalf("remove rejected overflow fixture: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO twin_model
		(tenant_id, model_id, version, kind, definition, schema, created_time, updated_time)
		VALUES ($1, 'boundary', '2147483647.0.0', 'DEVICE', '{}', '{}', 1, 1)`, migrationTenantA); err != nil {
		t.Fatalf("seed boundary version: %v", err)
	}
	if _, err := db.Exec(up); err != nil {
		t.Fatalf("apply up: %v", err)
	}
	// The migration is safe when its SQL is replayed by an operator.
	if _, err := db.Exec(up); err != nil {
		t.Fatalf("reapply idempotent up: %v", err)
	}

	assert0014CatalogColumnsAndBounds(t, db)
	assert0014TopologyIdentity(t, db)
	seed0014RollbackConflict(t, db)

	if _, err := db.Exec(down); err != nil {
		t.Fatalf("apply data-bearing down: %v", err)
	}
	assert0014Rollback(t, db)

	if _, err := db.Exec(up); err != nil {
		t.Fatalf("reapply up after data-bearing down: %v", err)
	}
	assertConstraintContains0014(t, db, "topology_edge", "topology_edge_pkey", "direction")
}

func readMigration0014(t *testing.T, name string) string {
	t.Helper()
	b, err := FS.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

func setup0014Prerequisites(t *testing.T, db *sql.DB) {
	t.Helper()
	statements := []string{
		`DROP TABLE IF EXISTS topology_edge CASCADE`,
		`DROP TABLE IF EXISTS twin_registry CASCADE`,
		`DROP TABLE IF EXISTS twin_model CASCADE`,
		`DROP FUNCTION IF EXISTS thingsflow_lock_bidirectional_edge() CASCADE`,
		`CREATE TABLE twin_model (
			tenant_id uuid NOT NULL, model_id varchar(255) NOT NULL,
			version varchar(64) NOT NULL DEFAULT '1.0.0', kind varchar(64) NOT NULL,
			definition jsonb NOT NULL DEFAULT '{}', schema jsonb NOT NULL DEFAULT '{}',
			created_time bigint NOT NULL, updated_time bigint NOT NULL,
			CONSTRAINT twin_model_pkey PRIMARY KEY (tenant_id, model_id, version))`,
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
			relation_type_group varchar(255) NOT NULL DEFAULT 'COMMON',
			relation_type varchar(255) NOT NULL, direction varchar(32) NOT NULL DEFAULT 'DIRECTED',
			metadata jsonb NOT NULL DEFAULT '{}', created_time bigint NOT NULL,
			updated_time bigint NOT NULL, version bigint NOT NULL DEFAULT 1,
			CONSTRAINT topology_edge_pkey PRIMARY KEY
			(tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type),
			CONSTRAINT topology_edge_direction_chk CHECK (direction IN ('DIRECTED')))`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("setup 0014 prerequisite: %v\n%s", err, statement)
		}
	}
}

func assert0014CatalogColumnsAndBounds(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, column := range []struct{ table, name string }{
		{"twin_model", "deprecated"},
		{"twin_registry", "model_id"},
		{"twin_registry", "model_version"},
	} {
		var exists bool
		if err := db.QueryRow(`SELECT EXISTS (
			SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema()
			AND table_name=$1 AND column_name=$2)`, column.table, column.name).Scan(&exists); err != nil || !exists {
			t.Fatalf("column %s.%s exists=%v err=%v", column.table, column.name, exists, err)
		}
	}
	for _, version := range []string{"01.0.0", "2147483648.0.0"} {
		if _, err := db.Exec(`INSERT INTO twin_model
			(tenant_id, model_id, version, kind, definition, schema, created_time, updated_time)
			VALUES ($1, $2, $3, 'DEVICE', '{}', '{}', 1, 1)`, migrationTenantA, "invalid_"+strings.ReplaceAll(version, ".", "_"), version); err == nil {
			t.Fatalf("invalid version %q passed migration constraint", version)
		}
	}
}

func assert0014TopologyIdentity(t *testing.T, db *sql.DB) {
	t.Helper()
	insert := `INSERT INTO topology_edge
		(tenant_id, from_id, from_type, to_id, to_type, relation_type_group,
		 relation_type, direction, metadata, created_time, updated_time, version)
		VALUES ($1,$2,'ASSET',$3,'DEVICE',$4,$5,$6,$7,10,20,1)`
	cases := []struct {
		tenant, group, relation, direction, metadata string
	}{
		{migrationTenantA, "COMMON", "Contains", "DIRECTED", `{"case":"directed"}`},
		{migrationTenantA, "COMMON", "Contains", "BIDIRECTIONAL", `{"case":"bidirectional"}`},
		{migrationTenantB, "COMMON", "Contains", "BIDIRECTIONAL", `{"case":"tenant"}`},
		{migrationTenantA, "ALARM", "Contains", "BIDIRECTIONAL", `{"case":"group"}`},
		{migrationTenantA, "COMMON", "Feeds", "BIDIRECTIONAL", `{"case":"type"}`},
	}
	for _, tc := range cases {
		if _, err := db.Exec(insert, tc.tenant, migrationFromID, migrationToID, tc.group, tc.relation, tc.direction, tc.metadata); err != nil {
			t.Fatalf("insert identity case %+v: %v", tc, err)
		}
	}
	if _, err := db.Exec(`INSERT INTO topology_edge
		(tenant_id, from_id, from_type, to_id, to_type, relation_type_group,
		 relation_type, direction, metadata, created_time, updated_time, version)
		VALUES ($1,$2,'DEVICE',$3,'ASSET','COMMON','Contains','BIDIRECTIONAL','{}',10,20,1)`,
		migrationTenantA, migrationToID, migrationFromID); err == nil {
		t.Fatal("inverse BIDIRECTIONAL duplicate passed unordered unique identity")
	}

	var functionDefinition string
	if err := db.QueryRow(`SELECT pg_get_functiondef('thingsflow_lock_bidirectional_edge()'::regprocedure)`).Scan(&functionDefinition); err != nil {
		t.Fatalf("read advisory-lock function: %v", err)
	}
	for _, fragment := range []string{"pg_advisory_xact_lock", "hashtextextended", "tenant_id", "relation_type_group", "relation_type", "smaller_endpoint", "larger_endpoint"} {
		if !strings.Contains(strings.ToLower(functionDefinition), strings.ToLower(fragment)) {
			t.Fatalf("advisory-lock function lacks %q: %s", fragment, functionDefinition)
		}
	}
}

func seed0014RollbackConflict(t *testing.T, db *sql.DB) {
	t.Helper()
	// Isolate the exact winner assertions from the identity cases above.
	if _, err := db.Exec(`DELETE FROM topology_edge`); err != nil {
		t.Fatalf("clear identity rows: %v", err)
	}
	statements := []string{
		`INSERT INTO topology_edge VALUES
		 ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa','11111111-1111-1111-1111-111111111111','ASSET',
		  '22222222-2222-2222-2222-222222222222','DEVICE','COMMON','Contains','BIDIRECTIONAL',
		  '{"winner":"bidir-new"}',100,300,5)`,
		`INSERT INTO topology_edge VALUES
		 ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa','11111111-1111-1111-1111-111111111111','ASSET',
		  '22222222-2222-2222-2222-222222222222','DEVICE','COMMON','Contains','DIRECTED',
		  '{"winner":"direct-old"}',50,200,7)`,
		`INSERT INTO topology_edge VALUES
		 ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa','22222222-2222-2222-2222-222222222222','DEVICE',
		  '11111111-1111-1111-1111-111111111111','ASSET','COMMON','Contains','DIRECTED',
		  '{"winner":"direct-tie"}',80,300,5)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("seed rollback conflict: %v", err)
		}
	}
}

func assert0014Rollback(t *testing.T, db *sql.DB) {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM topology_edge WHERE direction='BIDIRECTIONAL'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("bidirectional rows after down=%d err=%v", count, err)
	}
	type expectedRow struct {
		fromID, metadata          string
		created, updated, version int64
	}
	expected := []expectedRow{
		{migrationFromID, "bidir-new", 50, 300, 7},
		{migrationToID, "direct-tie", 80, 300, 5},
	}
	for _, want := range expected {
		var metadata string
		var created, updated, version int64
		if err := db.QueryRow(`SELECT metadata->>'winner', created_time, updated_time, version
			FROM topology_edge WHERE tenant_id=$1 AND from_id=$2 AND relation_type='Contains'`,
			migrationTenantA, want.fromID).Scan(&metadata, &created, &updated, &version); err != nil {
			t.Fatalf("read rollback orientation %s: %v", want.fromID, err)
		}
		if metadata != want.metadata || created != want.created || updated != want.updated || version != want.version {
			t.Fatalf("rollback orientation %s got metadata=%s created=%d updated=%d version=%d; want %+v",
				want.fromID, metadata, created, updated, version, want)
		}
	}
	assertConstraintContains0014(t, db, "topology_edge", "topology_edge_pkey", "tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type")
	assertConstraintContains0014(t, db, "topology_edge", "topology_edge_direction_chk", "'DIRECTED'")
}

func assertConstraintContains0014(t *testing.T, db *sql.DB, table, name, fragment string) {
	t.Helper()
	var definition string
	if err := db.QueryRow(`SELECT pg_get_constraintdef(c.oid)
		FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid
		WHERE r.relname=$1 AND c.conname=$2`, table, name).Scan(&definition); err != nil {
		t.Fatalf("read constraint %s: %v", name, err)
	}
	normalized := strings.ToLower(strings.ReplaceAll(definition, "\"", ""))
	if !strings.Contains(normalized, strings.ToLower(fragment)) {
		t.Fatalf("constraint %s=%q lacks %q", name, definition, fragment)
	}
}
