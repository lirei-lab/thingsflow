package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"flow-core/internal/policy"

	_ "github.com/lib/pq"
)

// TestPolicyCatalog0015 exercises the 0015_policy migration in an isolated
// schema: it must create the tenant-scoped policy catalog, be idempotent on
// reapply, round-trip through its down migration with data present, and
// re-create cleanly afterwards.
func TestPolicyCatalog0015(t *testing.T) {
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
	schema := fmt.Sprintf("migration_0015_test_%d", time.Now().UnixNano())
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

	up := readMigration0015(t, "migrations/0015_policy.up.sql")
	down := readMigration0015(t, "migrations/0015_policy.down.sql")

	if _, err := db.Exec(up); err != nil {
		t.Fatalf("apply up: %v", err)
	}
	// The migration is safe when its SQL is replayed by an operator.
	if _, err := db.Exec(up); err != nil {
		t.Fatalf("reapply idempotent up: %v", err)
	}

	// The catalog is tenant-scoped and versioned: two tenants may hold the
	// same policy_id, and one tenant may hold multiple immutable versions.
	// Each row carries kind (the column Store.Create inserts into), proving
	// the real migration table matches the store's write contract. Row order:
	// (tenant, policy_id, version, kind, definition, deprecated).
	now := time.Now().UnixMilli()
	for _, row := range [][]interface{}{
		{migrationTenantA, "owner", "1.0.0", "TWIN", `{"policyId":"owner","version":"1.0.0"}`, false},
		{migrationTenantA, "owner", "2.0.0", "TWIN", `{"policyId":"owner","version":"2.0.0"}`, false},
		{migrationTenantB, "owner", "1.0.0", "TWIN", `{"policyId":"owner","version":"1.0.0"}`, true},
	} {
		if _, err := db.Exec(`INSERT INTO policy
			(tenant_id, policy_id, version, kind, definition, schema, deprecated, created_time, updated_time)
			VALUES ($1, $2, $3, $4, $5::jsonb, $5::jsonb, $6, $7, $7)`,
			row[0], row[1], row[2], row[3], row[4], row[5], now); err != nil {
			t.Fatalf("seed policy row: %v", err)
		}
	}

	var rows int
	if err := db.QueryRow(`SELECT count(*) FROM policy`).Scan(&rows); err != nil {
		t.Fatalf("count policy: %v", err)
	}
	if rows != 3 {
		t.Fatalf("expected 3 seeded policy rows, got %d", rows)
	}

	// The kind column and the version CHECK backstop exist on the real table
	// (mirroring twin_model). An out-of-band non-canonical version is rejected.
	var kindColumn int
	if err := db.QueryRow(`SELECT count(*) FROM information_schema.columns
		WHERE table_name='policy' AND column_name='kind'`).Scan(&kindColumn); err != nil {
		t.Fatalf("check kind column: %v", err)
	}
	if kindColumn != 1 {
		t.Fatalf("expected policy.kind column, found %d", kindColumn)
	}
	var checkCount int
	if err := db.QueryRow(`SELECT count(*) FROM pg_constraint WHERE conname='policy_version_chk'`).Scan(&checkCount); err != nil {
		t.Fatalf("check version constraint: %v", err)
	}
	if checkCount != 1 {
		t.Fatalf("expected policy_version_chk, found %d", checkCount)
	}
	if _, err := db.Exec(`INSERT INTO policy
		(tenant_id, policy_id, version, kind, definition, schema, deprecated, created_time, updated_time)
		VALUES ($1, 'bad', 'not.a.version', 'TWIN', '{}', '{}', false, $2, $2)`,
		migrationTenantA, now); err == nil {
		t.Fatal("version CHECK must reject a non-canonical out-of-band version")
	}

	// Data-bearing down: dropping the catalog must succeed with rows present.
	if _, err := db.Exec(down); err != nil {
		t.Fatalf("apply data-bearing down: %v", err)
	}
	if _, err := db.Exec(`SELECT count(*) FROM policy`); err == nil {
		t.Fatal("policy table still exists after down")
	}

	if _, err := db.Exec(up); err != nil {
		t.Fatalf("reapply up after data-bearing down: %v", err)
	}
	// Down+up leaves an empty catalog; verify the fresh table accepts rows
	// (including the kind column).
	if _, err := db.Exec(`INSERT INTO policy
		(tenant_id, policy_id, version, kind, definition, schema, deprecated, created_time, updated_time)
		VALUES ($1, 'default', '1.0.0', 'TWIN', '{}', '{}', false, $2, $2)`,
		migrationTenantA, now); err != nil {
		t.Fatalf("insert into recreated catalog: %v", err)
	}

	// Strongest proof of the migration's store contract: the real policy.Store
	// (whose Create inserts into the kind column) works against the migrated
	// table — this is the exact operation POST /api/policies runs.
	if _, err := policy.NewStore(db).Create(context.Background(), migrationTenantA, []byte(`{
		"policyId": "owner", "version": "3.0.0", "kind": "TWIN",
		"subjects": ["tenant:`+migrationTenantA+`"], "resources": ["thing:/`+migrationTenantA+`"],
		"grants": [{"resource": "thing:/`+migrationTenantA+`", "actions": ["READ", "WRITE"]}], "revokes": []
	}`)); err != nil {
		t.Fatalf("policy.Store.Create against real migrated table: %v", err)
	}
}

func readMigration0015(t *testing.T, name string) string {
	t.Helper()
	b, err := FS.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

// TestPolicyCatalog0015InPlaceReplay exercises the operator replay path: a DB
// that applied the earlier 0015 before the kind column existed (a kind-less
// policy table) must gain kind — idempotently, with existing rows backfilled
// to 'TWIN' — so policy.Store.Create (which inserts kind) keeps working.
func TestPolicyCatalog0015InPlaceReplay(t *testing.T) {
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
	schema := fmt.Sprintf("migration_0015_replay_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create isolated schema: %v", err)
	}
	if _, err := db.Exec(`SET search_path TO ` + schema); err != nil {
		t.Fatalf("set isolated schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`)
		db.Close()
	})

	// Simulate the pre-fix state: a policy table WITHOUT kind, plus a row.
	if _, err := db.Exec(`CREATE TABLE policy (
		tenant_id uuid NOT NULL, policy_id varchar(255) NOT NULL,
		version varchar(64) NOT NULL, definition jsonb NOT NULL, schema jsonb NOT NULL,
		deprecated boolean NOT NULL DEFAULT false,
		created_time bigint NOT NULL, updated_time bigint NOT NULL,
		PRIMARY KEY (tenant_id, policy_id, version))`); err != nil {
		t.Fatalf("create kind-less policy table: %v", err)
	}
	now := time.Now().UnixMilli()
	if _, err := db.Exec(`INSERT INTO policy
		(tenant_id, policy_id, version, definition, schema, deprecated, created_time, updated_time)
		VALUES ($1, 'owner', '1.0.0', '{}', '{}', false, $2, $2)`,
		migrationTenantA, now); err != nil {
		t.Fatalf("seed kind-less row: %v", err)
	}

	// Replay the fixed 0015: CREATE TABLE IF NOT EXISTS is a no-op, but the
	// idempotent kind ALTER must add the column and backfill 'TWIN'.
	up := readMigration0015(t, "migrations/0015_policy.up.sql")
	if _, err := db.Exec(up); err != nil {
		t.Fatalf("replay fixed up over kind-less table: %v", err)
	}
	var kind string
	if err := db.QueryRow(`SELECT kind FROM policy WHERE policy_id='owner'`).Scan(&kind); err != nil {
		t.Fatalf("read backfilled kind: %v", err)
	}
	if kind != "TWIN" {
		t.Fatalf("expected backfilled kind 'TWIN', got %q", kind)
	}
	// policy.Store.Create works against the replayed table.
	if _, err := policy.NewStore(db).Create(context.Background(), migrationTenantA, []byte(`{
		"policyId": "owner", "version": "2.0.0", "kind": "TWIN",
		"subjects": ["tenant:`+migrationTenantA+`"], "resources": ["thing:/`+migrationTenantA+`"],
		"grants": [{"resource": "thing:/`+migrationTenantA+`", "actions": ["READ"]}], "revokes": []
	}`)); err != nil {
		t.Fatalf("Store.Create after in-place replay: %v", err)
	}
}
