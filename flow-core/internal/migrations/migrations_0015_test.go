package migrations

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

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
	now := time.Now().UnixMilli()
	for _, row := range [][]interface{}{
		{migrationTenantA, "owner", "1.0.0", `{"policyId":"owner","version":"1.0.0"}`, false},
		{migrationTenantA, "owner", "2.0.0", `{"policyId":"owner","version":"2.0.0"}`, false},
		{migrationTenantB, "owner", "1.0.0", `{"policyId":"owner","version":"1.0.0"}`, true},
	} {
		if _, err := db.Exec(`INSERT INTO policy
			(tenant_id, policy_id, version, definition, schema, deprecated, created_time, updated_time)
			VALUES ($1, $2, $3, $4::jsonb, $4::jsonb, $5, $6, $6)`,
			row[0], row[1], row[2], row[3], row[4], now); err != nil {
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
	// Down+up leaves an empty catalog; verify the fresh table accepts rows.
	if _, err := db.Exec(`INSERT INTO policy
		(tenant_id, policy_id, version, definition, schema, deprecated, created_time, updated_time)
		VALUES ($1, 'default', '1.0.0', '{}', '{}', false, $2, $2)`,
		migrationTenantA, now); err != nil {
		t.Fatalf("insert into recreated catalog: %v", err)
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
