package testdb

import (
	"database/sql"
	"os"
	"strings"
	"sync"
	"testing"

	_ "github.com/lib/pq"
)

// These tests pin the two properties the whole isolation scheme rests on. Both
// were verified against postgres:16 before the 29 harnesses were migrated, and
// both fail silently if someone "simplifies" Scoped into a `SET search_path`.

func dsn(t *testing.T) string {
	t.Helper()
	d := os.Getenv("FLOW_TEST_PG_DSN")
	if d == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	return d
}

// An unqualified DROP TABLE — which is what every migrated harness runs — must
// not reach a same-named table outside the throwaway schema. This is the whole
// point: the harnesses keep their destructive DDL, confinement makes it safe.
func TestScopedDropCannotReachRealTables(t *testing.T) {
	base := dsn(t)

	// Unique per run. A fixed name in `public` couples one run to the next --
	// the very defect this package exists to remove, and it duly showed up as a
	// stale row the first time the suite ran in parallel.
	canary := "testdb_canary_" + strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, strings.ToLower(t.Name())) + "_" + schemaName("c")

	admin, err := sql.Open("postgres", base)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer admin.Close()
	if _, err := admin.Exec(`CREATE TABLE public.` + canary + ` (x int)`); err != nil {
		t.Fatalf("seed canary: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(`DROP TABLE IF EXISTS public.` + canary) })
	if _, err := admin.Exec(`INSERT INTO public.` + canary + ` VALUES (1)`); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	scoped, err := sql.Open("postgres", Scoped(t, base))
	if err != nil {
		t.Fatalf("open scoped: %v", err)
	}
	defer scoped.Close()

	// Exactly what a harness does: drop, then recreate its own copy.
	if _, err := scoped.Exec(`DROP TABLE IF EXISTS ` + canary + ` CASCADE`); err != nil {
		t.Fatalf("scoped drop: %v", err)
	}
	if _, err := scoped.Exec(`CREATE TABLE ` + canary + ` (y text)`); err != nil {
		t.Fatalf("scoped create: %v", err)
	}

	var n int
	if err := admin.QueryRow(`SELECT count(*) FROM public.` + canary).Scan(&n); err != nil {
		t.Fatalf("the scoped harness destroyed a table outside its schema: %v", err)
	}
	if n != 1 {
		t.Errorf("public.%s lost rows: got %d, want 1", canary, n)
	}
}

// search_path must be a CONNECTION parameter, not a SET. sql.DB is a pool: a
// `SET search_path` binds only the connection that served it, so under
// concurrency later queries land on connections still pointed at public — the
// confinement failing open exactly when load makes it matter.
func TestScopedAppliesToEveryPooledConnection(t *testing.T) {
	base := dsn(t)

	scopedDSN := Scoped(t, base)
	if !strings.Contains(scopedDSN, "search_path=") {
		t.Fatalf("Scoped did not bind search_path in the DSN: %q", scopedDSN)
	}

	db, err := sql.Open("postgres", scopedDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(8)

	var wg sync.WaitGroup
	wrong := make(chan string, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var sp string
			if err := db.QueryRow("SHOW search_path").Scan(&sp); err != nil {
				wrong <- "query failed: " + err.Error()
				return
			}
			if !strings.Contains(sp, "t_") {
				wrong <- sp
			}
		}()
	}
	wg.Wait()
	close(wrong)

	if n := len(wrong); n != 0 {
		t.Errorf("%d/64 pooled connections were not confined (first: %q)", n, <-wrong)
	}
}

// Two Scoped calls must never collide, including within the same nanosecond —
// which is what subtests setting up back to back actually do.
func TestScopedSchemasAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		name := schemaName("TestSomething/case")
		if seen[name] {
			t.Fatalf("duplicate schema name generated: %s", name)
		}
		seen[name] = true
	}
}

func TestSchemaNameIsSafeAndReadable(t *testing.T) {
	// Test names carry slashes, spaces and case; all of that has to become a
	// legal identifier, and the origin must stay legible so a leaked schema
	// names its own culprit.
	got := schemaName("TestFoo/sub case #2")
	for _, bad := range []string{"/", " ", "#", "'", `"`, ";"} {
		if strings.Contains(got, bad) {
			t.Errorf("schema name %q contains unsafe %q", got, bad)
		}
	}
	if !strings.HasPrefix(got, "t_testfoo_sub_case") {
		t.Errorf("schema name lost its origin: %q", got)
	}
	if len(got) > 63 {
		t.Errorf("schema name exceeds the Postgres identifier limit: %d", len(got))
	}
}
