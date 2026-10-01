// Package testdb hands tests a Postgres connection confined to a private,
// throwaway schema.
//
// WHY. The database harnesses in this repo open FLOW_TEST_PG_DSN and then run
// `DROP TABLE IF EXISTS asset CASCADE` (and a dozen siblings) to get a clean
// slate. Against the disposable Postgres that CI starts as a service container
// that is fine. Against any database a human might plausibly point the variable
// at — a local dev stack, a shared scratch database, a pilot — it is a data-loss
// bug waiting for one careless export. The harnesses also collide with each
// other: two packages dropping and creating `asset` at the same moment leave
// each other with half a schema. CI hides that today by running `go test -p 1`,
// so the failure only appears for whoever runs `go test ./...` locally, where
// packages run in parallel by default.
//
// HOW. `Scoped` creates a schema named for the test and returns a DSN carrying
// `search_path=<schema>`. Every unqualified DDL statement the harness already
// runs then resolves inside that schema: `DROP TABLE asset` drops the copy the
// test made, never `public.asset`. Nothing in the harness bodies has to change.
//
// search_path is passed as a CONNECTION PARAMETER, not as a `SET` statement.
// sql.DB is a pool: `SET search_path` applies to whichever pooled connection
// happened to serve it, so under any concurrency the next query can land on a
// connection still pointed at `public` — which is precisely the confinement
// failing open, silently, exactly when it matters. As a DSN parameter the
// server applies it at connection setup, to every connection the pool opens.
// Verified against postgres:16 with lib/pq: 40 concurrent queries, 0 wrong.
package testdb

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// seq disambiguates two schemas created inside the same nanosecond, which does
// happen for subtests that set up back to back.
var seq atomic.Uint64

var unsafeChars = regexp.MustCompile(`[^a-z0-9_]+`)

// Scoped creates a throwaway schema and returns a DSN bound to it. The schema
// is dropped when the test ends.
//
// Call it in place of the raw DSN:
//
//	pool, err := sql.Open("postgres", testdb.Scoped(t, dsn))
//
// Accepts testing.TB so benchmarks get the same confinement as tests --
// a benchmark that drops real tables is no less destructive.
func Scoped(t testing.TB, dsn string) string {
	t.Helper()

	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("testdb: open admin connection: %v", err)
	}
	defer admin.Close()
	if err := admin.Ping(); err != nil {
		t.Fatalf("testdb: ping: %v", err)
	}

	schema := schemaName(t.Name())
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("testdb: create schema %s: %v", schema, err)
	}

	t.Cleanup(func() {
		// A fresh connection: the test's own pool is closed by its cleanup,
		// and cleanup order between the two is not guaranteed.
		cleaner, err := sql.Open("postgres", dsn)
		if err != nil {
			t.Logf("testdb: could not reopen to drop schema %s: %v", schema, err)
			return
		}
		defer cleaner.Close()
		if _, err := cleaner.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`); err != nil {
			t.Logf("testdb: drop schema %s: %v", schema, err)
		}
	})

	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "search_path=" + schema
}

// schemaName derives a readable, unique, injection-safe identifier. The test
// name is carried through so a leaked schema names its own culprit.
func schemaName(testName string) string {
	base := unsafeChars.ReplaceAllString(strings.ToLower(testName), "_")
	base = strings.Trim(base, "_")
	if len(base) > 24 {
		base = base[:24]
	}
	if base == "" {
		base = "test"
	}
	// Postgres truncates identifiers at 63 bytes; this stays well inside.
	return fmt.Sprintf("t_%s_%d_%d", base, time.Now().UnixNano(), seq.Add(1))
}
