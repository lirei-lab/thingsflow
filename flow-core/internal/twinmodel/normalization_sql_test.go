package twinmodel

import (
	"flow-core/internal/testdb"

	"database/sql"
	"os"
	"strings"
	"testing"

	_ "github.com/lib/pq"
)

func TestSQLNormalizationMatchesGo(t *testing.T) {
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	db, err := sql.Open("postgres", testdb.Scoped(t, dsn))
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}

	corpus := []string{
		"", "___", "Energy Meter", "A/B.C", "İST", "ß", "KELVIN",
		strings.Repeat("a", 255), strings.Repeat("b", 256), strings.Repeat("c", 300),
	}
	for _, raw := range corpus {
		var sqlCanonical string
		err := db.QueryRow(`SELECT trim(both '_' from regexp_replace(lower($1), '[^a-z0-9_]+', '_', 'g'))`, raw).Scan(&sqlCanonical)
		if err != nil {
			t.Fatalf("SQL normalize %q: %v", raw, err)
		}
		if got := canonicalModelID(raw); got != sqlCanonical {
			t.Errorf("canonicalModelID(%q) = %q, SQL = %q", raw, got, sqlCanonical)
		}
	}
}
