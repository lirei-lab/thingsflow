// Package db owns the Postgres connection pool that flow-core handlers
// share. It exists so handler-side code stops reaching into a
// `package main` global and instead depends on an importable name —
// the foundation for tests (which can swap the pool for a test DB) and
// for an eventual repository layer (which would wrap this with typed
// methods per-domain).
//
// For now it's a deliberately thin relocation — same connection
// management, same queries, just a different home. Helpers that used
// to call `pgDB.X(...)` now call `db.Pool.X(...)`. Domain queries
// stay in their original files; over time they can migrate to typed
// repos defined alongside.
package db

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// Pool is the shared *sql.DB used by every handler. Exported because
// the caller package (flow-core's package main) is not yet structured
// around dependency injection — handlers reach for db.Pool the same
// way they used to reach for the package-main pgDB. Setter is
// available for tests:
//
//	dbpkg.SetPoolForTest(t, fakeDB)
//	t.Cleanup(func() { dbpkg.SetPoolForTest(t, nil) })
var Pool *sql.DB

// Init opens the Postgres connection pool from environment variables.
// Returns the pool plus any error; on success Pool is also set so
// callers don't need to thread the value around. Idempotent in the
// happy path; if Pool is already non-nil this is a no-op.
//
// Recognized env (matches the helm chart and the docker-compose dev
// stack — see k8s/helm/thingsflow/templates/configmap.yaml):
//   - SPRING_DATASOURCE_URL          jdbc:postgresql://host:5432/db
//   - SPRING_DATASOURCE_USERNAME     postgres (default)
//   - SPRING_DATASOURCE_PASSWORD     postgres (default)
//   - PG_MAX_OPEN_CONNS              30 (default)
//   - PG_MAX_IDLE_CONNS              10 (default)
func Init() (*sql.DB, error) {
	if Pool != nil {
		return Pool, nil
	}

	dsn := buildDSN()
	pool, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}

	maxOpen, _ := strconv.Atoi(getEnv("PG_MAX_OPEN_CONNS", "30"))
	maxIdle, _ := strconv.Atoi(getEnv("PG_MAX_IDLE_CONNS", "10"))
	pool.SetMaxOpenConns(maxOpen)
	pool.SetMaxIdleConns(maxIdle)
	pool.SetConnMaxLifetime(5 * time.Minute)
	pool.SetConnMaxIdleTime(2 * time.Minute)

	if err := pool.Ping(); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	log.Printf("Postgres pool: maxOpen=%d maxIdle=%d connLifetime=5m", maxOpen, maxIdle)

	Pool = pool
	return pool, nil
}

// SetPoolForTest swaps the global pool. Tests should pass `nil` in
// cleanup to restore the empty state. The `t` parameter is unused but
// kept in the signature so callers must hold a *testing.T — flags this
// as a test-only API at the call site.
func SetPoolForTest(t any, pool *sql.DB) {
	Pool = pool
}

// Close shuts down the pool. Safe to call multiple times.
func Close() error {
	if Pool == nil {
		return nil
	}
	err := Pool.Close()
	Pool = nil
	return err
}

// buildDSN reads SPRING_DATASOURCE_* env vars and emits a libpq DSN.
// Carried over from the original InitPostgres for behavior parity:
// JDBC URL → postgres URL, username/password injection, sslmode=disable.
func buildDSN() string {
	dsnUrl := getEnv("SPRING_DATASOURCE_URL", "jdbc:postgresql://postgres:5432/thingsboard")
	dsnUrl = strings.Replace(dsnUrl, "jdbc:postgresql://", "postgres://", 1)

	user := getEnv("SPRING_DATASOURCE_USERNAME", "postgres")
	pass := getEnv("SPRING_DATASOURCE_PASSWORD", "postgres")

	parts := strings.Split(dsnUrl, "postgres://")
	if len(parts) == 2 {
		return fmt.Sprintf("postgres://%s:%s@%s?sslmode=disable", user, pass, parts[1])
	}
	return "postgres://postgres:postgres@postgres:5432/thingsboard?sslmode=disable"
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
