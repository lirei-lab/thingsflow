// Package migrations runs golang-migrate against the configured
// Postgres pool on startup. SQL files live in ./migrations/*.sql and
// are embedded into the binary at compile time, so the deployed image
// is self-contained.
//
// Adoption strategy for an existing install:
//
//   - Fresh installs: postgres docker-entrypoint-initdb.d runs the
//     bundled SQL files; the bridge then sees a fully provisioned
//     schema with no schema_migrations table. We create the table and
//     stamp version 1 ("baseline applied") so future migrations apply
//     on top of the running schema.
//
//   - Existing flow-core installs: same flow — schema is there but
//     unmigrated, so we stamp baseline before applying anything.
//
//   - New schema changes: drop a NNNN_*.up.sql / NNNN_*.down.sql pair
//     into ./migrations/. On the next bridge restart, golang-migrate
//     applies it transactionally.
//
// The migration step is best-effort: a failure logs and exits the
// process so kubelet/compose can restart and surface the error.
package migrations

import (
	"database/sql"
	"embed"
	"errors"
	"log"

	"github.com/golang-migrate/migrate/v4"
	pgmigrate "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// FS embeds ./migrations/*.sql into the binary so the deployed image
// is self-contained. New migrations only need to be dropped in place
// — a rebuild picks them up.
//
//go:embed migrations/*.sql
var FS embed.FS

// Run applies any pending migrations to db. First call on a fresh
// schema (with the bundled init scripts already run) detects the
// unmigrated state and stamps version 1 so the baseline migration is
// treated as already applied.
func Run(db *sql.DB) error {
	driver, err := pgmigrate.WithInstance(db, &pgmigrate.Config{})
	if err != nil {
		return err
	}
	src, err := iofs.New(FS, "migrations")
	if err != nil {
		return err
	}
	m, err := migrate.NewWithInstance("iofs", src, "postgres", driver)
	if err != nil {
		return err
	}

	version, dirty, err := m.Version()
	switch {
	case errors.Is(err, migrate.ErrNilVersion):
		if hasBootstrappedSchema(db) {
			log.Printf("migrations: existing schema detected, stamping baseline (v1) as applied")
			if ferr := m.Force(1); ferr != nil {
				return ferr
			}
		}
	case err != nil:
		return err
	default:
		if dirty {
			log.Printf("migrations: WARNING database is dirty at v%d — admin intervention required", version)
		}
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	v, _, _ := m.Version()
	log.Printf("migrations: schema at version %d", v)
	return nil
}

// hasBootstrappedSchema returns true if the core tables exist. Used to
// distinguish a brand-new database (where 0001 should run) from one
// provisioned by docker-entrypoint init scripts (where 0001 is already
// implicitly satisfied).
func hasBootstrappedSchema(db *sql.DB) bool {
	var exists bool
	err := db.QueryRow(`
		SELECT EXISTS (
		  SELECT 1 FROM pg_tables
		   WHERE schemaname = 'public' AND tablename = 'tenant'
		)`).Scan(&exists)
	if err != nil {
		return false
	}
	return exists
}
