-- Baseline marker for the migration runner.
--
-- The full schema is provisioned by /docker-entrypoint-initdb.d/*.sql on
-- a fresh postgres install (see k8s/helm/thingsflow/files/sql/). Once the
-- schema_migrations table is created by golang-migrate, this empty
-- migration locks in version 1 so any subsequent migrations apply on
-- top of the existing schema rather than re-running the bootstrap.
--
-- Do not add DDL here. New schema changes go in 0002+.
SELECT 1;
