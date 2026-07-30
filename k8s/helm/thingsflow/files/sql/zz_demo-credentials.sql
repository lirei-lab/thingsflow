--
-- DEMO-ONLY known passwords. Runs last (zz_ prefix) so it overwrites the
-- per-install random hashes seeded by 98_seed-tenant.sql / 99_system-data.sql.
--
-- Inclusion is gated:
--   * Helm: included in the postgres init ConfigMap ONLY when
--     flowCore.loadDemo=true (see templates/postgres.yaml). A production
--     install (loadDemo=false, the default) never mounts this file, so the
--     well-known demo passwords simply do not exist there.
--   * docker compose: the whole files/sql dir is bind-mounted into
--     /docker-entrypoint-initdb.d, so local dev always gets these — that is
--     what keeps the Quick Start login working out of the box.
--
-- Passwords set here: sysadmin/sysadmin and tenant/tenant. The bcrypt hashes
-- are generated fresh per install via pgcrypto, so NO password hash string is
-- committed to the repo (the upstream public hash is gone for good).
--
-- Postgres runs initdb scripts only on first cluster init, so this is a no-op
-- on an already-initialised data directory.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- sysadmin@thingsboard.org / sysadmin
UPDATE user_credentials
   SET password = crypt('sysadmin', gen_salt('bf', 10))
 WHERE user_id = '5a797660-4612-11e7-a919-92ebcb67fe33';

-- tenant@thingsboard.org / tenant
UPDATE user_credentials
   SET password = crypt('tenant', gen_salt('bf', 10))
 WHERE user_id = 'bbbbbbbb-2222-3333-4444-555555555555';
