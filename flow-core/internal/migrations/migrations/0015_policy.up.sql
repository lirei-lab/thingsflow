-- 0015_policy.up.sql — tenant-scoped, versioned policy catalog (R6).
--
-- policyId in twin_registry is today a string (tenant:<tid>:default). This
-- migration makes it resolvable to a real policy document: a tenant-scoped,
-- versioned catalog mirroring the twin_model shape (immutable versions,
-- deprecated flag, authored definition + normalized derived schema), so the
-- Phase 6 enforcer reads a deterministic document.
--
-- Columns:
--   tenant_id      owning tenant (isolation boundary)
--   policy_id      canonical id, e.g. "owner" or "maintenance"
--   version        canonical 3-part semver (mirrors twin_model validation)
--   definition     the authored policy document (subjects, resources, grants,
--                  revokes) — what the CRUD API round-trips
--   schema         the normalized derived schema the enforcer reads
--   deprecated     deprecate instead of delete (keeps twin references valid)
--   created_time / updated_time  epoch millis
CREATE TABLE IF NOT EXISTS policy (
    tenant_id    uuid NOT NULL,
    policy_id    varchar(255) NOT NULL,
    version      varchar(64) NOT NULL,
    definition   jsonb NOT NULL,
    schema       jsonb NOT NULL,
    deprecated   boolean NOT NULL DEFAULT false,
    created_time bigint NOT NULL,
    updated_time bigint NOT NULL,
    PRIMARY KEY (tenant_id, policy_id, version)
);

-- Canonical policy_id: lowercase, non-[a-z0-9_] runs to '_', no leading/trailing
-- underscores (mirrors twin_model.model_id). The 'default' policy is the one the
-- registry seeds as tenant:<tid>:default, so it must be expressible as a bare id.
CREATE INDEX IF NOT EXISTS policy_tenant_deprecated_idx
    ON policy (tenant_id, deprecated, policy_id);
