CREATE TABLE IF NOT EXISTS twin_model (
    tenant_id uuid NOT NULL,
    model_id varchar(255) NOT NULL,
    version varchar(64) NOT NULL DEFAULT '1.0.0',
    kind varchar(64) NOT NULL,
    definition jsonb NOT NULL DEFAULT '{}'::jsonb,
    schema jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_time bigint NOT NULL,
    updated_time bigint NOT NULL,
    CONSTRAINT twin_model_pkey PRIMARY KEY (tenant_id, model_id, version)
);

CREATE INDEX IF NOT EXISTS idx_twin_model_tenant_kind
    ON twin_model (tenant_id, kind);

CREATE TABLE IF NOT EXISTS twin_registry (
    tenant_id uuid NOT NULL,
    thing_id varchar(512) NOT NULL,
    entity_type varchar(255) NOT NULL,
    entity_id uuid NOT NULL,
    policy_id varchar(512) NOT NULL,
    definition varchar(512) NOT NULL,
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_time bigint NOT NULL,
    updated_time bigint NOT NULL,
    version bigint NOT NULL DEFAULT 1,
    CONSTRAINT twin_registry_tenant_thing_unq UNIQUE (tenant_id, thing_id),
    CONSTRAINT twin_registry_entity_unq UNIQUE (tenant_id, entity_type, entity_id),
    CONSTRAINT twin_registry_entity_type_chk CHECK (entity_type IN ('DEVICE', 'ASSET'))
);

CREATE INDEX IF NOT EXISTS idx_twin_registry_tenant_definition
    ON twin_registry (tenant_id, definition);

CREATE INDEX IF NOT EXISTS idx_twin_registry_tenant_entity
    ON twin_registry (tenant_id, entity_type, entity_id);

WITH src AS (
    SELECT tenant_id, id, created_time,
           tenant_id::text || ':device:' || id::text AS thing_id,
           'tenant:' || tenant_id::text || ':default' AS policy_id,
           'thingsflow:device:' ||
           COALESCE(NULLIF(trim(both '_' from regexp_replace(lower(COALESCE(type, 'default')), '[^a-z0-9_]+', '_', 'g')), ''), 'default') ||
           ':1.0.0' AS definition,
           COALESCE(NULLIF(trim(COALESCE(additional_info, '')), '')::jsonb, '{}'::jsonb) ||
           jsonb_build_object(
               'id', id::text,
               'entityType', 'DEVICE',
               'tenantId', tenant_id::text,
               'createdTime', created_time,
               'name', name,
               'type', COALESCE(type, ''),
               'label', COALESCE(label, '')
           ) AS attributes
      FROM device
     WHERE tenant_id IS NOT NULL
)
INSERT INTO twin_registry
    (tenant_id, thing_id, entity_type, entity_id, policy_id, definition, attributes, created_time, updated_time)
SELECT tenant_id, thing_id, 'DEVICE', id, policy_id, definition, attributes, created_time, created_time
  FROM src
ON CONFLICT (tenant_id, entity_type, entity_id) DO UPDATE
   SET thing_id = EXCLUDED.thing_id,
       policy_id = EXCLUDED.policy_id,
       definition = EXCLUDED.definition,
       attributes = EXCLUDED.attributes,
       updated_time = EXCLUDED.updated_time,
       version = twin_registry.version + 1
 WHERE twin_registry.thing_id IS DISTINCT FROM EXCLUDED.thing_id
    OR twin_registry.policy_id IS DISTINCT FROM EXCLUDED.policy_id
    OR twin_registry.definition IS DISTINCT FROM EXCLUDED.definition
    OR twin_registry.attributes IS DISTINCT FROM EXCLUDED.attributes;

WITH src AS (
    SELECT tenant_id, id, created_time,
           tenant_id::text || ':asset:' || id::text AS thing_id,
           'tenant:' || tenant_id::text || ':default' AS policy_id,
           'thingsflow:asset:' ||
           COALESCE(NULLIF(trim(both '_' from regexp_replace(lower(COALESCE(type, 'default')), '[^a-z0-9_]+', '_', 'g')), ''), 'default') ||
           ':1.0.0' AS definition,
           COALESCE(NULLIF(trim(COALESCE(additional_info, '')), '')::jsonb, '{}'::jsonb) ||
           jsonb_build_object(
               'id', id::text,
               'entityType', 'ASSET',
               'tenantId', tenant_id::text,
               'createdTime', created_time,
               'name', name,
               'type', COALESCE(type, ''),
               'label', COALESCE(label, '')
           ) AS attributes
      FROM asset
     WHERE tenant_id IS NOT NULL
)
INSERT INTO twin_registry
    (tenant_id, thing_id, entity_type, entity_id, policy_id, definition, attributes, created_time, updated_time)
SELECT tenant_id, thing_id, 'ASSET', id, policy_id, definition, attributes, created_time, created_time
  FROM src
ON CONFLICT (tenant_id, entity_type, entity_id) DO UPDATE
   SET thing_id = EXCLUDED.thing_id,
       policy_id = EXCLUDED.policy_id,
       definition = EXCLUDED.definition,
       attributes = EXCLUDED.attributes,
       updated_time = EXCLUDED.updated_time,
       version = twin_registry.version + 1
 WHERE twin_registry.thing_id IS DISTINCT FROM EXCLUDED.thing_id
    OR twin_registry.policy_id IS DISTINCT FROM EXCLUDED.policy_id
    OR twin_registry.definition IS DISTINCT FROM EXCLUDED.definition
    OR twin_registry.attributes IS DISTINCT FROM EXCLUDED.attributes;
