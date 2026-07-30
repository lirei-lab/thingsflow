CREATE TABLE IF NOT EXISTS topology_relation_type (
    name varchar(255) PRIMARY KEY,
    description varchar,
    allowed_from_types text[] NOT NULL DEFAULT ARRAY[]::text[],
    allowed_to_types text[] NOT NULL DEFAULT ARRAY[]::text[],
    is_directed boolean NOT NULL DEFAULT true,
    created_time bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS topology_edge (
    tenant_id uuid NOT NULL,
    from_id uuid NOT NULL,
    from_type varchar(255) NOT NULL,
    to_id uuid NOT NULL,
    to_type varchar(255) NOT NULL,
    relation_type_group varchar(255) NOT NULL DEFAULT 'COMMON',
    relation_type varchar(255) NOT NULL,
    direction varchar(32) NOT NULL DEFAULT 'DIRECTED',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_time bigint NOT NULL,
    updated_time bigint NOT NULL,
    version bigint NOT NULL DEFAULT 1,
    CONSTRAINT topology_edge_pkey PRIMARY KEY
        (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type),
    CONSTRAINT topology_edge_direction_chk CHECK (direction IN ('DIRECTED'))
);

CREATE INDEX IF NOT EXISTS idx_topology_edge_from
    ON topology_edge (tenant_id, relation_type_group, from_type, from_id, relation_type);

CREATE INDEX IF NOT EXISTS idx_topology_edge_to
    ON topology_edge (tenant_id, relation_type_group, to_type, to_id, relation_type);

CREATE INDEX IF NOT EXISTS idx_topology_edge_type
    ON topology_edge (tenant_id, relation_type, from_type, to_type);

INSERT INTO topology_relation_type
    (name, description, allowed_from_types, allowed_to_types, is_directed, created_time)
VALUES
    ('Contains', 'Physical or logical containment', ARRAY['ASSET'], ARRAY['ASSET','DEVICE'], true, (extract(epoch from now()) * 1000)::bigint),
    ('Manages', 'Operational management relationship', ARRAY['ASSET','CUSTOMER','TENANT'], ARRAY['ASSET','DEVICE','CUSTOMER'], true, (extract(epoch from now()) * 1000)::bigint),
    ('LocatedIn', 'Location relationship', ARRAY['DEVICE','ASSET'], ARRAY['ASSET'], true, (extract(epoch from now()) * 1000)::bigint),
    ('ConnectedTo', 'Network or process connectivity', ARRAY['DEVICE','ASSET'], ARRAY['DEVICE','ASSET'], true, (extract(epoch from now()) * 1000)::bigint),
    ('Feeds', 'Upstream feed relationship', ARRAY['DEVICE','ASSET'], ARRAY['DEVICE','ASSET'], true, (extract(epoch from now()) * 1000)::bigint),
    ('DependsOn', 'Operational dependency relationship', ARRAY['DEVICE','ASSET'], ARRAY['DEVICE','ASSET'], true, (extract(epoch from now()) * 1000)::bigint)
ON CONFLICT (name) DO UPDATE SET
    description = EXCLUDED.description,
    allowed_from_types = EXCLUDED.allowed_from_types,
    allowed_to_types = EXCLUDED.allowed_to_types,
    is_directed = EXCLUDED.is_directed;

CREATE OR REPLACE FUNCTION thingsflow_safe_jsonb(input text)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN COALESCE(NULLIF(input, ''), '{}')::jsonb;
EXCEPTION WHEN others THEN
    RETURN '{}'::jsonb;
END;
$$;

WITH resolved AS (
    SELECT COALESCE(fa.tenant_id, fd.tenant_id)::uuid AS tenant_id,
           r.from_id, r.from_type, r.to_id, r.to_type,
           COALESCE(NULLIF(r.relation_type_group, ''), 'COMMON') AS relation_type_group,
           r.relation_type,
           thingsflow_safe_jsonb(r.additional_info) AS metadata
      FROM relation r
      LEFT JOIN asset fa ON r.from_type = 'ASSET' AND fa.id = r.from_id
      LEFT JOIN device fd ON r.from_type = 'DEVICE' AND fd.id = r.from_id
      LEFT JOIN asset ta ON r.to_type = 'ASSET' AND ta.id = r.to_id
      LEFT JOIN device td ON r.to_type = 'DEVICE' AND td.id = r.to_id
     WHERE r.relation_type IN ('Contains', 'Manages', 'LocatedIn', 'ConnectedTo', 'Feeds', 'DependsOn')
       AND COALESCE(fa.tenant_id, fd.tenant_id) IS NOT NULL
       AND COALESCE(ta.tenant_id, td.tenant_id) = COALESCE(fa.tenant_id, fd.tenant_id)
)
INSERT INTO topology_edge
    (tenant_id, from_id, from_type, to_id, to_type, relation_type_group,
     relation_type, direction, metadata, created_time, updated_time, version)
SELECT tenant_id, from_id, from_type, to_id, to_type, relation_type_group,
       relation_type, 'DIRECTED', metadata,
       (extract(epoch from now()) * 1000)::bigint,
       (extract(epoch from now()) * 1000)::bigint,
       1
  FROM resolved
ON CONFLICT (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type)
DO NOTHING;

DROP FUNCTION IF EXISTS thingsflow_safe_jsonb(text);
