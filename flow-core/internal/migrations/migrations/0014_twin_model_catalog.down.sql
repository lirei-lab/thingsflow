INSERT INTO topology_edge
    (tenant_id, from_id, from_type, to_id, to_type, relation_type_group,
     relation_type, direction, metadata, created_time, updated_time, version)
SELECT DISTINCT ON
       (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type)
       tenant_id, from_id, from_type, to_id, to_type, relation_type_group,
       relation_type, 'DIRECTED', metadata, created_time, updated_time, version
  FROM (
        SELECT tenant_id, from_id, from_type, to_id, to_type,
               relation_type_group, relation_type, metadata,
               created_time, updated_time, version
          FROM topology_edge
         WHERE direction = 'BIDIRECTIONAL'
        UNION ALL
        SELECT tenant_id, to_id, to_type, from_id, from_type,
               relation_type_group, relation_type, metadata,
               created_time, updated_time, version
          FROM topology_edge
         WHERE direction = 'BIDIRECTIONAL'
       ) converted
 ORDER BY tenant_id, from_id, from_type, relation_type_group, relation_type,
          to_id, to_type, updated_time DESC, version DESC
ON CONFLICT
    (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type, direction)
DO UPDATE SET
    metadata = CASE
        WHEN (EXCLUDED.updated_time, EXCLUDED.version, 'BIDIRECTIONAL') >
             (topology_edge.updated_time, topology_edge.version, 'DIRECTED')
        THEN EXCLUDED.metadata
        ELSE topology_edge.metadata
    END,
    created_time = LEAST(topology_edge.created_time, EXCLUDED.created_time),
    updated_time = GREATEST(topology_edge.updated_time, EXCLUDED.updated_time),
    version = GREATEST(topology_edge.version, EXCLUDED.version);

DELETE FROM topology_edge WHERE direction = 'BIDIRECTIONAL';

DROP TRIGGER IF EXISTS topology_edge_bidirectional_lock ON topology_edge;
DROP FUNCTION IF EXISTS thingsflow_lock_bidirectional_edge();
DROP INDEX IF EXISTS topology_edge_bidirectional_unq;

ALTER TABLE topology_edge
    DROP CONSTRAINT IF EXISTS topology_edge_pkey;
ALTER TABLE topology_edge
    ADD CONSTRAINT topology_edge_pkey PRIMARY KEY
    (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type);

ALTER TABLE topology_edge
    DROP CONSTRAINT IF EXISTS topology_edge_direction_chk;
ALTER TABLE topology_edge
    ADD CONSTRAINT topology_edge_direction_chk CHECK (direction IN ('DIRECTED'));

ALTER TABLE twin_registry
    DROP COLUMN IF EXISTS model_version,
    DROP COLUMN IF EXISTS model_id;

ALTER TABLE twin_model
    DROP COLUMN IF EXISTS deprecated;
ALTER TABLE twin_model
    DROP CONSTRAINT IF EXISTS twin_model_version_chk;
