DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM twin_model
         WHERE version !~ '^(0|[1-9][0-9]{0,9})\.(0|[1-9][0-9]{0,9})\.(0|[1-9][0-9]{0,9})$'
            OR (split_part(version, '.', 1)::numeric > 2147483647
             OR split_part(version, '.', 2)::numeric > 2147483647
             OR split_part(version, '.', 3)::numeric > 2147483647)
    ) THEN
        RAISE EXCEPTION 'twin_model contains a non-canonical or int32-overflow version';
    END IF;
END;
$$;

ALTER TABLE twin_model
    DROP CONSTRAINT IF EXISTS twin_model_version_chk;
ALTER TABLE twin_model
    ADD CONSTRAINT twin_model_version_chk CHECK (
        version ~ '^(0|[1-9][0-9]{0,9})\.(0|[1-9][0-9]{0,9})\.(0|[1-9][0-9]{0,9})$'
        AND split_part(version, '.', 1)::numeric <= 2147483647
        AND split_part(version, '.', 2)::numeric <= 2147483647
        AND split_part(version, '.', 3)::numeric <= 2147483647
    );

ALTER TABLE twin_model
    ADD COLUMN IF NOT EXISTS deprecated boolean NOT NULL DEFAULT false;

ALTER TABLE twin_registry
    ADD COLUMN IF NOT EXISTS model_id varchar(255),
    ADD COLUMN IF NOT EXISTS model_version varchar(64);

ALTER TABLE topology_edge
    DROP CONSTRAINT IF EXISTS topology_edge_direction_chk;
ALTER TABLE topology_edge
    ADD CONSTRAINT topology_edge_direction_chk
    CHECK (direction IN ('DIRECTED', 'BIDIRECTIONAL'));

ALTER TABLE topology_edge
    DROP CONSTRAINT IF EXISTS topology_edge_pkey;
ALTER TABLE topology_edge
    ADD CONSTRAINT topology_edge_pkey PRIMARY KEY
    (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type, direction);

CREATE UNIQUE INDEX IF NOT EXISTS topology_edge_bidirectional_unq
    ON topology_edge (
        tenant_id,
        relation_type_group,
        relation_type,
        LEAST(from_type||':'||from_id::text,to_type||':'||to_id::text),
        GREATEST(from_type||':'||from_id::text,to_type||':'||to_id::text)
    )
    WHERE direction='BIDIRECTIONAL';

CREATE OR REPLACE FUNCTION thingsflow_lock_bidirectional_edge()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    from_endpoint text;
    to_endpoint text;
    smaller_endpoint text;
    larger_endpoint text;
    canonical_key text;
BEGIN
    IF NEW.direction <> 'BIDIRECTIONAL' THEN
        RETURN NEW;
    END IF;

    from_endpoint := length(NEW.from_type)::text || ':' || NEW.from_type ||
        length(NEW.from_id::text)::text || ':' || NEW.from_id::text;
    to_endpoint := length(NEW.to_type)::text || ':' || NEW.to_type ||
        length(NEW.to_id::text)::text || ':' || NEW.to_id::text;
    smaller_endpoint := LEAST(from_endpoint, to_endpoint);
    larger_endpoint := GREATEST(from_endpoint, to_endpoint);
    canonical_key :=
        length(NEW.tenant_id::text)::text || ':' || NEW.tenant_id::text ||
        length(NEW.relation_type_group)::text || ':' || NEW.relation_type_group ||
        length(NEW.relation_type)::text || ':' || NEW.relation_type ||
        length(smaller_endpoint)::text || ':' || smaller_endpoint ||
        length(larger_endpoint)::text || ':' || larger_endpoint;

    PERFORM pg_advisory_xact_lock(hashtextextended(canonical_key,0));
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS topology_edge_bidirectional_lock ON topology_edge;
CREATE TRIGGER topology_edge_bidirectional_lock
BEFORE INSERT OR UPDATE OF tenant_id, from_id, from_type, to_id, to_type,
    relation_type_group, relation_type, direction
ON topology_edge
FOR EACH ROW
EXECUTE FUNCTION thingsflow_lock_bidirectional_edge();
