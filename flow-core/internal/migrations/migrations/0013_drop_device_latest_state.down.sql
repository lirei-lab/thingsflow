CREATE TABLE IF NOT EXISTS device_latest_state (
    tenant_id uuid NOT NULL,
    entity_type varchar(32) NOT NULL DEFAULT 'DEVICE',
    entity_id uuid NOT NULL,
    telemetry jsonb NOT NULL DEFAULT '{}'::jsonb,
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_time bigint NOT NULL DEFAULT ((extract(epoch from now()) * 1000)::bigint),
    updated_time bigint NOT NULL DEFAULT ((extract(epoch from now()) * 1000)::bigint),
    PRIMARY KEY (tenant_id, entity_type, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_device_latest_state_entity
    ON device_latest_state (entity_type, entity_id);

CREATE INDEX IF NOT EXISTS idx_device_latest_state_telemetry_gin
    ON device_latest_state USING gin (telemetry);
