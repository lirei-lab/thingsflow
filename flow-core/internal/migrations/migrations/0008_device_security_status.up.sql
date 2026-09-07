ALTER TABLE device
    ADD COLUMN IF NOT EXISTS security_status varchar(32) NOT NULL DEFAULT 'ACTIVE';

UPDATE device
   SET security_status = 'ACTIVE'
 WHERE security_status IS NULL OR security_status = '';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'device_security_status_chk'
    ) THEN
        ALTER TABLE device
            ADD CONSTRAINT device_security_status_chk
            CHECK (security_status IN ('ACTIVE', 'SUSPENDED'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_device_tenant_security_status
    ON device (tenant_id, security_status);

-- Rebuild the three device_info views so they pick up the new column.
--
-- They are `SELECT d.*` over `device`, so their column list is fixed at
-- creation time: a view built before this migration keeps the OLD shape and
-- never shows security_status. Without this, up -> down -> up leaves a schema
-- that differs from the one it started with -- the views come back one column
-- short, silently, and only a view-based query would ever notice.
--
-- CREATE OR REPLACE cannot be used: it refuses to change a view's column list.
-- Definitions mirror k8s/helm/thingsflow/files/sql/02_schema-views.sql; keep
-- the two in step.
DROP VIEW IF EXISTS device_info_view CASCADE;
DROP VIEW IF EXISTS device_info_active_attribute_view CASCADE;
DROP VIEW IF EXISTS device_info_active_ts_view CASCADE;

CREATE VIEW device_info_active_attribute_view AS
SELECT d.*
       , c.title as customer_title
       , COALESCE((c.additional_info::json->>'isPublic')::bool, FALSE) as customer_is_public
       , d.type as device_profile_name
       , COALESCE(da.bool_v, FALSE) as active
FROM device d
         LEFT JOIN customer c ON c.id = d.customer_id
         LEFT JOIN attribute_kv da ON da.entity_id = d.id AND da.attribute_type = 2 AND da.attribute_key = (select key_id from key_dictionary  where key = 'active');

CREATE VIEW device_info_active_ts_view AS
SELECT d.*
       , c.title as customer_title
       , COALESCE((c.additional_info::json->>'isPublic')::bool, FALSE) as customer_is_public
       , d.type as device_profile_name
       , COALESCE(dt.bool_v, FALSE) as active
FROM device d
         LEFT JOIN customer c ON c.id = d.customer_id
         LEFT JOIN ts_kv_latest dt ON dt.entity_id = d.id and dt.key = (select key_id from key_dictionary where key = 'active');

-- Depends on the attribute view above, so it is rebuilt last.
CREATE VIEW device_info_view AS SELECT * FROM device_info_active_attribute_view;
