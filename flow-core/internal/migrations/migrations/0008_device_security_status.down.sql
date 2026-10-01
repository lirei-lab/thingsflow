-- Rolling this back means removing a column three views depend on.
--
-- device_info_active_attribute_view, device_info_active_ts_view and
-- device_info_view are all `SELECT d.*` over `device` (installed by the baseline
-- schema, k8s/helm/thingsflow/files/sql/02_schema-views.sql), so they pick up
-- every column the table has -- including this one. Postgres therefore refuses
-- the DROP COLUMN outright and this migration could never roll back:
--
--   pq: cannot drop column security_status of table device because other
--       objects depend on it, view device_info_active_attribute_view depends on
--       column security_status of table device
--
-- `DROP COLUMN ... CASCADE` would "succeed" by silently deleting all three
-- views and leaving the schema three objects short, with nothing in the up
-- direction that recreates them -- a rollback that quietly breaks every device
-- list query is worse than one that refuses.
--
-- So: drop the views, drop the column, rebuild the views from the same
-- definitions the baseline installs. Rebuilt after the column is gone, `SELECT
-- d.*` yields exactly the pre-migration shape, which is the point.
--
-- Keep these definitions in step with 02_schema-views.sql. They are duplicated
-- deliberately: a down migration has to restore the state it disturbed, and it
-- cannot reach into the baseline to do it.

DROP VIEW IF EXISTS device_info_view CASCADE;
DROP VIEW IF EXISTS device_info_active_attribute_view CASCADE;
DROP VIEW IF EXISTS device_info_active_ts_view CASCADE;

DROP INDEX IF EXISTS idx_device_tenant_security_status;
ALTER TABLE device DROP CONSTRAINT IF EXISTS device_security_status_chk;
ALTER TABLE device DROP COLUMN IF EXISTS security_status;

CREATE OR REPLACE VIEW device_info_active_attribute_view AS
SELECT d.*
       , c.title as customer_title
       , COALESCE((c.additional_info::json->>'isPublic')::bool, FALSE) as customer_is_public
       , d.type as device_profile_name
       , COALESCE(da.bool_v, FALSE) as active
FROM device d
         LEFT JOIN customer c ON c.id = d.customer_id
         LEFT JOIN attribute_kv da ON da.entity_id = d.id AND da.attribute_type = 2 AND da.attribute_key = (select key_id from key_dictionary  where key = 'active');

CREATE OR REPLACE VIEW device_info_active_ts_view AS
SELECT d.*
       , c.title as customer_title
       , COALESCE((c.additional_info::json->>'isPublic')::bool, FALSE) as customer_is_public
       , d.type as device_profile_name
       , COALESCE(dt.bool_v, FALSE) as active
FROM device d
         LEFT JOIN customer c ON c.id = d.customer_id
         LEFT JOIN ts_kv_latest dt ON dt.entity_id = d.id and dt.key = (select key_id from key_dictionary where key = 'active');

-- Depends on the attribute view above, so it is rebuilt last.
CREATE OR REPLACE VIEW device_info_view AS SELECT * FROM device_info_active_attribute_view;
