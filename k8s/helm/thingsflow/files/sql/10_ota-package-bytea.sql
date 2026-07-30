--
-- TB classic uses postgres Large Object (oid) for ota_package.data — that
-- requires lo_create/lo_write/lo_read from clients and complicates the bridge
-- which uses a single transaction-friendly bytea path.
-- Switch to bytea, which lib/pq writes directly with `[]byte`. For OTA
-- payloads up to ~1 GB this is more than enough.
--
-- Idempotent: skip if already bytea or if there's existing data we'd
-- truncate.
--
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_name = 'ota_package' AND column_name = 'data'
       AND data_type = 'oid'
  ) AND NOT EXISTS (SELECT 1 FROM ota_package WHERE data IS NOT NULL) THEN
    ALTER TABLE ota_package ALTER COLUMN data DROP DEFAULT;
    ALTER TABLE ota_package ALTER COLUMN data TYPE bytea USING NULL;
  END IF;
END $$;
