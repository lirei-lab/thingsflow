DROP INDEX IF EXISTS idx_device_profile_provision_enabled;
ALTER TABLE device_profile DROP COLUMN IF EXISTS provision_device_secret_hash;
