ALTER TABLE device_profile
    ADD COLUMN IF NOT EXISTS provision_device_secret_hash varchar(255);

CREATE INDEX IF NOT EXISTS idx_device_profile_provision_enabled
    ON device_profile (provision_device_key, provision_type)
    WHERE provision_device_key IS NOT NULL;
