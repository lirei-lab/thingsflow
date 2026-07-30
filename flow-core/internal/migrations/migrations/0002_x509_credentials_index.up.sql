-- Speed up X.509 cert auth lookups by ensuring the partial index on
-- (credentials_type, credentials_id) is in place. The baseline schema
-- already has a UNIQUE on credentials_id alone, but a partial index
-- restricted to X509 rows is cheaper to traverse on installs that have
-- many ACCESS_TOKEN rows.
CREATE INDEX IF NOT EXISTS idx_device_credentials_x509
    ON device_credentials (credentials_id)
    WHERE credentials_type = 'X509_CERTIFICATE';
