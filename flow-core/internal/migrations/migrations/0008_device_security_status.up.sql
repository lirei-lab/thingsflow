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
