DROP INDEX IF EXISTS idx_device_tenant_security_status;
ALTER TABLE device DROP CONSTRAINT IF EXISTS device_security_status_chk;
ALTER TABLE device DROP COLUMN IF EXISTS security_status;
