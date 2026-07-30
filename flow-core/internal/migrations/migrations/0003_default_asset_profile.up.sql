-- Backfill the default asset_profile that 98_seed-tenant.sql now creates on
-- fresh installs. Existing tenants that came up before this seed need it
-- too — the UI's Asset Profiles page expects at least one row and asset
-- creates resolve `is_default = true` to fill in `asset_profile_id`.
--
-- One default profile per tenant. Skip tenants that already have one.
INSERT INTO asset_profile (id, created_time, tenant_id, name, is_default, description)
SELECT
    gen_random_uuid(),
    EXTRACT(EPOCH FROM now()) * 1000,
    t.id,
    'default',
    true,
    'Default asset profile (backfilled)'
  FROM tenant t
 WHERE NOT EXISTS (
       SELECT 1 FROM asset_profile ap
        WHERE ap.tenant_id = t.id
          AND ap.is_default = true
 );
