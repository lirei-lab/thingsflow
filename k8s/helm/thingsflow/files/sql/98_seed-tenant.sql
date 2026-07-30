--
-- Default tenant + tenant admin + default device profile.
-- Idempotent: re-running on an existing DB is a no-op (ON CONFLICT DO NOTHING).
--
-- Credentials seeded:
--   sysadmin@thingsboard.org / sysadmin    (already set in 99_system-data.sql)
--   tenant@thingsboard.org   / tenant      (this file)
--
-- The bcrypt hash below corresponds to the literal string "tenant" with cost=10.
-- Regenerate with: htpasswd -bnBC 10 "" tenant | tr -d ':\n'
-- (replace the leading $2y$ with $2a$ for jBCrypt compatibility).
--

-- Default tenant profile ----------------------------------------------------
INSERT INTO tenant_profile (id, created_time, name, profile_data, description, is_default, isolated_tb_core, isolated_tb_rule_engine)
VALUES (
  '13814000-1dd2-11b2-8080-808080808080',
  1592576748000,
  'Default',
  '{"configuration":{"type":"DEFAULT"}}'::jsonb,
  'Default tenant profile',
  true, false, false
) ON CONFLICT (id) DO NOTHING;

-- Default tenant ------------------------------------------------------------
INSERT INTO tenant (id, created_time, tenant_profile_id, title, region, email)
VALUES (
  'aaaaaaaa-1dd2-11b2-8080-808080808080',
  1592576748000,
  '13814000-1dd2-11b2-8080-808080808080',
  'Default Tenant',
  'Global',
  'tenant@thingsboard.org'
) ON CONFLICT (id) DO NOTHING;

-- Tenant admin user ---------------------------------------------------------
INSERT INTO tb_user (id, created_time, tenant_id, customer_id, authority, email, first_name, last_name)
VALUES (
  'bbbbbbbb-2222-3333-4444-555555555555',
  1592576748000,
  'aaaaaaaa-1dd2-11b2-8080-808080808080',
  '13814000-1dd2-11b2-8080-808080808080',
  'TENANT_ADMIN',
  'tenant@thingsboard.org',
  'Tenant', 'Admin'
) ON CONFLICT (id) DO NOTHING;

INSERT INTO user_credentials (id, created_time, enabled, password, user_id, activate_token, reset_token)
VALUES (
  'cccccccc-2222-3333-4444-555555555555',
  1592576748000,
  true,
  '$2a$10$gtCvxNxBvHhoXy6QJNKmbOfvSty3uW/yDHt1vaH0Ox32o/H3g4tlW',
  'bbbbbbbb-2222-3333-4444-555555555555',
  NULL, NULL
) ON CONFLICT (user_id) DO NOTHING;

-- Default device profile for the default tenant ----------------------------
INSERT INTO device_profile (id, created_time, tenant_id, name, type, transport_type, profile_data, is_default, description)
VALUES (
  'dddddddd-1dd2-11b2-8080-808080808080',
  1592576748000,
  'aaaaaaaa-1dd2-11b2-8080-808080808080',
  'default',
  'DEFAULT',
  'DEFAULT',
  '{"configuration":{"type":"DEFAULT"},"transportConfiguration":{"type":"DEFAULT"},"alarms":[]}'::jsonb,
  true,
  'Default device profile'
) ON CONFLICT (id) DO NOTHING;

-- Default asset profile for the default tenant ------------------------------
-- TB classic auto-creates this on first tenant access; we ship it preinstalled
-- so the UI's Asset Profiles page is non-empty and asset creates have a
-- profile to reference without an extra round trip.
INSERT INTO asset_profile (id, created_time, tenant_id, name, is_default, description)
VALUES (
  'eeeeeeee-1dd2-11b2-8080-808080808080',
  1592576748000,
  'aaaaaaaa-1dd2-11b2-8080-808080808080',
  'default',
  true,
  'Default asset profile'
) ON CONFLICT (id) DO NOTHING;
