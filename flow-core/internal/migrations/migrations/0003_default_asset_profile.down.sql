-- Down migration is intentionally left as a no-op. We never want to delete
-- a tenant's default asset_profile automatically — assets reference it via
-- asset_profile_id and dropping it would orphan rows.
SELECT 1;
