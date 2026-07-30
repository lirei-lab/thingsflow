-- The first descriptor backfill stuffed the entire <tb:metadata> JSON
-- (behavior, tags, properties, …) into resource.descriptor. The TB UI
-- expects descriptor to follow the ImageDescriptor schema (mediaType,
-- width, height, size, etag) — the SCADA metadata stays inside the SVG
-- and is parsed at runtime by the symbol viewer.
--
-- Drop the rows so the bridge reseeds them with a correct descriptor
-- on next start. Only system-scoped SCADA seed rows are touched.
DELETE FROM resource
 WHERE tenant_id = '13814000-1dd2-11b2-8080-808080808080'
   AND resource_type = 'IMAGE'
   AND resource_sub_type = 'SCADA_SYMBOL'
   AND (descriptor IS NULL OR NOT (descriptor::jsonb ? 'mediaType'));
