-- The first SCADA symbol seed wrote rows without a `descriptor`. The
-- TB UI parses the embedded <tb:metadata> JSON inside each SVG to
-- decide how to preview, label, and animate symbols — without it the
-- gallery shows empty rows.
--
-- Drop the existing system-scoped SCADA rows so the bridge reseeds
-- them on next start with the full descriptor populated. Tenants
-- never reference these IDs directly (the symbols are looked up by
-- resource_key in widget configs) so deletion is safe.
DELETE FROM resource
 WHERE tenant_id = '13814000-1dd2-11b2-8080-808080808080'
   AND resource_type = 'IMAGE'
   AND resource_sub_type = 'SCADA_SYMBOL'
   AND descriptor IS NULL;
