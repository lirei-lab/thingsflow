-- Re-classify previously-seeded SCADA symbols. We initially stored them
-- with resource_type='SCADA_SYMBOL' (no sub_type), but the TB UI filters
-- the SCADA Symbols gallery by resource_type='IMAGE' AND
-- resource_sub_type='SCADA_SYMBOL'. This migration moves them to the
-- shape the UI expects.
UPDATE resource
   SET resource_type = 'IMAGE',
       resource_sub_type = 'SCADA_SYMBOL'
 WHERE resource_type = 'SCADA_SYMBOL';
