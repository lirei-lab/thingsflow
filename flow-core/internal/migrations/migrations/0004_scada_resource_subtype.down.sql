UPDATE resource
   SET resource_type = 'SCADA_SYMBOL',
       resource_sub_type = NULL
 WHERE resource_type = 'IMAGE'
   AND resource_sub_type = 'SCADA_SYMBOL';
