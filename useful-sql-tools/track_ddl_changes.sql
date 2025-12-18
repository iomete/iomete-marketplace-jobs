SELECT 
  regexp_extract(resourcePath, '^([^\\.\\/]+)', 1) AS catalog,
  regexp_extract(resourcePath, '^[^\\.\\/]+[\\.^\\/]+([^\\.\\/\\$]+)', 1) AS schema,
  regexp_extract(resourcePath, '^[^\\.\\/]+[\\.^\\/]+[^\\.\\/\\$]+[\\.^\\/\\$]+([^\\.\\/\\$]+)', 1) AS table,
  user,
  action, 
  eventTime as event_time
FROM spark_catalog.iomete_system_db.iomete_spark_audit_external_table 
WHERE 
  day >= '20251125' AND 
  resourceType = 'table' AND 
  accessResult = 1 AND
  action in ('create', 'drop', 'alter')
ORDER BY resourcePath;