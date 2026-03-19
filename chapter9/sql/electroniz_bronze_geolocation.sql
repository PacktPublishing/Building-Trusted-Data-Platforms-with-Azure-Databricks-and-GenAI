CREATE OR REFRESH STREAMING TABLE electroniz_catalog.electroniz_bronze_schema.electroniz_bronze_geolocation
AS SELECT *
FROM STREAM read_files(
  'abfss://raw@electronizlanding.dfs.core.windows.net/geolocation/',
  format => 'csv',
  schema => 'ip1 string, ip2 string, country_code string, country_name string',
  header => false,
  recursiveFileLookup => true
)
