CREATE OR REFRESH STREAMING TABLE electroniz_catalog.electroniz_bronze_schema.electroniz_bronze_currency
AS SELECT *
FROM STREAM read_files(
  'abfss://raw@electronizlanding.dfs.core.windows.net/currency_conversion/',
  format => 'json',
  recursiveFileLookup => true
)
