CREATE OR REFRESH STREAMING TABLE electroniz_catalog.electroniz_bronze_schema.electroniz_bronze_inventory
AS SELECT *
FROM STREAM read_files(
  'abfss://raw@electronizlanding.dfs.core.windows.net/sales/\\[dbo\\].\\[inventory\\]/',
  format => 'csv',
  schema => 'inventory_date string, product string, inventory integer, updated_at timestamp',
  header => false,
  recursiveFileLookup => true
)
