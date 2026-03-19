CREATE OR REFRESH STREAMING TABLE electroniz_catalog.electroniz_bronze_schema.electroniz_bronze_products
AS SELECT *
FROM STREAM read_files(
  'abfss://raw@electronizlanding.dfs.core.windows.net/sales/\\[dbo\\].\\[products\\]/',
  format => 'csv',
  schema => 'product_code string, product_name string, product_category string, updated_at timestamp',
  header => false,
  recursiveFileLookup => true
)
