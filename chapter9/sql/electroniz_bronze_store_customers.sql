CREATE OR REFRESH STREAMING TABLE electroniz_catalog.electroniz_bronze_schema.electroniz_bronze_store_customers
AS SELECT *
FROM STREAM read_files(
  'abfss://raw@electronizlanding.dfs.core.windows.net/sales/\\[dbo\\].\\[store_customers\\]/',
  format => 'csv',
  schema => 'customer_id integer, customer_name string, address string, city string, postalcode string, country string, phone string, email string, credit_card string, updated_at timestamp',
  header => false,
  recursiveFileLookup => true
)
