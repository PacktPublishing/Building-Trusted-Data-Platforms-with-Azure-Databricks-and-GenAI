CREATE OR REFRESH STREAMING TABLE electroniz_catalog.electroniz_bronze_schema.electroniz_bronze_store_orders
AS SELECT *
FROM STREAM read_files(
  'abfss://raw@electronizlanding.dfs.core.windows.net/sales/\\[dbo\\].\\[store_orders\\]/',
  format => 'csv',
  schema => 'order_number integer, customer_id integer, product_id integer, order_date string, units integer, sale_price string, currency string, order_mode string, updated_at timestamp',
  header => false,
  recursiveFileLookup => true
)
