CREATE OR REFRESH STREAMING TABLE electroniz_catalog.electroniz_bronze_schema.electroniz_bronze_product_reviews
AS
WITH raw_messages AS (
  SELECT
    CAST(Body AS STRING) AS body_str,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
  FROM STREAM read_files(
    'abfss://raw@electronizlanding.dfs.core.windows.net/electroniz-ecommerce-ns/reviews/',
    format => 'avro',
    recursiveFileLookup => true
  )
),
extracted AS (
  SELECT
    get_json_object(body_str, '$[0].data') AS data_json,
    _source_file,
    _ingested_at
  FROM raw_messages
)
SELECT
  from_json(data_json, 'map<string, string>') AS data,
  _source_file,
  _ingested_at
FROM extracted
