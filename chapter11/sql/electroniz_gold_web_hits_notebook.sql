-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Data Product: Web Hits by Country
-- MAGIC **Domain:** Marketing  
-- MAGIC **Target Schema:** `electroniz_catalog.electroniz_gold_marketing_schema`  
-- MAGIC **Source:** `electroniz_catalog.electroniz_silver_schema.electroniz_silver_logs_geolocation`  
-- MAGIC **Mode:** Continuous Streaming

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW electroniz_product_web_hits_by_country (
  country_code COMMENT 'ISO country code identifying the geographic origin of the web hit',
  country_name COMMENT 'Full country name corresponding to the ISO country code',
  total_hits COMMENT 'Total number of web server hits originating from this country',
  unique_visitors COMMENT 'Count of distinct IP addresses representing unique visitors from this country',
  unique_pages_requested COMMENT 'Count of distinct URL paths requested by visitors from this country',
  first_hit_time COMMENT 'Timestamp of the earliest recorded web hit from this country',
  last_hit_time COMMENT 'Timestamp of the most recent recorded web hit from this country'
)
COMMENT 'Gold-layer materialized view aggregating web server hits by country for marketing analytics. Provides geographic traffic distribution, unique visitor counts, and page engagement metrics per country. Part of the Web Hits by Country data product in the marketing domain.'
TBLPROPERTIES (
  'quality' = 'gold',
  'domain' = 'marketing',
  'data_product' = 'web hits by country'
)
AS
SELECT
  country_code,
  country_name,
  COUNT(*) AS total_hits,
  COUNT(DISTINCT remote_ip) AS unique_visitors,
  COUNT(DISTINCT request) AS unique_pages_requested,
  MIN(event_time) AS first_hit_time,
  MAX(event_time) AS last_hit_time
FROM electroniz_catalog.electroniz_silver_schema.electroniz_silver_logs_geolocation
GROUP BY country_code, country_name