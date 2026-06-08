-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 4 · Spark Declarative Pipeline — bronze → silver
-- MAGIC Create a new Pipeline in the UI, point it at this notebook, and run an update. Streaming table bronze + materialized view silver.

-- COMMAND ----------

-- Bronze: incremental ingest from a landing volume via Auto Loader
CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT 'Raw orders from landing'
AS SELECT *,
          _metadata.file_path              AS source_file,
          _metadata.file_modification_time AS source_mtime
   FROM STREAM read_files(
     '/Volumes/main/learn/landing/orders',
     format        => 'json',
     schemaHints   => 'id BIGINT, amount DOUBLE'
   );

-- COMMAND ----------

-- Silver: cleanse with data-quality expectations
CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT positive_amount  CHECK (amount > 0),                              -- WARN
  CONSTRAINT non_null_id      CHECK (id IS NOT NULL)      ON VIOLATION DROP ROW,
  CONSTRAINT known_region     CHECK (region IN ('EU','US','APAC')) ON VIOLATION DROP ROW
)
COMMENT 'Cleansed orders'
AS SELECT
     id,
     amount,
     upper(region) AS region,
     to_date(order_date, 'yyyy-MM-dd') AS order_date
   FROM STREAM(bronze_orders);

-- COMMAND ----------

-- Gold: pipeline-managed materialized view
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue
COMMENT 'Daily revenue per region'
AS SELECT order_date, region, sum(amount) AS revenue
   FROM   silver_orders
   GROUP BY order_date, region;