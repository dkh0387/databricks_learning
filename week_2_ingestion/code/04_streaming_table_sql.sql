-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 2 · Streaming Table via SQL (wraps Auto Loader)
-- MAGIC The SQL form for incremental ingestion — runs inside a **Spark Declarative Pipeline** (or as a scheduled streaming
-- MAGIC table outside a pipeline on serverless SQL).

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS main.learn;
CREATE VOLUME IF NOT EXISTS main.learn.landing;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Inside a Spark Declarative Pipeline
-- MAGIC Create a new pipeline in the UI ("New ETL pipeline"), point it at this notebook, and run the update.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE main.learn.orders_bronze_sdp
COMMENT 'Bronze layer — raw orders from landing volume'
AS SELECT
     *,
     _metadata.file_path                    AS source_file,
     _metadata.file_modification_time       AS source_mtime
   FROM STREAM read_files(
     '/Volumes/main/learn/landing/sdp_orders',
     format              => 'json',
     schemaHints         => 'amount DOUBLE, id BIGINT',
     schemaEvolutionMode => 'addNewColumns'
   );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Outside a pipeline — scheduled streaming table on serverless SQL
-- MAGIC The same syntax with a `SCHEDULE` clause runs as a scheduled refresh.

-- COMMAND ----------

-- Example (uncomment if you have serverless SQL with this feature enabled):
-- CREATE OR REFRESH STREAMING TABLE main.learn.orders_bronze_scheduled
-- SCHEDULE EVERY 1 HOUR
-- AS SELECT *
-- FROM STREAM read_files(
--   '/Volumes/main/learn/landing/sdp_orders',
--   format => 'json'
-- );