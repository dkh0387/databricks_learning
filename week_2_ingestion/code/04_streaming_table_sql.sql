-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 2 · Streaming Table via SQL (wraps Auto Loader)
-- MAGIC The SQL form for incremental ingestion. Drop this notebook into a **Spark Declarative Pipeline**
-- MAGIC and run an update.
-- MAGIC
-- MAGIC > **Heads-up.** This notebook mirrors the Spark Declarative Pipelines (SDP) concepts, but the
-- MAGIC > `CREATE OR REFRESH STREAMING TABLE` / `MATERIALIZED VIEW` statements may fail when run directly in a
-- MAGIC > notebook outside a pipeline or serverless context, e.g.:
-- MAGIC > `The operation CREATE is not allowed: Cannot CREATE the Streaming Table ... in Serverless Generic Compute
-- MAGIC > for your workspace. Enable it by enrolling in the Serverless Generic Compute Materialized View/Streaming
-- MAGIC > Table workspace feature preview. SQLSTATE: 42601`

-- COMMAND ----------

-- 1. ORDERS bronze — incremental from JSON
CREATE OR REFRESH STREAMING TABLE dea_learning.bronze.orders_bronze_sdp
COMMENT 'Raw orders, one row per order with a nested items array'
AS SELECT
     *,
     _metadata.file_path              AS source_file,
     _metadata.file_modification_time AS source_mtime
   FROM STREAM read_files(
     '/Volumes/dea_learning/raw/landing/orders',
     format              => 'json',
     schemaHints         => 'order_id BIGINT, customer_id BIGINT, amount DOUBLE, order_ts TIMESTAMP',
     schemaEvolutionMode => 'addNewColumns'
   );

-- COMMAND ----------

-- 2. CUSTOMERS bronze — incremental from CSV
CREATE OR REFRESH STREAMING TABLE dea_learning.bronze.customers_bronze_sdp
COMMENT 'Raw customer snapshot from CSV seed'
AS SELECT
     *,
     _metadata.file_path AS source_file
   FROM STREAM read_files(
     '/Volumes/dea_learning/raw/landing/customers',
     format       => 'csv',
     header       => true,
     schemaHints  => 'customer_id BIGINT, signup_date DATE'
   );

-- COMMAND ----------

-- 3. ITEMS bronze — small, batch refresh as a materialized view is simpler
CREATE OR REFRESH MATERIALIZED VIEW dea_learning.bronze.items_bronze_sdp
AS SELECT *
   FROM read_files(
     '/Volumes/dea_learning/raw/landing/items',
     format => 'csv',
     header => true
   );
