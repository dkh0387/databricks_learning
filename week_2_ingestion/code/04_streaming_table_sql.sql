-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 2 · Streaming Table via SQL (wraps Auto Loader)
-- MAGIC The SQL form for incremental ingestion. Drop this notebook into a **Spark Declarative Pipeline**
-- MAGIC and run an update.
-- MAGIC
-- MAGIC > **Heads-up.** This notebook mirrows `04_streaming_table_sql` ETL pipeline, all `CREATE OR REFRESH STREAMING TABLE` operations could fail by running them inside the notebook due to: `The operation CREATE is not allowed: Cannot CREATE the Streaming Table `dea_learning`.`bronze`.`orders_bronze_sdp` in Serverless Generic Compute for your workspace. Enable it by enrolling in the Serverless Generic Compute Materialized View/Streaming Table workspace feature preview. If you do not see the beta feature preview available in workspace, please contact your Databricks representative. SQLSTATE: 42601`

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
CREATE OR REFRESH STREAMING TABLE dea_learning.bronze.customers_bronze
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
CREATE OR REFRESH MATERIALIZED VIEW dea_learning.bronze.items_bronze
AS SELECT *
   FROM read_files(
     '/Volumes/dea_learning/raw/landing/items',
     format => 'csv',
     header => true
   );
