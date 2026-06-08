-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 2 · Streaming Table via SQL (wraps Auto Loader)
-- MAGIC The SQL form for incremental ingestion. Drop this notebook into a **Spark Declarative Pipeline**
-- MAGIC and run an update.
-- MAGIC
-- MAGIC > **Heads-up.** This notebook targets `dea_learning.bronze.orders_bronze`, the same table that
-- MAGIC > `02_autoloader_incremental_batch.py` writes to via the Python API. Run one or the other in isolation, or
-- MAGIC > rename the target here, e.g. `orders_bronze_sdp`, to keep both around.

-- COMMAND ----------

-- 1. ORDERS bronze — incremental from JSON
CREATE OR REFRESH STREAMING TABLE dea_learning.bronze.orders_bronze
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