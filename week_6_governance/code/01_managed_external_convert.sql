-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 6 · Managed ↔ External — applied to an `orders_archive` table
-- MAGIC Showcases the conversion on a realistic scenario: an archive table whose files we want to keep around
-- MAGIC after the table itself is dropped (external), and later promote into managed status without downtime.
-- MAGIC Requires DBR 17.0+ or serverless; Delta format.

-- COMMAND ----------

USE CATALOG dea_learning;

-- For a clean "files outlive table" lesson, prefer an EXTERNAL volume (cloud path you own).
-- The managed-volume fallback below works for this conversion demo because the table itself
-- is external — but the volume's own UC-managed lifecycle is unrelated to the table.
-- CREATE EXTERNAL VOLUME IF NOT EXISTS dea_learning.raw.archive
--   LOCATION '<CLOUD_PATH>/archive'
--   WITH (STORAGE CREDENTIAL <STORAGE_CREDENTIAL>);
CREATE VOLUME IF NOT EXISTS dea_learning.raw.archive;   -- fallback: managed volume

-- COMMAND ----------

-- 1. Start with an EXTERNAL Delta table pointing at the archive volume
CREATE OR REPLACE TABLE silver.orders_archive (
  order_id   BIGINT,
  customer_id BIGINT,
  amount     DOUBLE,
  order_ts   TIMESTAMP
)
USING DELTA
LOCATION '/Volumes/dea_learning/raw/archive/orders';

INSERT INTO silver.orders_archive
SELECT order_id, customer_id, amount, order_ts
FROM   bronze.orders_bronze;

DESCRIBE EXTENDED silver.orders_archive;     -- Type = EXTERNAL

-- COMMAND ----------

-- 2. Promote to MANAGED (no downtime; keeps history, name, perms, views)
ALTER TABLE silver.orders_archive SET MANAGED;

DESCRIBE EXTENDED silver.orders_archive;     -- Type = MANAGED

-- COMMAND ----------

-- 3. Roll back to EXTERNAL at a different location
ALTER TABLE silver.orders_archive UNSET MANAGED
  LOCATION '/Volumes/dea_learning/raw/archive/orders_back';

DESCRIBE EXTENDED silver.orders_archive;     -- Type = EXTERNAL again

-- COMMAND ----------

-- 4. Files behind the EXTERNAL table survive a DROP
DROP TABLE silver.orders_archive;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/Volumes/dea_learning/raw/archive/orders_back"))

-- COMMAND ----------

-- 5. Re-attach without re-writing data
CREATE TABLE silver.orders_archive
USING DELTA
LOCATION '/Volumes/dea_learning/raw/archive/orders_back';

SELECT count(*) AS recovered_rows FROM silver.orders_archive;