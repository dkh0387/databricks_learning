-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 6 · Managed ↔ External — applied to an `orders_archive` table
-- MAGIC Showcases the conversion on a realistic scenario: an archive table whose files we want to keep around
-- MAGIC after the table itself is dropped (external), and later promote into managed status without downtime.
-- MAGIC Requires DBR 17.3 LTS+ or serverless; Delta format.

-- COMMAND ----------

USE CATALOG dea_learning;

-- 1. Start with an EXTERNAL Delta table at a cloud path you own.
-- The path must live under a configured UC external location (external location +
-- storage credential set up by an admin). Table locations must NOT overlap volumes —
-- a LOCATION under /Volumes/... is invalid.
CREATE OR REPLACE TABLE silver.orders_archive (
  order_id   BIGINT,
  customer_id BIGINT,
  amount     DOUBLE,
  order_ts   TIMESTAMP
)
USING DELTA
LOCATION 's3://<your-external-location-bucket>/archive/orders';

INSERT INTO silver.orders_archive
SELECT order_id, customer_id, amount, order_ts
FROM   bronze.orders_bronze;

DESCRIBE EXTENDED silver.orders_archive;     -- Type = EXTERNAL

-- COMMAND ----------

-- 2. Promote to MANAGED (no downtime; keeps history, name, perms, views)
ALTER TABLE silver.orders_archive SET MANAGED;

DESCRIBE EXTENDED silver.orders_archive;     -- Type = MANAGED

-- COMMAND ----------

-- 3. Roll back to EXTERNAL — no location clause: UNSET MANAGED only reverts a prior
-- SET MANAGED (within 14 days) back to the ORIGINAL external location
ALTER TABLE silver.orders_archive UNSET MANAGED;

DESCRIBE EXTENDED silver.orders_archive;     -- Type = EXTERNAL again

-- COMMAND ----------

-- 4. Files behind the EXTERNAL table survive a DROP
DROP TABLE silver.orders_archive;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("s3://<your-external-location-bucket>/archive/orders"))

-- COMMAND ----------

-- 5. Re-attach without re-writing data (path must be under a configured UC external location)
CREATE TABLE silver.orders_archive
USING DELTA
LOCATION 's3://<your-external-location-bucket>/archive/orders';

SELECT count(*) AS recovered_rows FROM silver.orders_archive;