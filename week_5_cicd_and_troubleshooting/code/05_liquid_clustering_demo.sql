-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 5 · Liquid Clustering + Predictive Optimization on `silver_orders`
-- MAGIC Apply the optimization features to a real table from our medallion pipeline.

-- COMMAND ----------

USE CATALOG dea_learning;

-- 1. Re-create silver_orders with Liquid Clustering keys
CREATE OR REPLACE TABLE silver.silver_orders_lc (
  order_id        BIGINT,
  customer_id     BIGINT,
  order_ts        TIMESTAMP,
  order_date      DATE,
  status          STRING,
  currency        STRING,
  amount          DOUBLE,
  discount_amount DOUBLE
)
USING DELTA
CLUSTER BY (customer_id, order_date);

-- COMMAND ----------

-- Bulk-load 200k synthetic orders from existing silver
INSERT INTO silver.silver_orders_lc
SELECT
  id AS order_id,
  (id % 20) + 1 AS customer_id,
  timestampadd(SECOND, id, TIMESTAMP'2026-06-01 00:00:00') AS order_ts,
  date_add(DATE'2026-06-01', cast(id / 5000 AS INT)) AS order_date,
  CASE WHEN id % 4 = 0 THEN 'cancelled' ELSE 'placed' END AS status,
  CASE WHEN id % 3 = 0 THEN 'USD' ELSE 'EUR' END AS currency,
  rand() * 200 AS amount,
  0.0 AS discount_amount
FROM range(0, 200000);

-- 2. Inspect clustering metadata
DESCRIBE DETAIL silver.silver_orders_lc;   -- look for clusteringColumns

-- COMMAND ----------

-- 3. Incremental clustering work (new/changed files only)
OPTIMIZE silver.silver_orders_lc;

-- 4. Switch keys — full rewrite required after a key change
ALTER TABLE silver.silver_orders_lc CLUSTER BY (status, order_date);
OPTIMIZE silver.silver_orders_lc FULL;

DESCRIBE DETAIL silver.silver_orders_lc;

-- COMMAND ----------

-- 5. Let Predictive Optimization pick keys (UC managed + DBR 15.4 LTS+)
ALTER TABLE silver.silver_orders_lc CLUSTER BY AUTO;

-- Or disable clustering
-- ALTER TABLE silver.silver_orders_lc CLUSTER BY NONE;

-- COMMAND ----------

-- 6. Predictive Optimization at catalog level — applies to all UC managed Delta tables underneath
ALTER CATALOG dea_learning ENABLE PREDICTIVE OPTIMIZATION;

DESCRIBE EXTENDED silver.silver_orders_lc;   -- look for "Predictive Optimization" line

-- COMMAND ----------

-- 7. VACUUM (default 7-day retention)
VACUUM silver.silver_orders_lc;