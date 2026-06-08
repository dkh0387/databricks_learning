-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 2 · COPY INTO
-- MAGIC Idempotent incremental file load into a Delta table.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS main.learn;
CREATE VOLUME IF NOT EXISTS main.learn.landing;

-- COMMAND ----------

-- Seed the landing volume with two JSON files
-- MAGIC %python
-- MAGIC import json, time, os
-- MAGIC base = "/Volumes/main/learn/landing/orders"
-- MAGIC dbutils.fs.mkdirs(base)
-- MAGIC dbutils.fs.put(f"{base}/orders_2026-06-01.json",
-- MAGIC   '{"id":1,"amount":9.99,"region":"EU"}\n{"id":2,"amount":19.99,"region":"US"}',
-- MAGIC   overwrite=True)
-- MAGIC dbutils.fs.put(f"{base}/orders_2026-06-02.json",
-- MAGIC   '{"id":3,"amount":4.99,"region":"EU"}',
-- MAGIC   overwrite=True)

-- COMMAND ----------

-- Empty target table
CREATE OR REPLACE TABLE main.learn.orders_bronze (
  id      BIGINT,
  amount  DOUBLE,
  region  STRING
) USING DELTA;

-- COMMAND ----------

-- Load — first run picks up both files
COPY INTO main.learn.orders_bronze
FROM '/Volumes/main/learn/landing/orders'
FILEFORMAT = JSON
FORMAT_OPTIONS ('inferSchema' = 'true')
COPY_OPTIONS  ('mergeSchema' = 'true');

SELECT count(*) AS rows FROM main.learn.orders_bronze;

-- COMMAND ----------

-- Re-run is a no-op (idempotency)
COPY INTO main.learn.orders_bronze
FROM '/Volumes/main/learn/landing/orders'
FILEFORMAT = JSON;

SELECT count(*) AS rows FROM main.learn.orders_bronze;   -- still the same count

-- COMMAND ----------

-- Drop a new file and re-run — only that file ingests
-- MAGIC %python
-- MAGIC dbutils.fs.put("/Volumes/main/learn/landing/orders/orders_2026-06-03.json",
-- MAGIC   '{"id":4,"amount":29.99,"region":"DE"}',
-- MAGIC   overwrite=True)

COPY INTO main.learn.orders_bronze
FROM '/Volumes/main/learn/landing/orders'
FILEFORMAT = JSON;

SELECT * FROM main.learn.orders_bronze ORDER BY id;

-- COMMAND ----------

-- Force re-process of every file
COPY INTO main.learn.orders_bronze
FROM '/Volumes/main/learn/landing/orders'
FILEFORMAT = JSON
COPY_OPTIONS ('force' = 'true');