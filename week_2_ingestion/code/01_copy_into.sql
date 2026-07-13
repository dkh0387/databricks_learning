-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 2 · COPY INTO — load `customers` and `items` to bronze
-- MAGIC Idempotent incremental load of two seed datasets.
-- MAGIC Run `00_setup_catalog_and_seed.py` first so the files land in `/Volumes/dea_learning/raw/landing/`.

-- COMMAND ----------

-- 1. CUSTOMERS — CSV → bronze table
CREATE OR REPLACE TABLE dea_learning.bronze.customers_bronze (
  customer_id   BIGINT,
  name          STRING,
  email         STRING,
  country       STRING,
  signup_date   DATE,
  tier          STRING
) USING DELTA;

-- NOTE: table was created with customer_id as BIGINT, not INT
--       so we need to cast it here
COPY INTO dea_learning.bronze.customers_bronze
FROM (
  SELECT
    customer_id::BIGINT AS customer_id,
    name,
    email,
    country,
    signup_date::DATE AS signup_date,
    tier
  FROM '/Volumes/dea_learning/raw/landing/customers'
)
FILEFORMAT = CSV
-- inferSchema: sample the CSV and derive real column types instead of all-STRING
-- (costs an extra read pass; the ::BIGINT / ::DATE casts above would cover it too)
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'delimiter' = ',')
COPY_OPTIONS  ('mergeSchema' = 'false');

SELECT count(*) AS rows FROM dea_learning.bronze.customers_bronze;
SELECT * FROM dea_learning.bronze.customers_bronze ORDER BY customer_id LIMIT 5;

-- COMMAND ----------

-- 2. Idempotency — re-running picks up nothing new
COPY INTO dea_learning.bronze.customers_bronze
FROM (
  SELECT
    customer_id::BIGINT AS customer_id,
    name,
    email,
    country,
    signup_date::DATE AS signup_date,
    tier
  FROM '/Volumes/dea_learning/raw/landing/customers'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true');

SELECT count(*) AS rows FROM dea_learning.bronze.customers_bronze;   -- same as before

-- COMMAND ----------

-- 3. ITEMS — CSV → bronze table
CREATE OR REPLACE TABLE dea_learning.bronze.items_bronze (
  item_id    STRING,
  name       STRING,
  category   STRING,
  price      DOUBLE,
  in_stock   BOOLEAN
) USING DELTA;

COPY INTO dea_learning.bronze.items_bronze
FROM (
  SELECT
    item_id,
    name,
    category,
    price::DOUBLE AS price,
    in_stock::BOOLEAN AS in_stock
  FROM '/Volumes/dea_learning/raw/landing/items'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true');

SELECT * FROM dea_learning.bronze.items_bronze ORDER BY item_id;

-- COMMAND ----------

-- 4. Force re-process every file (`force=true`) — useful for full backfill
COPY INTO dea_learning.bronze.customers_bronze
FROM (
  SELECT
    customer_id::BIGINT AS customer_id,
    name,
    email,
    country,
    signup_date::DATE AS signup_date,
    tier
  FROM '/Volumes/dea_learning/raw/landing/customers'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
COPY_OPTIONS  ('force' = 'true');

-- COMMAND ----------

-- 5. One-shot alternative — CREATE TABLE AS read_files()
-- Equivalent to a single COPY INTO for a known set of files. Not incremental.
CREATE OR REPLACE TABLE dea_learning.bronze.items_bronze_v2 AS
SELECT
  *
FROM
  (
    SELECT
      item_id,
      name,
      category,
      price::DOUBLE AS price,
      in_stock::BOOLEAN AS in_stock
    FROM
      read_files('/Volumes/dea_learning/raw/landing/items', format => 'csv', header => true)
  );
