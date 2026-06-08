-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 4 · `AUTO CDC INTO` — customers SCD Type 1 and SCD Type 2
-- MAGIC Consumes the CDC events from `week_4_pipelines_and_jobs/data/cdc/customers_cdc_events.json`
-- MAGIC (uploaded into the landing volume by Week 2's `00_setup_catalog_and_seed.py`).
-- MAGIC
-- MAGIC Replaces the old `APPLY CHANGES INTO` DLT syntax. Place this notebook inside a Spark Declarative Pipeline.

-- COMMAND ----------

-- 1. Stream of CDC events from the landing volume
CREATE OR REFRESH STREAMING TABLE cdc_customer_events
COMMENT 'Raw INSERT/UPDATE/DELETE events from the source DB CDC feed'
AS SELECT *
   FROM STREAM read_files(
     '/Volumes/dea_learning/raw/landing/cdc',
     format       => 'json',
     schemaHints  => 'customer_id BIGINT, signup_date DATE, change_ts TIMESTAMP'
   );

-- COMMAND ----------

-- 2. Target SCD Type 1 — only the current state
CREATE OR REFRESH STREAMING TABLE customers_scd1;

CREATE FLOW customers_scd1_flow AS AUTO CDC INTO customers_scd1
FROM STREAM cdc_customer_events
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY change_ts
COLUMNS * EXCEPT (op, change_ts)
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- 3. Target SCD Type 2 — full change history with __START_AT / __END_AT validity windows
CREATE OR REFRESH STREAMING TABLE customers_scd2;

CREATE FLOW customers_scd2_flow AS AUTO CDC INTO customers_scd2
FROM STREAM cdc_customer_events
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY change_ts
COLUMNS * EXCEPT (op, change_ts)
STORED AS SCD TYPE 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC After the update completes, query the targets:
-- MAGIC ```sql
-- MAGIC SELECT * FROM customers_scd1 ORDER BY customer_id;
-- MAGIC
-- MAGIC SELECT customer_id, name, email, tier, __START_AT, __END_AT
-- MAGIC FROM   customers_scd2
-- MAGIC WHERE  customer_id IN (13, 21, 22)
-- MAGIC ORDER BY customer_id, __START_AT;
-- MAGIC ```
-- MAGIC * `customer_id = 13` (Maya Patel) — two SCD2 rows: the original Week 2 seed + email change.
-- MAGIC * `customer_id = 6` (Faisal Khan) — present in SCD1 only as deleted; SCD2 preserves the prior row with `__END_AT` set.
-- MAGIC * `customer_id = 21` (Ulrich Becker) — INSERT then UPDATE → two SCD2 rows.