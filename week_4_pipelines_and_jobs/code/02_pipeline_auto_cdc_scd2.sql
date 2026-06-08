-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 4 · `AUTO CDC INTO` — SCD Type 1 and SCD Type 2
-- MAGIC Modern declarative pipelines syntax (replaces DLT's `APPLY CHANGES INTO`).
-- MAGIC Place this notebook inside a Spark Declarative Pipeline.

-- COMMAND ----------

-- Source: a streaming table of CDC events.
-- In a real pipeline this comes from a Lakeflow Connect managed connector or from `STREAM read_files(...)`.
CREATE OR REFRESH STREAMING TABLE cdc_events
AS SELECT * FROM STREAM read_files(
     '/Volumes/main/learn/landing/cdc_events',
     format        => 'json',
     schemaHints   => 'id BIGINT, name STRING, email STRING, country STRING, op STRING, change_ts TIMESTAMP'
   );

-- COMMAND ----------

-- Target for SCD Type 1 — current snapshot
CREATE OR REFRESH STREAMING TABLE customers_scd1;

CREATE FLOW customers_scd1_flow AS AUTO CDC INTO customers_scd1
FROM STREAM cdc_events
KEYS (id)
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY change_ts
COLUMNS * EXCEPT (op, change_ts)
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- Target for SCD Type 2 — history-preserving with __START_AT / __END_AT
CREATE OR REFRESH STREAMING TABLE customers_scd2;

CREATE FLOW customers_scd2_flow AS AUTO CDC INTO customers_scd2
FROM STREAM cdc_events
KEYS (id)
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY change_ts
COLUMNS * EXCEPT (op, change_ts)
STORED AS SCD TYPE 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC After running an update, inspect:
-- MAGIC * `customers_scd1` — one row per id, latest values.
-- MAGIC * `customers_scd2` — multiple rows per id; current row has `__END_AT IS NULL`.