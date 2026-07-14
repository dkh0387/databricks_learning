-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bundle copy · `AUTO CDC INTO` — customers SCD Type 1 and SCD Type 2 (env-agnostic)
-- MAGIC Copy of `week_4_pipelines_and_jobs/code/02_pipeline_auto_cdc_scd2.sql`, deployed by the Week-5 bundle.
-- MAGIC `${catalog}` / `${landing_path}` come from the pipeline `configuration` block in `databricks.yml`.
-- MAGIC Consumes the CDC events uploaded by `src/setup_catalog_and_seed.py` (ingest_setup job task).
-- MAGIC
-- MAGIC Replaces the old `APPLY CHANGES INTO` DLT syntax. Place this notebook inside a Spark Declarative Pipeline.

-- COMMAND ----------

-- 1. Stream of CDC events from the landing volume
CREATE OR REFRESH STREAMING TABLE ${catalog}.bronze.cdc_customer_events
COMMENT 'Raw INSERT/UPDATE/DELETE events from the source DB CDC feed'
AS SELECT *
   FROM STREAM read_files(
     '${landing_path}/cdc',
     format       => 'json',
     schemaHints  => 'customer_id BIGINT, signup_date DATE, change_ts TIMESTAMP'
   );

-- COMMAND ----------

-- 2. Target SCD Type 1 — only the current state
CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.customers_scd1;

CREATE FLOW customers_scd1_flow AS AUTO CDC INTO ${catalog}.silver.customers_scd1
FROM STREAM ${catalog}.bronze.cdc_customer_events
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY change_ts
COLUMNS * EXCEPT (op, change_ts)
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- 3. Target SCD Type 2 — full change history with __START_AT / __END_AT validity windows
-- CDF enabled: AUTO CDC mutates this table (upserts, deletes, __END_AT updates), so it cannot be
-- consumed with STREAM(...) downstream. The Change Data Feed re-encodes those mutations as an
-- append-only feed of change events — consumed in 08_cdf_downstream_consumer.py.
CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.customers_scd2
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE FLOW customers_scd2_flow AS AUTO CDC INTO ${catalog}.silver.customers_scd2
FROM STREAM ${catalog}.bronze.cdc_customer_events
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY change_ts
COLUMNS * EXCEPT (op, change_ts)
STORED AS SCD TYPE 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC After the update completes, query the targets:
-- MAGIC ```sql
-- MAGIC SELECT * FROM ${catalog}.silver.customers_scd1 ORDER BY customer_id;
-- MAGIC
-- MAGIC SELECT customer_id, name, email, tier, __START_AT, __END_AT
-- MAGIC FROM   ${catalog}.silver.customers_scd2
-- MAGIC WHERE  customer_id IN (13, 21, 22)
-- MAGIC ORDER BY customer_id, __START_AT;
-- MAGIC ```
-- MAGIC * `customer_id = 13` (Maya Patel) — two SCD2 rows: the original Week 2 seed + email change.
-- MAGIC * `customer_id = 6` (Faisal Khan) — INSERTed then DELETEd. **Removed** from SCD1; SCD2 retains the inserted row with `__END_AT` set to the deletion time.
-- MAGIC * `customer_id = 21` (Ulrich Becker) — INSERT then UPDATE → two SCD2 rows.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Going further downstream — why CDF?
-- MAGIC `customers_scd2` is the target of an AUTO CDC flow, so it **mutates** and cannot be a streaming source
-- MAGIC (`STREAM(customers_scd2)` fails — not append-only). Options downstream:
-- MAGIC * **MV** — if downstream only needs current/aggregated state: `... FROM customers_scd2 WHERE __END_AT IS NULL`
-- MAGIC   (MV refresh reconciles, copes with mutations — no CDF needed).
-- MAGIC * **CDF** — if downstream must *stream*: the `delta.enableChangeDataFeed` property above makes the table
-- MAGIC   publish its mutations as append-only change events (`_change_type`, `_commit_version`).
-- MAGIC   Consumer example: `08_cdf_downstream_consumer.py` (readChangeFeed → `foreachBatch` + MERGE).
-- MAGIC
-- MAGIC Same trick one level down the chain: volume events (append-only) → AUTO CDC applies → SCD2 mutates →
-- MAGIC CDF re-encodes the mutations as an append-only log → downstream stream reads → MERGE applies at the next target.