-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 5 · Liquid Clustering + Predictive Optimization
-- MAGIC Hands-on with the optimization features the exam asks about.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS main.learn;
USE main.learn;

-- 1. Create a Liquid-clustered table with explicit keys
CREATE OR REPLACE TABLE events_lc (
  id         BIGINT,
  user_id    BIGINT,
  event_type STRING,
  event_ts   TIMESTAMP,
  payload    STRING
)
USING DELTA
CLUSTER BY (user_id, event_ts);

-- COMMAND ----------

-- Bulk-load some data
INSERT INTO events_lc
SELECT
  id,
  (id % 1000) AS user_id,
  CASE WHEN id % 3 = 0 THEN 'click' WHEN id % 3 = 1 THEN 'view' ELSE 'purchase' END AS event_type,
  timestampadd(SECOND, id, TIMESTAMP'2026-06-01 00:00:00'),
  repeat('x', 100)
FROM range(0, 100000);

-- 2. Inspect clustering metadata
DESCRIBE DETAIL events_lc;     -- look for clusteringColumns

-- COMMAND ----------

-- 3. Incremental clustering work (touches new/changed files only)
OPTIMIZE events_lc;

-- 4. Switch keys — must do a FULL OPTIMIZE to materialize the new layout
ALTER TABLE events_lc CLUSTER BY (event_type, event_ts);
OPTIMIZE events_lc FULL;

DESCRIBE DETAIL events_lc;

-- COMMAND ----------

-- 5. Let Predictive Optimization pick keys (UC managed + DBR 15.4 LTS+ + serverless)
ALTER TABLE events_lc CLUSTER BY AUTO;

-- Disable clustering entirely
-- ALTER TABLE events_lc CLUSTER BY NONE;

-- COMMAND ----------

-- 6. Predictive Optimization — runs OPTIMIZE / VACUUM / ANALYZE automatically on UC managed Delta.
-- Inheritance: account → catalog → schema → table.
ALTER CATALOG main         ENABLE PREDICTIVE OPTIMIZATION;
ALTER SCHEMA  main.learn   INHERIT PREDICTIVE OPTIMIZATION;     -- inherit from catalog

DESCRIBE EXTENDED events_lc;   -- look for "Predictive Optimization" line

-- COMMAND ----------

-- 7. Manual VACUUM — remove tombstoned files (default 7 day retention)
VACUUM events_lc;                       -- safe
-- VACUUM events_lc RETAIN 168 HOURS;   -- explicit