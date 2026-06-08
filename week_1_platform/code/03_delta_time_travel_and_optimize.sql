-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 1 · Delta Time Travel, OPTIMIZE, Liquid Clustering
-- MAGIC Practice the Delta features the exam asks about.

-- COMMAND ----------

USE main.learn;

CREATE OR REPLACE TABLE events (
  id        BIGINT,
  user_id   BIGINT,
  event_ts  TIMESTAMP,
  amount    DOUBLE
);

-- Three commit versions
INSERT INTO events VALUES (1, 100, current_timestamp(),  9.99);
INSERT INTO events VALUES (2, 101, current_timestamp(), 19.99);
UPDATE events SET amount = 11.50 WHERE id = 1;

-- COMMAND ----------

-- Inspect history
DESCRIBE HISTORY events;

-- COMMAND ----------

-- Time travel by version
SELECT * FROM events VERSION AS OF 1;

-- Time travel by timestamp — adjust to a timestamp you observed above
-- SELECT * FROM events TIMESTAMP AS OF '2026-06-08T10:00:00';

-- COMMAND ----------

-- Restore to a prior version (creates a new commit, never destructive)
RESTORE TABLE events TO VERSION AS OF 1;
DESCRIBE HISTORY events;

-- COMMAND ----------

-- File compaction
OPTIMIZE events;

-- Show that small files were consolidated
DESCRIBE DETAIL events;

-- COMMAND ----------

-- Convert this table to Liquid Clustering (DBR 13.3+ explicit, 15.4 LTS+ AUTO)
ALTER TABLE events CLUSTER BY (user_id);

-- Or let Predictive Optimization decide (UC managed only, DBR 15.4 LTS+)
-- ALTER TABLE events CLUSTER BY AUTO;

-- Force a full rewrite to materialize the new clustering keys
OPTIMIZE events FULL;

DESCRIBE DETAIL events;   -- look at clusteringColumns

-- COMMAND ----------

-- Vacuum tombstoned files (default 7-day retention)
-- VACUUM events;             -- safe; uses default retention
-- VACUUM events RETAIN 0 HOURS;  -- destructive; requires bypassing retention check