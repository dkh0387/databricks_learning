-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 3 · MERGE INTO drills
-- MAGIC Runnable companion to `learn_data_transformation.md` §6. Works on a small demo copy of
-- MAGIC `customers_silver` so the real table stays untouched. Run cells top to bottom.

-- COMMAND ----------

-- 1. Setup — demo target (5 customers) + a source batch with all interesting cases
CREATE OR REPLACE TABLE dea_learning.silver.customers_merge_demo AS
SELECT customer_id, name, email, region, tier
FROM   dea_learning.silver.customers_silver
WHERE  customer_id <= 5;

CREATE OR REPLACE TEMPORARY VIEW customer_updates AS
SELECT * FROM VALUES
  (1,  'Anna Update',  'anna.new@example.com',  'EU',   'gold',   false),  -- existing → UPDATE
  (2,  'Ben Delete',   'ben@example.com',       'NA',   'basic',  true),   -- existing → DELETE
  (99, 'Nora New',     'nora@example.com',      'APAC', 'silver', false)   -- unknown  → INSERT
AS t(customer_id, name, email, region, tier, is_deleted);

SELECT * FROM dea_learning.silver.customers_merge_demo ORDER BY customer_id;

-- COMMAND ----------

-- 2. The full upsert: DELETE + UPDATE + INSERT in ONE atomic statement.
--    Clause order matters: every WHEN MATCHED except the last needs an AND condition.
MERGE INTO dea_learning.silver.customers_merge_demo AS t
USING customer_updates AS s
  ON t.customer_id = s.customer_id
WHEN MATCHED AND s.is_deleted THEN
  DELETE
WHEN MATCHED THEN
  UPDATE SET t.name = s.name, t.email = s.email, t.region = s.region, t.tier = s.tier
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, email, region, tier)
  VALUES (s.customer_id, s.name, s.email, s.region, s.tier);

-- Expect: id 1 updated, id 2 gone, id 99 inserted, ids 3-5 untouched
SELECT * FROM dea_learning.silver.customers_merge_demo ORDER BY customer_id;

-- COMMAND ----------

-- 3. Idempotency: re-running the SAME merge changes nothing (unlike a blind INSERT/append).
--    (id 2 no longer matches → nothing to delete; id 1 is set to identical values; id 99 exists.)
MERGE INTO dea_learning.silver.customers_merge_demo AS t
USING customer_updates AS s
  ON t.customer_id = s.customer_id
WHEN MATCHED AND s.is_deleted THEN DELETE
WHEN MATCHED THEN UPDATE SET t.name = s.name, t.email = s.email, t.region = s.region, t.tier = s.tier
WHEN NOT MATCHED THEN INSERT (customer_id, name, email, region, tier)
                      VALUES (s.customer_id, s.name, s.email, s.region, s.tier);

-- COMMAND ----------

-- 4. THE classic exam error: multiple source rows match one target row.
CREATE OR REPLACE TEMPORARY VIEW dirty_updates AS
SELECT * FROM VALUES
  (3, 'Carla V1', 'carla.v1@example.com', TIMESTAMP'2026-07-01 08:00:00'),
  (3, 'Carla V2', 'carla.v2@example.com', TIMESTAMP'2026-07-01 09:30:00')   -- same key twice!
AS t(customer_id, name, email, updated_at);

-- Uncomment to see it fail:
-- DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE
-- MERGE INTO dea_learning.silver.customers_merge_demo t
-- USING dirty_updates s ON t.customer_id = s.customer_id
-- WHEN MATCHED THEN UPDATE SET t.name = s.name, t.email = s.email;

-- COMMAND ----------

-- 5. The fix: dedup the source FIRST with the §5 row_number() pattern (latest wins).
MERGE INTO dea_learning.silver.customers_merge_demo AS t
USING (
  SELECT customer_id, name, email
  FROM (
    SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
    FROM dirty_updates
  ) WHERE rn = 1
) AS s
  ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET t.name = s.name, t.email = s.email;

-- Expect: id 3 carries the 09:30 version (Carla V2)
SELECT * FROM dea_learning.silver.customers_merge_demo WHERE customer_id = 3;

-- COMMAND ----------

-- 6. Insert-only MERGE — cheap dedup ingestion for sources that re-deliver rows
--    (e.g. downstream of cloudFiles.allowOverwrites): existing keys are simply skipped.
MERGE INTO dea_learning.silver.customers_merge_demo AS t
USING customer_updates AS s
  ON t.customer_id = s.customer_id
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, email, region, tier)
  VALUES (s.customer_id, s.name, s.email, s.region, s.tier);

-- COMMAND ----------

-- 7. WHEN NOT MATCHED BY SOURCE (DBR 12.2+) — full sync: remove target rows the source no longer has.
--    Careful: with a partial/incremental batch this deletes everything not in the batch!
MERGE INTO dea_learning.silver.customers_merge_demo AS t
USING customer_updates AS s
  ON t.customer_id = s.customer_id
WHEN MATCHED AND NOT s.is_deleted THEN
  UPDATE SET t.name = s.name, t.email = s.email, t.region = s.region, t.tier = s.tier
WHEN NOT MATCHED BY SOURCE THEN
  DELETE;

-- Expect: only ids 1 and 99 remain (2 was flagged deleted earlier; 3-5 are not in the source)
SELECT * FROM dea_learning.silver.customers_merge_demo ORDER BY customer_id;

-- COMMAND ----------

-- 8. Schema evolution: source brings a NEW column the target doesn't have (DBR 15.2+).
CREATE OR REPLACE TEMPORARY VIEW updates_with_new_col AS
SELECT 1 AS customer_id, 'Anna Update' AS name, 'de' AS preferred_language;

MERGE WITH SCHEMA EVOLUTION INTO dea_learning.silver.customers_merge_demo AS t
USING updates_with_new_col AS s
  ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Expect: new column preferred_language, filled for id 1, NULL elsewhere
SELECT * FROM dea_learning.silver.customers_merge_demo ORDER BY customer_id;

-- COMMAND ----------

-- 9. Cleanup
DROP TABLE IF EXISTS dea_learning.silver.customers_merge_demo;