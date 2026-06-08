-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 3 · Gold-layer object choices
-- MAGIC Table vs View vs Materialized View vs Streaming Table — when each is right.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS main.learn;

-- Reuse the silver table from `01_silver_cleansing.py`. If you haven't run it, mock one:
CREATE OR REPLACE TABLE main.learn.orders_silver (
  order_id BIGINT, customer_id BIGINT, region STRING, amount DOUBLE, order_date DATE
) USING DELTA;

INSERT INTO main.learn.orders_silver VALUES
  (1, 100, 'EU', 9.99,  DATE'2026-06-01'),
  (2, 101, 'US', 19.99, DATE'2026-06-01'),
  (3, 100, 'EU', 4.99,  DATE'2026-06-02'),
  (4, 102, 'EU', 29.99, DATE'2026-06-02');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Regular Delta TABLE
-- MAGIC Use when: you need custom MERGE/UPDATE/DELETE logic; downstream consumers want a normal table.

-- COMMAND ----------

CREATE OR REPLACE TABLE main.learn.daily_revenue_table AS
SELECT order_date, region, sum(amount) AS revenue
FROM   main.learn.orders_silver
GROUP BY order_date, region;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. VIEW
-- MAGIC Use when: lightweight projection/filter/security boundary; data changes constantly and you don't want storage.

-- COMMAND ----------

CREATE OR REPLACE VIEW main.learn.eu_orders AS
SELECT * FROM main.learn.orders_silver WHERE region = 'EU';

SELECT * FROM main.learn.eu_orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. MATERIALIZED VIEW (inside a Spark Declarative Pipeline)
-- MAGIC Use when: expensive aggregation; consumers can tolerate minutes of staleness; want pipeline-managed refresh.
-- MAGIC NOTE: incremental refresh requires serverless. Cannot be a streaming source.

-- COMMAND ----------

-- Put this statement inside a pipeline notebook; do not run it standalone.
-- CREATE OR REFRESH MATERIALIZED VIEW main.learn.daily_revenue_mv AS
-- SELECT order_date, region, sum(amount) AS revenue
-- FROM   main.learn.orders_silver
-- GROUP BY order_date, region;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. STREAMING TABLE (inside a Spark Declarative Pipeline)
-- MAGIC Use when: continuous arrival, exactly-once incremental, downstream wants to read as a stream too.

-- COMMAND ----------

-- Inside a pipeline notebook only:
-- CREATE OR REFRESH STREAMING TABLE main.learn.orders_stream
-- AS SELECT * FROM STREAM read_files('/Volumes/main/learn/landing/orders', format => 'json');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Decision summary
-- MAGIC | Need | Pick |
-- MAGIC | --- | --- |
-- MAGIC | Custom MERGE/UPDATE/DELETE | TABLE |
-- MAGIC | Continuous arrival, exactly-once | STREAMING TABLE |
-- MAGIC | Expensive aggregation, tolerate staleness | MATERIALIZED VIEW |
-- MAGIC | Cheap projection / security view | VIEW |