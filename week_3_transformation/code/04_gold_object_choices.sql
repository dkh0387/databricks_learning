-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 3 · Gold-layer object choices on the orders/customers domain
-- MAGIC Same business question (daily revenue per region), four different objects, four different tradeoffs.

-- COMMAND ----------

USE CATALOG dea_learning;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Regular Delta TABLE
-- MAGIC Use when: you need custom MERGE/UPDATE/DELETE logic; downstream wants a normal table.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.daily_revenue_table AS
SELECT to_date(o.order_ts) AS order_date,
       c.region,
       sum(o.amount)       AS revenue,
       count(*)            AS orders
FROM   bronze.orders_bronze o
JOIN   silver.customers_silver c USING (customer_id)
GROUP BY to_date(o.order_ts), c.region;

SELECT * FROM gold.daily_revenue_table ORDER BY order_date, region;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. VIEW
-- MAGIC Use when: lightweight projection / filter / security boundary; data changes frequently.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.eu_orders AS
SELECT o.*
FROM   bronze.orders_bronze o
JOIN   silver.customers_silver c USING (customer_id)
WHERE  c.region = 'EU';

SELECT count(*) FROM gold.eu_orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. MATERIALIZED VIEW (inside a Spark Declarative Pipeline)
-- MAGIC Use when: expensive aggregation; consumers tolerate minutes of staleness; pipeline-managed refresh.
-- MAGIC NOTE: incremental refresh requires serverless. CANNOT be a streaming source.

-- COMMAND ----------

-- Place this statement inside a pipeline notebook (week_4/code/01_pipeline_bronze_silver.sql shows the full pipeline).
-- CREATE OR REFRESH MATERIALIZED VIEW gold.daily_revenue_mv AS
-- SELECT to_date(o.order_ts) AS order_date, c.region, sum(o.amount) AS revenue
-- FROM   bronze.orders_bronze o
-- JOIN   silver.customers_silver c USING (customer_id)
-- GROUP BY to_date(o.order_ts), c.region;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. STREAMING TABLE (inside a Spark Declarative Pipeline)
-- MAGIC Use when: continuous arrival, exactly-once incremental, downstream wants to read as a stream too.

-- COMMAND ----------

-- Inside a pipeline notebook only:
-- CREATE OR REFRESH STREAMING TABLE bronze.orders_bronze
-- AS SELECT * FROM STREAM read_files('/Volumes/dea_learning/raw/landing/orders', format => 'json');