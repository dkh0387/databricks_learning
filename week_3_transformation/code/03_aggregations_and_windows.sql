-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 3 · Aggregations and Window functions
-- MAGIC The exam asks specifically about `approx_count_distinct` vs `count(DISTINCT)` and about window patterns.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (1, 100, 'EU', 9.99,  TIMESTAMP'2026-06-01 10:00:00'),
  (2, 100, 'EU', 19.99, TIMESTAMP'2026-06-01 11:00:00'),
  (3, 101, 'US', 4.99,  TIMESTAMP'2026-06-01 12:00:00'),
  (4, 100, 'EU', 29.99, TIMESTAMP'2026-06-02 10:00:00'),
  (5, 102, 'EU', 4.99,  TIMESTAMP'2026-06-02 11:00:00')
AS t(order_id, customer_id, region, amount, order_ts);

-- COMMAND ----------

-- Basic aggregations
SELECT region,
       count(*)                       AS rows,
       count(DISTINCT customer_id)    AS uniq_exact,
       approx_count_distinct(customer_id, 0.05) AS uniq_approx,
       sum(amount)                    AS revenue,
       avg(amount)                    AS aov
FROM   orders
GROUP BY region;

-- COMMAND ----------

-- summary() / describe() equivalents
SELECT min(amount), max(amount), mean(amount), stddev(amount) FROM orders;

-- COMMAND ----------

-- Window: running totals + previous value per customer
SELECT
  order_id, customer_id, order_ts, amount,
  sum(amount) OVER w_run AS running_total,
  lag(amount) OVER w_run AS prev_amount,
  row_number() OVER w_run AS order_seq
FROM orders
WINDOW w_run AS (PARTITION BY customer_id ORDER BY order_ts);

-- COMMAND ----------

-- Window: rank orders by amount inside each region
SELECT
  order_id, region, amount,
  rank()       OVER (PARTITION BY region ORDER BY amount DESC) AS rnk,
  dense_rank() OVER (PARTITION BY region ORDER BY amount DESC) AS dense_rnk,
  percent_rank() OVER (PARTITION BY region ORDER BY amount DESC) AS pct
FROM orders;

-- COMMAND ----------

-- Pivot
SELECT *
FROM (
  SELECT region, customer_id, amount FROM orders
)
PIVOT (
  sum(amount) FOR region IN ('EU' AS revenue_eu, 'US' AS revenue_us)
);