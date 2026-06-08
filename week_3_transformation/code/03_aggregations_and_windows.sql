-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 3 · Aggregations and Window functions on the orders/customers/items domain
-- MAGIC Builds the queries that feed Week 4's gold layer.

-- COMMAND ----------

USE CATALOG dea_learning;

-- COMMAND ----------

-- Flat line items
CREATE OR REPLACE TEMP VIEW order_items AS
SELECT
  o.order_id, o.customer_id, o.order_ts, o.status, o.currency, o.amount AS order_amount,
  item.item_id, item.quantity, item.unit_price,
  item.quantity * item.unit_price AS line_total,
  to_date(o.order_ts) AS order_date
FROM   bronze.orders_bronze o
LATERAL VIEW explode(o.items) AS item;

-- COMMAND ----------

-- 1. Per-region revenue with exact vs approximate distinct customers
SELECT c.region,
       count(*)                                     AS line_items,
       count(DISTINCT oi.order_id)                  AS orders_exact,
       count(DISTINCT oi.customer_id)               AS customers_exact,
       approx_count_distinct(oi.customer_id, 0.05)  AS customers_approx,
       sum(oi.line_total)                           AS revenue,
       avg(oi.line_total)                           AS avg_line
FROM   order_items oi
JOIN   silver.customers_silver c USING (customer_id)
GROUP BY c.region
ORDER BY revenue DESC;

-- COMMAND ----------

-- 2. Top items by revenue
SELECT i.item_id, i.name, i.category,
       sum(oi.quantity)   AS units_sold,
       sum(oi.line_total) AS revenue
FROM   order_items oi
JOIN   bronze.items_bronze i USING (item_id)
GROUP BY i.item_id, i.name, i.category
ORDER BY revenue DESC
LIMIT 10;

-- COMMAND ----------

-- 3. Running total per customer (window)
SELECT
  customer_id, order_id, order_ts, order_amount,
  sum(order_amount) OVER w AS running_spend,
  row_number()      OVER w AS order_seq,
  lag(order_amount) OVER w AS prev_order_amount
FROM (
  SELECT DISTINCT order_id, customer_id, order_ts, order_amount FROM order_items
)
WINDOW w AS (PARTITION BY customer_id ORDER BY order_ts)
ORDER BY customer_id, order_ts;

-- COMMAND ----------

-- 4. Rank orders within each region
SELECT customer_id, region, order_id, order_amount,
       rank()       OVER (PARTITION BY region ORDER BY order_amount DESC) AS rnk,
       dense_rank() OVER (PARTITION BY region ORDER BY order_amount DESC) AS dense_rnk,
       percent_rank() OVER (PARTITION BY region ORDER BY order_amount DESC) AS pct
FROM (
  SELECT DISTINCT oi.order_id, oi.customer_id, oi.order_amount, c.region
  FROM   order_items oi
  JOIN   silver.customers_silver c USING (customer_id)
);

-- COMMAND ----------

-- 5. Pivot — daily revenue by region.
-- NOTE: PIVOT only surfaces explicitly named values; any region missing from the IN list is dropped.
-- The IN list below covers every region produced by silver_customers' country-to-region mapping.
SELECT *
FROM (
  SELECT to_date(o.order_ts) AS d, c.region, o.amount
  FROM   bronze.orders_bronze o
  JOIN   silver.customers_silver c USING (customer_id)
)
PIVOT (
  sum(amount) FOR region IN (
    'EU'    AS revenue_eu,
    'NA'    AS revenue_na,
    'APAC'  AS revenue_apac,
    'LATAM' AS revenue_latam,
    'EMEA'  AS revenue_emea,
    'OTHER' AS revenue_other
  )
)
ORDER BY d;