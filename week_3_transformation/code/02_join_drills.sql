-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 3 · Every join type
-- MAGIC One file you can rip through to internalise the join syntax the exam tests.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (1, 'EU', 100),
  (2, 'US', 200),
  (3, 'EU', 300),
  (4, 'JP', 400)
AS t(order_id, region, customer_id);

CREATE OR REPLACE TEMP VIEW customers AS
SELECT * FROM VALUES
  (100, 'Anna',   'EU'),
  (200, 'Bob',    'US'),
  (500, 'Carlos', 'EU')      -- not in orders
AS t(customer_id, name, region);

-- COMMAND ----------

-- INNER
SELECT 'inner' AS kind, o.order_id, c.name
FROM   orders o JOIN customers c USING (customer_id);

-- COMMAND ----------

-- LEFT (keep all orders)
SELECT 'left' AS kind, o.order_id, c.name
FROM   orders o LEFT JOIN customers c USING (customer_id);

-- COMMAND ----------

-- RIGHT (keep all customers)
SELECT 'right' AS kind, o.order_id, c.name
FROM   orders o RIGHT JOIN customers c USING (customer_id);

-- COMMAND ----------

-- FULL OUTER
SELECT 'full' AS kind, o.order_id, c.name
FROM   orders o FULL OUTER JOIN customers c USING (customer_id);

-- COMMAND ----------

-- LEFT SEMI — "does the customer exist?" (no customer cols in output)
SELECT 'semi' AS kind, *
FROM   orders o LEFT SEMI JOIN customers c USING (customer_id);

-- COMMAND ----------

-- LEFT ANTI — "which orders have no matching customer?"
SELECT 'anti' AS kind, *
FROM   orders o LEFT ANTI JOIN customers c USING (customer_id);

-- COMMAND ----------

-- CROSS — cartesian
SELECT 'cross' AS kind, o.order_id, c.name
FROM   orders o CROSS JOIN customers c
LIMIT 20;

-- COMMAND ----------

-- BROADCAST hint — force a small-side broadcast
SELECT /*+ BROADCAST(c) */ o.order_id, c.name
FROM   orders o JOIN customers c USING (customer_id);

-- COMMAND ----------

-- MULTI-key join
CREATE OR REPLACE TEMP VIEW prices AS
SELECT * FROM VALUES
  ('EU', 'A', 1.10),
  ('EU', 'B', 1.20),
  ('US', 'A', 1.00)
AS t(region, sku, price);

CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('EU', 'A', 5),
  ('EU', 'B', 3),
  ('US', 'A', 7)
AS t(region, sku, qty);

SELECT s.region, s.sku, s.qty, p.price, s.qty * p.price AS revenue
FROM   sales s JOIN prices p ON s.region = p.region AND s.sku = p.sku;

-- COMMAND ----------

-- UNION vs UNION ALL
SELECT 1 AS x UNION     SELECT 1 AS x UNION     SELECT 2 AS x;   -- 2 rows
SELECT 1 AS x UNION ALL SELECT 1 AS x UNION ALL SELECT 2 AS x;   -- 3 rows