-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 3 · Every join type — drilled on orders × customers × items
-- MAGIC Use the silver tables from `01_silver_cleansing.py` and the bronze items table from Week 2.

-- COMMAND ----------

USE CATALOG dea_learning;

-- Quick aliases for the drill
CREATE OR REPLACE TEMP VIEW c AS SELECT * FROM dea_learning.silver.customers_silver;
CREATE OR REPLACE TEMP VIEW i AS SELECT * FROM dea_learning.bronze.items_bronze;
-- Flat order_items projection (one row per line item)
CREATE OR REPLACE TEMP VIEW oi AS
SELECT
  order_id, customer_id,
  item.item_id, item.quantity, item.unit_price,
  amount AS order_amount, currency
FROM dea_learning.bronze.orders_bronze
LATERAL VIEW explode(items) AS item;

-- COMMAND ----------

-- INNER — every line item with its catalog metadata
SELECT 'inner' AS kind, oi.order_id, oi.item_id, i.name, i.category
FROM   oi JOIN i USING (item_id);

-- COMMAND ----------

-- LEFT — keep every line item even when the catalog row is missing
SELECT 'left' AS kind, oi.order_id, oi.item_id, i.name
FROM   oi LEFT JOIN i USING (item_id);

-- COMMAND ----------

-- LEFT SEMI — orders that bought at least one catalog item ("which orders matched?")
SELECT DISTINCT order_id
FROM   oi
LEFT SEMI JOIN i USING (item_id);

-- COMMAND ----------

-- LEFT ANTI — line items whose SKU is NOT in the catalog (data-quality red flag)
SELECT *
FROM   oi
LEFT ANTI JOIN i USING (item_id);

-- COMMAND ----------

-- FULL OUTER — find catalog items never ordered AND orders for unknown items
SELECT i.item_id AS catalog_sku, oi.item_id AS order_sku, i.name
FROM   i FULL OUTER JOIN oi USING (item_id);

-- COMMAND ----------

-- BROADCAST hint — items is small enough to broadcast for free
SELECT /*+ BROADCAST(i) */ oi.order_id, oi.item_id, i.category, oi.quantity * oi.unit_price AS line_total
FROM   oi JOIN i USING (item_id);

-- COMMAND ----------

-- MULTI-key join — customers × orders flattened to one row per (customer, order)
SELECT c.customer_id, c.name, c.region, o.order_id, o.amount, o.currency
FROM   c
JOIN   dea_learning.bronze.orders_bronze o ON c.customer_id = o.customer_id
ORDER BY c.customer_id, o.order_id;

-- COMMAND ----------

-- UNION vs UNION ALL — combine line items from two days, dedup vs keep all
SELECT * FROM oi WHERE order_id IN (1001, 1002) UNION
SELECT * FROM oi WHERE order_id IN (1002, 1003);

SELECT count(*) AS rows_dedup FROM (
  SELECT * FROM oi WHERE order_id IN (1001, 1002) UNION
  SELECT * FROM oi WHERE order_id IN (1002, 1003)
);

SELECT count(*) AS rows_all FROM (
  SELECT * FROM oi WHERE order_id IN (1001, 1002) UNION ALL
  SELECT * FROM oi WHERE order_id IN (1002, 1003)
);