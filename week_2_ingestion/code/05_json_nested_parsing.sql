-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 2 · JSON & nested data — orders.items
-- MAGIC The orders payload has a nested `items: ARRAY<STRUCT<...>>`. Three idioms for working with it:
-- MAGIC schema-on-read (`read_files`), `from_json` against a stringified column, and VARIANT.

-- COMMAND ----------

-- 1. Schema-on-read — Auto Loader / read_files sees the nested struct
SELECT order_id, customer_id, amount, items
FROM   read_files('/Volumes/dea_learning/raw/landing/orders', format => 'json')
LIMIT 5;

-- COMMAND ----------

-- 2. Explode the items array → one row per line item (this is what silver does)
-- read_files infers real types (unlike Auto Loader), so items is already ARRAY<STRUCT<...>> — no from_json needed.
SELECT
  order_id,
  customer_id,
  amount  AS order_amount,
  item.item_id,
  item.quantity,
  item.unit_price,
  item.quantity * item.unit_price AS line_total
FROM   read_files('/Volumes/dea_learning/raw/landing/orders', format => 'json')
LATERAL VIEW explode(items) AS item;

-- COMMAND ----------

-- 3. from_json against a stringified payload (when the JSON arrived as a STRING column)
WITH raw AS (
  SELECT
    1001 AS event_id,
    '{"order_id":1001,"customer_id":1,"items":[{"item_id":"SKU-A001","quantity":2,"unit_price":29.99}]}' AS payload
)
SELECT
  event_id,
  parsed.order_id,
  parsed.customer_id,
  parsed.items
FROM (
  SELECT
    event_id,
    from_json(payload,
      'STRUCT<order_id: BIGINT,
              customer_id: BIGINT,
              items: ARRAY<STRUCT<item_id: STRING, quantity: INT, unit_price: DOUBLE>>>') AS parsed
  FROM raw
);

-- COMMAND ----------

-- 4. Lightweight extraction without parsing the whole structure
WITH raw AS (
  SELECT '{"order_id":1001,"customer_id":1,"amount":139.97}' AS payload
)
SELECT
  get_json_object(payload, '$.order_id')   AS order_id,
  get_json_object(payload, '$.customer_id') AS customer_id,
  get_json_object(payload, '$.amount')     AS amount
FROM raw;

-- json_tuple pulls multiple top-level keys at once, but it's a generator —
-- it must be used via LATERAL VIEW, not aliased inline in SELECT.
WITH raw AS (
  SELECT '{"order_id":1001,"customer_id":1,"amount":139.97}' AS payload
)
SELECT t.cust, t.amt
FROM   raw
LATERAL VIEW json_tuple(payload, 'customer_id', 'amount') t AS cust, amt;

-- COMMAND ----------

-- 5. VARIANT (DBR 15.3+) — schema-less JSON storage, query with the `:` operator
CREATE OR REPLACE TABLE dea_learning.bronze.orders_variant (
  event_id BIGINT,
  raw      VARIANT
);

INSERT INTO dea_learning.bronze.orders_variant
SELECT order_id,
       parse_json(to_json(struct(order_id, customer_id, amount, items)))
FROM   read_files('/Volumes/dea_learning/raw/landing/orders', format => 'json');

SELECT event_id,
       raw:order_id::BIGINT       AS order_id,
       raw:customer_id::BIGINT    AS customer_id,
       raw:amount::DOUBLE         AS amount,
       raw:items[0].item_id::STRING AS first_sku
FROM   dea_learning.bronze.orders_variant
ORDER BY event_id;