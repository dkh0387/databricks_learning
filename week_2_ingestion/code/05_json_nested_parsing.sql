-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 2 · JSON & nested data
-- MAGIC Three idioms: schema-on-read via `read_files`, `from_json` against a stringified column, and VARIANT.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS main.learn;

-- COMMAND ----------

-- Sample nested JSON in a single string column
CREATE OR REPLACE TEMP VIEW raw_payloads AS
SELECT *
FROM VALUES
  (1, '{"customer":{"id":100,"email":"a@x.com"},"items":[{"sku":"A","qty":2},{"sku":"B","qty":1}]}'),
  (2, '{"customer":{"id":101,"email":"b@x.com"},"items":[{"sku":"C","qty":5}]}')
AS t(event_id, payload);

SELECT * FROM raw_payloads;

-- COMMAND ----------

-- Parse with from_json against an explicit schema
SELECT
  event_id,
  parsed.customer.id    AS customer_id,
  parsed.customer.email AS customer_email,
  parsed.items          AS items_array
FROM (
  SELECT event_id,
         from_json(payload,
           'STRUCT<customer: STRUCT<id: BIGINT, email: STRING>,
                   items: ARRAY<STRUCT<sku: STRING, qty: INT>>>') AS parsed
  FROM raw_payloads
);

-- COMMAND ----------

-- Explode the items array — one row per item
SELECT event_id, item.sku, item.qty
FROM (
  SELECT event_id,
         explode(from_json(payload,
           'STRUCT<customer: STRUCT<id: BIGINT, email: STRING>,
                   items: ARRAY<STRUCT<sku: STRING, qty: INT>>>').items) AS item
  FROM raw_payloads
);

-- COMMAND ----------

-- Lightweight extraction without parsing the whole structure
SELECT event_id,
       get_json_object(payload, '$.customer.email')   AS email,
       json_tuple(payload, 'customer', 'items')       AS (customer_str, items_str)
FROM raw_payloads;

-- COMMAND ----------

-- VARIANT (DBR 15.3+) — schema-less binary JSON, query with the `:` operator
CREATE OR REPLACE TABLE main.learn.events_variant (event_id BIGINT, raw VARIANT);

INSERT INTO main.learn.events_variant
SELECT event_id, parse_json(payload) FROM raw_payloads;

SELECT event_id,
       raw:customer.id::BIGINT      AS customer_id,
       raw:customer.email::STRING   AS email,
       raw:items[0].sku::STRING     AS first_sku
FROM   main.learn.events_variant;