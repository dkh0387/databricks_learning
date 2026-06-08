-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 4 · The full medallion pipeline
-- MAGIC End-to-end Spark Declarative Pipeline over the unified domain:
-- MAGIC * **Bronze** — incremental ingest from the landing volume.
-- MAGIC * **Silver** — cleansed, deduplicated, line items exploded, expectations enforced.
-- MAGIC * **Gold** — business aggregates ready for BI.
-- MAGIC
-- MAGIC Create a new pipeline in the UI (*New ETL pipeline*), point it at this notebook, set the target catalog to
-- MAGIC `dea_learning`, and run an update.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT 'Raw orders — one row per order with a nested items array'
AS SELECT *,
          _metadata.file_path AS source_file
   FROM STREAM read_files(
     '/Volumes/dea_learning/raw/landing/orders',
     format        => 'json',
     schemaHints   => 'order_id BIGINT, customer_id BIGINT, amount DOUBLE, order_ts TIMESTAMP'
   );

CREATE OR REFRESH STREAMING TABLE bronze_customers
COMMENT 'Raw customer rows from CSV seed'
AS SELECT *
   FROM STREAM read_files(
     '/Volumes/dea_learning/raw/landing/customers',
     format       => 'csv',
     header       => true,
     schemaHints  => 'customer_id BIGINT, signup_date DATE'
   );

CREATE OR REFRESH MATERIALIZED VIEW bronze_items
COMMENT 'Item catalog — small, batch-refreshed'
AS SELECT *
   FROM read_files(
     '/Volumes/dea_learning/raw/landing/items',
     format => 'csv',
     header => true
   );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_customers (
  CONSTRAINT valid_id    CHECK (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_email CHECK (email RLIKE '.+@.+\\..+') ON VIOLATION DROP ROW
)
COMMENT 'Cleansed customer dimension'
AS SELECT
     customer_id,
     initcap(trim(name))                        AS name,
     lower(trim(email))                         AS email,
     upper(trim(country))                       AS country,
     CASE
       WHEN upper(country) IN ('DE','FR','IT','ES','NL','PL','IE','SE','CZ','AT','BE','DK','FI') THEN 'EU'
       WHEN upper(country) IN ('US','CA')                                                         THEN 'NA'
       WHEN upper(country) IN ('JP','CN','IN','SG','AU','KR')                                      THEN 'APAC'
       WHEN upper(country) IN ('BR','MX','AR','CL','CO')                                           THEN 'LATAM'
       WHEN upper(country) IN ('AE','SA','EG','ZA')                                                THEN 'EMEA'
       ELSE 'OTHER'
     END                                        AS region,
     to_date(signup_date)                       AS signup_date,
     tier
   FROM STREAM(bronze_customers);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT positive_amount CHECK (amount > 0)                  ON VIOLATION DROP ROW,
  CONSTRAINT valid_status    CHECK (status IN ('placed','shipped','delivered','cancelled')),
  CONSTRAINT valid_currency  CHECK (length(currency) = 3)
)
COMMENT 'Orders without line items'
AS SELECT
     order_id,
     customer_id,
     order_ts,
     to_date(order_ts) AS order_date,
     lower(status)     AS status,
     upper(currency)   AS currency,
     amount,
     coalesce(discount_amount, 0.0) AS discount_amount
   FROM STREAM(bronze_orders);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_order_items (
  CONSTRAINT valid_qty   CHECK (quantity > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_price CHECK (unit_price >= 0)
)
COMMENT 'One row per line item — explode of orders.items joined to items'
AS SELECT
     o.order_id,
     o.customer_id,
     o.order_ts,
     to_date(o.order_ts) AS order_date,
     item.item_id,
     item.quantity,
     item.unit_price,
     item.quantity * item.unit_price AS line_total,
     o.currency
   FROM STREAM(bronze_orders) o
   LATERAL VIEW explode(o.items) AS item;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_items
COMMENT 'Cleansed item catalog'
AS SELECT
     item_id,
     initcap(trim(name)) AS name,
     lower(category)     AS category,
     price,
     in_stock
   FROM bronze_items;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue
COMMENT 'Daily revenue by region — primary BI surface'
AS SELECT
     o.order_date,
     c.region,
     count(DISTINCT o.order_id) AS orders,
     sum(o.amount)              AS revenue,
     avg(o.amount)              AS avg_order_amount
   FROM   silver_orders o
   JOIN   silver_customers c USING (customer_id)
   GROUP BY o.order_date, c.region;

-- COMMAND ----------

-- NOTE on ordering: ORDER BY in a materialized view definition does NOT guarantee stored
-- ordering — Delta storage layout is driven by clustering / Z-order. Order at query time.
CREATE OR REFRESH MATERIALIZED VIEW gold_top_customers
COMMENT 'Lifetime customer value ranking'
AS SELECT
     c.customer_id,
     c.name,
     c.region,
     count(DISTINCT o.order_id) AS lifetime_orders,
     sum(o.amount)              AS lifetime_spend
   FROM   silver_orders o
   JOIN   silver_customers c USING (customer_id)
   GROUP BY c.customer_id, c.name, c.region;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_top_items
COMMENT 'Bestsellers by revenue'
AS SELECT
     oi.item_id,
     i.name,
     i.category,
     sum(oi.quantity)   AS units_sold,
     sum(oi.line_total) AS revenue
   FROM   silver_order_items oi
   JOIN   silver_items i USING (item_id)
   GROUP BY oi.item_id, i.name, i.category;