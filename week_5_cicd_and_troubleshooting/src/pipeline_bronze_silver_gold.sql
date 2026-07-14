-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bundle copy · The full medallion pipeline (env-agnostic)
-- MAGIC Copy of `week_4_pipelines_and_jobs/code/01_pipeline_bronze_silver_gold.sql`, deployed by the Week-5 bundle.
-- MAGIC `${catalog}` and `${landing_path}` are **pipeline configuration parameters** injected per target from
-- MAGIC `databricks.yml` (SDP substitutes `${key}` from the pipeline `configuration` block into the SQL text) —
-- MAGIC dev/staging/prod each publish into their own catalog.
-- MAGIC
-- MAGIC All datasets are fully qualified (`${catalog}.bronze/silver/gold.*`) so each lands in its layer schema.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze

-- COMMAND ----------
CREATE OR REFRESH STREAMING TABLE ${catalog}.bronze.bronze_orders
COMMENT 'Raw orders — one row per order with a nested items array'
AS SELECT *,
          _metadata.file_path AS source_file
   FROM STREAM read_files(
     '${landing_path}/orders',
     format        => 'json',
     schemaHints   => 'order_id BIGINT, customer_id BIGINT, amount DOUBLE, order_ts TIMESTAMP'
   );

CREATE OR REFRESH STREAMING TABLE ${catalog}.bronze.bronze_customers
COMMENT 'Raw customer rows from CSV seed'
AS SELECT *
   FROM STREAM read_files(
     '${landing_path}/customers',
     format       => 'csv',
     header       => true,
     schemaHints  => 'customer_id BIGINT, signup_date DATE'
   );

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.bronze.bronze_items
COMMENT 'Item catalog — small, batch-refreshed'
AS SELECT *
   FROM read_files(
     '${landing_path}/items',
     format => 'csv',
     header => true
   );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.silver_customers (
  CONSTRAINT valid_id    EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_email EXPECT (email RLIKE '.+@.+\\..+') ON VIOLATION DROP ROW
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
   FROM STREAM(${catalog}.bronze.bronze_customers);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.silver_orders (
  CONSTRAINT positive_amount EXPECT (amount > 0)                  ON VIOLATION DROP ROW,
  CONSTRAINT valid_status    EXPECT (status IN ('placed','shipped','delivered','cancelled')),
  CONSTRAINT valid_currency  EXPECT (length(currency) = 3)
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
   FROM STREAM(${catalog}.bronze.bronze_orders);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.silver_order_items (
  CONSTRAINT valid_qty   EXPECT (quantity > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_price EXPECT (unit_price >= 0)
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
   FROM STREAM(${catalog}.bronze.bronze_orders) o
   LATERAL VIEW explode(from_json(o.items, 'ARRAY<STRUCT<item_id BIGINT, quantity INT, unit_price DOUBLE>>')) AS item;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.silver_items
COMMENT 'Cleansed item catalog'
AS SELECT
     item_id,
     initcap(trim(name)) AS name,
     lower(category)     AS category,
     price,
     in_stock
   FROM ${catalog}.bronze.bronze_items;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.gold_daily_revenue
COMMENT 'Daily revenue by region — primary BI surface'
AS SELECT
     o.order_date,
     c.region,
     count(DISTINCT o.order_id) AS orders,
     sum(o.amount)              AS revenue,
     avg(o.amount)              AS avg_order_amount
   FROM   ${catalog}.silver.silver_orders o
   JOIN   ${catalog}.silver.silver_customers c USING (customer_id)
   GROUP BY o.order_date, c.region;

-- COMMAND ----------

-- NOTE on ordering: ORDER BY in a materialized view definition does NOT guarantee stored
-- ordering — Delta storage layout is driven by clustering / Z-order. Order at query time.
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.gold_top_10_customers
  COMMENT 'Lifetime customer value ranking' AS
SELECT
  *
FROM
  (
    SELECT
      customer_id,
      name,
      region,
      lifetime_orders,
      lifetime_spend,
      row_number() OVER (PARTITION BY region ORDER BY lifetime_spend DESC) AS rank
    FROM
      (
        SELECT
          c.customer_id,
          c.name,
          c.region,
          count(DISTINCT o.order_id) AS lifetime_orders,
          sum(o.amount) AS lifetime_spend
        FROM
          ${catalog}.silver.silver_orders o JOIN ${catalog}.silver.silver_customers c USING (customer_id)
        GROUP BY
          c.customer_id,
          c.name,
          c.region
      )
  )
WHERE
  rank <= 10;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.gold_top_10_items
  COMMENT 'Bestsellers by revenue' AS
SELECT
  *
FROM
  (
    SELECT
      item_id,
      name,
      category,
      units_sold,
      revenue,
      row_number() OVER (PARTITION BY category ORDER BY revenue DESC) AS rank_revenue,
      row_number() OVER (PARTITION BY category ORDER BY units_sold DESC) AS rank_units
    FROM
      (
        SELECT
          oi.item_id,
          i.name,
          i.category,
          sum(oi.quantity) AS units_sold,
          sum(oi.line_total) AS revenue
        FROM
          ${catalog}.silver.silver_order_items oi JOIN ${catalog}.silver.silver_items i USING (item_id)
        GROUP BY
          oi.item_id,
          i.name,
          i.category
      )
  )
WHERE
  rank_revenue <= 10
  OR rank_units <= 10;
