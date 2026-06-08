-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 6 · ABAC — governed tags + tag-driven policies
-- MAGIC Tag the PII columns once across all silver/gold tables. An ABAC policy applies the email mask everywhere
-- MAGIC the `pii=true` tag exists — no per-table `ALTER` after that.

-- COMMAND ----------

USE CATALOG dea_learning;

-- COMMAND ----------

-- 1. Column-level governed tags on every PII column in the medallion
ALTER TABLE silver.silver_customers
  ALTER COLUMN email   SET TAGS ('pii' = 'true', 'pii_class' = 'email');
ALTER TABLE silver.silver_customers
  ALTER COLUMN name    SET TAGS ('pii' = 'true', 'pii_class' = 'name');

-- 2. Table-level classification
ALTER TABLE silver.silver_customers
  SET TAGS ('classification' = 'restricted', 'domain' = 'customers');

ALTER TABLE silver.silver_orders
  SET TAGS ('classification' = 'internal',   'domain' = 'orders');

-- 3. Inspect tags
SELECT * FROM system.information_schema.column_tags
WHERE  catalog_name = 'dea_learning' AND tag_name = 'pii';

SELECT * FROM system.information_schema.table_tags
WHERE  catalog_name = 'dea_learning';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create the ABAC policy in the UI
-- MAGIC 1. **Catalog Explorer → Policies → Create policy**.
-- MAGIC 2. **Type:** *Column mask*.
-- MAGIC 3. **Condition:** column has tag `pii=true`.
-- MAGIC 4. **Action:** apply UDF `dea_learning.sec.mask_email` (defined in `03_row_filter_column_mask.sql`).
-- MAGIC 5. **Principals:** all users EXCEPT group `pii_readers`.
-- MAGIC 6. Save.
-- MAGIC
-- MAGIC From that moment, every existing and future column tagged `pii=true` in `dea_learning` gets masked
-- MAGIC automatically. Drop the manual `ALTER TABLE … SET MASK` from `03_row_filter_column_mask.sql` — ABAC replaces it.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Row-level ABAC policy
-- MAGIC 1. Tag tables with their domain: `domain=customers`, `domain=orders` (already done above).
-- MAGIC 2. Create a *Row filter* policy: when `domain=customers`, apply UDF `region_filter` on column `region`.
-- MAGIC 3. The UDF reads the row's `region` value and decides if the caller is allowed to see it.