-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 6 · ABAC — governed tags + tag-driven policies
-- MAGIC Tag columns once; policies apply masks/filters everywhere automatically.
-- MAGIC ABAC policy creation is done in the **Catalog Explorer → Policies** UI; this notebook covers the SQL parts
-- MAGIC (tagging) and shows the pattern.

-- COMMAND ----------

-- 1. Define column-level governed tags on PII columns
ALTER TABLE main.learn.customers
  ALTER COLUMN email SET TAGS ('pii' = 'true', 'pii_class' = 'email');

-- 2. Table-level classification
ALTER TABLE main.learn.customers
  SET TAGS ('classification' = 'restricted');

-- 3. List tags
SELECT * FROM system.information_schema.column_tags
WHERE  catalog_name = 'main' AND schema_name = 'learn';

SELECT * FROM system.information_schema.table_tags
WHERE  catalog_name = 'main' AND schema_name = 'learn';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create the ABAC policy in the UI
-- MAGIC 1. **Catalog Explorer → Policies → Create policy**.
-- MAGIC 2. **Type:** *Column mask*.
-- MAGIC 3. **Condition:** column has tag `pii=true`.
-- MAGIC 4. **Action:** apply the mask UDF `main.sec.mask_email`.
-- MAGIC 5. **Principals:** all users EXCEPT group `pii_readers` (i.e., the policy applies *to* everyone else).
-- MAGIC 6. Save.
-- MAGIC
-- MAGIC From that moment, every existing and future column tagged `pii=true` in the catalog gets the mask applied
-- MAGIC automatically — no per-table `ALTER` needed.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Row-level ABAC policy
-- MAGIC 1. Tag tables with their data-domain: `domain=customers`, `domain=finance`, etc.
-- MAGIC 2. Create a *Row filter* policy: when `domain=customers`, apply UDF `region_filter` on column `region`.
-- MAGIC 3. The UDF takes the row's `region` value and decides if the caller can see it.

-- COMMAND ----------

-- Inspect policies attached to a securable (Databricks-internal view)
-- SHOW POLICIES ON TABLE main.learn.customers;