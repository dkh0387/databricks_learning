-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 6 · Row filters and column masks on `customers_silver`
-- MAGIC * Row filter by `region` — each regional team sees only their customers.
-- MAGIC * Column mask on `email` — only `pii_readers` see the real address.
-- MAGIC
-- MAGIC Manual per-table approach. For account-wide rules see `04_abac_tagging.sql`.
-- MAGIC
-- MAGIC We use week 3's plain Delta table `dea_learning.silver.customers_silver`. On
-- MAGIC pipeline-managed streaming tables / MVs (like week 4's `silver_customers`), row
-- MAGIC filters/masks must be declared in the pipeline definition (`WITH ROW FILTER`), not via `ALTER TABLE`.

-- COMMAND ----------

USE CATALOG dea_learning;

-- COMMAND ----------

-- 1. Row filter — admins see everything; regional teams only their region; everyone else nothing.
-- Covers every region the cleansing derives: EU, NA, APAC, LATAM, EMEA, OTHER.
-- Two artifacts carry this mapping: week 3 writes the plain Delta table `customers_silver`
-- (used here); week 4's pipeline owns the streaming table `silver_customers`.
CREATE OR REPLACE FUNCTION sec.region_filter(region STRING)
RETURNS BOOLEAN
RETURN
  is_account_group_member('admins')
  OR (is_account_group_member('eu_team')    AND region = 'EU')
  OR (is_account_group_member('na_team')    AND region = 'NA')
  OR (is_account_group_member('apac_team')  AND region = 'APAC')
  OR (is_account_group_member('latam_team') AND region = 'LATAM')
  OR (is_account_group_member('emea_team')  AND region = 'EMEA');
  -- 'OTHER' is admin-only by design (no team owns it).

ALTER TABLE silver.customers_silver
  SET ROW FILTER sec.region_filter ON (region);

-- COMMAND ----------

-- Verify — you should only see rows for the regions your group can access
SELECT * FROM silver.customers_silver ORDER BY customer_id;

-- COMMAND ----------

-- 2. Column mask — partial-mask email unless caller is in `pii_readers`
CREATE OR REPLACE FUNCTION sec.mask_email(email STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('pii_readers') THEN email
    ELSE regexp_replace(email, '(^.)(.*)(@.*$)', '$1***$3')
  END;

ALTER TABLE silver.customers_silver
  ALTER COLUMN email SET MASK sec.mask_email;

-- COMMAND ----------

SELECT customer_id, name, email, region FROM silver.customers_silver ORDER BY customer_id;

-- COMMAND ----------

-- Cleanup (uncomment to remove)
-- ALTER TABLE silver.customers_silver DROP ROW FILTER;
-- ALTER TABLE silver.customers_silver ALTER COLUMN email DROP MASK;