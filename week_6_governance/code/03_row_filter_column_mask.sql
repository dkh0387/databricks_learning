-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 6 · Row filters and column masks on `silver_customers`
-- MAGIC * Row filter by `region` — each regional team sees only their customers.
-- MAGIC * Column mask on `email` — only `pii_readers` see the real address.
-- MAGIC
-- MAGIC Manual per-table approach. For account-wide rules see `04_abac_tagging.sql`.

-- COMMAND ----------

USE CATALOG dea_learning;

-- COMMAND ----------

-- 1. Row filter — admins see everything; regional teams only their region; everyone else nothing.
CREATE OR REPLACE FUNCTION sec.region_filter(region STRING)
RETURNS BOOLEAN
RETURN
  IS_ACCOUNT_GROUP_MEMBER('admins')
  OR (IS_ACCOUNT_GROUP_MEMBER('eu_team')   AND region = 'EU')
  OR (IS_ACCOUNT_GROUP_MEMBER('na_team')   AND region = 'NA')
  OR (IS_ACCOUNT_GROUP_MEMBER('apac_team') AND region = 'APAC');

ALTER TABLE silver.silver_customers
  SET ROW FILTER sec.region_filter ON (region);

-- COMMAND ----------

-- Verify — you should only see rows for the regions your group can access
SELECT * FROM silver.silver_customers ORDER BY customer_id;

-- COMMAND ----------

-- 2. Column mask — partial-mask email unless caller is in `pii_readers`
CREATE OR REPLACE FUNCTION sec.mask_email(email STRING)
RETURNS STRING
RETURN
  CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii_readers') THEN email
    ELSE regexp_replace(email, '(^.)(.*)(@.*$)', '$1***$3')
  END;

ALTER TABLE silver.silver_customers
  ALTER COLUMN email SET MASK sec.mask_email;

-- COMMAND ----------

SELECT customer_id, name, email, region FROM silver.silver_customers ORDER BY customer_id;

-- COMMAND ----------

-- Cleanup (uncomment to remove)
-- ALTER TABLE silver.silver_customers DROP ROW FILTER;
-- ALTER TABLE silver.silver_customers ALTER COLUMN email DROP MASK;