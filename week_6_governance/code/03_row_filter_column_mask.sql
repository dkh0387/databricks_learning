-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 6 · Row filters and column masks
-- MAGIC Manual per-table approach. For account-wide rules see `04_abac_tagging.sql`.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS main.sec;
USE main.learn;

CREATE OR REPLACE TABLE customers (
  id     INT,
  email  STRING,
  region STRING,
  spend  DOUBLE
);

INSERT INTO customers VALUES
  (1, 'anna@x.com',   'EU', 1200),
  (2, 'bob@y.com',    'US', 4500),
  (3, 'carlos@z.com', 'EU', 800),
  (4, 'dora@w.com',   'APAC', 6700);

-- COMMAND ----------

-- 1. Row filter — admins see everything; EU/US teams see only their region.
CREATE OR REPLACE FUNCTION main.sec.region_filter(region STRING)
RETURNS BOOLEAN
RETURN
  IS_ACCOUNT_GROUP_MEMBER('admins')
  OR (IS_ACCOUNT_GROUP_MEMBER('eu_team') AND region = 'EU')
  OR (IS_ACCOUNT_GROUP_MEMBER('us_team') AND region = 'US');

ALTER TABLE main.learn.customers
  SET ROW FILTER main.sec.region_filter ON (region);

-- COMMAND ----------

-- Verify (you should only see rows your group can see)
SELECT * FROM main.learn.customers;

-- COMMAND ----------

-- 2. Column mask — partial-mask email unless the caller is in `pii_readers`
CREATE OR REPLACE FUNCTION main.sec.mask_email(email STRING)
RETURNS STRING
RETURN
  CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii_readers') THEN email
    ELSE regexp_replace(email, '(^.)(.*)(@.*$)', '$1***$3')
  END;

ALTER TABLE main.learn.customers
  ALTER COLUMN email SET MASK main.sec.mask_email;

-- COMMAND ----------

SELECT * FROM main.learn.customers;

-- COMMAND ----------

-- Remove the filter and mask if you want to clean up
-- ALTER TABLE main.learn.customers DROP ROW FILTER;
-- ALTER TABLE main.learn.customers ALTER COLUMN email DROP MASK;