-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 6 · ABAC — governed tags + tag-driven policies
-- MAGIC Tag the PII columns once across all silver/gold tables. An ABAC policy applies the email mask everywhere
-- MAGIC the `pii=true` tag exists — no per-table `ALTER` after that.

-- COMMAND ----------

USE CATALOG dea_learning;

-- COMMAND ----------

-- 1. Column-level governed tags on every PII column in the medallion.
-- Week 3's plain Delta table `customers_silver` — on pipeline-managed streaming tables
-- (like week 4's `silver_customers`) tags belong in the pipeline definition, not ALTER.
ALTER TABLE silver.customers_silver
  ALTER COLUMN email   SET TAGS ('pii' = 'true', 'pii_class' = 'email');
ALTER TABLE silver.customers_silver
  ALTER COLUMN name    SET TAGS ('pii' = 'true', 'pii_class' = 'name');

-- 2. Table-level classification
ALTER TABLE silver.customers_silver
  SET TAGS ('classification' = 'restricted', 'domain' = 'customers');

-- 3. Inspect tags
SELECT * FROM system.information_schema.column_tags
WHERE  catalog_name = 'dea_learning' AND tag_name = 'pii';

SELECT * FROM system.information_schema.table_tags
WHERE  catalog_name = 'dea_learning';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create the ABAC policies — SQL (`CREATE POLICY`)
-- MAGIC The tags above are only labels until a policy matches on them. Alternative to SQL:
-- MAGIC **Catalog Explorer → Policies → Create policy** builds the same thing in the UI.

-- COMMAND ----------

-- 4. Column-mask policy: mask every pii-tagged column for everyone except pii_readers.
-- Who sees clear text is decided by the POLICY (TO ... EXCEPT ...): for pii_readers the
-- policy simply does not apply and the UDF is never invoked. The UDF therefore shrinks to a
-- pure transformation — value in, masked value out — unlike sec.mask_email from
-- 03_row_filter_column_mask.sql, which still carries its own is_account_group_member() check
-- because in the manual setup the UDF is the only place where the audience can be decided.
CREATE OR REPLACE FUNCTION sec.mask_email_value(email STRING)
RETURNS STRING
RETURN regexp_replace(email, '(^.)(.*)(@.*$)', '$1***$3');

CREATE OR REPLACE POLICY mask_pii
ON SCHEMA silver
COMMENT 'Mask every pii-tagged column for non-privileged users'
COLUMN MASK sec.mask_email_value
TO `account users` EXCEPT `pii_readers`
FOR TABLES
MATCH COLUMNS has_tag_value('pii', 'true') AS pii_col
ON COLUMN pii_col;

-- 5. Row-filter policy: on tables tagged domain=customers, apply region_filter,
-- binding the UDF argument to the column that carries the region tag.
ALTER TABLE silver.customers_silver
  ALTER COLUMN region SET TAGS ('region_col' = 'true');

CREATE OR REPLACE POLICY hide_regions
ON SCHEMA silver
COMMENT 'Region-filter all customer-domain tables'
ROW FILTER sec.region_filter
TO `account users` EXCEPT `admins`
FOR TABLES
WHEN has_tag_value('domain', 'customers')
MATCH COLUMNS has_tag('region_col') AS r
USING COLUMNS (r);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC From that moment, every existing and future column tagged `pii=true` in the schema gets masked and
-- MAGIC every `domain=customers` table gets row-filtered — automatically, no per-table `ALTER`.
-- MAGIC Drop the manual `ALTER TABLE … SET MASK` / `SET ROW FILTER` from `03_row_filter_column_mask.sql` — ABAC replaces both.