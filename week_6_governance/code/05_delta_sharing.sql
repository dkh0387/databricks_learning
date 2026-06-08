-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 6 · Delta Sharing
-- MAGIC Read-only cross-org sharing of UC objects. No data copy.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Provider side (you publish)

-- COMMAND ----------

-- 1. Create a share
CREATE SHARE IF NOT EXISTS finance_share;

-- 2. Add tables / schemas to it
ALTER SHARE finance_share ADD TABLE  main.learn.customers;
ALTER SHARE finance_share ADD SCHEMA main.public;          -- whole schema

-- View its contents
DESCRIBE SHARE finance_share;
SHOW ALL IN SHARE finance_share;

-- COMMAND ----------

-- 3. Create a recipient (provide their Delta-Sharing identifier or use a token-based open-share)
-- Databricks-to-Databricks (D2D):
-- CREATE RECIPIENT partner_acme USING ID 'azure:eastus:abc-123-xyz';

-- Open sharing (token-based, for non-Databricks consumers):
-- CREATE RECIPIENT partner_open;
-- DESCRIBE RECIPIENT partner_open;  -- shows activation_link to share with partner

-- 4. Grant share access to the recipient
-- GRANT SELECT ON SHARE finance_share TO RECIPIENT partner_acme;

-- COMMAND ----------

-- 5. Audit who is consuming the share
SELECT * FROM system.information_schema.shares;
SELECT * FROM system.information_schema.recipients;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Recipient side (you consume)
-- MAGIC Run on the *recipient* workspace, which must also be UC.

-- COMMAND ----------

-- Inspect available providers
SHOW PROVIDERS;

-- Mount the share as a read-only catalog
-- CREATE CATALOG acme_data USING SHARE acme.finance_share;

-- Query as any other UC catalog
-- SELECT * FROM acme_data.learn.customers LIMIT 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Caveats
-- MAGIC * Recipients **cannot** see row filters or column masks attached to shared tables — apply them at the source via a view.
-- MAGIC * Sharing is **read-only**; recipients cannot write.
-- MAGIC * Delta format only (Parquet for open clients).
-- MAGIC * Streaming tables are not directly shareable — share a normal table or materialized view instead.