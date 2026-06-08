-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 6 · Delta Sharing — publish `gold_daily_revenue` to a partner
-- MAGIC Read-only, zero-copy cross-org sharing of our final BI surface.

-- COMMAND ----------

USE CATALOG dea_learning;

-- COMMAND ----------

-- ## Provider side (you publish)

-- 1. Share
CREATE SHARE IF NOT EXISTS dea_revenue_share;

-- 2. Add tables / schemas / views
ALTER SHARE dea_revenue_share ADD MATERIALIZED VIEW gold.gold_daily_revenue;
ALTER SHARE dea_revenue_share ADD MATERIALIZED VIEW gold.gold_top_items;
-- Do NOT share silver.silver_customers — contains PII.

DESCRIBE SHARE dea_revenue_share;
SHOW ALL IN SHARE dea_revenue_share;

-- COMMAND ----------

-- 3. Create a recipient
-- Databricks-to-Databricks:
-- CREATE RECIPIENT partner_acme USING ID 'azure:eastus:abc-123-xyz';

-- Open sharing (any tool implementing the open Delta Sharing protocol):
-- CREATE RECIPIENT partner_open;
-- DESCRIBE RECIPIENT partner_open;   -- shows activation_link to share with the partner

-- 4. Grant share access to the recipient
-- GRANT SELECT ON SHARE dea_revenue_share TO RECIPIENT partner_acme;

-- COMMAND ----------

-- 5. Audit who is consuming
SELECT * FROM system.information_schema.shares     WHERE share_name = 'dea_revenue_share';
SELECT * FROM system.information_schema.recipients;

-- COMMAND ----------

-- ## Recipient side (you consume — on the partner's workspace)

-- Inspect available providers
-- SHOW PROVIDERS;

-- Mount the share as a read-only catalog
-- CREATE CATALOG acme_revenue USING SHARE acme.dea_revenue_share;

-- Query as any other UC catalog
-- SELECT order_date, region, revenue FROM acme_revenue.gold.gold_daily_revenue ORDER BY order_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Caveats
-- MAGIC * Recipients **cannot** see row filters / column masks attached to shared tables — enforce them at the source via a view.
-- MAGIC * Sharing is **read-only**.
-- MAGIC * Delta format only (Parquet for open clients).
-- MAGIC * Streaming tables are not directly shareable — share a normal table or materialized view instead.
-- MAGIC * In our pipeline, `gold_daily_revenue` and `gold_top_items` are materialized views, which **are** shareable.