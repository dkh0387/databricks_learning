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

-- 2. Add tables / schemas / views.
-- Materialized views are shared via the dedicated ADD MATERIALIZED VIEW clause
-- (the documented ALTER SHARE clauses are ADD TABLE / ADD SCHEMA / ADD VIEW /
-- ADD MATERIALIZED VIEW / ADD VOLUME / ADD MODEL).
ALTER SHARE dea_revenue_share ADD MATERIALIZED VIEW gold.gold_daily_revenue;
ALTER SHARE dea_revenue_share ADD MATERIALIZED VIEW gold.gold_top_10_items;
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

-- 5. Inspect shares and recipients (the system.information_schema does NOT include
-- shares/recipients — use SHOW commands and DESCRIBE; for usage trails query the audit log).
SHOW SHARES;
SHOW RECIPIENTS;
DESCRIBE SHARE dea_revenue_share;

-- Audit who actually accessed the share (consumer activity):
SELECT event_time, user_identity.email, action_name, request_params
FROM   system.access.audit
WHERE  service_name = 'unityCatalog'
  AND  action_name LIKE '%Share%'
ORDER BY event_time DESC
LIMIT 50;

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
-- MAGIC * Tables with row filters / column masks **cannot be added to a share** at all — share a view that applies the same logic instead.
-- MAGIC * Sharing is **read-only**.
-- MAGIC * Delta format only (Parquet for open clients).
-- MAGIC * Streaming tables are not directly shareable — share a normal table or materialized view instead.
-- MAGIC * In our pipeline, `gold_daily_revenue` and `gold_top_10_items` are materialized views, which **are** shareable (via `ADD MATERIALIZED VIEW`).