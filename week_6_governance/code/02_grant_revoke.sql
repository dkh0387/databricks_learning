-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 6 · GRANT / REVOKE on the medallion catalog
-- MAGIC Three-level traversal rule: a user needs `USE CATALOG` + `USE SCHEMA` + a leaf privilege to read a table.

-- COMMAND ----------

USE CATALOG dea_learning;

-- COMMAND ----------

-- Replace these placeholders with real principals in your account.

-- 1. Analysts: read-only on the gold layer.
-- The UC-idiomatic form: GRANT SELECT ON SCHEMA cascades to all current AND future
-- tables / views / MVs in that schema. Use the per-object form (ON TABLE / ON VIEW) only
-- when you want to scope to a specific object.
-- GRANT USE CATALOG ON CATALOG dea_learning           TO `analysts`;
-- GRANT USE SCHEMA  ON SCHEMA  dea_learning.gold      TO `analysts`;
-- GRANT SELECT      ON SCHEMA  dea_learning.gold      TO `analysts`;   -- cascades to all objects

-- COMMAND ----------

-- 2. Engineers: full control of bronze + silver, read on gold
-- GRANT ALL PRIVILEGES ON SCHEMA dea_learning.bronze TO `data_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA dea_learning.silver TO `data_engineers`;
-- GRANT USE CATALOG    ON CATALOG dea_learning       TO `data_engineers`;
-- GRANT USE SCHEMA     ON SCHEMA  dea_learning.gold  TO `data_engineers`;
-- GRANT SELECT         ON SCHEMA  dea_learning.gold  TO `data_engineers`;   -- inherits to all tables

-- COMMAND ----------

-- 3. Prod service principal: write on bronze + silver only
-- GRANT USE CATALOG ON CATALOG dea_learning            TO `prod-deployer-sp`;
-- GRANT USE SCHEMA  ON SCHEMA  dea_learning.bronze     TO `prod-deployer-sp`;
-- GRANT MODIFY      ON SCHEMA  dea_learning.bronze     TO `prod-deployer-sp`;   -- inherits to all tables
-- GRANT USE SCHEMA  ON SCHEMA  dea_learning.silver     TO `prod-deployer-sp`;
-- GRANT MODIFY      ON SCHEMA  dea_learning.silver     TO `prod-deployer-sp`;

-- COMMAND ----------

-- 4. Marketing user: restrict to a view that filters to their region
CREATE OR REPLACE VIEW gold.eu_daily_revenue AS
SELECT * FROM gold.gold_daily_revenue WHERE region = 'EU';

-- GRANT USE CATALOG ON CATALOG dea_learning              TO `eu-marketing@example.com`;
-- GRANT USE SCHEMA  ON SCHEMA  dea_learning.gold         TO `eu-marketing@example.com`;
-- GRANT SELECT       ON VIEW    dea_learning.gold.eu_daily_revenue TO `eu-marketing@example.com`;

-- COMMAND ----------

-- 5. Exam trap: UC does NOT support DENY — that is legacy Hive metastore table ACLs.
-- To restrict access in UC, REVOKE or simply don't grant.

-- COMMAND ----------

-- 6. Revoke
-- REVOKE SELECT ON TABLE dea_learning.silver.silver_customers FROM `analysts`;

-- COMMAND ----------

-- Inspect
SHOW GRANTS ON SCHEMA dea_learning.gold;
SHOW GRANTS ON TABLE  dea_learning.silver.silver_customers;
SHOW GRANTS ON VIEW   dea_learning.gold.eu_daily_revenue;

-- COMMAND ----------

-- Transfer ownership (no revoke needed; owner has implicit management rights)
-- ALTER TABLE dea_learning.silver.silver_customers OWNER TO `data_platform_admins`;