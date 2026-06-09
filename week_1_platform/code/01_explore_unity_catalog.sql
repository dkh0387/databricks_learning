-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 1 · Explore Unity Catalog
-- MAGIC Walks the three-level namespace and the metadata commands that surface tables, schemas and grants.
-- MAGIC Import this file into your Databricks workspace and run cell by cell.

-- COMMAND ----------

-- List all catalogs visible to you
SHOW CATALOGS;

-- COMMAND ----------

-- List all external locations visible to you
-- Note: the location is needed in the next step to create a catalog
SHOW EXTERNAL LOCATIONS;

-- COMMAND ----------

-- Setup Unity Catalog
CREATE CATALOG IF NOT EXISTS dea_learning MANAGED LOCATION 'abfss://unity-catalog-storage@dbstorageghdo4vkcqfmqq.dfs.core.windows.net/185960349365378/dea_learning';
CREATE SCHEMA IF NOT EXISTS dea_learning.raw;
CREATE SCHEMA IF NOT EXISTS dea_learning.bronze;
CREATE SCHEMA IF NOT EXISTS dea_learning.silver;
CREATE SCHEMA IF NOT EXISTS dea_learning.gold;
CREATE SCHEMA IF NOT EXISTS dea_learning.sec;
CREATE VOLUME IF NOT EXISTS dea_learning.raw.landing;

-- COMMAND ----------

-- Pick one and list its schemas
SHOW SCHEMAS IN dea_learning;

-- COMMAND ----------

-- List tables, views, volumes inside a schema
USE CATALOG dea_learning;
SHOW TABLES IN raw;
SHOW VIEWS IN raw;
SHOW VOLUMES IN raw;

-- COMMAND ----------

-- The current session context — useful for parameter-free demos
SELECT
  current_catalog()  AS current_catalog,
  current_schema()   AS current_schema,
  current_user()     AS current_user,
  session_user()     AS session_user;

-- COMMAND ----------

-- Three-level namespace in action
USE CATALOG dea_learning;
USE SCHEMA  bronze;

-- Unqualified names now resolve under dea_learning.bronze
-- SELECT * FROM customers_bronze LIMIT 10;     -- after Week 2 has run

-- COMMAND ----------

-- Information schema — works in UC
SELECT table_catalog, table_schema, table_name, table_type, created
FROM   system.information_schema.tables
WHERE  table_catalog = 'dea_learning'
ORDER BY created DESC
LIMIT 20;

-- COMMAND ----------

-- Audit trail for who-did-what
SELECT event_time, user_identity.email AS user, action_name, request_params.full_name_arg AS object_name
FROM   system.access.audit
WHERE  event_date >= current_date() - INTERVAL 1 DAYS
ORDER BY event_time DESC
LIMIT 20;
