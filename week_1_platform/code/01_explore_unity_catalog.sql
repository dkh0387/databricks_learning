-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 1 · Explore Unity Catalog
-- MAGIC Walks the three-level namespace and the metadata commands that surface tables, schemas and grants.
-- MAGIC Import this file into your Databricks workspace and run cell by cell.

-- COMMAND ----------

-- List all catalogs visible to you
SHOW CATALOGS;

-- COMMAND ----------

-- Pick one and list its schemas
SHOW SCHEMAS IN main;

-- COMMAND ----------

-- List tables, views, volumes inside a schema
SHOW TABLES   IN main.default;
SHOW VIEWS    IN main.default;
SHOW VOLUMES  IN main.default;

-- COMMAND ----------

-- The current session context — useful for parameter-free demos
SELECT
  current_catalog()  AS current_catalog,
  current_schema()   AS current_schema,
  current_user()     AS current_user,
  session_user()     AS session_user;

-- COMMAND ----------

-- Three-level namespace in action
USE CATALOG main;
USE SCHEMA  default;

-- This now resolves as main.default.my_table (if it exists)
-- SELECT * FROM my_table LIMIT 10;

-- COMMAND ----------

-- Information schema — works in UC
SELECT table_catalog, table_schema, table_name, table_type, created
FROM   system.information_schema.tables
WHERE  table_catalog = 'main'
ORDER BY created DESC
LIMIT 20;

-- COMMAND ----------

-- Audit trail for who-did-what
SELECT event_time, user_identity.email AS user, action_name, request_params.full_name_arg AS object_name
FROM   system.access.audit
WHERE  event_date >= current_date() - INTERVAL 1 DAYS
ORDER BY event_time DESC
LIMIT 20;