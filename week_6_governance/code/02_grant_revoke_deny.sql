-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 6 · GRANT / REVOKE / DENY in Unity Catalog
-- MAGIC Three-level traversal rule: a user needs USE CATALOG + USE SCHEMA + a leaf privilege to read a table.

-- COMMAND ----------

USE main.learn;

CREATE OR REPLACE TABLE employees (
  id      INT,
  name    STRING,
  salary  DOUBLE,
  city    STRING
) USING DELTA;

INSERT INTO employees VALUES
  (1, 'Anna',   2500, 'Paris'),
  (2, 'Thomas', 3000, 'London'),
  (3, 'Bilal',  3500, 'Paris');

CREATE OR REPLACE VIEW paris_employees AS
SELECT * FROM employees WHERE city = 'Paris';

-- COMMAND ----------

-- Replace these placeholders with real principals in your account
-- account-level group:
-- GRANT USE CATALOG ON CATALOG main           TO `analysts`;
-- GRANT USE SCHEMA  ON SCHEMA  main.learn     TO `analysts`;
-- GRANT SELECT       ON TABLE  main.learn.employees TO `analysts`;

-- COMMAND ----------

-- Engineers get full schema control
-- GRANT ALL PRIVILEGES ON SCHEMA main.learn TO `data_engineers`;

-- Service principal that runs prod jobs
-- GRANT USE CATALOG ON CATALOG main TO `prod-deployer-sp`;
-- GRANT MODIFY ON TABLE main.learn.employees TO `prod-deployer-sp`;

-- COMMAND ----------

-- Limit a specific person to the view only (cannot see London rows)
-- GRANT SELECT ON VIEW main.learn.paris_employees TO `adam@example.com`;

-- COMMAND ----------

-- DENY beats GRANT, including grants inherited via group membership
-- DENY SELECT ON TABLE main.learn.employees TO `contractors`;

-- Revoke when no longer needed
-- REVOKE SELECT ON TABLE main.learn.employees FROM `analysts`;

-- COMMAND ----------

-- Inspect what's granted
SHOW GRANTS ON TABLE main.learn.employees;
SHOW GRANTS ON VIEW  main.learn.paris_employees;
SHOW GRANTS ON SCHEMA main.learn;

-- COMMAND ----------

-- Transfer ownership (no revoke needed; new owner has implicit management rights)
-- ALTER TABLE main.learn.employees OWNER TO `data_platform_admins`;