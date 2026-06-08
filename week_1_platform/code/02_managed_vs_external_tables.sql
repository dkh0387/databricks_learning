-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 1 · Managed vs External Tables
-- MAGIC Demonstrates the lifecycle difference: dropping a managed table deletes files; dropping an external table does not.
-- MAGIC Replace `dea_learning.playground` with a catalog/schema you can create objects in.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS dea_learning.playground;
USE dea_learning.playground;

-- COMMAND ----------

-- MANAGED table — no LOCATION clause. UC picks the managed storage path.
CREATE OR REPLACE TABLE managed_orders (id INT, amount DOUBLE);
INSERT INTO managed_orders VALUES (1, 9.99), (2, 19.99);

DESCRIBE EXTENDED managed_orders;   -- look at "Type" = MANAGED and "Location"

-- COMMAND ----------

-- Confirm files exist at the location
DESCRIBE DETAIL managed_orders;     -- look at numFiles and location

-- COMMAND ----------

-- EXTERNAL table — explicit LOCATION on a volume or cloud path you own.
-- First create a volume (managed) to hold the files cleanly.
CREATE VOLUME IF NOT EXISTS dea_learning.raw.external_data;

CREATE OR REPLACE TABLE external_orders (id INT, amount DOUBLE)
USING DELTA
LOCATION '/Volumes/dea_learning/raw/external_data/external_orders';

INSERT INTO external_orders VALUES (1, 9.99), (2, 19.99);

DESCRIBE EXTENDED external_orders;   -- Type = EXTERNAL

-- COMMAND ----------

-- Drop both and observe
DROP TABLE managed_orders;
DROP TABLE external_orders;

-- MAGIC %python
-- MAGIC # Files behind the external table should still exist; managed table's files are gone.
-- MAGIC display(dbutils.fs.ls("/Volumes/dea_learning/raw/external_data/external_orders"))

-- COMMAND ----------

-- Recover the external table — same path, no data re-write needed
CREATE TABLE external_orders
USING DELTA
LOCATION '/Volumes/dea_learning/raw/external_data/external_orders';

SELECT * FROM external_orders;