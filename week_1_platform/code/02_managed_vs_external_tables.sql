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

-- EXTERNAL table — explicit LOCATION at a path outside any UC-managed storage.
-- For the "files survive table drop" lesson to be unambiguous, the table must live in either an
-- EXTERNAL VOLUME or an EXTERNAL LOCATION (cloud path you control). A managed volume mixes
-- UC's volume lifecycle with the table's lifecycle and muddies the demo.
--
-- Requires: an existing EXTERNAL LOCATION covering the path (the external location itself references a
-- storage credential — CREATE EXTERNAL VOLUME takes no STORAGE CREDENTIAL clause).
-- Replace <CLOUD_PATH> below with a path under that external location.

-- CREATE EXTERNAL VOLUME IF NOT EXISTS dea_learning.raw.external_data
--   LOCATION '<CLOUD_PATH>/external_data';                             -- e.g. s3://my-bucket/dea_demo/external_data

CREATE OR REPLACE TABLE external_orders (id INT, amount DOUBLE)
USING DELTA
LOCATION 'abfss://unity-catalog-storage@dbstorageghdo4vkcqfmqq.dfs.core.windows.net/185960349365378/external/external_orders';

INSERT INTO external_orders VALUES (1, 9.99), (2, 19.99);

DESCRIBE EXTENDED external_orders;   -- Type = EXTERNAL

-- COMMAND ----------

-- Drop both and observe
DROP TABLE managed_orders;
DROP TABLE external_orders;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Managed table's files are gone (UC deleted them).
-- MAGIC # External table's files survive because they live in an external volume / cloud location.
-- MAGIC try:
-- MAGIC     display(dbutils.fs.ls("abfss://unity-catalog-storage@dbstorageghdo4vkcqfmqq.dfs.core.windows.net/185960349365378/dea_learning/__unitystorage/catalogs/3e756225-87cf-4374-9e79-9a3db1c5fe24/tables/20525318-7259-42f6-9d15-489c355cabf7"))
-- MAGIC except Exception as e:
-- MAGIC     print(f"✓ Managed table path is not accessible (UC controls managed storage): {e}")
-- MAGIC
-- MAGIC print("\n✓ External table files still exist:")
-- MAGIC display(dbutils.fs.ls("abfss://unity-catalog-storage@dbstorageghdo4vkcqfmqq.dfs.core.windows.net/185960349365378/external/external_orders"))

-- COMMAND ----------

-- Recover the external table — same path, no data re-write needed
CREATE TABLE external_orders
USING DELTA
LOCATION 'abfss://unity-catalog-storage@dbstorageghdo4vkcqfmqq.dfs.core.windows.net/185960349365378/external/external_orders';

SELECT * FROM external_orders;
