-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 6 · Managed ↔ External table conversion
-- MAGIC Requires DBR 17.0+ or serverless; Delta format.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS main.learn;
CREATE VOLUME IF NOT EXISTS main.learn.external_data;
USE main.learn;

-- COMMAND ----------

-- Start with an EXTERNAL Delta table
CREATE OR REPLACE TABLE orders_ext (id INT, amount DOUBLE)
USING DELTA
LOCATION '/Volumes/main/learn/external_data/orders';

INSERT INTO orders_ext VALUES (1, 9.99), (2, 19.99);

DESCRIBE EXTENDED orders_ext;     -- Type = EXTERNAL

-- COMMAND ----------

-- Convert to MANAGED (no downtime; keeps history, name, perms, views)
ALTER TABLE orders_ext SET MANAGED;

DESCRIBE EXTENDED orders_ext;     -- Type = MANAGED

-- COMMAND ----------

-- Rollback: convert back to EXTERNAL at a new location
ALTER TABLE orders_ext UNSET MANAGED LOCATION '/Volumes/main/learn/external_data/orders_back';

DESCRIBE EXTENDED orders_ext;     -- Type = EXTERNAL again