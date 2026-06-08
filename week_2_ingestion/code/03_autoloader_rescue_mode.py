# Databricks notebook source
# MAGIC %md
# MAGIC # Week 2 · Auto Loader — `rescue` mode and `_rescued_data`
# MAGIC `rescue` never evolves the schema; unmatched fields land in `_rescued_data`. Inspect it to catch upstream drift.

# COMMAND ----------

CATALOG = "main"
SCHEMA  = "learn"
VOL     = "landing"
TARGET  = f"{CATALOG}.{SCHEMA}.orders_rescue"

LANDING     = f"/Volumes/{CATALOG}/{SCHEMA}/{VOL}/rescue_orders"
SCHEMA_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOL}/_checkpoints/rescue_orders/schema"
CHECKPOINT  = f"/Volumes/{CATALOG}/{SCHEMA}/{VOL}/_checkpoints/rescue_orders/checkpoint"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOL}")

# COMMAND ----------

# Seed with one canonical-schema file plus one drift file
dbutils.fs.put(f"{LANDING}/orders_001.json",
               '{"id":1,"amount":9.99,"region":"EU"}',
               overwrite=True)
dbutils.fs.put(f"{LANDING}/orders_drift.json",
               '{"id":2,"amount":"not-a-number","region":"US","unexpected_field":"xyz"}',
               overwrite=True)

# COMMAND ----------

# Rescue mode — pin amount type so the type mismatch is captured rather than silently coerced
(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", SCHEMA_PATH)
   .option("cloudFiles.schemaEvolutionMode", "rescue")
   .option("cloudFiles.schemaHints", "amount DOUBLE")
   .load(LANDING)
   .writeStream
   .option("checkpointLocation", CHECKPOINT)
   .trigger(availableNow=True)
   .toTable(TARGET)
   .awaitTermination())

# COMMAND ----------

# All rows landed — drift is preserved inside _rescued_data
display(spark.sql(f"""
  SELECT id, amount, region, _rescued_data, _metadata.file_path
  FROM   {TARGET}
"""))

# COMMAND ----------

# Operational query: surface any file with rescued data so an engineer can investigate
display(spark.sql(f"""
  SELECT DISTINCT _metadata.file_path AS file_path, _rescued_data
  FROM   {TARGET}
  WHERE  _rescued_data IS NOT NULL
"""))