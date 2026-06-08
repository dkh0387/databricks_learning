# Databricks notebook source
# MAGIC %md
# MAGIC # Week 2 · Auto Loader — incremental batch (`availableNow`)
# MAGIC Same primitives as streaming but processes whatever's pending and stops. Ideal for hourly/daily Jobs.

# COMMAND ----------

CATALOG = "main"
SCHEMA  = "learn"
VOL     = "landing"
TARGET  = f"{CATALOG}.{SCHEMA}.orders_bronze_al"

LANDING       = f"/Volumes/{CATALOG}/{SCHEMA}/{VOL}/al_orders"
SCHEMA_PATH   = f"/Volumes/{CATALOG}/{SCHEMA}/{VOL}/_checkpoints/al_orders/schema"
CHECKPOINT    = f"/Volumes/{CATALOG}/{SCHEMA}/{VOL}/_checkpoints/al_orders/checkpoint"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOL}")

# COMMAND ----------

# Seed three files
for fname, body in [
    ("orders_001.json", '{"id":1,"amount":9.99,"region":"EU"}'),
    ("orders_002.json", '{"id":2,"amount":19.99,"region":"US"}'),
    ("orders_003.json", '{"id":3,"amount":4.99,"region":"EU"}'),
]:
    dbutils.fs.put(f"{LANDING}/{fname}", body, overwrite=True)

# COMMAND ----------

# First run — Auto Loader infers schema, lands rows, exits
(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", SCHEMA_PATH)
   .load(LANDING)
   .writeStream
   .option("checkpointLocation", CHECKPOINT)
   .trigger(availableNow=True)
   .toTable(TARGET)
   .awaitTermination())

display(spark.table(TARGET))

# COMMAND ----------

# Re-run with no new files — does nothing
(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", SCHEMA_PATH)
   .load(LANDING)
   .writeStream
   .option("checkpointLocation", CHECKPOINT)
   .trigger(availableNow=True)
   .toTable(TARGET)
   .awaitTermination())

print("Row count after no-op:", spark.table(TARGET).count())

# COMMAND ----------

# Drop a NEW file with a new column → schema evolution
dbutils.fs.put(f"{LANDING}/orders_004.json",
               '{"id":4,"amount":29.99,"region":"DE","currency":"EUR"}',
               overwrite=True)

# MAGIC %md
# MAGIC With the default `addNewColumns` evolution mode, this run **will fail once** with
# MAGIC `UnknownFieldException` — by design. The schema location is updated; the next run picks up `currency`.

# COMMAND ----------

# Expect failure on this run
try:
    (spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", SCHEMA_PATH)
       .load(LANDING)
       .writeStream
       .option("checkpointLocation", CHECKPOINT)
       .trigger(availableNow=True)
       .toTable(TARGET)
       .awaitTermination())
except Exception as e:
    print("Caught expected failure:", type(e).__name__, str(e)[:200])

# COMMAND ----------

# Second run picks up the new column
(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", SCHEMA_PATH)
   .load(LANDING)
   .writeStream
   .option("checkpointLocation", CHECKPOINT)
   .trigger(availableNow=True)
   .toTable(TARGET)
   .awaitTermination())

display(spark.table(TARGET))