# Databricks notebook source
# MAGIC %md
# MAGIC # Week 2 · Auto Loader — `rescue` mode against `orders_drift.json`
# MAGIC `orders_drift.json` was crafted with:
# MAGIC * a row where `amount` is the string `"not-a-number"` (type mismatch),
# MAGIC * a row where `status` was uppercased into `Status` (case mismatch),
# MAGIC * rows with extra fields (`channel`, `referral_code`) the schema doesn't know about.
# MAGIC
# MAGIC `rescue` mode keeps every row and captures the drift inside `_rescued_data`.

# COMMAND ----------

CATALOG = "dea_learning"
TARGET  = f"{CATALOG}.bronze.orders_bronze_rescue"

LANDING     = f"/Volumes/{CATALOG}/raw/landing/orders_drift"
SCHEMA_PATH = f"/Volumes/{CATALOG}/raw/landing/_checkpoints/orders_rescue/schema"
CHECKPOINT  = f"/Volumes/{CATALOG}/raw/landing/_checkpoints/orders_rescue/checkpoint"

# COMMAND ----------

(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", SCHEMA_PATH)
   .option("cloudFiles.schemaEvolutionMode", "rescue")
   .option("cloudFiles.schemaHints",
           "order_id BIGINT, customer_id BIGINT, amount DOUBLE, status STRING, currency STRING")
   .load(LANDING)
   .writeStream
   .option("checkpointLocation", CHECKPOINT)
   .trigger(availableNow=True)
   .toTable(TARGET)
   .awaitTermination())

# COMMAND ----------

# Every row landed — drift preserved in _rescued_data
display(spark.sql(f"""
  SELECT order_id, customer_id, amount, status, _rescued_data, _metadata.file_name AS source_file
  FROM   {TARGET}
"""))

# COMMAND ----------

# Operational query: surface every row whose source file produced rescued data
display(spark.sql(f"""
  SELECT _metadata.file_path AS file_path,
         count(*)             AS rescued_rows
  FROM   {TARGET}
  WHERE  _rescued_data IS NOT NULL
  GROUP BY _metadata.file_path
"""))