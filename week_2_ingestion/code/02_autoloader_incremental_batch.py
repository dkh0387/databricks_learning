# Databricks notebook source
# MAGIC %md
# MAGIC # Week 2 · Auto Loader — orders into bronze (incremental batch via `availableNow`)
# MAGIC Three JSON files arrive over three days; the third one adds a `discount_amount` field so you can observe
# MAGIC `addNewColumns` schema evolution end-to-end.

# COMMAND ----------

CATALOG = "dea_learning"
TARGET  = f"{CATALOG}.bronze.orders_bronze"

LANDING     = f"/Volumes/{CATALOG}/raw/landing/orders"
SCHEMA_PATH = f"/Volumes/{CATALOG}/raw/landing/_checkpoints/orders/schema"
CHECKPOINT  = f"/Volumes/{CATALOG}/raw/landing/_checkpoints/orders/checkpoint"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run 1 — only `orders_2026-06-01.json` and `orders_2026-06-02.json` are present
# MAGIC Auto Loader infers the schema (including the nested `items` array of structs), lands rows, exits.

# COMMAND ----------

(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", SCHEMA_PATH)
   .option("cloudFiles.schemaHints", "order_id BIGINT, customer_id BIGINT, amount DOUBLE, order_ts TIMESTAMP")
   .load(LANDING)
   .writeStream
   .option("checkpointLocation", CHECKPOINT)
   .trigger(availableNow=True)
   .toTable(TARGET)
   .awaitTermination())

display(spark.sql(f"""
  SELECT order_id, customer_id, status, currency, amount,
         size(items) AS line_item_count,
         _metadata.file_name AS source
  FROM   {TARGET}
  ORDER BY order_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run 2 — `orders_2026-06-03.json` arrives with the new `discount_amount` field
# MAGIC With the default `addNewColumns` evolution mode this run **fails once** with `UnknownFieldException` — by design.
# MAGIC The schema location is updated; the next run picks up the new column.

# COMMAND ----------

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
    print("Caught expected failure:", type(e).__name__)
    print(str(e)[:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run 3 — restart picks up the new column and ingests day 3

# COMMAND ----------

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

display(spark.sql(f"""
  SELECT order_id, customer_id, status, amount, discount_amount,
         _metadata.file_name AS source
  FROM   {TARGET}
  ORDER BY order_id
"""))