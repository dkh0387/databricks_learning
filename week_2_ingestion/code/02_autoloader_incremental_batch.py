# Databricks notebook source
# MAGIC %md
# MAGIC # Week 2 · Auto Loader — orders into bronze (incremental batch via `availableNow`)
# MAGIC Three JSON files arrive over three days; the third one adds a `discount_amount` field so you can observe
# MAGIC `addNewColumns` schema evolution end-to-end.
# MAGIC
# MAGIC `00_setup_catalog_and_seed.py` uploaded all three files at once. The cells below partition the demo into
# MAGIC two runs by **physically moving day-3 aside** before the first run, then putting it back.

# COMMAND ----------

CATALOG = "dea_learning"
TARGET  = f"{CATALOG}.bronze.orders_bronze"

LANDING     = f"/Volumes/{CATALOG}/raw/landing/orders"
HOLDOUT     = f"/Volumes/{CATALOG}/raw/landing/_holdout"
SCHEMA_PATH = f"/Volumes/{CATALOG}/raw/landing/_checkpoints/orders/schema"
CHECKPOINT  = f"/Volumes/{CATALOG}/raw/landing/_checkpoints/orders/checkpoint"

DAY3 = "orders_2026-06-03.json"

# COMMAND ----------

# Reset: drop target + checkpoints so we can replay cleanly. Move day-3 out of the landing dir.
dbutils.fs.rm(SCHEMA_PATH, recurse=True)
dbutils.fs.rm(CHECKPOINT,  recurse=True)
spark.sql(f"DROP TABLE IF EXISTS {TARGET}")

dbutils.fs.mkdirs(HOLDOUT)
try:
    dbutils.fs.mv(f"{LANDING}/{DAY3}", f"{HOLDOUT}/{DAY3}")
    print(f"Moved {DAY3} to holdout.")
except Exception:
    print(f"{DAY3} already moved to holdout.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run 1 — only days 1 and 2 visible
# MAGIC Auto Loader infers the schema (including the nested `items` array of structs), lands rows, exits cleanly.

# COMMAND ----------

(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option(
        "cloudFiles.schemaHints",
        "order_id BIGINT, customer_id BIGINT, amount DOUBLE, order_ts TIMESTAMP",
    )
    .load(LANDING)
    .writeStream.option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TARGET)
    .awaitTermination()
)

display(
    spark.sql(f"""
  SELECT order_id, customer_id, status, currency, amount,
         size(from_json(items, 'ARRAY<STRUCT<item_id: STRING, quantity: INT, unit_price: DOUBLE>>')) AS line_item_count,
         _metadata.file_name AS source
  FROM   {TARGET}
  ORDER BY order_id
""")
)

# COMMAND ----------

# Put day 3 back so the next run sees it
dbutils.fs.mv(f"{HOLDOUT}/{DAY3}", f"{LANDING}/{DAY3}")
print(f"Restored {DAY3} to landing.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run 2 — day 3 arrives with the new `discount_amount` field
# MAGIC With the default `addNewColumns` evolution mode this run **fails once** with `UnknownFieldException` — by design.
# MAGIC The schema location is updated; the next run picks up the new column.

# COMMAND ----------

try:
    (spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", SCHEMA_PATH)
       .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # default but just to be explicit
       .load(LANDING)
       .writeStream
       .option("checkpointLocation", CHECKPOINT)
       .option("mergeSchema", "true")
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
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # default but just to be explicit
    .load(LANDING)
    .writeStream
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET)
    .awaitTermination())

display(spark.sql(f"""
  SELECT order_id, customer_id, status, amount, discount_amount,
         _metadata.file_name AS source
  FROM   {TARGET}
  ORDER BY order_id
"""))
