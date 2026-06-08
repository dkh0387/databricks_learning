# Databricks notebook source
# MAGIC %md
# MAGIC # Week 2 · One-time setup
# MAGIC Create the catalog/schemas/volume used by every later week, then upload the data files from
# MAGIC `week_2_ingestion/data/` (and `week_4_pipelines_and_jobs/data/cdc/` if you cloned the repo into Databricks
# MAGIC Git Folders) into the landing volume so file paths line up.
# MAGIC
# MAGIC Run this once. Re-running is safe.

# COMMAND ----------

CATALOG = "dea_learning"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ["raw", "bronze", "silver", "gold", "sec"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.landing")

print("Catalog and schemas ready.")
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload source files to the landing volume
# MAGIC If you imported the repo via Databricks Git Folders, the files are already on the workspace filesystem and you can
# MAGIC just `cp` them into the volume. Adjust `REPO_PATH` to wherever your Git Folder lives.

# COMMAND ----------

REPO_PATH    = "/Workspace/Users/<your-user>/databricks_learning"   # <-- EDIT
LANDING      = f"/Volumes/{CATALOG}/raw/landing"

import os

mappings = [
    (f"{REPO_PATH}/week_2_ingestion/data/customers/customers_seed.csv",            f"{LANDING}/customers/customers_seed.csv"),
    (f"{REPO_PATH}/week_2_ingestion/data/items/items.csv",                         f"{LANDING}/items/items.csv"),
    (f"{REPO_PATH}/week_2_ingestion/data/orders/orders_2026-06-01.json",           f"{LANDING}/orders/orders_2026-06-01.json"),
    (f"{REPO_PATH}/week_2_ingestion/data/orders/orders_2026-06-02.json",           f"{LANDING}/orders/orders_2026-06-02.json"),
    (f"{REPO_PATH}/week_2_ingestion/data/orders/orders_2026-06-03.json",           f"{LANDING}/orders/orders_2026-06-03.json"),
    (f"{REPO_PATH}/week_2_ingestion/data/orders/orders_drift.json",                f"{LANDING}/orders_drift/orders_drift.json"),
    (f"{REPO_PATH}/week_4_pipelines_and_jobs/data/cdc/customers_cdc_events.json",  f"{LANDING}/cdc/customers_cdc_events.json"),
]

for src, dst in mappings:
    dbutils.fs.mkdirs(os.path.dirname(dst))
    dbutils.fs.cp(f"file:{src}", dst, recurse=False)

display(dbutils.fs.ls(LANDING))