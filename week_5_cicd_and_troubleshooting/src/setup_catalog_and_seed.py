# Databricks notebook source
# MAGIC %md
# MAGIC # Bundle copy · Environment setup + seed (parameterized)
# MAGIC Copy of `week_2_ingestion/code/00_setup_catalog_and_seed.py`, parameterized by the `catalog` widget —
# MAGIC the bundle job's `ingest_setup` task passes it via `base_parameters` (`${var.catalog}` per target), so the
# MAGIC first run of each target creates its own catalog (`dea_learning_dev` / `_staging` / `_prod`), schemas,
# MAGIC landing volume, and seed data.
# MAGIC
# MAGIC Re-running is safe (everything is IF NOT EXISTS / overwrite-by-copy).

# COMMAND ----------

# text() registers the widget with a default so the notebook also works run manually;
# in the job the base_parameters value wins.
dbutils.widgets.text("catalog", "dea_learning_dev")
CATALOG = dbutils.widgets.get("catalog")
MANAGED_LOCATION = f"abfss://unity-catalog-storage@dbstorageghdo4vkcqfmqq.dfs.core.windows.net/185960349365378/{CATALOG}"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} MANAGED LOCATION '{MANAGED_LOCATION}'")
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

REPO_PATH    = "/Workspace/Users/denis.khaskin@codecentric.de/databricks_learning"   # <-- EDIT
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

# COMMAND ----------

# Verify raw files were copied properly (%sql magic can't interpolate Python variables — use spark.sql)
display(spark.sql(f"SELECT * FROM read_files('/Volumes/{CATALOG}/raw/landing/customers/')"))
