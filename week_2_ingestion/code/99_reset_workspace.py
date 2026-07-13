# Databricks notebook source
# MAGIC %md
# MAGIC # Full reset — wipe everything and start fresh
# MAGIC Counterpart to `00_setup_catalog_and_seed.py`. Drops **all** objects the course creates so you can rerun
# MAGIC everything from scratch.
# MAGIC
# MAGIC ⚠️ **Destructive.** `DROP CATALOG ... CASCADE` removes every schema, table, streaming table, materialized
# MAGIC view, function, and volume in `dea_learning` — including the landing volume (seed files, checkpoints, schema
# MAGIC locations). Managed-table data is recoverable via `UNDROP TABLE` for ~7 days; files are cleaned up within
# MAGIC ~30 days.
# MAGIC
# MAGIC What this notebook does NOT delete (no SQL API — see the last cell):
# MAGIC - the Lakeflow Spark Declarative **Pipeline** (week 4)
# MAGIC - the Lakeflow **Job** (week 4)
# MAGIC - Git Folders / notebooks in your workspace

# COMMAND ----------

CATALOG = "dea_learning"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Delta Sharing objects (week 6) — metastore-level, outside the catalog
# MAGIC Wrapped in try/except: they only exist if you ran `05_delta_sharing.sql`, and dropping them needs
# MAGIC metastore-admin (or owner) rights.

# COMMAND ----------

for stmt in [
    "DROP SHARE IF EXISTS dea_revenue_share",
    "DROP RECIPIENT IF EXISTS partner_acme",
]:
    try:
        spark.sql(stmt)
        print(f"OK:      {stmt}")
    except Exception as e:
        print(f"SKIPPED: {stmt} — {str(e).splitlines()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Drop the catalog with everything in it
# MAGIC One statement removes all five schemas (`raw`, `bronze`, `silver`, `gold`, `sec`) and every object inside —
# MAGIC tables from weeks 1–3, the pipeline's streaming tables and MVs, row-filter/mask functions, tags, the landing
# MAGIC volume with all files and checkpoints.

# COMMAND ----------

spark.sql(f"DROP CATALOG IF EXISTS {CATALOG} CASCADE")
print(f"Catalog {CATALOG} dropped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delete pipeline and job (manual or CLI)
# MAGIC These live outside Unity Catalog and cannot be dropped via SQL. Either delete them in the UI
# MAGIC (**Jobs & Pipelines** → kebab menu → Delete) or via the Databricks CLI:
# MAGIC
# MAGIC ```bash
# MAGIC databricks pipelines list-pipelines | grep -i medallion   # find the pipeline id
# MAGIC databricks pipelines delete <pipeline-id>
# MAGIC
# MAGIC databricks jobs list | grep -i medallion                  # find the job id
# MAGIC databricks jobs delete <job-id>
# MAGIC ```
# MAGIC
# MAGIC Note: deleting the pipeline in the UI usually offers to drop its tables too — irrelevant here, the
# MAGIC catalog drop above already removed them.
# MAGIC
# MAGIC ## 4. Start fresh
# MAGIC Rerun `00_setup_catalog_and_seed.py` — it recreates the catalog, schemas, landing volume, and seed files.