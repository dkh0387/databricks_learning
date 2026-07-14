# Databricks notebook source
# MAGIC %md
# MAGIC # Week 4 · CDF downstream consumer — streaming FROM a mutating table
# MAGIC `customers_scd2` is the target of an AUTO CDC flow (`02_pipeline_auto_cdc_scd2.sql`), so it mutates and
# MAGIC cannot be read with `STREAM(...)`. Because the pipeline enables `delta.enableChangeDataFeed = true` on it,
# MAGIC we can stream its **Change Data Feed** instead: every mutation arrives as an appended change event
# MAGIC (`_change_type` = insert / update_preimage / update_postimage / delete, plus `_commit_version`).
# MAGIC
# MAGIC This notebook keeps a gold replica `customers_cdf` in sync via `foreachBatch` + MERGE.
# MAGIC Run it as a notebook task (or standalone) — NOT inside the declarative pipeline.

# COMMAND ----------

SOURCE = "dea_learning.silver.customers_scd2"
TARGET = "dea_learning.gold.customers_cdf"
CHECKPOINT = "/Volumes/dea_learning/raw/checkpoints/customers_cdf/checkpoint"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Target table — empty clone of the SCD2 schema (without the CDF metadata columns)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET} AS
    SELECT * FROM {SOURCE} WHERE 1 = 0
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read the change feed
# MAGIC Not the table rows — the **change events**. The stream stays append-only and exactly-once.

# COMMAND ----------

changes = (spark.readStream
    .option("readChangeFeed", "true")
    .table(SOURCE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Apply per micro-batch with MERGE
# MAGIC SCD2 rows are versioned, so the merge key is **composite**: `customer_id` + `__START_AT`.
# MAGIC Per key we keep only the latest change in the batch (`_commit_version` wins), drop pre-images,
# MAGIC and exclude the CDF metadata columns from what gets written.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import row_number, desc
from pyspark.sql.window import Window

CDF_META = {"_change_type", "_commit_version", "_commit_timestamp", "rn"}


def apply_changes(batch_df, batch_id):
    w = Window.partitionBy("customer_id", "__START_AT").orderBy(desc("_commit_version"))
    latest = (batch_df
        .filter("_change_type != 'update_preimage'")   # pre-images are noise for a replica
        .withColumn("rn", row_number().over(w))
        .filter("rn = 1"))                             # last change per SCD2 version wins

    data_cols = [c for c in latest.columns if c not in CDF_META]
    col_map = {c: f"s.{c}" for c in data_cols}

    (DeltaTable.forName(spark, TARGET).alias("t")
      .merge(latest.alias("s"),
             "t.customer_id = s.customer_id AND t.__START_AT = s.__START_AT")
      .whenMatchedDelete("s._change_type = 'delete'")
      .whenMatchedUpdate(set=col_map)
      .whenNotMatchedInsert(condition="s._change_type != 'delete'", values=col_map)
      .execute())


(changes.writeStream
   .foreachBatch(apply_changes)
   .option("checkpointLocation", CHECKPOINT)
   .trigger(availableNow=True)       # drain all pending changes, then stop — job-friendly
   .start()
   .awaitTermination())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify

# COMMAND ----------

display(spark.sql(f"""
    SELECT customer_id, name, email, tier, __START_AT, __END_AT
    FROM {TARGET}
    ORDER BY customer_id, __START_AT
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC The chain, end to end:
# MAGIC ```
# MAGIC volume events (append-only)
# MAGIC   → AUTO CDC INTO applies them        → customers_scd2 MUTATES
# MAGIC   → CDF re-encodes the mutations      → append-only change-event feed
# MAGIC   → this stream reads the feed        → foreachBatch + MERGE applies at customers_cdf
# MAGIC ```
# MAGIC Every streaming *edge* stays append-only; mutation happens only at the *targets*.
# MAGIC Inspect the raw feed in batch mode with: `SELECT * FROM table_changes('{SOURCE}', 1)`.