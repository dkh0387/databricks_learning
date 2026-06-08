# Databricks notebook source
# MAGIC %md
# MAGIC # Week 3 · Bronze → Silver cleansing
# MAGIC Null handling, type casting, string hygiene, and deterministic dedup with `row_number()`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "main"
SCHEMA  = "learn"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Build a messy bronze
bronze = spark.createDataFrame([
    (1, " Anna ",   "2500",  "Paris",   "2026-06-01", None),
    (2, "Thomas",   "3000",  "LONDON",  "2026-06-01", None),
    (3, "Bilal",    "abc",   "paris",   "2026-06-01", "duplicate"),
    (3, "Bilal",    "3500",  "Paris",   "2026-06-02", "newer"),     # same id, newer date
    (4, None,       "2000",  "Paris",   "2026-06-01", None),
    (5, "Sophie",   None,    "London",  "2026-06-01", None),
], "id INT, name STRING, salary_str STRING, city STRING, updated_at_str STRING, note STRING")

display(bronze)

# COMMAND ----------

# 1) Type casting + string hygiene
silver_typed = (bronze
    .withColumn("salary",     F.col("salary_str").cast("double"))        # "abc" → null
    .withColumn("updated_at", F.to_date("updated_at_str", "yyyy-MM-dd"))
    .withColumn("name",       F.initcap(F.trim("name")))
    .withColumn("city",       F.initcap(F.trim("city")))
    .drop("salary_str", "updated_at_str", "note")
)

display(silver_typed)

# COMMAND ----------

# 2) Null handling
no_critical_nulls = silver_typed.dropna(subset=["id", "name", "updated_at"])
filled            = silver_typed.fillna({"salary": 0.0, "name": "unknown"})

print("After dropna critical:", no_critical_nulls.count())
print("After fillna:         ", filled.count())

# COMMAND ----------

# 3) Deterministic dedup: keep latest row per id
w = Window.partitionBy("id").orderBy(F.desc("updated_at"))

silver = (silver_typed
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn"))

display(silver)

# COMMAND ----------

# Write silver
(silver.write
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(f"{CATALOG}.{SCHEMA}.employees_silver"))

spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.employees_silver").show()