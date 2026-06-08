# Databricks notebook source
# MAGIC %md
# MAGIC # Week 3 · Bronze → Silver — customers
# MAGIC Clean nulls, normalize types, dedup. Outputs `dea_learning.silver.customers_silver`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "dea_learning"

# COMMAND ----------

# Start from the bronze landed by Week 2
bronze = spark.table(f"{CATALOG}.bronze.customers_bronze")
display(bronze)

# COMMAND ----------

# 1. Type casting + string hygiene + region derivation
silver_typed = (bronze
    .withColumn("customer_id", F.col("customer_id").cast("bigint"))
    .withColumn("signup_date", F.to_date("signup_date", "yyyy-MM-dd"))
    .withColumn("name",        F.initcap(F.trim("name")))
    .withColumn("email",       F.lower(F.trim("email")))
    .withColumn("country",     F.upper(F.trim("country")))
    .withColumn("region",
        F.when(F.col("country").isin("DE","FR","IT","ES","NL","PL","IE","SE","CZ","AT","BE","DK","FI"), "EU")
         .when(F.col("country").isin("US","CA"),                                                         "NA")
         .when(F.col("country").isin("JP","CN","IN","SG","AU","KR"),                                      "APAC")
         .when(F.col("country").isin("BR","MX","AR","CL","CO"),                                           "LATAM")
         .when(F.col("country").isin("AE","SA","EG","ZA"),                                                "EMEA")
         .otherwise("OTHER"))
)

# COMMAND ----------

# 2. Drop rows missing the natural key or critical fields
silver_typed = silver_typed.dropna(subset=["customer_id", "email"])

# 3. Deterministic dedup — keep most recent row per customer_id (latest signup_date wins)
w = Window.partitionBy("customer_id").orderBy(F.desc("signup_date"))

customers_silver = (silver_typed
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn"))

display(customers_silver.orderBy("customer_id"))

# COMMAND ----------

# Write to silver
(customers_silver.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.customers_silver"))

spark.sql(f"SELECT count(*) AS rows FROM {CATALOG}.silver.customers_silver").show()