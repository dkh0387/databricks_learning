# Databricks notebook source
# MAGIC %md
# MAGIC # Week 5 · Induce data skew, then fix it
# MAGIC Run cell-by-cell on a small (single-driver / 2-worker) cluster. Open the Spark UI Stage view between cells.

# COMMAND ----------

from pyspark.sql import functions as F

# Build a skewed dataset: customer 1 has 1M rows, every other customer has 100.
NUM_CUSTOMERS = 1000

skewed = (
    spark.range(0, 1_000_000)
         .withColumn("customer_id", F.lit(1))
         .union(
             spark.range(0, NUM_CUSTOMERS * 100)
                  .withColumn("customer_id", (F.col("id") % NUM_CUSTOMERS) + 2)
         )
         .withColumn("amount", F.rand() * 100)
)

skewed.createOrReplaceTempView("orders_skewed")

customers = spark.range(1, NUM_CUSTOMERS + 2).toDF("customer_id") \
                 .withColumn("name", F.concat(F.lit("cust_"), F.col("customer_id")))
customers.createOrReplaceTempView("customers")

# COMMAND ----------

# Baseline: a join that will skew on customer_id = 1
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")   # turn skew-join handling OFF to see the pain

baseline = spark.sql("""
  SELECT c.name, sum(o.amount) AS total
  FROM   orders_skewed o JOIN customers c USING (customer_id)
  GROUP BY c.name
""")

baseline.write.format("noop").mode("overwrite").save()           # cheap action that still runs all stages

# Open Spark UI → Jobs → click the long-running stage → look at Summary Metrics.
# You will see: Max task duration >> 75th percentile; Max shuffle read >> Median.

# COMMAND ----------

# Fix 1 — turn AQE skew-join back on (default-on in DBR 13+; we disabled it above)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")

baseline.write.format("noop").mode("overwrite").save()

# Inspect the SQL view in Spark UI — you should see "Skew Splitter" nodes added by AQE.

# COMMAND ----------

# Fix 2 — manual salting (works without AQE; useful when skew is too extreme for AQE alone)
SALT_BUCKETS = 16

salted = spark.sql(f"""
  SELECT customer_id,
         pmod(hash(rand()), {SALT_BUCKETS}) AS salt,
         amount
  FROM   orders_skewed
""")

customers_salted = (customers
    .withColumn("salt", F.explode(F.array([F.lit(i) for i in range(SALT_BUCKETS)])))
)

salted.createOrReplaceTempView("orders_salted")
customers_salted.createOrReplaceTempView("customers_salted")

salted_result = spark.sql("""
  SELECT c.name, sum(o.amount) AS total
  FROM   orders_salted o JOIN customers_salted c
    ON   o.customer_id = c.customer_id AND o.salt = c.salt
  GROUP BY c.name
""")

salted_result.write.format("noop").mode("overwrite").save()

# Compare in Spark UI: task duration distribution should now be flat.