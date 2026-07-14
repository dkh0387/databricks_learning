# Databricks notebook source
# MAGIC %md
# MAGIC # Week 5 · Induce + fix data skew on orders × customers
# MAGIC **Purpose:** a training lab for exam §6 — you can't practice *diagnosing* skew on healthy data, so this
# MAGIC notebook manufactures the pathology on purpose, makes it visible in the Spark UI, then fixes it twice.
# MAGIC
# MAGIC **Why skew is a problem:** a stage finishes when its *slowest* task finishes. All rows of one join key land in
# MAGIC one partition = one task on one core — a hot key is indivisible. The whale's task grinds for minutes while every
# MAGIC other core sits idle (you pay for the whole cluster, compute on one core), and its oversized working set spills
# MAGIC or OOMs. Note: raising `spark.sql.shuffle.partitions` does NOT help — `hash(key) % n` still sends every whale
# MAGIC row to the same partition. See `learn_troubleshooting.md` ("Why skew hurts").
# MAGIC
# MAGIC One customer ("the whale") gets a million synthetic orders. Customer 1 in our seed is the chosen victim.
# MAGIC Open the Spark UI Stage view between cells to see what skew looks like — and what AQE / salting do to it.
# MAGIC (Third fix not shown: since `customers` is tiny, a broadcast join would remove the shuffle — and thus the skew —
# MAGIC entirely; salting is demonstrated because it also works when BOTH sides are large.)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "dea_learning"

# COMMAND ----------

# Build a heavily skewed orders dataset:
#   customer_id = 1 has 1M orders, every other customer has 100
WHALE_ID         = 1
NUM_CUSTOMERS    = 20         # matches the seed
ORDERS_PER_OTHER = 100

whale_orders = (spark.range(0, 1_000_000)
    .withColumn("customer_id", F.lit(WHALE_ID))
    .withColumn("amount",      F.rand() * 100))

other_orders = (spark.range(0, (NUM_CUSTOMERS - 1) * ORDERS_PER_OTHER)
    .withColumn("customer_id", (F.col("id") % (NUM_CUSTOMERS - 1)) + 2)   # ids 2..20
    .withColumn("amount",      F.rand() * 100))

orders_skewed = whale_orders.union(other_orders)
orders_skewed.createOrReplaceTempView("orders_skewed")

# Use the real silver customers table from Week 3
customers = spark.table(f"{CATALOG}.silver.customers_silver").select("customer_id", "name", "region")
customers.createOrReplaceTempView("customers")

# COMMAND ----------

# Baseline: disable AQE skew-join so you can see the pain in Spark UI
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")

baseline = spark.sql("""
  SELECT c.name, c.region, sum(o.amount) AS total
  FROM   orders_skewed o JOIN customers c USING (customer_id)
  GROUP BY c.name, c.region
""")

baseline.write.format("noop").mode("overwrite").save()

# Open Spark UI → Jobs → click the long-running stage → Summary Metrics.
# Look for: Max task duration >> 75th percentile; Max shuffle read >> Median.

# COMMAND ----------

# Fix 1 — turn AQE skew-join handling back on (default-on in DBR 13+; we disabled it above)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")

baseline.write.format("noop").mode("overwrite").save()

# Open the SQL tab in Spark UI — you should see "Skew Splitter" nodes added by AQE.

# COMMAND ----------

# Fix 2 — manual salting (works without AQE; useful when skew is too extreme for AQE alone)
SALT_BUCKETS = 16

salted = spark.sql(f"""
  SELECT customer_id,
         pmod(hash(rand()), {SALT_BUCKETS}) AS salt,
         amount
  FROM   orders_skewed
""")

customers_salted = customers.withColumn(
    "salt", F.explode(F.array([F.lit(i) for i in range(SALT_BUCKETS)])))

salted.createOrReplaceTempView("orders_salted")
customers_salted.createOrReplaceTempView("customers_salted")

salted_result = spark.sql("""
  SELECT c.name, c.region, sum(o.amount) AS total
  FROM   orders_salted o JOIN customers_salted c
    ON   o.customer_id = c.customer_id AND o.salt = c.salt
  GROUP BY c.name, c.region
""")

salted_result.write.format("noop").mode("overwrite").save()

# Compare in Spark UI: task duration distribution should now be flat.