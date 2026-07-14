# Databricks notebook source
# MAGIC %md
# MAGIC # Week 4 · Region report — for-each target notebook
# MAGIC Runs once **per region** from the job's `for_each_region` task (`03_lakeflow_job_definition.json`).
# MAGIC The region arrives as the task parameter `region` (`{{input}}` from the for-each inputs list).
# MAGIC Reads the gold layer produced by the medallion pipeline and prints a compact revenue report.

# COMMAND ----------

dbutils.widgets.text("region", "EU")
REGION = dbutils.widgets.get("region")
print(f"Building report for region: {REGION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily revenue for the region

# COMMAND ----------

daily = spark.sql(f"""
    SELECT order_date, orders, revenue, avg_order_amount
    FROM   dea_learning.gold.gold_daily_revenue
    WHERE  region = '{REGION}'
    ORDER BY order_date
""")
display(daily)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top customers in the region

# COMMAND ----------

top_customers = spark.sql(f"""
    SELECT rank, name, lifetime_orders, lifetime_spend
    FROM   dea_learning.gold.gold_top_10_customers
    WHERE  region = '{REGION}'
    ORDER BY rank
""")
display(top_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary line (shows up in the job run output)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dea_learning.gold.summary (
# MAGIC     region STRING,
# MAGIC     timestamp TIMESTAMP,
# MAGIC     summary STRING,
# MAGIC     PRIMARY KEY (region, timestamp)
# MAGIC );

# COMMAND ----------

row = daily.selectExpr(
    "count(*) AS days", "coalesce(sum(orders), 0) AS orders", "coalesce(sum(revenue), 0.0) AS revenue"
).first()
summary = f"[{REGION}] {row.days} day(s), {row.orders} order(s), total revenue {row.revenue:.2f}"
print(summary)

# COMMAND ----------

spark.sql(
    "INSERT INTO dea_learning.gold.summary VALUES (:region, current_date(), :summary_val)",
    args={"region": REGION, "summary_val": summary}
);
