# Databricks notebook source
# MAGIC %md
# MAGIC # Week 5 · Unit testing the silver transformation
# MAGIC Tests the customer-cleansing function used in Week 3. Uses `pyspark.testing.utils` —
# MAGIC works inside a notebook and inside a CI-runner pytest.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual

# The function under test (mirrors week_3_transformation/code/01_silver_cleansing.py)
def normalize_customers(df):
    """Clean nulls, normalise case, dedup by customer_id keeping latest signup_date."""
    typed = (df
        .withColumn("customer_id", F.col("customer_id").cast("bigint"))
        .withColumn("signup_date", F.to_date("signup_date", "yyyy-MM-dd"))
        .withColumn("name",        F.initcap(F.trim("name")))
        .withColumn("email",       F.lower(F.trim("email")))
        .withColumn("country",     F.upper(F.trim("country"))))
    no_nulls = typed.dropna(subset=["customer_id", "email"])
    w = Window.partitionBy("customer_id").orderBy(F.desc("signup_date"))
    return (no_nulls
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn"))

# COMMAND ----------

# Test 1 — happy path
def test_normalize_basic():
    input_df = spark.createDataFrame([
        (1, " Anna Müller ", "ANNA.MULLER@example.com", "de", "2025-01-15"),
        (2, "Bob Johnson",   "Bob.Johnson@Example.com", "us", "2025-02-20"),
    ], "customer_id BIGINT, name STRING, email STRING, country STRING, signup_date STRING")

    expected = spark.createDataFrame([
        (1, "Anna Müller",  "anna.muller@example.com", "DE", "2025-01-15"),
        (2, "Bob Johnson",  "bob.johnson@example.com", "US", "2025-02-20"),
    ], "customer_id BIGINT, name STRING, email STRING, country STRING, signup_date STRING") \
       .withColumn("signup_date", F.to_date("signup_date"))

    assertDataFrameEqual(normalize_customers(input_df), expected)
    print("test_normalize_basic ✓")

# Test 2 — row without an email is dropped
def test_normalize_drops_missing_email():
    input_df = spark.createDataFrame([
        (1, "Anna",  "anna@x.com", "DE", "2025-01-15"),
        (2, "Ghost", None,         "DE", "2025-02-20"),
    ], "customer_id BIGINT, name STRING, email STRING, country STRING, signup_date STRING")

    result = normalize_customers(input_df)
    assert result.count() == 1, f"expected 1 row, got {result.count()}"
    print("test_normalize_drops_missing_email ✓")

# Test 3 — duplicate customer_id is deduplicated to the latest signup_date
def test_normalize_dedupes_to_latest():
    input_df = spark.createDataFrame([
        (1, "Anna Old", "anna@x.com", "DE", "2025-01-15"),
        (1, "Anna New", "anna@x.com", "DE", "2025-06-01"),
    ], "customer_id BIGINT, name STRING, email STRING, country STRING, signup_date STRING")

    result = normalize_customers(input_df).collect()
    assert len(result) == 1
    assert result[0]["name"] == "Anna New"
    print("test_normalize_dedupes_to_latest ✓")

test_normalize_basic()
test_normalize_drops_missing_email()
test_normalize_dedupes_to_latest()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running from CI
# MAGIC Move `normalize_customers` into `src/transform.py` and the tests into `tests/test_transform.py`,
# MAGIC then `pytest tests/` in your CI runner — `pyspark.testing.utils` works headless.