# Databricks notebook source
# MAGIC %md
# MAGIC # Week 5 · Unit testing PySpark transformations
# MAGIC Uses `pyspark.testing.utils` — works inside a notebook and inside a CI-runner pytest.

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual

# The function under test
def normalize_customers(df):
    """Trim and title-case names, lower-case emails, drop rows without an id."""
    return (df
        .filter(F.col("id").isNotNull())
        .withColumn("name",  F.initcap(F.trim(F.col("name"))))
        .withColumn("email", F.lower(F.col("email"))))

# COMMAND ----------

# Test 1 — happy path
def test_normalize_basic():
    input_df = spark.createDataFrame([
        (1, " anna ", "ANNA@X.COM"),
        (2, "thomas", "Thomas@x.com"),
    ], "id INT, name STRING, email STRING")

    expected = spark.createDataFrame([
        (1, "Anna",   "anna@x.com"),
        (2, "Thomas", "thomas@x.com"),
    ], "id INT, name STRING, email STRING")

    assertDataFrameEqual(normalize_customers(input_df), expected)
    print("test_normalize_basic ✓")

# Test 2 — null-id row is dropped
def test_normalize_drops_null_id():
    input_df = spark.createDataFrame([
        (1,    "anna",  "a@x.com"),
        (None, "ghost", "g@x.com"),
    ], "id INT, name STRING, email STRING")

    result = normalize_customers(input_df)
    assert result.count() == 1, f"expected 1 row, got {result.count()}"
    print("test_normalize_drops_null_id ✓")

# Test 3 — schema unchanged
def test_normalize_preserves_schema():
    input_df = spark.createDataFrame([], "id INT, name STRING, email STRING")
    assertSchemaEqual(normalize_customers(input_df).schema, input_df.schema)
    print("test_normalize_preserves_schema ✓")

test_normalize_basic()
test_normalize_drops_null_id()
test_normalize_preserves_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running from CI
# MAGIC Move the function and tests into `src/transform.py` and `tests/test_transform.py`.
# MAGIC Then `pytest tests/` in your CI runner — `pyspark.testing.utils` works headless.