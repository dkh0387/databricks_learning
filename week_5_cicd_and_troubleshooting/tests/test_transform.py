"""Headless unit tests for src/transform.py — the CI counterpart of code/04_pyspark_unit_test.py.

Run from the bundle root:  pytest tests/
(tests/ is excluded from bundle sync — it runs in CI, not in the workspace.)
"""

from pyspark.sql import functions as F
from pyspark.testing.utils import assertDataFrameEqual

from src.transform import normalize_customers

SCHEMA = "customer_id BIGINT, name STRING, email STRING, country STRING, signup_date STRING"


def test_normalize_basic(spark):
    input_df = spark.createDataFrame([
        (1, " Anna Müller ", "ANNA.MULLER@example.com", "de", "2025-01-15"),
        (2, "Bob Johnson",   "Bob.Johnson@Example.com", "us", "2025-02-20"),
    ], SCHEMA)

    expected = spark.createDataFrame([
        (1, "Anna Müller", "anna.muller@example.com", "DE", "2025-01-15"),
        (2, "Bob Johnson", "bob.johnson@example.com", "US", "2025-02-20"),
    ], SCHEMA).withColumn("signup_date", F.to_date("signup_date"))

    assertDataFrameEqual(normalize_customers(input_df), expected)


def test_normalize_drops_missing_email(spark):
    input_df = spark.createDataFrame([
        (1, "Anna",  "anna@x.com", "DE", "2025-01-15"),
        (2, "Ghost", None,         "DE", "2025-02-20"),
    ], SCHEMA)

    result = normalize_customers(input_df)
    assert result.count() == 1


def test_normalize_dedupes_to_latest(spark):
    input_df = spark.createDataFrame([
        (1, "Anna Old", "anna@x.com", "DE", "2025-01-15"),
        (1, "Anna New", "anna@x.com", "DE", "2025-06-01"),
    ], SCHEMA)

    result = normalize_customers(input_df).collect()
    assert len(result) == 1
    assert result[0]["name"] == "Anna New"