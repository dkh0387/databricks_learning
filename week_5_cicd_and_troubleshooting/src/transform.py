"""Silver transformation logic, factored out of the notebooks so it is unit-testable in CI.

Mirrors week_3_transformation/code/01_silver_cleansing.py; the interactive walkthrough of these
tests lives in code/04_pyspark_unit_test.py. Run the tests headless with `pytest tests/` from the
bundle root (this folder's parent of tests/) — requires a local `pip install pyspark`.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


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