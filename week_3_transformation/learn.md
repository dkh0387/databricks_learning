# Data Transformation and Modeling

Covers exam **§3 (22%)** — the largest weighted section. Supplements the hands-on Udemy notebooks `2.1`–`2.4` and `3.1`–`3.3`. Read after `../week_2_ingestion/learn_lakeflow_connect.md` (bronze) and before `../week_5_cicd_and_troubleshooting/learn_troubleshooting.md` (perf knobs in depth).

## 1. Medallion transformation pattern

```
Bronze (raw, append-only)
   │   cleanse, type cast, drop bad rows
   ▼
Silver (modeled, deduplicated, conformed)
   │   join, aggregate, business logic
   ▼
Gold  (business / BI / ML feature ready)
```

Bronze = "what arrived". Silver = "what is true". Gold = "what's useful".

## 2. Cleaning bronze → silver

### Null handling

```python
# Drop rows where any/key columns are null
df.dropna()                                 # any null
df.dropna(subset=["customer_id"])           # specific col(s)
df.dropna(how="all")                        # only if every col is null
df.dropna(thresh=3)                         # keep rows with ≥3 non-nulls

# Fill nulls
df.fillna(0)                                # all numeric cols → 0
df.fillna({"city": "unknown", "age": 0})    # per-col map
df.na.replace("N/A", None, subset=["city"]) # treat sentinel as null
```

```sql
SELECT *
FROM bronze.orders
WHERE customer_id IS NOT NULL                 -- explicit
  AND coalesce(amount, 0) > 0;                -- null-safe arithmetic
```

### Type casting / standardization

```python
from pyspark.sql.functions import col, to_date, to_timestamp

silver = (bronze
  .withColumn("order_id",   col("order_id").cast("long"))
  .withColumn("order_date", to_date("order_date_str", "yyyy-MM-dd"))
  .withColumn("created_at", to_timestamp("created_at"))
  .withColumn("amount",     col("amount").cast("decimal(18,2)"))
)
```

```sql
SELECT
  CAST(order_id   AS BIGINT)              AS order_id,
  TO_DATE(order_date_str, 'yyyy-MM-dd')   AS order_date,
  CAST(amount     AS DECIMAL(18,2))       AS amount
FROM bronze.orders;
```

### String hygiene

`lower`, `upper`, `trim`, `ltrim`, `rtrim`, `regexp_replace`, `regexp_extract`, `initcap`.

```sql
SELECT trim(lower(email)) AS email,
       regexp_replace(phone, '[^0-9]', '') AS phone_digits
FROM bronze.users;
```

## 3. Joining DataFrames

Every join type in one place — memorise the names exactly as the exam phrases them.

```python
a.join(b, on="key", how="inner")               # default
a.join(b, on="key", how="left")                # = "left_outer"
a.join(b, on="key", how="right")               # = "right_outer"
a.join(b, on="key", how="full")                # = "full_outer" / "outer"
a.join(b, on="key", how="left_semi")           # rows of a that match in b (no b cols)
a.join(b, on="key", how="left_anti")           # rows of a with NO match in b
a.crossJoin(b)                                 # cartesian — no key

# Multi-key
a.join(b, on=["country", "city"], how="inner")

# Different column names on each side
a.join(b, a.cust_id == b.id, how="left")

# Broadcast join hint (small b)
from pyspark.sql.functions import broadcast
a.join(broadcast(b), "key")
```

```sql
-- BROADCAST hint — items catalog is small (~15 rows), perfect to broadcast onto each order
SELECT /*+ BROADCAST(i) */ o.order_id, item.item_id, i.name, i.category
FROM   dea_learning.bronze.orders_bronze o
LATERAL VIEW explode(o.items) AS item
JOIN   dea_learning.bronze.items_bronze i ON item.item_id = i.item_id;

-- UNION vs UNION ALL
SELECT * FROM a UNION     SELECT * FROM b;     -- deduplicated
SELECT * FROM a UNION ALL SELECT * FROM b;     -- concatenated, faster
SELECT * FROM a INTERSECT SELECT * FROM b;
SELECT * FROM a EXCEPT    SELECT * FROM b;
```

### Join-type decision

| Use | When |
| --- | --- |
| `inner` | Only rows present in both |
| `left` | Keep all left rows; nullable right cols |
| `full` | Keep everything, mark unmatched sides null |
| `left_semi` | "Does this exist?" filter without dragging right cols |
| `left_anti` | "What's missing?" — find unmatched left rows |
| `broadcast` | Right side is small (< few hundred MB) |
| `cross` | All combinations (rare; expensive) |

## 4. Column, row, and structure manipulation

### Add / drop / rename / split

```python
df.withColumn("total", col("qty") * col("price"))
df.withColumnRenamed("old", "new")
df.drop("col1", "col2")

# Split + multiple columns
df.withColumn("first_name", split(col("full_name"), " ").getItem(0)) \
  .withColumn("last_name",  split(col("full_name"), " ").getItem(1))
```

### Filter

```python
df.filter(col("amount") > 100)
df.where("status = 'PAID' AND amount > 0")
df.filter(col("country").isin("DE", "FR", "IT"))
df.filter(col("email").rlike(".+@.+\\..+"))
```

### Explode arrays and structs

```python
from pyspark.sql.functions import explode, explode_outer, posexplode, col

# array column "items" → one row per element. Prefer .select() over .withColumn() —
# explode is a generator; using it in withColumn is deprecated.
df.select("*", explode("items").alias("item"))            # drops rows with empty/null
df.select("*", explode_outer("items").alias("item"))      # keeps them as null
df.select(posexplode("items").alias("pos", "item"))       # returns (pos, col) pair

# struct column → flatten
df.select("id", "address.*")                       # struct fields become top-level cols
```

### Pivot

```python
df.groupBy("month").pivot("region").sum("sales")
```

## 5. Deduplication and aggregation

### Deduplication

```python
df.distinct()                                   # all-column dedup
df.dropDuplicates(["customer_id"])              # arbitrary winner — fragile
```

**Deterministic dedup** (keep latest per key — common silver-layer pattern):

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

w = Window.partitionBy("customer_id").orderBy(desc("updated_at"))
silver = (bronze
  .withColumn("rn", row_number().over(w))
  .filter("rn = 1")
  .drop("rn"))
```

```sql
SELECT * EXCEPT(rn) FROM (
  SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
  FROM bronze.customers
) WHERE rn = 1;
```

### Aggregations

```python
from pyspark.sql.functions import count, countDistinct, approx_count_distinct, sum, avg, mean, min, max

df.agg(
  count("*"),
  countDistinct("customer_id"),                  # exact, expensive
  approx_count_distinct("customer_id", 0.05),    # HyperLogLog, ~5% error, fast
  sum("amount"),
  avg("amount"),
  mean("amount"),                                # alias of avg
).show()

df.groupBy("country").agg(sum("amount").alias("total")).orderBy(desc("total"))
df.summary()                                     # count, mean, stddev, min, p25, p50, p75, max
df.describe("amount")                            # subset of summary
```

```sql
SELECT country,
       count(*)                       AS rows,
       count(DISTINCT customer_id)    AS uniq_customers,
       approx_count_distinct(email)   AS uniq_emails_approx,
       sum(amount)                    AS revenue,
       avg(amount)                    AS aov
FROM silver.orders
GROUP BY country;
```

`approx_count_distinct` vs `count(DISTINCT …)`: approx is **way cheaper** at scale because no full shuffle dedup; default relative SD 5%. Exam scenario: "fastest count of unique users on a 10 B row table" → approx.

### Window functions

```sql
SELECT customer_id, order_id, amount,
       sum(amount)        OVER (PARTITION BY customer_id ORDER BY order_ts) AS running_total,
       row_number()       OVER (PARTITION BY customer_id ORDER BY order_ts) AS order_seq,
       lag(amount, 1)     OVER (PARTITION BY customer_id ORDER BY order_ts) AS prev_amount,
       rank()             OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rnk
FROM silver.orders;
```

## 6. Performance tuning — knobs you must know

| Conf | Default | Raise when | Lower when |
| --- | --- | --- | --- |
| `spark.sql.shuffle.partitions` | 200 | Spill on shuffle; partitions > 200 MB | Many tiny tasks; partitions < 10 MB |
| `spark.default.parallelism` | cluster cores × 2 | RDD ops creating too few partitions | Rare — usually leave alone |
| `spark.sql.autoBroadcastJoinThreshold` | 10 MB | Small dim table not being broadcast | Driver OOM during broadcast |
| `spark.executor.memory` | instance-dependent | Executor OOM, high GC time, spill | Cost > performance gain |
| `spark.driver.memory` | instance-dependent | Driver OOM (often from `collect()`) | Cost > performance gain |

### Setting them

```python
# Runtime (session-scoped) — most knobs work here
spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)        # disable
```

Executor / driver memory cannot be set at runtime — configure in the cluster UI under
**Compute → cluster → Advanced options → Spark → Spark config**, one `key value` per line:

```
spark.executor.memory 14g
spark.driver.memory   14g
```

### Re-measure after tuning

Don't guess — open the Spark UI stage page and verify max/median ratios moved (see `../week_5_cicd_and_troubleshooting/learn_troubleshooting.md` for thresholds).

### Photon

Vectorized C++ engine. Toggle at cluster level. Free speed-up on SQL/DataFrame workloads. **No benefit** on Python/Scala UDFs or RDDs. Code change: none.

## 7. Gold-layer object selection

Four choices in Unity Catalog. Pick by **freshness need** vs **compute cost** vs **incrementality**.

| Object | What it stores | Refresh model | Streaming source? | Best for |
| --- | --- | --- | --- | --- |
| **Table** | Materialized rows on disk | You manage refresh (job, MERGE) | Yes | General-purpose silver/gold; custom incremental logic |
| **View** | Just a SQL query — no data | Computed at query time | No | Cheap aliases, security boundaries, frequently-changing logic |
| **Materialized View** | Materialized result of a query | Pipeline-refresh (full or incremental on serverless) | No (cannot be read as stream) | BI dashboards with expensive joins/aggregates; trade staleness for query speed |
| **Streaming Table** | Append-only physical table | Auto-loader / `STREAM` source; tracked by checkpoint | Yes (both as sink and source) | Bronze/silver incremental ingest; CDC; near-real-time pipelines |

### Decision tree

```
Need to write custom MERGE / UPDATE / DELETE logic?
  └─ Yes → Table

Is the data continuously arriving and you want exactly-once incremental processing?
  └─ Yes → Streaming Table

Is the underlying query expensive and consumers tolerate minutes/hours of staleness?
  └─ Yes → Materialized View

Otherwise (lightweight projection / filter / security view)?
  └─ View
```

### Syntax

```sql
-- Regular Delta table
CREATE TABLE dea_learning.gold.daily_sales (...) USING DELTA;

-- View
CREATE OR REPLACE VIEW dea_learning.gold.eu_customers AS
SELECT * FROM dea_learning.silver.customers WHERE region = 'EU';

-- Materialized view (in a Spark Declarative Pipeline)
CREATE OR REFRESH MATERIALIZED VIEW dea_learning.gold.daily_revenue AS
SELECT order_date, sum(amount) AS revenue
FROM dea_learning.silver.orders
GROUP BY order_date;

-- Streaming table (in a Spark Declarative Pipeline) — bronze target, fed by raw files
CREATE OR REFRESH STREAMING TABLE dea_learning.bronze.orders_bronze AS
SELECT * FROM STREAM read_files('/Volumes/dea_learning/raw/landing/orders', format => 'json');
```

### Limits to remember

- Materialized views: **cannot** be a streaming source (use a streaming table if downstream needs `STREAM`).
- Streaming tables: append-only by default; deletes/updates land via `AUTO CDC INTO`.
- Views: no caching — every query re-executes the SQL.
- Materialized view incremental refresh: **serverless only** (classic = full refresh).

## 8. Data quality checks

### In Spark Declarative Pipelines (formerly DLT)

```sql
CREATE OR REFRESH STREAMING TABLE silver.orders (
  CONSTRAINT valid_amount  CHECK (amount > 0),                              -- WARN
  CONSTRAINT valid_id      CHECK (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_country CHECK (country IS NOT NULL)  ON VIOLATION FAIL UPDATE
) AS SELECT * FROM STREAM bronze.orders;
```

```python
@dlt.table()
@dlt.expect("valid_amount", "amount > 0")                  # WARN
@dlt.expect_or_drop("valid_id", "order_id IS NOT NULL")    # DROP
@dlt.expect_or_fail("valid_country", "country IS NOT NULL")# FAIL
def silver_orders():
    # For sibling pipeline tables, prefer dlt.read_stream(...) over spark.readStream.table(...)
    # — it registers the dependency in the pipeline graph rather than going through the metastore.
    return dlt.read_stream("bronze_orders")
```

Three modes:
- **WARN (default)** — record violation in event log, keep the row.
- **DROP** — drop offending row, log violation.
- **FAIL** — abort the pipeline update.

Inspect violations:
```sql
SELECT * FROM event_log("dea_learning.pipelines.orders_pipeline")
WHERE event_type = 'flow_progress'
  AND details:flow_progress:metrics:expectations IS NOT NULL;
```

### Outside pipelines — Delta table constraints

```sql
ALTER TABLE silver.orders
  ADD CONSTRAINT pos_amount CHECK (amount > 0);

ALTER TABLE silver.orders
  ALTER COLUMN order_id SET NOT NULL;
```

Rejected writes fail the transaction. Use this for invariants you never want to allow.

## 9. Exam-day quick reference

- `dropDuplicates` is non-deterministic without an `orderBy`; pair with `row_number()` window for "latest wins".
- `approx_count_distinct(col, rsd)` — exam shorthand for "fast distinct count at scale".
- Join names: `inner`, `left`, `right`, `full`, `left_semi`, `left_anti`, `cross`.
- `UNION` deduplicates; `UNION ALL` does not (and is faster).
- `explode` drops null/empty arrays; `explode_outer` keeps them.
- View = no storage; Materialized View = stored + refreshed; Streaming Table = append + checkpoint; Table = you manage.
- Materialized view incremental refresh requires **serverless**.
- `spark.sql.shuffle.partitions = 200` is the default — change it to fix spill / tiny tasks.
- `spark.sql.autoBroadcastJoinThreshold = 10 MB` — raise to broadcast bigger dims, `-1` to disable.
- Spark Declarative Pipeline expectations (formerly DLT): `expect` = warn, `expect_or_drop` = drop, `expect_or_fail` = stop pipeline.

## References

- [DataFrame transformations (PySpark)](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [SQL built-in functions](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions)
- [Joins](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-join)
- [Window functions](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions)
- [Materialized views](https://docs.databricks.com/aws/en/views/materialized)
- [Streaming tables](https://docs.databricks.com/aws/en/tables/streaming)
- [Expectations in Spark Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/expectations)
- [Performance tuning — Adaptive Query Execution](https://docs.databricks.com/aws/en/optimizations/aqe)