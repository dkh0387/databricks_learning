# Week 3 Glossary — Transformation and Modeling

| Term | Definition |
| --- | --- |
| **DataFrame** | Distributed table abstraction in Spark — schema + rows, lazily evaluated. |
| **Lazy evaluation** | Spark builds a query plan; transformations don't execute until an action triggers them. |
| **Action** | Operation that triggers execution (`show`, `count`, `collect`, `write`). |
| **Transformation** | Operation that builds the plan (`select`, `filter`, `join`, `groupBy`). |
| **Narrow transformation** | No shuffle (`filter`, `select`). |
| **Wide transformation** | Requires shuffle (`groupBy`, `join`, `distinct`). |
| **Shuffle** | Repartitioning data across executors — the most expensive Spark operation. |
| **Inner join** | Only matched rows from both sides. |
| **Left join (`left_outer`)** | Keep all left rows; right nullable when no match. |
| **Right join** | Mirror of left. |
| **Full join (`outer`)** | Everything from both sides; nulls fill missing. |
| **Left semi join** | Rows from left that match in right; right cols NOT included. |
| **Left anti join** | Rows from left that DO NOT match in right. |
| **Cross join** | Cartesian product (every left × every right). |
| **Broadcast join** | Small right side is sent to every executor, no shuffle. Hint: `/*+ BROADCAST(t) */`. |
| **Sort-merge join** | Default for large joins — both sides sorted+merged. |
| **AQE** | Adaptive Query Execution — runtime re-optimization (default on). Coalesces tiny partitions, splits skewed ones. |
| **`UNION`** | SQL: combine two result sets, deduplicated. PySpark `df.union()` does NOT dedup (UNION ALL semantics) — chain `.distinct()`. |
| **`UNION ALL`** | SQL: combine without dedup — faster. Matches PySpark `df.union()` behavior. |
| **`explode`** | Turn an array column into one row per element. Drops null/empty arrays. |
| **`explode_outer`** | Like `explode` but keeps rows with null/empty arrays. |
| **Window function** | Aggregation over a partition+order without grouping (`row_number()`, `lag`, `sum() OVER (PARTITION BY…)`). |
| **`row_number()`** | Distinct rank per partition — the canonical "latest record per key" tool. |
| **`dropDuplicates`** | Keeps an arbitrary row per key — use a `row_number()` window for latest-wins. |
| **`distinct`** | Dedup across all columns. |
| **`approx_count_distinct`** | HyperLogLog approximate distinct count — fast at scale (default ~5% rel SD). |
| **Aggregation** | `sum`, `avg`/`mean`, `min`, `max`, `count`, `countDistinct`. |
| **Pivot** | Reshape rows to columns: `groupBy(x).pivot(col).agg(...)`. |
| **`spark.sql.shuffle.partitions`** | Number of partitions after a shuffle (default 200). Target ~100–200 MB/partition. |
| **`spark.default.parallelism`** | Default partition count for RDD ops without explicit partitioning. |
| **`spark.sql.autoBroadcastJoinThreshold`** | Max size (bytes) for broadcast join eligibility (default 10 MB). |
| **Skew** | One partition holds dramatically more data than others — straggler tasks. |
| **Spill** | Data written from memory to disk during shuffle when memory runs out. |
| **Photon** | Vectorized C++ execution engine. Toggle at cluster level. Free speed-up on SQL/DataFrame; no benefit on Python/Scala UDFs or RDDs. |
| **`coalesce` (function)** | Null-handling: returns the first non-null argument across columns — `coalesce(a, b, 0)`. |
| **`coalesce` (DataFrame method)** | Partition operation: reduces the number of partitions without a full shuffle — `df.coalesce(8)`. |
| **`MERGE INTO`** | Atomic upsert on a Delta table: `WHEN MATCHED UPDATE / DELETE`, `WHEN NOT MATCHED INSERT`, optional `WHEN NOT MATCHED BY SOURCE`. Fails if multiple source rows match one target row — dedup source first. |
| **`foreachBatch`** | Streaming sink hook that runs a function per micro-batch — the way to upsert (MERGE) from a stream, since streaming writes are otherwise append-only. |
| **`AUTO CDC INTO`** | Declarative-pipeline replacement for hand-written MERGE (formerly `APPLY CHANGES INTO`); handles ordering via `SEQUENCE BY` and SCD Type 1/2. |
| **Gold table** | Persisted Delta table you manage manually (custom MERGE/UPDATE/DELETE). |
| **Gold view** | Saved SQL query; no storage; recomputed at query time. |
| **Materialized view** | Stored query result; refreshed by a pipeline; CANNOT be a streaming source (refresh updates/deletes existing rows — not append-only, which streaming requires). |
| **Streaming table** | Append-only persisted table fed by a stream; CAN be both sink and source. |
| **Pipeline expectation** | Data quality assertion inside a Spark Declarative Pipeline. |
| **`expect`** | Warn on violation; row kept. |
| **`expect_or_drop`** | Drop offending row. |
| **`expect_or_fail`** | Abort the pipeline update. |
| **Delta CHECK constraint** | Persisted invariant enforced on all writes (`ALTER TABLE … ADD CONSTRAINT … CHECK (…)`). |