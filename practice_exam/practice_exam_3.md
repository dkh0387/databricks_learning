# Practice Exam 3 — Databricks Certified Data Engineer Associate (May 2026 outline)

45 multiple-choice questions · 90 minutes · pass target ≥ 32/45 (~70%).
Question distribution mirrors the official exam guide weights (same as Practice Exams 1–2, no
question overlap):

| § | Section | Weight | Questions |
| --- | --- | --- | --- |
| 1 | Databricks Intelligence Platform | 6% | Q1–Q3 |
| 2 | Data Ingestion and Loading | 21% | Q4–Q12 |
| 3 | Data Transformation and Modeling | 22% | Q13–Q22 |
| 4 | Working with Lakeflow Jobs | 16% | Q23–Q29 |
| 5 | Implementing CI/CD | 10% | Q30–Q34 |
| 6 | Troubleshooting, Monitoring, Optimization | 10% | Q35–Q38 |
| 7 | Governance and Security | 15% | Q39–Q45 |

Rules of the real exam: exactly **one** correct answer per question, no partial credit, no docs access.
Answer key with explanations and repo references is at the bottom — don't scroll past Q45 until you're done.

---

## § 1 — Databricks Intelligence Platform

**Q1.** Which statement about **Photon** is correct?

- A. It is a Python library that must be imported in every notebook
- B. It only accelerates streaming workloads
- C. It is a vectorized engine that accelerates SQL and DataFrame workloads; it is toggled at the compute level and included on serverless
- D. It replaces the Delta transaction log
- E. It requires rewriting queries in C++

**Q2.** An `UPDATE` statement changes 100 rows of a Delta table. What happens physically in storage?

- A. The affected Parquet files are edited in place
- B. New Parquet files containing the updated data are written and the transaction log records the change; the previous files remain for time travel until vacuumed
- C. The rows are stored in a separate delta sidecar file that readers merge at query time forever
- D. The whole table is rewritten on every UPDATE
- E. Delta rejects UPDATE statements; only INSERT is supported

**Q3.** Which `CREATE TABLE` statement produces an **external** table in Unity Catalog?

- A. `CREATE TABLE t (id INT)` in a schema with managed storage
- B. `CREATE TABLE t AS SELECT * FROM src`
- C. `CREATE TEMP VIEW t AS SELECT 1`
- D. `CREATE TABLE t (id INT) USING DELTA LOCATION 's3://bucket/path'`
- E. `CREATE MATERIALIZED VIEW t AS SELECT * FROM src`

## § 2 — Data Ingestion and Loading

**Q4.** Source Parquet files now contain a new column `discount_code`. The existing nightly
`COPY INTO` load should pick it up and add it to the target table. Which change is needed?

- A. `COPY_OPTIONS ('mergeSchema' = 'true')`
- B. `FORMAT_OPTIONS ('header' = 'true')`
- C. `COPY_OPTIONS ('force' = 'true')`
- D. Recreate the target table on every run
- E. Nothing — `COPY INTO` always evolves the schema

**Q5.** Auto Loader ingests JSON with schema inference, and **every** column in the resulting table
is typed `STRING`, even numeric ones. Why, and what is the fix?

- A. JSON files were corrupted; re-deliver them
- B. The schema location is missing; add `cloudFiles.schemaLocation`
- C. For untyped formats like JSON/CSV, inference defaults all columns to STRING to avoid type drift; set `cloudFiles.inferColumnTypes = true` (or use schema hints) to infer real types
- D. The cluster runtime is too old for typed inference
- E. `_rescued_data` swallowed the type information

**Q6.** Which statement about Auto Loader's **file discovery modes** is correct?

- A. File notification mode is the default; directory listing must be enabled explicitly
- B. Directory listing is the default; file notification mode (cloud event queues) is opt-in and recommended for very high file volumes
- C. Both modes must be active at the same time
- D. Directory listing requires an SNS/SQS queue
- E. File notification mode only works with CSV files

**Q7.** For audit purposes, each bronze row must record **which source file** it came from. Which
approach is built-in?

- A. Parse the file name out of `_rescued_data`
- B. Maintain a manual mapping table of files to batches
- C. Enable `cloudFiles.includeSourceFile = true`
- D. Select the `_metadata.file_path` column during the read
- E. Query the checkpoint RocksDB files with SQL

**Q8.** A vendor delivers raw CSV files that must land in Unity-Catalog-governed storage before
ingestion, so that access to the raw files themselves is controlled by UC permissions. Where should
the files land?

- A. In a Unity Catalog **volume** (e.g., `/Volumes/dea_learning/raw/landing/`)
- B. In the DBFS root under `dbfs:/FileStore`
- C. On the driver's local disk
- D. In the `_delta_log` directory of the target table
- E. In a workspace folder next to the notebooks

**Q9.** A regulated pipeline requires that the ingestion stream **stops immediately** when a new,
unreviewed column appears in the source — the schema must never change without a human decision, and
nothing should be auto-captured elsewhere. Which schema evolution mode fits?

- A. `addNewColumns`
- B. `rescue`
- C. `none`
- D. `mergeSchema`
- E. `failOnNewColumns`

**Q10.** A team loads a few hundred CSV files per night from cloud storage. They prefer plain SQL,
run on a schedule, and have no streaming requirements. File volume will stay modest. Which tool is
the natural fit?

- A. A continuous Auto Loader stream on an always-on cluster
- B. `COPY INTO`, scheduled as a SQL task — idempotent, SQL-native, ideal for modest batch file volumes
- C. A Lakeflow Connect managed connector
- D. Structured Streaming with Kafka
- E. Manual notebook uploads

**Q11.** A silver table column `customer` is typed `STRUCT<id BIGINT, address STRUCT<city STRING>>`.
How is the city selected in SQL?

- A. `SELECT customer.address.city FROM t`
- B. `SELECT from_json(customer, 'city STRING').city FROM t`
- C. `SELECT customer:address:city FROM t` — the colon operator is required for structs
- D. `SELECT explode(customer.address) FROM t`
- E. `SELECT get(customer, 'address.city') FROM t`

**Q12.** A small on-prem PostgreSQL reference table (~50k rows) must be copied into a bronze Delta
table every night. Which implementation follows the standard pattern?

- A. Delta Sharing from PostgreSQL
- B. `COPY INTO` with a `jdbc://` path
- C. A notebook using the Spark JDBC reader writing to the Delta table, scheduled as a Lakeflow Job
- D. Auto Loader with `cloudFiles.format = "postgres"`
- E. Exporting the table to Excel and uploading it weekly

## § 3 — Data Transformation and Modeling

**Q13.** Rows without a `customer_id` are useless downstream and must be removed during
bronze → silver. Which call removes exactly those rows?

- A. `df.dropna()` — drops any row containing any null
- B. `df.dropna(subset=["customer_id"])`
- C. `df.fillna({"customer_id": 0})`
- D. `df.filter(col("customer_id") == 0)`
- E. `df.dropna(how="all")`

**Q14.** A silver table needs a derived column `total = qty * price`. Which PySpark call adds it?

- A. `df.withColumn("total", col("qty") * col("price"))`
- B. `df.select("total")`
- C. `df.withColumnRenamed("qty", "total")`
- D. `df.agg(sum("qty") * sum("price"))`
- E. `df.groupBy("total").count()`

**Q15.** For a data quality report, the team needs all orders whose `customer_id` does **not** exist
in the customers dimension — only the order columns, one row per bad order. Which join?

- A. `orders INNER JOIN customers`
- B. `orders FULL OUTER JOIN customers`
- C. `orders LEFT SEMI JOIN customers`
- D. `orders LEFT ANTI JOIN customers`
- E. `orders CROSS JOIN customers WHERE customers.id IS NULL`

**Q16.** The column `email` contains NULLs. What is the difference between `count(*)` and
`count(email)` in an aggregation?

- A. They always return the same number
- B. `count(*)` ignores NULL rows; `count(email)` counts them
- C. `count(*)` counts all rows; `count(email)` counts only rows where `email` is not NULL
- D. `count(email)` fails on NULL values
- E. `count(*)` is deprecated in Spark SQL

**Q17.** Monthly revenue currently sits in one row per (month, region). The report needs **one row
per month with one column per region**. Which operation reshapes it?

- A. `explode()` on the region column
- B. `df.groupBy("month").pivot("region").sum("revenue")`
- C. `unpivot` / `stack()`
- D. `df.transpose()`
- E. A self cross join per region

**Q18.** For churn analysis, each order row needs the **amount of the same customer's previous
order** as an additional column. Which construct does this?

- A. `lag(order_amount) OVER (PARTITION BY customer_id ORDER BY order_ts)`
- B. `min(order_amount) OVER (PARTITION BY customer_id)`
- C. A self join on `order_id - 1`
- D. `first(order_amount)` in a `GROUP BY customer_id`
- E. `collect_list(order_amount)` without ordering

**Q19.** A nightly batch receives changed customer records. Existing customers must be updated,
new ones inserted — in a single atomic statement against the silver Delta table. Which statement?

- A. `INSERT OVERWRITE` the whole table
- B. `UPDATE` followed by `INSERT` in two separate transactions
- C. `MERGE INTO silver.customers USING updates ON ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...`
- D. `COPY INTO` with deduplication
- E. `CREATE OR REPLACE TABLE` from the union

**Q20.** A DataFrame job performs a large `groupBy` aggregation. Which configuration controls the
**number of partitions created by that shuffle**?

- A. `spark.default.parallelism` — it governs all partitioning in Spark SQL
- B. `spark.sql.shuffle.partitions` — `spark.default.parallelism` applies to RDD operations, not SQL/DataFrame shuffles
- C. `spark.executor.cores`
- D. `spark.sql.autoBroadcastJoinThreshold`
- E. `spark.driver.memory`

**Q21.** Which of the following most likely causes an **out-of-memory error on the driver** (not the
executors)?

- A. A shuffle join between two large tables
- B. Too many shuffle partitions
- C. Data skew on a hot key
- D. Calling `collect()` / `toPandas()` on a large DataFrame
- E. A broadcast join of a 5 MB dimension table

**Q22.** A declarative pipeline declares:

```sql
CONSTRAINT plausible_age EXPECT (age BETWEEN 0 AND 120)
```

— with **no** `ON VIOLATION` clause. A batch contains 50 violating rows. What happens?

- A. The 50 rows are written to the target anyway, and the violation count is recorded in the event log (warn-only default)
- B. The 50 rows are dropped
- C. The update fails
- D. The rows are moved to `_rescued_data`
- E. The constraint is ignored entirely unless a mode is specified

## § 4 — Working with Lakeflow Jobs

**Q23.** Which statement correctly distinguishes **task retries** from **repair run**?

- A. They are two names for the same feature
- B. Retries re-execute a failing task automatically during the run; repair is a manual re-execution of a failed run afterwards, re-running only failed/skipped tasks
- C. Repair is automatic; retries are triggered manually in the UI
- D. Retries exist only for SQL tasks
- E. Repair always re-runs every task, including succeeded ones

**Q24.** A Lakeflow Spark Declarative Pipeline feeds nightly batch reports. Compute should spin up,
refresh all datasets once, and shut down. Which pipeline execution mode is appropriate?

- A. Continuous mode
- B. Development mode
- C. Triggered mode
- D. Streaming mode with `processingTime = "24h"`
- E. Manual mode

**Q25.** The data team must be informed by e-mail whenever the nightly pipeline **fails**, but not
on success. What is the built-in way?

- A. Configure a failure notification (e-mail on the failure event) on the pipeline/job
- B. A downstream task that polls the run state every minute
- C. A cron job reading the driver log for stack traces
- D. Grant the team `SELECT` on the event log and ask them to check daily
- E. Continuous mode, which e-mails automatically

**Q26.** In a job DAG, tasks `load_eu` and `load_us` must both start after `setup` finishes, run in
parallel, and task `aggregate` must start only after **both** loads succeed. How is this configured?

- A. All four tasks in a strict sequence
- B. `load_eu` and `load_us` in one for-each task, `aggregate` with no dependencies
- C. A continuous job with four notebooks
- D. `load_eu` and `load_us` each depend on `setup`; `aggregate` depends on `load_eu` **and** `load_us` (run if: All succeeded)
- E. Three separate jobs connected by cron offsets

**Q27.** A saved Databricks SQL query computes daily KPI checks and must run as a step of the
nightly job DAG, executed on a SQL warehouse. Which task type is designed for this?

- A. A notebook task with `%sql`
- B. A SQL task referencing the saved query and a SQL warehouse
- C. A pipeline task
- D. A dashboard task
- E. A for-each task

**Q28.** Customer records arrive as CDC events (inserts, updates, deletes with a sequence column).
The silver table must keep **full history** — every previous version of each customer as its own
row with validity metadata. Which declarative-pipeline feature implements this without hand-written
MERGE logic?

- A. `ON VIOLATION DROP ROW` expectations
- B. A materialized view over the CDC feed
- C. `AUTO CDC INTO ... SEQUENCE BY ... STORED AS SCD TYPE 2`
- D. `COPY INTO` with `force = true`
- E. A streaming table with `dropDuplicates()`

**Q29.** A team builds a bronze → silver → gold flow that needs streaming tables, automatic
dependency resolution between datasets, declarative data-quality expectations, and managed compute —
with orchestration from the nightly job. What is the recommended architecture?

- A. A Lakeflow Spark Declarative Pipeline for the medallion datasets, triggered via a pipeline task in the Lakeflow Job
- B. One large notebook task containing all transformations
- C. Forty-five SQL tasks, one per table
- D. A continuous job with `while True` notebooks
- E. Separate all-purpose clusters per layer, coordinated manually

## § 5 — Implementing CI/CD

**Q30.** A bundle variable `catalog` is set in three places: a default in the top-level `variables`
block, a value in the `prod` target, and `--var="catalog=hotfix"` on the CLI call. Which value wins
at deploy time?

- A. The top-level default — defaults always win
- B. The `prod` target value — targets are final
- C. The deployment aborts due to the conflict
- D. `hotfix` — the CLI flag has the highest precedence (CLI flag > env var > target > default)
- E. Whichever was defined first in the YAML

**Q31.** Deployment paths in a bundle should include the environment name automatically, e.g.
`/Shared/deployments/dev/...` vs `/Shared/deployments/prod/...`, without defining a variable per
target. Which substitution provides the current target's name?

- A. `${env.name}`
- B. `${bundle.target}`
- C. `${workspace.host}`
- D. `${var.target}` — targets are ordinary variables
- E. `$TARGET` shell expansion

**Q32.** Deploying a bundle with `mode: production` fails with an error demanding an explicit run
identity. What is the cause?

- A. Production mode is only available on serverless
- B. The workspace token expired
- C. `mode: production` requires an explicit `run_as` (typically a service principal) so production resources never run under an arbitrary deploying user
- D. `run_as` is only needed for pipelines, not jobs
- E. The bundle name conflicts with another bundle

**Q33.** Pull requests against `main` should automatically fail CI when someone breaks the bundle
configuration (bad YAML, unknown resource fields, missing references). Which check belongs in the
PR pipeline?

- A. `databricks bundle validate` — checks the bundle config for errors without deploying
- B. `databricks bundle deploy -t prod` — deploying is the only way to find errors
- C. `databricks bundle run --dry`
- D. A screenshot comparison of the jobs UI
- E. `git fsck` on the repository

**Q34.** What does `databricks bundle deploy -t staging` actually do?

- A. It only copies notebook files; jobs must be created manually afterwards
- B. It uploads the bundle's code/files to the staging workspace and creates or updates the declared resources (jobs, pipelines) according to the target's configuration
- C. It runs all jobs defined in the bundle immediately
- D. It creates a Git tag and pushes it
- E. It validates the YAML without changing the workspace

## § 6 — Troubleshooting, Monitoring, Optimization

**Q35.** A job cluster intermittently fails to start with `CLOUD_PROVIDER_LAUNCH_FAILURE`. What
does this indicate and what is a reasonable response?

- A. The Databricks control plane is down; wait for the status page
- B. The init script has a bug; rewrite it
- C. The cloud provider refused the VMs (quota / capacity / IAM) — check quotas, switch instance type or zone, and retry
- D. The DBR version is deprecated; upgrade
- E. The job's notebook has a syntax error

**Q36.** A Delta table receives thousands of tiny files per day from frequent small writes, and read
queries are slowing down. Which maintenance operation directly addresses this?

- A. `VACUUM` — it merges small files
- B. `OPTIMIZE` — compacts small files into larger ones (~1 GB target)
- C. `ANALYZE TABLE` — statistics remove the small files
- D. `FSCK REPAIR TABLE`
- E. `RESTORE TABLE` to an earlier version

**Q37.** A nightly run regularly finishes hours late. The engineer suspects one upstream task delays
everything but doesn't know which. Where is this visible most directly?

- A. In the job run's DAG/timeline view in the Lakeflow Jobs UI — per-task start/end times reveal the blocking upstream task
- B. In the Delta transaction logs of all involved tables
- C. In the cloud provider's billing console
- D. In `spark.conf` of the cluster
- E. In the workspace audit log

**Q38.** A large fact table is queried mostly with filters on `customer_id` (high cardinality) and
`order_date`. The team wants data layout optimization **without** Hive-style partitioning, and the
ability to change the layout keys later without rewriting the table. What should they use?

- A. `PARTITIONED BY (customer_id)` — one directory per customer
- B. Z-ordering, re-run after every write
- C. A second copy of the table sorted differently
- D. Liquid Clustering: `CLUSTER BY (customer_id, order_date)` on the Delta table
- E. Bucketing with 4096 buckets

## § 7 — Governance and Security

**Q39.** Analysts must read raw CSV files stored in the UC volume
`dea_learning.raw.landing` directly (e.g., via `read_files`). Besides `USE CATALOG` and
`USE SCHEMA`, which privilege do they need?

- A. `SELECT` on the volume
- B. `READ VOLUME` on the volume
- C. `READ FILES` on the metastore
- D. `EXECUTE` on the volume
- E. `BROWSE` on the catalog

**Q40.** The security team asks: "Who ran `SELECT` on `dea_learning.silver.silver_orders` last
week?" Where is this answered with a SQL query?

- A. `DESCRIBE HISTORY dea_learning.silver.silver_orders`
- B. The Delta transaction log JSON files
- C. The `system.access.audit` system table, filtered on the action and table name
- D. The driver logs of the SQL warehouse
- E. It cannot be answered retroactively

**Q41.** A gold table feeds an executive dashboard, and the team must document **which upstream
tables and columns** it is derived from. What is the recommended source of this information?

- A. Unity Catalog's automatic lineage (Catalog Explorer → Lineage tab, or the `system.access.table_lineage` / `column_lineage` tables)
- B. A manually maintained wiki page
- C. Parsing all notebook code with regex
- D. The Delta transaction log
- E. Table comments written by developers

**Q42.** A partner company that does **not** use Databricks needs ongoing read access to a curated
gold dataset. Which Databricks capability is designed for this?

- A. A read-only PAT for the partner
- B. Adding the partner to the workspace as guests
- C. Nightly CSV e-mail exports
- D. Delta Sharing — an open protocol; recipients can read shared tables without being Databricks customers
- E. JDBC access to the SQL warehouse with a shared password

**Q43.** A data engineer with `USE CATALOG` and `USE SCHEMA` on `dea_learning.silver` tries
`CREATE TABLE dea_learning.silver.new_table (...)` and gets a permission error. Which privilege is
missing?

- A. `MODIFY` on the schema
- B. `CREATE TABLE` on the schema
- C. `WRITE FILES` on the catalog
- D. `ALL PRIVILEGES` on the metastore
- E. `APPLY TAG` on the schema

**Q44.** Which requirement must a **column mask** function meet so it can be attached to the
`email STRING` column?

- A. It must return a BOOLEAN verdict per row
- B. It must be written in Python
- C. It must return a value of the same (or castable) type as the column — e.g., a STRING like `'****'` or the original value
- D. It must not reference the masked column itself
- E. It must be owned by the metastore admin

**Q45.** Data stewards should be able to **discover** tables and schemas of a catalog in Catalog
Explorer (see names and metadata) without being able to query data and without `USE CATALOG`
traversal rights. Which privilege enables exactly this?

- A. `BROWSE` on the catalog
- B. `SELECT` on the catalog
- C. `USE SCHEMA` on every schema
- D. `EXECUTE` on the catalog
- E. `READ VOLUME` on all volumes

---
---

## Answer key & explanations

> Score yourself first: ≥ 32/45 ≈ pass level. Review the referenced repo file for every miss.

| # | Answer | Explanation | Review |
| --- | --- | --- | --- |
| 1 | **C** | Photon is Databricks' vectorized C++ engine for SQL/DataFrame workloads — a compute-level toggle, included on serverless. No code changes required. | `week_1_platform/glossary.md` |
| 2 | **B** | Delta is copy-on-write: updates write new Parquet files and commit the change to `_delta_log`; superseded files are tombstoned and remain readable via time travel until `VACUUM`. | `week_1_platform/learn.md` |
| 3 | **D** | A `LOCATION` clause at a caller-provided path makes the table external (metastore owns metadata only). Without `LOCATION`, tables are managed. | `week_1_platform/code/02_managed_vs_external_tables.sql` |
| 4 | **A** | Schema evolution for `COPY INTO` is a *copy* option: `COPY_OPTIONS ('mergeSchema' = 'true')`. Format options configure parsing; `force` re-loads files but doesn't add columns. | `week_2_ingestion/learn_deep_dive.md` §2 |
| 5 | **C** | Untyped formats (JSON/CSV/XML) are inferred as all-STRING by design; `cloudFiles.inferColumnTypes = true` or `schemaHints` produce real types. | `week_2_ingestion/learn_deep_dive.md` §3 |
| 6 | **B** | Directory listing is the default discovery mode. File notification (cloud event queues) is opt-in — the recommended switch at very high file volumes. | `week_2_ingestion/learn_deep_dive.md` §3 |
| 7 | **D** | The built-in `_metadata` column exposes file metadata — `_metadata.file_path` gives the source file per row. Option C is an invented setting. | `week_2_ingestion/learn_deep_dive.md` §3 |
| 8 | **A** | UC volumes are the governed home for non-tabular files — raw landing zones live in a volume, with access controlled via UC privileges. DBFS root is deprecated for new content. | `week_2_ingestion/learn_deep_dive.md` §1 |
| 9 | **E** | `failOnNewColumns` fails the stream on unknown fields and never evolves the schema — the strict human-review mode. `rescue` would silently capture the data instead of stopping. | `week_2_ingestion/learn_deep_dive.md` §3 |
| 10 | **B** | Modest nightly file volumes + SQL preference + no streaming = `COPY INTO` on a schedule. Auto Loader shines at large/continuous volumes; managed connectors are for SaaS/DB sources. | `week_2_ingestion/learn_deep_dive.md` §1–2 |
| 11 | **A** | Typed STRUCT columns use dot notation: `customer.address.city`. The colon operator is for JSON strings / VARIANT; `from_json` is for parsing strings. | `week_2_ingestion/learn_deep_dive.md` §5 |
| 12 | **C** | Databases are read with the Spark JDBC reader in a notebook, scheduled via Lakeflow Jobs. `COPY INTO`/Auto Loader read files, not JDBC sources. | `week_2_ingestion/learn_deep_dive.md` §6 |
| 13 | **B** | `dropna(subset=["customer_id"])` removes only rows where that specific column is null. Bare `dropna()` would drop rows with a null in *any* column. | `week_3_transformation/learn_data_transformation.md` §1 |
| 14 | **A** | `withColumn("total", col("qty") * col("price"))` adds a derived column. | `week_3_transformation/learn_data_transformation.md` §4 |
| 15 | **D** | Left anti join returns left rows **without** a match, left columns only — the "what's missing?" join. Left semi is the opposite (rows *with* a match). | `week_3_transformation/learn_data_transformation.md` §3 |
| 16 | **C** | `count(*)` counts rows; `count(col)` counts non-NULL values of that column. Classic exam nuance. | `week_3_transformation/code/03_aggregations_and_windows.sql` |
| 17 | **B** | `groupBy().pivot().agg()` turns row values into columns — long-to-wide reshaping. Note: PIVOT only surfaces explicitly listed values. | `week_3_transformation/learn_data_transformation.md` §4 |
| 18 | **A** | `lag()` over a window partitioned by customer and ordered by time returns the previous row's value — no self join needed. | `week_3_transformation/code/03_aggregations_and_windows.sql` |
| 19 | **C** | `MERGE INTO` is the atomic upsert: `WHEN MATCHED UPDATE`, `WHEN NOT MATCHED INSERT`, one transaction. | `week_3_transformation/learn_data_transformation.md` §6 |
| 20 | **B** | `spark.sql.shuffle.partitions` (default 200) sets post-shuffle partition count for SQL/DataFrame ops; `spark.default.parallelism` applies to RDD operations. | `week_3_transformation/learn_data_transformation.md` §7 |
| 21 | **D** | `collect()`/`toPandas()` pull the whole result to the driver — the textbook driver OOM. Shuffles, skew, and executor-side issues OOM executors instead. | `week_3_transformation/learn_data_transformation.md` §7 |
| 22 | **A** | Default expectation behavior (no `ON VIOLATION`) is warn-only: violating rows are **written** and counts land in the pipeline event log. | `week_4_pipelines_and_jobs/learn_pipelines.md` |
| 23 | **B** | Retries = automatic per-task recovery during the run; repair = manual recovery of a failed run afterwards, re-running only failed/skipped tasks (parameters can be overwritten). | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 24 | **C** | Triggered mode refreshes datasets once per update and releases compute — the batch pattern. Continuous keeps everything near-real-time on always-on compute. | `week_4_pipelines_and_jobs/learn_pipelines.md` |
| 25 | **A** | Failure notifications (e-mail on the failure event) are built into pipelines/jobs — no polling required. Events available: failure, success, start. | `week_4_pipelines_and_jobs/learn_pipelines.md` |
| 26 | **D** | Fan-out/fan-in via task dependencies: both loads depend on `setup`; `aggregate` depends on both loads with "All succeeded". | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 27 | **B** | SQL tasks execute saved queries (also dashboard refreshes and alerts) on a SQL warehouse as first-class DAG steps. | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 28 | **C** | `AUTO CDC INTO` (formerly `APPLY CHANGES INTO`) generates CDC merge logic declaratively; `SEQUENCE BY` handles ordering and `STORED AS SCD TYPE 2` keeps full history rows. | `week_4_pipelines_and_jobs/learn_pipelines.md` |
| 29 | **A** | Declarative pipelines provide streaming tables, dependency resolution, expectations, and managed compute; the nightly job orchestrates them via a pipeline task. | `week_4_pipelines_and_jobs/learn_pipelines.md` |
| 30 | **D** | Variable precedence, high → low: CLI `--var` flag > `BUNDLE_VAR_*` env var > target `variables` block > top-level default. | `week_5_cicd_and_troubleshooting/learn_cicd.md` |
| 31 | **B** | `${bundle.target}` resolves to the deployment target's name; other built-ins include `${bundle.name}` and `${workspace.current_user.userName}`. | `week_5_cicd_and_troubleshooting/learn_cicd.md` |
| 32 | **C** | `mode: production` deliberately requires an explicit `run_as` (typically a service principal) so prod resources don't run as whichever human deployed last. | `week_5_cicd_and_troubleshooting/learn_cicd.md` |
| 33 | **A** | `databricks bundle validate` checks the bundle configuration without touching the workspace — the standard PR gate before any deploy. | `week_5_cicd_and_troubleshooting/learn_cicd.md` |
| 34 | **B** | `bundle deploy` uploads the code/files and creates or updates the declared resources (jobs, pipelines) per the target's config. It does not run them — that's `bundle run`. | `week_5_cicd_and_troubleshooting/learn_cicd.md` |
| 35 | **C** | `CLOUD_PROVIDER_LAUNCH_FAILURE` = the cloud refused the VMs (quota/capacity/IAM). Check quotas, try another instance type/zone, retry. | `week_5_cicd_and_troubleshooting/learn_troubleshooting.md` |
| 36 | **B** | Small-file compaction is exactly what `OPTIMIZE` does (~1 GB target files). `VACUUM` deletes tombstoned files — it never merges. | `week_5_cicd_and_troubleshooting/learn_troubleshooting.md` |
| 37 | **A** | The run's DAG/timeline view shows per-task start/end and states — the direct way to spot the blocking upstream task. | `week_5_cicd_and_troubleshooting/learn_troubleshooting.md` |
| 38 | **D** | Liquid Clustering (`CLUSTER BY`) organizes data by the declared keys without Hive partitioning, and the keys can be changed later without a table rewrite. High-cardinality partitioning (A) would create millions of directories. | `week_5_cicd_and_troubleshooting/learn_troubleshooting.md` |
| 39 | **B** | Files in volumes are governed by `READ VOLUME` / `WRITE VOLUME` on the volume securable (plus catalog/schema traversal). `READ FILES` applies to external locations, not volumes. | `week_6_governance/learn.md` |
| 40 | **C** | UC audit logs are queryable in the `system.access.audit` system table — filter by action and object name. `DESCRIBE HISTORY` shows *writes* to the Delta table, not reads. | `week_6_governance/learn.md` |
| 41 | **A** | UC captures lineage automatically (DBR 11.3+) at table and column level — Catalog Explorer's Lineage tab or `system.access.table_lineage` / `column_lineage`. | `week_6_governance/learn.md` |
| 42 | **D** | Delta Sharing is the open sharing protocol; recipients consume shared tables with open connectors without being Databricks customers. | `week_6_governance/code/05_delta_sharing.sql` |
| 43 | **B** | Creating child objects requires the `CREATE TABLE` privilege on the schema (in addition to the traversal privileges). `MODIFY` governs DML on existing tables. | `week_6_governance/learn.md` |
| 44 | **C** | A mask UDF takes the column value (plus optional other columns) and must return the same or a castable type — e.g., `'****'` or the real value per caller. BOOLEAN returns are for row filters. | `week_6_governance/learn.md` |
| 45 | **A** | `BROWSE` allows metadata discovery in the UI without `USE` traversal or data access — built for stewards and catalogs of record. | `week_6_governance/learn.md` |

### Score interpretation

| Score | Meaning |
| --- | --- |
| ≥ 40 | Exam-ready on this material — book it |
| 32–39 | Pass level; review every miss before the real exam |
| < 32 | Re-study the weeks with the most misses, retake in a few days |
