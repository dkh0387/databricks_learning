# Practice Exam 1 — Databricks Certified Data Engineer Associate (May 2026 outline)

45 multiple-choice questions · 90 minutes · pass target ≥ 32/45 (~70%).
Question distribution mirrors the official exam guide weights:

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

**Q1.** A company uses Databricks with classic (non-serverless) compute. Where does the actual data
processing of a Spark job take place?

- A. In the Databricks control plane, managed by Databricks
- B. In the compute plane inside the customer's own cloud account
- C. In a Databricks-owned cloud account shared across customers
- D. In the Unity Catalog metastore
- E. On the driver node in the control plane, with executors in the customer account

**Q2.** A data engineer needs to run a production ETL workload every night at 02:00. The workload
runs for about 40 minutes and no interactive development happens on this compute. Which compute
type is the most cost-effective and appropriate choice?

- A. All-purpose compute, kept running 24/7 so the job starts instantly
- B. A SQL warehouse in serverless mode
- C. Job compute that is created for the run and terminates afterwards
- D. All-purpose compute with auto-termination after 4 hours
- E. A high-concurrency cluster shared with the BI team

**Q3.** Which component of Delta Lake provides ACID transaction guarantees on top of Parquet files
in cloud object storage?

- A. The Unity Catalog metastore
- B. The Hive metastore
- C. The `_delta_log` transaction log with ordered JSON commit files
- D. Z-ordering of the underlying Parquet files
- E. The checkpoint location of the writing stream

## § 2 — Data Ingestion and Loading

**Q4.** A data engineer runs the same `COPY INTO` statement against the same source directory twice.
New files were added between the two runs. What happens on the second run?

- A. All files are loaded again, producing duplicates
- B. The statement fails because the files already exist in the table
- C. Only the new files are loaded; previously loaded files are skipped
- D. The table is truncated and fully reloaded
- E. Nothing is loaded until `force = true` is set

**Q5.** An Auto Loader stream is defined as follows and fails immediately on start:

```python
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .load("/Volumes/dea_learning/raw/orders_landing"))
```

What is the most likely cause?

- A. `cloudFiles.format` must be uppercase `JSON`
- B. The required option `cloudFiles.schemaLocation` is missing and no explicit schema was provided
- C. Auto Loader cannot read from Unity Catalog volumes
- D. `checkpointLocation` must be set on the reader, not the writer
- E. JSON sources always require `cloudFiles.schemaHints`

**Q6.** An Auto Loader stream uses the default schema evolution mode. A new column appears in the
incoming JSON files. What is the expected behavior?

- A. The new column is silently dropped
- B. The stream fails once with an `UnknownFieldException`; on restart the schema includes the new column and processing continues
- C. The new column is written into `_rescued_data` forever
- D. The stream keeps running and adds the column mid-batch without interruption
- E. The stream fails permanently until the schema location is deleted

**Q7.** A source system overwrites an already-delivered file `orders_2026-07-01.json` in the landing
zone with corrected values. The Auto Loader stream that already ingested this file keeps running but
the corrections never appear in the bronze table. Why?

- A. The checkpoint is corrupted and must be deleted
- B. Auto Loader processes each file path exactly once by default; overwrites are ignored unless `cloudFiles.allowOverwrites = true` is set
- C. JSON files cannot be modified after ingestion
- D. The schema location must be reset to pick up changed files
- E. `_rescued_data` captured the corrections instead

**Q8.** An Auto Loader job ingests from a bucket that receives millions of small files per day.
Directory listing has become slow and expensive. What is the recommended change?

- A. Increase the cluster size so listing is parallelized
- B. Switch to `COPY INTO`, which scales better than Auto Loader
- C. Enable file notification mode (`cloudFiles.useNotifications = true`, or file events on the external location) so cloud events push new-file notifications
- D. Reduce the trigger interval to list less often
- E. Partition the source directory by ingestion date

**Q9.** What is the purpose of the `_rescued_data` column that Auto Loader adds by default?

- A. It stores the full raw file content for auditing
- B. It captures data that could not be parsed into the expected schema (missing fields, type mismatches, case mismatches) so nothing is silently lost
- C. It stores rows that violated pipeline expectations
- D. It holds the previous version of updated rows
- E. It contains file metadata such as path and modification time

**Q10.** A data engineer needs a **one-time** load of a directory of Parquet files into a new
Unity Catalog table using SQL, with no need for incremental processing later. Which approach is the
simplest fit?

- A. `CREATE TABLE ... AS SELECT * FROM read_files('<path>', format => 'parquet')`
- B. Auto Loader with `trigger(availableNow=True)`
- C. A Lakeflow Connect managed connector
- D. `COPY INTO` scheduled every hour
- E. A JDBC connection to the storage account

**Q11.** A team must continuously replicate data from Salesforce and an on-prem SQL Server database
into Unity Catalog tables with minimal custom code, including handling of schema drift and CDC.
Which Databricks capability is designed for this?

- A. Auto Loader with file notification mode
- B. `COPY INTO` with `mergeSchema = true`
- C. Lakeflow Connect managed connectors
- D. Structured Streaming with a custom JDBC reader
- E. Partner Connect with a manually managed Kafka cluster

**Q12.** A notebook reads a large table over JDBC and only one executor does all the work. Which
change enables parallel reads?

- A. Increase `spark.sql.shuffle.partitions`
- B. Set `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions` on the JDBC reader
- C. Use `fetchsize = 0` to disable batching
- D. Cache the DataFrame before the read
- E. Switch the cluster to Photon

## § 3 — Data Transformation and Modeling

**Q13.** A bronze table has a STRING column `payload` containing JSON like
`{"order_id": 42, "amount": 19.99}`. Which approach converts it into queryable columns in SQL?

- A. `SELECT payload.order_id FROM bronze.orders`
- B. `SELECT from_json(payload, 'order_id BIGINT, amount DOUBLE') AS p FROM bronze.orders`
- C. `SELECT explode(payload) FROM bronze.orders`
- D. `SELECT CAST(payload AS STRUCT) FROM bronze.orders`
- E. `SELECT get_json(payload) FROM bronze.orders`

**Q14.** A silver table has a column `items` of type `ARRAY<STRUCT<sku:STRING, qty:INT>>`. The goal
is one output row per array element. Which function achieves this?

- A. `flatten(items)`
- B. `explode(items)`
- C. `split(items, ',')`
- D. `from_json(items)`
- E. `unnest(items)` inside a `PIVOT`

**Q15.** An `orders` table must be enriched with customer attributes, keeping **all** orders even
when no matching customer exists. Which join does this?

- A. `orders INNER JOIN customers`
- B. `orders LEFT OUTER JOIN customers`
- C. `orders RIGHT OUTER JOIN customers`
- D. `orders CROSS JOIN customers`
- E. `orders LEFT SEMI JOIN customers`

**Q16.** A fact table with 2 billion rows is joined to a 20 MB dimension table, and the Spark UI
shows an expensive shuffle of the large table. What is the most effective optimization?

- A. Increase executor memory so the shuffle fits in RAM
- B. Ensure the small table is broadcast (e.g., `broadcast()` hint or a sufficient `spark.sql.autoBroadcastJoinThreshold`)
- C. Convert the join to a cross join with a post-filter
- D. Repartition both tables to 10,000 partitions
- E. Collect the dimension table to the driver and join in pandas

**Q17.** What is the difference between `UNION` and `UNION ALL` in Spark SQL?

- A. `UNION` removes duplicate rows from the combined result; `UNION ALL` keeps all rows
- B. `UNION ALL` removes duplicates; `UNION` keeps all rows
- C. `UNION` requires identical column names; `UNION ALL` matches by position
- D. There is no difference in Spark SQL
- E. `UNION ALL` also merges the schemas of both inputs

**Q18.** New customer records arrive with occasional exact duplicates on `customer_id`, and the
latest record (by `updated_at`) must win. Which pattern is correct?

- A. `SELECT DISTINCT * FROM customers_raw`
- B. `dropDuplicates()` with no arguments
- C. A window: `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC)` and filter `= 1`
- D. `GROUP BY customer_id` with `first(updated_at)`
- E. `LIMIT 1` per customer via a correlated subquery

**Q19.** A dashboard needs the number of distinct visitors on a 3-billion-row events table. Exact
precision is not required, but query speed matters. Which function fits best?

- A. `COUNT(DISTINCT visitor_id)`
- B. `approx_count_distinct(visitor_id)`
- C. `COUNT(*)` divided by the average visit count
- D. `SUM(1)` over a `GROUP BY visitor_id`
- E. `count_if(visitor_id IS NOT NULL)`

**Q20.** A job writes a small aggregated result (a few MB) but runs with the default
`spark.sql.shuffle.partitions = 200`, producing hundreds of tiny tasks and files. What is the
appropriate fix?

- A. Increase shuffle partitions to 2000 for more parallelism
- B. Lower `spark.sql.shuffle.partitions` (or rely on AQE partition coalescing) to match the actual data volume
- C. Increase `spark.default.parallelism` on the driver
- D. Disable Adaptive Query Execution
- E. Set executor memory to 64 GB

**Q21.** A gold-layer aggregation over a large fact table must be queried by BI tools with low
latency, refresh automatically on a schedule, and avoid recomputing everything when only some
input data changed. Which object type is the best fit in Unity Catalog?

- A. A standard view
- B. A temporary view created by the nightly job
- C. A materialized view
- D. A streaming table reading the BI queries
- E. An external Parquet table refreshed by overwrite

**Q22.** In a Lakeflow Spark Declarative Pipeline, a silver table is declared with:

```sql
CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
```

What happens to records with `amount <= 0`?

- A. The pipeline update fails
- B. They are written to the table and flagged in a metadata column
- C. They are dropped from the target, and the violation count is recorded in the pipeline event log
- D. They are quarantined into an automatic `_rejected` table
- E. They are rescued into `_rescued_data`

## § 4 — Working with Lakeflow Jobs

**Q23.** A job task occasionally fails due to transient network errors and succeeds when re-run
manually. What is the appropriate way to make the job resilient?

- A. Schedule the job twice per night
- B. Configure automatic retries (with retry interval) on the task
- C. Wrap the notebook in a `while True` loop
- D. Set the job to continuous mode
- E. Add a second identical task in parallel

**Q24.** A workflow must run a `full_load` task when a task value `mode` equals `"full"`, and an
`incremental_load` task otherwise. Which Lakeflow Jobs feature implements this?

- A. A for-each task over both tasks
- B. An If/else condition task that branches on the task value
- C. Two separate jobs with different schedules
- D. A file arrival trigger on each branch
- E. Run-if dependency set to "All failed"

**Q25.** A notebook task must be executed once per country in a list of 25 countries, with the
country passed as a parameter and limited concurrency. Which feature fits best?

- A. 25 hard-coded tasks in the DAG
- B. A for-each task iterating over the country list with a nested task
- C. A continuous job filtering by country
- D. A SQL task with a `LOOP` statement
- E. Task retries set to 25

**Q26.** New vendor files land irregularly (sometimes hourly, sometimes not for days) in a Unity
Catalog volume. Processing should start shortly after files arrive without wasting compute in
between. Which trigger type is the best fit?

- A. A cron schedule every 5 minutes
- B. A continuous job
- C. A file arrival trigger on the volume location
- D. A manual trigger invoked by the vendor
- E. A table update trigger on the bronze table

**Q27.** A gold refresh job must run only after an upstream silver Delta table has actually received
new data — not on a fixed schedule. Which trigger type is designed for this?

- A. File arrival trigger
- B. Cron schedule with a guard task that fails when there is no new data
- C. Table update trigger on the silver table
- D. Continuous mode
- E. Webhook trigger from the writing job

**Q28.** In a job DAG, a cleanup task must run **regardless of whether** the upstream tasks
succeeded or failed. Which task dependency setting achieves this?

- A. Depends on: none
- B. "Run if dependencies" set to **All done**
- C. "Run if dependencies" set to **All succeeded**
- D. Retries set to unlimited
- E. A separate job triggered by the first job's success

**Q29.** Which statement about Lakeflow Jobs task types is correct?

- A. A pipeline task can trigger a Lakeflow Spark Declarative Pipeline update as part of the job DAG
- B. SQL tasks can only run queries, not refresh dashboards
- C. Notebook tasks cannot receive parameters
- D. Each job can contain only one task type
- E. Dashboard tasks run Python notebooks in disguise

## § 5 — Implementing CI/CD

**Q30.** Which Git operations can a data engineer perform directly from a Databricks Git Folder in
the workspace UI?

- A. Only read-only cloning; all writes happen in the Git provider
- B. Clone, create branches, commit, push, pull; pull requests are then created in the Git provider
- C. Clone and merge PRs, including code review approval
- D. Only commit; branching is not supported
- E. Force-push and history rewriting

**Q31.** What is the correct Databricks CLI command sequence to check a Declarative Automation
Bundle for errors and then deploy it to a target environment?

- A. `databricks bundle validate` → `databricks bundle deploy -t <target>`
- B. `databricks bundle build` → `databricks bundle push`
- C. `databricks fs cp` → `databricks jobs create`
- D. `databricks bundle plan` → `databricks bundle apply`
- E. `databricks repos update` → `databricks bundle sync`

**Q32.** A team maintains one bundle codebase but needs a smaller cluster and a dev catalog in
development, and a larger cluster and prod catalog in production. How is this handled idiomatically
in a Declarative Automation Bundle?

- A. Two separate Git repositories, one per environment
- B. Manually editing `databricks.yml` before each deployment
- C. Defining `targets` (e.g., `dev`, `prod`) in `databricks.yml` with variables and per-target overrides
- D. Environment variables read inside every notebook
- E. A post-deploy script that patches the job JSON

**Q33.** Which assets can a Declarative Automation Bundle define and deploy? (Choose the most
complete correct answer.)

- A. Only notebooks
- B. Jobs, Lakeflow Spark Declarative Pipelines, and other workspace assets such as notebooks and files
- C. Only cluster policies
- D. Unity Catalog metastores
- E. Only SQL warehouses

**Q34.** On every merge to the `main` branch, the latest bundle should be deployed automatically to
a staging workspace. What is the recommended implementation?

- A. A Databricks job that polls the Git repository
- B. A CI pipeline (e.g., GitHub Actions) that installs the Databricks CLI and runs `databricks bundle deploy -t staging`, authenticating with a service principal
- C. A Git Folder in the staging workspace that auto-pulls `main` every minute
- D. Asking each engineer to deploy manually after merging
- E. A webhook that copies notebooks via the workspace export API

## § 6 — Troubleshooting, Monitoring, Optimization

**Q35.** In the Spark UI, a stage shows 199 tasks finishing in seconds while one task runs for
20 minutes with a much larger shuffle read size. What is the most likely cause?

- A. Disk spill on all executors
- B. Data skew — one partition key holds a disproportionate share of the rows
- C. Too few shuffle partitions
- D. A broadcast join that exceeded the threshold
- E. Garbage collection pauses on the driver

**Q36.** A job's Spark UI shows large values for "Spill (memory)" and "Spill (disk)" in a stage's
task metrics. What does this indicate, and what is a reasonable first response?

- A. The cluster ran out of disk; add more storage
- B. Task working sets exceed available execution memory, so Spark spills to disk; increase memory per task (larger executors or fewer concurrent tasks) or reduce partition size
- C. The Delta log is corrupted; run `FSCK REPAIR TABLE`
- D. Too many small files; run `OPTIMIZE`
- E. The driver is undersized; enable autoscaling

**Q37.** Which statement about **Liquid Clustering** is correct?

- A. It must be combined with Hive-style partitioning for best results
- B. It replaces Hive-style partitioning and Z-ordering; clustering keys are declared with `CLUSTER BY` and can be changed later without rewriting the table
- C. It only works on external Parquet tables
- D. It physically sorts the entire table on every write
- E. It is enabled by setting `delta.autoOptimize.optimizeWrite`

**Q38.** What does **Predictive Optimization** do for Unity Catalog managed tables?

- A. It automatically runs maintenance operations such as `OPTIMIZE` and `VACUUM` where they provide benefit, without manual scheduling
- B. It predicts query results and caches them
- C. It rewrites user SQL into more efficient plans at parse time
- D. It automatically converts external tables to managed tables
- E. It scales the SQL warehouse based on forecasted load

## § 7 — Governance and Security

**Q39.** What happens to the underlying data files when a **managed** Unity Catalog table is
dropped, compared to an **external** table?

- A. Both keep their data files; only metadata is removed
- B. Managed: data files are deleted (after a retention window); external: data files remain in the external location
- C. Managed: data files remain; external: data files are deleted
- D. Both delete their data files immediately
- E. Dropping external tables is not permitted in Unity Catalog

**Q40.** A user receives `SELECT` on table `dea_learning.silver.orders` but still cannot query it.
What is the most likely missing piece?

- A. `SELECT` must also be granted on the catalog
- B. The user additionally needs `USE CATALOG` on `dea_learning` and `USE SCHEMA` on `silver`
- C. The user must be the table owner
- D. `READ FILES` on the underlying volume is missing
- E. The table must first be converted to a managed table

**Q41.** Which statement grants a group read access to all current **and future** tables in the
schema `dea_learning.silver`?

- A. `GRANT SELECT ON TABLE dea_learning.silver.* TO data_readers`
- B. `GRANT SELECT ON SCHEMA dea_learning.silver TO data_readers`
- C. `GRANT USAGE ON SCHEMA dea_learning.silver TO data_readers`
- D. `GRANT SELECT ON FUTURE TABLES IN SCHEMA dea_learning.silver TO data_readers`
- E. A grant per table is always required

**Q42.** Non-EU analysts must not see rows where `region = 'EU'` — transparently, on the same
table, without creating views. Which Unity Catalog feature implements this?

- A. A dynamic view with `is_member()`
- B. A row filter function attached via `ALTER TABLE ... SET ROW FILTER`
- C. Column masking on the `region` column
- D. `DENY SELECT` on the EU partition
- E. A separate table per region

**Q43.** The `email` column of a customer table must appear as `****` for everyone except members
of the `pii_readers` group, while all other columns stay fully visible. Which mechanism is designed
for this?

- A. A row filter on the `email` column
- B. A column mask function attached via `ALTER TABLE ... ALTER COLUMN email SET MASK`
- C. `REVOKE SELECT` on the `email` column
- D. Encrypting the column with a UDF at write time
- E. Moving `email` into a separate restricted table

**Q44.** A governance team wants to centrally enforce that **every** column tagged `pii` across the
metastore is masked, without editing each table individually. Which Unity Catalog capability
supports this?

- A. Per-table column masks maintained by each data owner
- B. ABAC (attribute-based access control) policies that apply masking/row-filtering based on tags
- C. Dynamic views generated by a nightly job
- D. Lakehouse Federation
- E. Delta Sharing recipient profiles

**Q45.** A nightly deployment pipeline needs to run jobs and access Unity Catalog objects without
being tied to a human user account. What is the recommended identity?

- A. A shared personal access token from the team lead's account
- B. A service principal with the required grants
- C. The workspace admin's credentials stored in the notebook
- D. An anonymous access mode on the metastore
- E. A group named after the pipeline

---
---

## Answer key & explanations

> Score yourself first: ≥ 32/45 ≈ pass level. Review the referenced repo file for every miss.

| # | Answer | Explanation | Review |
| --- | --- | --- | --- |
| 1 | **B** | With classic compute, clusters run in the customer's cloud account (compute plane); the control plane hosts the UI, job scheduler, and metadata. | `week_1_platform/learn.md` |
| 2 | **C** | Job compute is created per run, terminates on completion, and is billed at a lower rate than all-purpose compute — the standard choice for scheduled production workloads. | `week_1_platform/learn.md` |
| 3 | **C** | ACID comes from the `_delta_log` transaction log: ordered JSON commits define the table state atomically. | `week_1_platform/learn.md` |
| 4 | **C** | `COPY INTO` tracks loaded files in the table's commit log — it is idempotent; only new files load. `force = true` overrides this. | `week_2_ingestion/learn_deep_dive.md` §2 |
| 5 | **B** | Auto Loader with schema inference requires `cloudFiles.schemaLocation`; without it (and without an explicit schema) the stream throws on start. | `week_2_ingestion/learn_deep_dive.md` §3 |
| 6 | **B** | Default mode `addNewColumns`: the stream fails once on the unknown field, records the new schema version, and succeeds after restart. | `week_2_ingestion/learn_deep_dive.md` §3 |
| 7 | **B** | Auto Loader reads each file exactly once (tracked by path in the checkpoint). Overwrites are ignored unless `cloudFiles.allowOverwrites = true`; prefer an immutable landing zone. | `week_2_ingestion/learn_deep_dive.md` §3 |
| 8 | **C** | File notification mode replaces expensive `LIST` calls with cloud event queues (SNS/SQS, Event Grid, Pub/Sub) — the recommended mode at millions of files. | `week_2_ingestion/learn_deep_dive.md` §3 |
| 9 | **B** | `_rescued_data` captures fields that don't fit the expected schema (missing/mistyped/case-mismatched) so no data is silently lost. | `week_2_ingestion/learn_deep_dive.md` §3 |
| 10 | **A** | `CREATE TABLE AS read_files()` is the simple one-shot SQL load. It is not incremental — for that, use `COPY INTO` or Auto Loader. | `week_2_ingestion/learn_deep_dive.md` §1 |
| 11 | **C** | Lakeflow Connect managed connectors handle SaaS (Salesforce) and databases (SQL Server) with CDC and schema drift, fully managed. | `week_2_ingestion/learn_lakeflow_connect.md` |
| 12 | **B** | JDBC parallelism requires `partitionColumn` + `lowerBound` + `upperBound` + `numPartitions`; otherwise a single executor reads everything. | `week_2_ingestion/learn_deep_dive.md` §6 |
| 13 | **B** | `from_json(col, schema)` parses a JSON string into a struct; a schema string is required. Dot-notation (A) only works on already-typed structs. | `week_2_ingestion/learn_deep_dive.md` §5 |
| 14 | **B** | `explode()` emits one row per array element. `flatten` merges nested arrays; `unnest` is not Spark SQL. | `week_3_transformation/learn_data_transformation.md` |
| 15 | **B** | Left outer join keeps every left-side (orders) row and fills unmatched customer columns with NULL. | `week_3_transformation/learn_data_transformation.md` |
| 16 | **B** | A 20 MB dimension is a textbook broadcast-join candidate: ship the small table to every executor and skip shuffling the 2B-row fact table. | `week_3_transformation/learn_data_transformation.md` |
| 17 | **A** | SQL `UNION` deduplicates the combined result; `UNION ALL` keeps everything (and is cheaper — no dedup shuffle). | `week_3_transformation/learn_data_transformation.md` |
| 18 | **C** | `ROW_NUMBER()` partitioned by the key, ordered by recency, filtered to 1 — the standard latest-record-wins dedup. `dropDuplicates()` keeps an arbitrary row. | `week_3_transformation/code/03_aggregations_and_windows.sql` |
| 19 | **B** | `approx_count_distinct` (HyperLogLog) trades a small error bound for large speed gains on huge cardinalities. | `week_3_transformation/learn_data_transformation.md` |
| 20 | **B** | 200 shuffle partitions for a few MB creates tiny-task overhead and small files; lower the setting or let AQE coalesce partitions. | `week_3_transformation/learn_data_transformation.md` |
| 21 | **C** | Materialized views precompute results, refresh on schedule, and can refresh incrementally when inputs allow — built for BI serving in UC. | `week_3_transformation/learn_data_transformation.md` |
| 22 | **C** | `ON VIOLATION DROP ROW` drops offending records and records violation metrics in the pipeline event log. Default (no clause) keeps rows and only logs; `FAIL UPDATE` aborts. | `week_4_pipelines_and_jobs/learn_pipelines.md` |
| 23 | **B** | Task-level automatic retries with an interval are the built-in mechanism for transient failures. | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 24 | **B** | The If/else condition task branches the DAG based on a task value or expression. | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 25 | **B** | The for-each task iterates over a list, passing each element as a parameter to a nested task, with configurable concurrency. | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 26 | **C** | File arrival triggers fire when new files land in a storage/volume location — no idle compute, no polling schedule to tune. | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 27 | **C** | Table update triggers are the data-driven counterpart to file arrival: the job starts when the monitored Delta table is updated. | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 28 | **B** | "Run if dependencies = All done" executes the task after upstreams finish regardless of success/failure — the cleanup pattern. | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 29 | **A** | Pipeline tasks trigger a Lakeflow Spark Declarative Pipeline update inside the job DAG; jobs freely mix task types, and SQL tasks can also refresh dashboards. | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 30 | **B** | Git Folders support clone, branch, commit, push, pull in the workspace; PR creation and review happen in the Git provider. | `week_5_cicd_and_troubleshooting/learn_cicd.md` |
| 31 | **A** | The bundle lifecycle is `validate` → `deploy -t <target>` (→ `run` to execute). `plan/apply` is Terraform, not the bundles CLI. | `week_5_cicd_and_troubleshooting/learn_cicd.md` |
| 32 | **C** | `targets` in `databricks.yml` with variables/overrides is the idiomatic single-codebase, multi-environment setup for bundles. | `week_5_cicd_and_troubleshooting/learn_cicd.md` |
| 33 | **B** | Bundles package jobs, Lakeflow Spark Declarative Pipelines, and workspace assets (notebooks, files) as declarative config + code. | `week_5_cicd_and_troubleshooting/learn_cicd.md` |
| 34 | **B** | CI runner + Databricks CLI + `bundle deploy -t staging`, authenticated as a service principal — the documented CI/CD pattern. | `week_5_cicd_and_troubleshooting/learn_cicd.md` |
| 35 | **B** | One straggler task with outsized shuffle read = data skew on a hot key. Mitigate with AQE skew handling, salting, or broadcast. | `week_5_cicd_and_troubleshooting/learn_troubleshooting.md` |
| 36 | **B** | Spill metrics mean execution memory was exhausted and Spark wrote to disk; give tasks more memory or shrink partitions. | `week_5_cicd_and_troubleshooting/learn_troubleshooting.md` |
| 37 | **B** | Liquid Clustering (`CLUSTER BY`) supersedes partitioning/Z-ordering and allows changing clustering keys without a table rewrite. | `week_5_cicd_and_troubleshooting/learn_troubleshooting.md` |
| 38 | **A** | Predictive Optimization automatically schedules `OPTIMIZE`/`VACUUM` on UC managed tables where beneficial. | `week_5_cicd_and_troubleshooting/learn_troubleshooting.md` |
| 39 | **B** | Managed tables: UC controls the storage — dropping deletes data (after retention). External tables: only metadata is removed; files stay. | `week_6_governance/learn.md` |
| 40 | **B** | The UC privilege hierarchy requires `USE CATALOG` and `USE SCHEMA` on the parents in addition to `SELECT` on the table. | `week_6_governance/learn.md` |
| 41 | **B** | Granting `SELECT` at schema level cascades to all current and future tables in the schema. | `week_6_governance/learn.md` |
| 42 | **B** | Row filters (a boolean SQL UDF attached with `SET ROW FILTER`) restrict visible rows transparently on the table itself. | `week_6_governance/learn.md` |
| 43 | **B** | Column masks (`ALTER COLUMN ... SET MASK`) transform a single column's values per caller, leaving other columns untouched. | `week_6_governance/learn.md` |
| 44 | **B** | ABAC applies row-filter/masking policies centrally based on tags (e.g., `pii`) instead of per-table configuration. | `week_6_governance/learn.md` |
| 45 | **B** | Service principals are the recommended non-human identity for automation — grantable, auditable, not tied to a person. | `week_6_governance/learn.md` |

### Score interpretation

| Score | Meaning |
| --- | --- |
| ≥ 40 | Exam-ready on this material — book it |
| 32–39 | Pass level; review every miss before the real exam |
| < 32 | Re-study the weeks with the most misses, retake in a few days |