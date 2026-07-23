# Practice Exam 2 — Databricks Certified Data Engineer Associate (May 2026 outline)

45 multiple-choice questions · 90 minutes · pass target ≥ 36/45 (80% per the official Certification FAQ).
Question distribution mirrors the official exam guide weights (same as Practice Exam 1, no
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

**Q1.** A team of analysts runs short, ad-hoc SQL queries and refreshes BI dashboards throughout
the day. Queries must start within seconds without anyone managing cluster configuration. Which
compute type fits best?

- A. An all-purpose cluster shared by all analysts
- B. Job compute created per query
- C. A classic SQL warehouse with a large minimum size
- D. A serverless SQL warehouse
- E. A single-node cluster per analyst

**Q2.** A silver table was accidentally updated with wrong values two hours ago. The engineer wants
to **query** the table's state as it was before the bad update (version 41). Which statement does
this?

- A. `RESTORE TABLE silver.orders TO VERSION AS OF 41` — the only way to see old data
- B. `SELECT * FROM silver.orders VERSION AS OF 41`
- C. `DESCRIBE HISTORY silver.orders VERSION 41`
- D. `SELECT * FROM silver.orders WHERE _version = 41`
- E. `SHOW VERSIONS IN silver.orders`

**Q3.** In the fully qualified name `dea_learning.silver.orders`, what does each level represent in
Unity Catalog's namespace?

- A. catalog → schema → table
- B. metastore → catalog → table
- C. workspace → database → table
- D. catalog → database cluster → table
- E. schema → catalog → table

## § 2 — Data Ingestion and Loading

**Q4.** A `COPY INTO` statement loaded a directory last week. The source files were since corrected
in place, and the team wants to **reload all files** into the (now truncated) table. Which option
enables this?

- A. `FORMAT_OPTIONS ('mergeSchema' = 'true')`
- B. Dropping and recreating the target table is the only way
- C. `COPY_OPTIONS ('force' = 'true')`
- D. `FORMAT_OPTIONS ('inferSchema' = 'true')`
- E. Running the statement twice in the same session

**Q5.** A landing zone receives thousands of new JSON files continuously, around the clock, and the
table must stay near-current. The team currently reruns `COPY INTO` every 5 minutes and directory
listing is getting slower as the directory grows into the millions of files. What is the recommended
approach?

- A. Switch to Auto Loader, which scales to high file volumes (file-notification mode) and processes files incrementally as a stream
- B. Keep `COPY INTO` but run it every minute
- C. Load the whole directory with `read_files()` on each run
- D. Export the files to a database and use JDBC
- E. Increase the driver size so listing goes faster

**Q6.** Auto Loader schema inference types the column `amount` as `STRING` because the first sampled
files quote the values. The team wants `amount` inferred as `DOUBLE` while keeping inference for all
other columns. Which option achieves this?

- A. `cloudFiles.inferColumnTypes = false`
- B. A full explicit schema on the reader
- C. `cloudFiles.schemaEvolutionMode = 'rescue'`
- D. `cloudFiles.schemaHints = "amount DOUBLE"`
- E. Casting the column in a downstream silver query only

**Q7.** An Auto Loader ingestion should run as a **nightly job**: process every file that arrived
since the last run, then stop and release the compute. Which trigger accomplishes this?

- A. `.trigger(processingTime="24 hours")` on an always-on cluster
- B. `.trigger(availableNow=True)`
- C. `.trigger(continuous="1 second")`
- D. No trigger — Auto Loader always runs once and stops
- E. `.trigger(once=False)`

**Q8.** A team provides an explicit schema to Auto Loader and wants the stream to **never change
that schema**, but new/unexpected fields must still be captured somewhere instead of being lost.
Which schema evolution mode is designed for this?

- A. `addNewColumns`
- B. `failOnNewColumns`
- C. `rescue`
- D. `none` without any other option
- E. `mergeSchema`

**Q9.** An engineer deletes the checkpoint directory of a running Auto Loader stream to "fix" an
unrelated error, then restarts the stream with the same source and target. What is the consequence?

- A. The stream loses its ingestion history and reprocesses **all** files in the source directory, risking duplicates in the target
- B. Nothing — file-tracking state lives in the target table
- C. The stream resumes exactly where it left off using the schema location
- D. The stream fails permanently because the checkpoint cannot be recreated
- E. Only files from the last 7 days are reprocessed

**Q10.** Data must be pulled nightly from a third-party **REST API** (JSON responses, API key auth)
into a bronze Delta table. Which implementation matches Databricks' recommended patterns?

- A. A Lakeflow Connect managed connector — REST APIs are a standard connector source
- B. A notebook that calls the API with a Python HTTP client and writes to the Delta table, scheduled as a Lakeflow Job
- C. `COPY INTO` pointed at the API URL
- D. Auto Loader with `cloudFiles.format = "rest"`
- E. A SQL warehouse with an `HTTP` external location

**Q11.** Incoming event payloads are highly heterogeneous JSON — fields differ from event to event
and new fields appear constantly. The team wants to store the payload **without pre-defining a
schema** and still query nested fields efficiently with the `:` path operator. Which column type is
designed for this?

- A. `STRING` with `from_json()` at query time
- B. `MAP<STRING, STRING>`
- C. `STRUCT` with `mergeSchema` enabled
- D. `VARIANT` (populated via `parse_json()`)
- E. `BINARY`

**Q12.** Which of the following sources requires a Lakeflow Connect **managed** connector rather
than a standard file-based approach (Auto Loader / `COPY INTO`)?

- A. JSON files in an S3 bucket
- B. CSV files in a Unity Catalog volume
- C. A Workday or ServiceNow application
- D. Parquet files in ADLS
- E. Files uploaded through the workspace UI

## § 3 — Data Transformation and Modeling

**Q13.** During bronze → silver cleaning, nulls in `city` must become `"unknown"` and nulls in
`age` must become `0`, in one operation. Which PySpark call does this?

- A. `df.dropna(subset=["city", "age"])`
- B. `df.fillna({"city": "unknown", "age": 0})`
- C. `df.replace("null", "unknown")`
- D. `df.na.drop(how="all")`
- E. `df.coalesce("city", "age")`

**Q14.** A bronze column `created_at` is a STRING like `"2026-07-01 10:30:00"`. Which function
converts it to a proper `TIMESTAMP` column in the silver layer?

- A. `to_timestamp(col("created_at"))`
- B. `date_format(col("created_at"), "yyyy-MM-dd")`
- C. `unix_timestamp()` alone, keeping the BIGINT
- D. `col("created_at").cast("date")`
- E. `from_utc_timestamp(col("created_at"))` with no format

**Q15.** A price-simulation table needs **every combination** of 500 stores and 365 calendar days
(182,500 rows), with no join key relating the two tables. Which join produces this?

- A. `stores INNER JOIN calendar ON 1=1` is invalid syntax, so it can't be done in SQL
- B. `stores LEFT JOIN calendar`
- C. `stores FULL OUTER JOIN calendar`
- D. `stores LEFT SEMI JOIN calendar`
- E. `stores CROSS JOIN calendar`

**Q16.** Two DataFrames must be joined on **both** `country` and `city`. Which PySpark call is
correct?

- A. `a.join(b, on="country_city")`
- B. `a.join(b, on=["country", "city"], how="inner")`
- C. `a.join(b, on="country").join(b, on="city")`
- D. `a.crossJoin(b).filter("country AND city")`
- E. `a.union(b).dropDuplicates(["country", "city"])`

**Q17.** A column `full_name` contains values like `"Ada Lovelace"`. The silver table needs separate
`first_name` and `last_name` columns. Which approach is correct?

- A. `explode(col("full_name"))` into two rows
- B. `substring(col("full_name"), 1, 10)` for both columns
- C. `split(col("full_name"), " ")` and select the elements with `.getItem(0)` / `.getItem(1)`
- D. `from_json(col("full_name"), 'first STRING, last STRING')`
- E. `pivot("full_name")` on a whitespace grouping

**Q18.** A raw extract accidentally contains some rows twice — identical in **every** column. The
silver load must keep exactly one copy of each row. Which is the most direct fix?

- A. `df.dropDuplicates()` with no arguments (or `SELECT DISTINCT *`)
- B. `df.dropDuplicates(["id"])` on the key column only
- C. A `ROW_NUMBER()` window ordered by `updated_at`
- D. `GROUP BY` all columns with `MAX()` on each
- E. `df.distinct("all")`

**Q19.** An engineer wants a quick statistical profile (count, mean, stddev, min, quartiles, max) of
all numeric columns of a DataFrame during exploration. Which single call provides this?

- A. `df.count()` per column in a loop
- B. `df.explain(True)`
- C. `df.printSchema()`
- D. `df.summary()`
- E. `df.approxQuantile()` without arguments

**Q20.** In a Lakeflow Spark Declarative Pipeline, a bronze dataset must **incrementally and
continuously append** new records from a streaming source (Auto Loader). Each input record is
processed exactly once. Which dataset type is correct?

- A. A streaming table
- B. A materialized view
- C. A standard view
- D. A temporary view
- E. An external Parquet table

**Q21.** A reusable piece of query logic must always reflect the **current** state of its source
tables at query time, must not consume any storage, and needs no refresh schedule. Query cost at
read time is acceptable. Which object fits?

- A. A materialized view
- B. A streaming table
- C. A standard view
- D. A shallow clone refreshed nightly
- E. A Delta table maintained by MERGE

**Q22.** A declarative pipeline loads financial transactions. If **any** record has a NULL
`transaction_id`, the entire update must **abort** so bad data never lands. Which expectation
clause implements this?

- A. `CONSTRAINT valid_id EXPECT (transaction_id IS NOT NULL)`
- B. `CONSTRAINT valid_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW`
- C. `CONSTRAINT valid_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION QUARANTINE`
- D. `CONSTRAINT valid_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION FAIL UPDATE`
- E. A `WHERE transaction_id IS NOT NULL` filter in the dataset query

## § 4 — Working with Lakeflow Jobs

**Q23.** Task `ingest` computes the number of loaded rows, and the downstream task `report` needs
that number as an input. Which mechanism passes such a small value between tasks of the same job
run?

- A. Writing it to a temp Delta table and reading it downstream
- B. `dbutils.jobs.taskValues.set(...)` in `ingest`, `dbutils.jobs.taskValues.get(...)` in `report`
- C. A global Python variable — tasks share the interpreter
- D. Environment variables set on the cluster
- E. Task values cannot be passed; use one big notebook instead

**Q24.** A 10-task job run failed at task 7 due to a bad parameter; tasks 1–6 succeeded and wrote
correct results. The engineer fixes the parameter value. What is the most efficient recovery?

- A. Trigger a completely new job run
- B. Clone the job and run only tasks 7–10 manually
- C. Use **Repair run**, overwriting the failed task's parameter — only failed and skipped tasks re-execute
- D. Delete the run history and restart
- E. Mark task 7 as succeeded manually

**Q25.** The final step of a nightly gold job must refresh an AI/BI dashboard so stakeholders see
fresh numbers each morning. Which Lakeflow Jobs task type does this natively?

- A. A notebook task that screenshots the dashboard
- B. A pipeline task
- C. A for-each task
- D. A dashboard task
- E. A SQL task with `REFRESH TABLE`

**Q26.** An on-call notification task must execute **only when at least one** of its three upstream
tasks failed — and stay skipped when everything succeeds. Which "Run if dependencies" setting is
correct?

- A. All done
- B. At least one failed
- C. All failed
- D. None failed
- E. All succeeded

**Q27.** A revenue report must be generated every Monday at 08:00 regardless of when data arrived.
Which trigger type is appropriate?

- A. A cron schedule (time-based trigger)
- B. A file arrival trigger
- C. A table update trigger
- D. Continuous mode
- E. A manual trigger with a reminder email

**Q28.** A gold aggregation task depends on three upstream loads. Some upstreams may be legitimately
**skipped** (their branch didn't run), and the gold task should still run — but it must **not** run
if any upstream actually **failed**. Which dependency condition matches?

- A. All succeeded
- B. All done
- C. None failed
- D. At least one succeeded
- E. At least one failed

**Q29.** An upstream task computes a **list of table names that changed today** and the same
maintenance notebook must run once per changed table. The list differs on every run. How is this
modeled in Lakeflow Jobs?

- A. 50 pre-created tasks, one per possible table
- B. A for-each task whose input array is a task value produced by the upstream task, with the nested notebook receiving each element as a parameter
- C. A continuous job that watches all tables
- D. A repair run per table
- E. A cron schedule per table

## § 5 — Implementing CI/CD

**Q30.** Which file makes a directory a Declarative Automation Bundle, and what does it declare?

- A. `databricks.yml` — bundle name, targets (environments), and resources (jobs, pipelines) to deploy
- B. `bundle.json` — only the workspace URL
- C. `settings.yaml` — cluster policies
- D. `pom.xml` — bundle dependencies
- E. `MANIFEST.md` — a human-readable deployment description

**Q31.** A bundle has been deployed to the `dev` target. Which CLI command **executes** the job
`orders_etl` that the bundle defined?

- A. `databricks jobs submit --json @job.json`
- B. `databricks bundle deploy orders_etl`
- C. `databricks bundle run orders_etl -t dev`
- D. `databricks bundle validate -t dev`
- E. `databricks workspace run orders_etl`

**Q32.** After deploying a bundle to the `dev` target, an engineer notices all job names carry a
`[dev <username>]` prefix and every schedule is paused. What explains this?

- A. A deployment bug — redeploy to remove the prefixes
- B. The `dev` target uses `mode: development`, which prefixes resource names and pauses schedules so engineers can share a workspace safely
- C. The service principal lacks permissions, so jobs deploy in a locked state
- D. `mode: production` was set on the `dev` target
- E. The workspace is in maintenance mode

**Q33.** A bundle must deploy the same pipeline with catalog `dev_catalog` in development and
`prod_catalog` in production, without duplicating the resource definition. What is the idiomatic
mechanism?

- A. Two copies of the pipeline YAML, one per environment
- B. A sed script in CI that rewrites the YAML before deploy
- C. Comments in the YAML that the deployer toggles manually
- D. A bundle variable (e.g., `${var.catalog}`) with a different value per target
- E. Hardcoding the prod catalog and overriding it by hand in the dev UI

**Q34.** A data engineer must develop a fix in an existing repo using Databricks Git Folders. Which
workflow is correct?

- A. Edit directly on `main` in the Git Folder and force-push
- B. Download the repo as a ZIP, edit locally, re-upload notebooks
- C. Create a feature branch in the Git Folder, commit and push changes from the workspace, then open a pull request in the Git provider
- D. Create the pull request inside the Databricks workspace UI, including review and merge
- E. Clone a second workspace and copy notebooks across

## § 6 — Troubleshooting, Monitoring, Optimization

**Q35.** A nightly job has grown slower over the past month, but nobody knows which week the
slowdown started or which task causes it. Where should the engineer look **first**?

- A. The driver logs of tonight's run only
- B. The job's run history in the Lakeflow Jobs UI — run durations over time and per-task times reveal the trend and the offending task
- C. The Ganglia metrics of a random past run
- D. The Delta transaction log of the target table
- E. Restart the job with a bigger cluster and compare

**Q36.** A job cluster fails to start with `INIT_SCRIPT_FAILURE`. What is the correct next step to
diagnose it?

- A. Retry until it starts — init scripts are flaky by nature
- B. Switch the cluster to a different instance type
- C. Read the init script logs (e.g., under the configured cluster log path) to see the script's output and exit code
- D. Delete the checkpoint of the affected stream
- E. Downgrade the Databricks Runtime

**Q37.** One notebook needs `pandas 2.x`, but the shared all-purpose cluster has `pandas 1.x`
installed as a cluster library, and other users depend on it. What is the recommended way to avoid
the conflict?

- A. Install the newer version notebook-scoped via `%pip install pandas==2.x` so it applies only to that notebook's session
- B. Upgrade the cluster library and tell other users to migrate
- C. Uninstall pandas from the cluster
- D. Run the notebook on the driver via `%sh pip install`
- E. Pin the version in `spark.conf`

**Q38.** A query with `TIMESTAMP AS OF '2026-06-01'` on a Delta table fails with a "file not found /
version not available" style error, although it worked a month ago. What is the most likely cause?

- A. Time travel is limited to 24 hours by default
- B. `VACUUM` removed the data files of old versions that exceeded the retention period, so those versions can no longer be reconstructed
- C. The table was renamed, which resets history
- D. `OPTIMIZE` compacted the files and deleted the history
- E. The Delta log only ever keeps 10 versions

## § 7 — Governance and Security

**Q39.** A security team asks for `DENY SELECT` on a sensitive Unity Catalog table for a specific
group. What is the correct response?

- A. `DENY SELECT ON TABLE ... TO group` — works as in SQL Server
- B. `DENY` must be executed by the metastore admin
- C. Set `spark.databricks.acl.deny = true` first
- D. Unity Catalog does not support `DENY` — access is controlled with `GRANT`/`REVOKE` only (deny-by-default); `DENY` exists only in legacy Hive metastore table ACLs
- E. Use `REVOKE ALL PRIVILEGES ON METASTORE` instead

**Q40.** Before anyone can create an **external table** at `s3://corp-data/sales/`, which Unity
Catalog securables must an admin have configured for that path?

- A. A Delta Sharing share covering the bucket
- B. A storage credential (wrapping the IAM role) and an external location that binds the path to that credential
- C. A catalog with managed storage at that path
- D. A volume mounted on the cluster
- E. A `dbfs:/mnt` mount point

**Q41.** The engineer who created `dea_learning.silver.orders` leaves the company. The platform
team wants the `data_platform_admins` group to fully manage the table (grants, drops) going
forward. Which statement accomplishes this?

- A. `GRANT SELECT ON TABLE dea_learning.silver.orders TO data_platform_admins`
- B. `GRANT MODIFY ON TABLE dea_learning.silver.orders TO data_platform_admins`
- C. `ALTER TABLE dea_learning.silver.orders OWNER TO data_platform_admins`
- D. Deleting the user cascades ownership automatically to their group
- E. `CREATE OR REPLACE TABLE` under the admin account

**Q42.** A view is defined as:

```sql
CREATE VIEW gold.customers_v AS
SELECT id,
       CASE WHEN is_member('pii_readers') THEN email ELSE '***' END AS email
FROM   silver.customers;
```

Which Unity Catalog security pattern is this?

- A. A dynamic view — per-caller row/column security expressed with `is_member()` inside the view definition
- B. A column mask attached to the base table
- C. A row filter on `silver.customers`
- D. An ABAC policy
- E. A materialized view with security labels

**Q43.** An **external** Delta table must become a **managed** table — keeping its name, history,
permissions, and dependent views, without copying data by hand. Which statement does this
(DBR 17.0+ / serverless)?

- A. `CREATE TABLE new_t AS SELECT * FROM old_t` then drop the old table
- B. `ALTER TABLE silver.orders_archive SET MANAGED`
- C. `CONVERT TO DELTA silver.orders_archive`
- D. External tables can never become managed
- E. `ALTER TABLE silver.orders_archive SET LOCATION NULL`

**Q44.** An ABAC policy should mask every column tagged `pii = true` for non-privileged users,
across an entire catalog. What drives **which columns** the policy applies to?

- A. Column names matching a regex in the policy
- B. A manually maintained list of table/column pairs
- C. Governed tags assigned to the columns — the policy matches on the tag, not on individual objects
- D. The columns' data types
- E. Lineage relationships to a seed table

**Q45.** Forty analysts need `SELECT` on the gold schema. New analysts join and others leave every
month. What is the recommended way to manage their access?

- A. Add the analysts to a group and grant `SELECT` on the schema to the group — membership changes then require no new grants
- B. Grant `SELECT` to each analyst individually for auditability
- C. Share a service principal's token among the analysts
- D. Make each analyst an owner of the schema
- E. Grant `ALL PRIVILEGES` on the catalog to all users

---
---

## Answer key & explanations

> Score yourself first: ≥ 36/45 = pass level (80%, official Certification FAQ). Review the referenced repo file for every miss.

| # | Answer | Explanation | Review |
| --- | --- | --- | --- |
| 1 | **D** | Serverless SQL warehouses start in seconds, are fully managed, and are the default recommendation for BI/ad-hoc SQL. Classic warehouses take minutes; all-purpose clusters are for development. | `week_1_platform/learn.md` |
| 2 | **B** | Time travel *query* syntax is `SELECT ... VERSION AS OF n` (or `TIMESTAMP AS OF`). `RESTORE` rewrites the table instead of just reading an old version. | `week_1_platform/code/03_delta_time_travel_and_optimize.sql` |
| 3 | **A** | UC's three-level namespace is `catalog.schema.object`. The metastore sits above catalogs and is not part of the name. | `week_1_platform/learn.md` |
| 4 | **C** | `COPY INTO` is idempotent by default; `COPY_OPTIONS ('force' = 'true')` reprocesses already-loaded files — the full-refresh override. `mergeSchema` is about columns, not reloads. | `week_2_ingestion/learn_deep_dive.md` §2 |
| 5 | **A** | Continuous arrival + millions of files is Auto Loader territory: incremental streaming ingestion, file-notification mode for scale. `COPY INTO` is for smaller/one-off batch loads. | `week_2_ingestion/learn_deep_dive.md` §3 |
| 6 | **D** | `cloudFiles.schemaHints` forces types for named columns while inference handles the rest. A full explicit schema would disable inference entirely. | `week_2_ingestion/learn_deep_dive.md` §3 |
| 7 | **B** | `availableNow=True` drains everything pending, then stops — the incremental-batch pattern for scheduled jobs. `processingTime` keeps the stream (and cluster) alive. | `week_2_ingestion/learn_deep_dive.md` §3 |
| 8 | **C** | Mode `rescue`: the schema never evolves; unmatched fields land in `_rescued_data`. `failOnNewColumns` loses nothing but *fails*; `none` silently ignores unless a rescue column is configured. | `week_2_ingestion/learn_deep_dive.md` §3 |
| 9 | **A** | The checkpoint (RocksDB) is where Auto Loader tracks which files were ingested. Deleting it erases that memory: on restart every file is "new" — full reprocessing and likely duplicates. | `week_2_ingestion/learn_deep_dive.md` §3 |
| 10 | **B** | REST APIs are ingested with client code in a notebook (Python HTTP client), scheduled via Lakeflow Jobs. Lakeflow Connect covers specific SaaS/DB sources, not arbitrary REST APIs; `COPY INTO`/Auto Loader read files, not APIs. | `week_2_ingestion/learn_deep_dive.md` §6 |
| 11 | **D** | `VARIANT` stores schema-less semi-structured data in a binary form, populated with `parse_json()` and queried with the `:` operator — built for heterogeneous JSON. | `week_2_ingestion/learn_deep_dive.md` §5 |
| 12 | **C** | Workday/ServiceNow are SaaS applications — Lakeflow Connect **managed** connectors. Files in S3/ADLS/volumes are standard file-based ingestion (Auto Loader, `COPY INTO`, `read_files`). | `week_2_ingestion/learn_lakeflow_connect.md` |
| 13 | **B** | `fillna` with a per-column dict replaces nulls per column in one call. `dropna` deletes rows instead of fixing them. | `week_3_transformation/learn_data_transformation.md` §1 |
| 14 | **A** | `to_timestamp()` parses a string into a TIMESTAMP. `date_format` goes the other way (timestamp → string); casting to `date` loses the time part. | `week_3_transformation/learn_data_transformation.md` §1 |
| 15 | **E** | Every-combination-with-no-key is the definition of a cross join (cartesian product). | `week_3_transformation/learn_data_transformation.md` §3 |
| 16 | **B** | Multi-key equi-joins take a list: `on=["country", "city"]`. Chaining two joins against the same table (C) is wrong logic. | `week_3_transformation/learn_data_transformation.md` §3 |
| 17 | **C** | `split()` produces an array; `.getItem(0)` / `.getItem(1)` select the elements into new columns. `explode` would create extra *rows*, not columns. | `week_3_transformation/learn_data_transformation.md` §4 |
| 18 | **A** | Exact full-row duplicates → `dropDuplicates()` without arguments (equivalent to `SELECT DISTINCT *`). Key-based dedup (B) could drop legitimate rows that differ in other columns. | `week_3_transformation/learn_data_transformation.md` §5 |
| 19 | **D** | `df.summary()` returns count, mean, stddev, min, quartiles, max for numeric columns in one call (`describe()` is the smaller subset). | `week_3_transformation/learn_data_transformation.md` §5 |
| 20 | **A** | Streaming tables are the declarative-pipeline dataset for exactly-once, incremental append from streaming sources. Materialized views recompute from batch inputs. | `week_4_pipelines_and_jobs/learn_pipelines.md` |
| 21 | **C** | A standard view stores only the query — always current, zero storage, no refresh; you pay compute at read time. A materialized view trades storage/refresh for read speed — the opposite trade-off. | `week_3_transformation/learn_data_transformation.md` §6 |
| 22 | **D** | `ON VIOLATION FAIL UPDATE` aborts the pipeline update on the first violating record. Default (A) only logs; `DROP ROW` silently removes rows; `QUARANTINE` does not exist. | `week_4_pipelines_and_jobs/learn_pipelines.md` |
| 23 | **B** | Task values (`dbutils.jobs.taskValues.set/get`) are the built-in channel for small values between tasks in a run. Tasks run in separate contexts — no shared variables. | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 24 | **C** | Repair run re-executes only failed/skipped tasks and supports overwriting parameters of the failed tasks; succeeded tasks 1–6 are not recomputed. | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 25 | **D** | Dashboard tasks refresh an AI/BI dashboard as a first-class task type in the job DAG. | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 26 | **B** | "At least one failed" fires the task exactly when ≥1 upstream failed — the alerting pattern. "All done" would also run on success. | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 27 | **A** | A fixed weekly deadline is a time-based trigger — cron schedule. File-arrival/table-update triggers are data-driven and fire at unpredictable times. | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 28 | **C** | "None failed" runs the task when upstreams succeeded **or were skipped**, but not when any failed — exactly the skip-tolerant, failure-intolerant case. "All succeeded" would block on a skip. | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 29 | **B** | For-each iterates over an input array — which can be a task value from an upstream task — running the nested task once per element with the element as parameter. | `week_4_pipelines_and_jobs/learn_jobs.md` |
| 30 | **A** | `databricks.yml` is the bundle's root config: bundle name, `targets` (dev/staging/prod), and `resources` (jobs, pipelines, workspace assets). | `week_5_cicd_and_troubleshooting/learn_cicd.md` |
| 31 | **C** | `databricks bundle run <resource_key> -t <target>` triggers a deployed job/pipeline. `deploy` only ships the definitions; `validate` only checks them. | `week_5_cicd_and_troubleshooting/learn_cicd.md` |
| 32 | **B** | `mode: development` intentionally prefixes resource names with `[dev <user>]` and pauses schedules so multiple engineers can deploy into a shared workspace without collisions or accidental runs. | `week_5_cicd_and_troubleshooting/learn_cicd.md` |
| 33 | **D** | Bundle `variables` referenced as `${var.catalog}` with per-target values are the idiomatic single-definition, multi-environment mechanism. | `week_5_cicd_and_troubleshooting/learn_cicd.md` |
| 34 | **C** | Git Folders support branch/commit/push from the workspace; the pull request (review, merge) happens in the Git provider. Direct pushes to `main` and in-workspace PRs are not the pattern. | `week_5_cicd_and_troubleshooting/learn_cicd.md` |
| 35 | **B** | The Jobs run history shows run durations over time and per-task breakdowns — the first stop for "when did it get slow and which task". Logs of a single run can't show a trend. | `week_5_cicd_and_troubleshooting/learn_troubleshooting.md` |
| 36 | **C** | `INIT_SCRIPT_FAILURE` means the script exited non-zero; its stdout/stderr land in the cluster's init script logs — read them to find the actual error. | `week_5_cicd_and_troubleshooting/learn_troubleshooting.md` |
| 37 | **A** | Notebook-scoped libraries (`%pip install`) apply only to that notebook's session and don't leak to the cluster — the standard isolation for version conflicts. | `week_5_cicd_and_troubleshooting/learn_troubleshooting.md` |
| 38 | **B** | `VACUUM` physically deletes files no longer referenced by versions inside the retention window; time travel to versions older than that becomes impossible. `OPTIMIZE` never deletes history. | `week_5_cicd_and_troubleshooting/learn_troubleshooting.md` |
| 39 | **D** | UC is deny-by-default and supports only `GRANT`/`REVOKE`. `DENY` exists solely in legacy Hive-metastore table ACLs — a classic exam trap. | `week_6_governance/learn.md` |
| 40 | **B** | External-path access = storage credential (cloud identity) + external location (path ↔ credential binding), plus privileges on the external location. | `week_6_governance/learn.md` |
| 41 | **C** | `ALTER TABLE ... OWNER TO <group>` transfers ownership — the management right over grants and lifecycle. Plain `SELECT`/`MODIFY` grants don't allow managing permissions. | `week_6_governance/learn.md` |
| 42 | **A** | `is_member()` inside a view definition = dynamic view: per-caller column/row security, with grants given on the view instead of the base table. | `week_6_governance/learn.md` |
| 43 | **B** | `ALTER TABLE ... SET MANAGED` (DBR 17.0+/serverless) promotes an external Delta table to managed in place — name, history, permissions, views preserved; `UNSET MANAGED` can revert within 14 days. | `week_6_governance/code/01_managed_external_convert.sql` |
| 44 | **C** | ABAC policies match on governed **tags** — tag the columns `pii`, and the policy applies wherever the tag appears, with no per-table configuration. | `week_6_governance/learn.md` |
| 45 | **A** | Grant to a **group**; joiners/leavers are handled by group membership, not by new grants. Per-user grants don't scale and token sharing is an anti-pattern. | `week_6_governance/learn.md` |

### Score interpretation

| Score | Meaning |
| --- | --- |
| ≥ 41 | Exam-ready on this material — book it |
| 36–40 | Pass level; review every miss before the real exam |
| < 36 | Re-study the weeks with the most misses, retake in a few days |
