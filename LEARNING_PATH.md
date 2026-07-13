# Databricks Certified Data Engineer Associate — Learning Path

Target exam: **May 4, 2026** version (current as of June 2026).
Source: [Official Exam Guide PDF](https://www.databricks.com/sites/default/files/2026-05/databricks-certified-data-engineer-associate-exam-guide-may-2026-000.pdf) · [Cert landing page](https://www.databricks.com/learn/certification/data-engineer-associate)

## Exam facts

| Item | Value |
| --- | --- |
| Questions | 45 multiple choice |
| Time | 90 minutes |
| Cost | USD 200 |
| Pass mark | ~70% (not officially published; 32/45 is the consensus) |
| Validity | 2 years |
| Prereqs | None; ~6 months hands-on Databricks recommended |
| Delivery | Online proctored or test center |
| Code samples | SQL where possible, otherwise Python |

## Exam outline (May 4, 2026)

> The May 2026 revision rewrote the previous 5-section outline into **7 sections** and renamed DLT → **Lakeflow Spark Declarative Pipelines**, Databricks Asset Bundles → **Declarative Automation Bundles**, Databricks Repos → **Databricks Git Folders**.

| § | Section | Weight |
| --- | --- | --- |
| 1 | Databricks Intelligence Platform | 6% |
| 2 | Data Ingestion and Loading | 21% |
| 3 | Data Transformation and Modeling | 22% |
| 4 | Working with Lakeflow Jobs | 16% |
| 5 | Implementing CI/CD | 10% |
| 6 | Troubleshooting, Monitoring, and Optimization | 10% |
| 7 | Governance and Security | 15% |

### § 1 — Databricks Intelligence Platform (6%)
- Core components: architecture, Delta Lake, Unity Catalog.
- Compute services: characteristics, limits, cost models; pick the right one per workload (all-purpose / job / SQL warehouse / serverless / high-concurrency — the last is legacy, superseded by shared access mode / serverless).

### § 2 — Data Ingestion and Loading (21%)
- Ingestion patterns: batch, streaming, incremental — from local files, Lakeflow Connect standard connectors, Lakeflow Connect managed connectors.
- `COPY INTO` from ADLS / S3 / GCS into UC-governed tables.
- Auto Loader with schema enforcement + schema evolution, both directory-listing and file-notification modes.
- Lakeflow Connect for enterprise sources into UC tables.
- JDBC / ODBC / REST clients from notebooks, scheduled with Lakeflow Jobs.
- Choose between Auto Loader / Lakeflow Connect / partner connectors based on volume, frequency, types, governance.
- Semi-structured / nested data (JSON) into UC Delta tables.

### § 3 — Data Transformation and Modeling (22%)
- Clean bronze → silver with PySpark/SQL: nulls, type standardization.
- DataFrame combination: inner / left / broadcast / multiple-key / cross / union / union all.
- Column & row manipulation: add / drop / split / rename / filter / explode.
- Deduplication, aggregations: `count`, approx count distinct, mean, summary.
- Tuning knobs: `spark.sql.shuffle.partitions`, `spark.default.parallelism`, executor/driver memory, `spark.sql.autoBroadcastJoinThreshold`.
- Gold layer object choices: materialized views vs views vs streaming tables vs tables — when to use which for BI/analytics in UC.
- Data quality checks / expectations on Silver and Gold.

### § 4 — Working with Lakeflow Jobs (16%)
- Control flow: retries, conditional (branching) tasks, looping (for-each).
- Task types: notebook, SQL query, dashboard, pipeline; DAG dependencies.
- Schedules and trigger types: scheduled, file arrival, table update.
- Time-based vs data-driven triggers.

### § 5 — Implementing CI/CD (10%)
- Databricks Git Folders (formerly Repos): branching, commit, push, PR via Databricks Git integration.
- Declarative Automation Bundles (formerly Databricks Asset Bundles, DAB): variables and overrides across dev/test/prod with one codebase. (This repo keeps the "DAB" abbreviation for brevity.)
- Deploy DABs to package Jobs, Lakeflow Spark Declarative Pipelines, and workspace assets.
- Databricks CLI for validate/deploy/manage of DABs in automated workflows.

### § 6 — Troubleshooting, Monitoring, Optimization (10%)
- Trend job performance via Lakeflow Jobs run history.
- Lakeflow Jobs UI for pipeline health, DAG inspection, upstream blockers, run times, failure rates.
- Spark UI stage metrics → data skew, shuffle, disk spill detection.
- Liquid Clustering and Predictive Optimization features.
- Diagnose cluster startup failure, library conflicts, OOM.

### § 7 — Governance and Security (15%)
- Managed vs external tables in Unity Catalog: create / modify / delete / convert.
- Access control via UI and SQL: `GRANT` / `REVOKE` / `DENY` to users, groups, service principals; security hierarchy. (Note: `DENY` exists only in legacy Hive metastore table ACLs — Unity Catalog supports `GRANT`/`REVOKE` only; classic exam distractor.)
- Column-level masking, row-level security.
- UC ABAC policies for centralized row-filtering and column-masking on sensitive data.

## Recommended training (official)

Instructor-led: **Data Engineering with Databricks**.
Self-paced (Databricks Academy — free with account):
1. Data Ingestion with Lakeflow Connect
2. Deploy Workloads with Lakeflow Jobs
3. Build Data Pipelines with Lakeflow Spark Declarative Pipeline
4. DevOps Essentials for Data Engineering
5. Data Interoperability with Unity Catalog
6. Get Started with Data Governance on Databricks

## Worked data project

A single e-commerce domain (`customers` × `orders` × `items`) threads through every week and culminates in a full medallion pipeline in Week 4. All code samples use the `dea_learning` catalog with `raw` / `bronze` / `silver` / `gold` / `sec` schemas (five schemas; `raw` holds the landing volume).

- `DATA_MODEL.md` — domain schema, source files, week-by-week thread.
- `PIPELINE.md` — end-to-end medallion pipeline with diagrams, expectations, orchestration, governance overlay, and run-it-yourself instructions.

## Repo coverage vs. exam — week → folder map

| Exam section | Week folder | Notes |
| --- | --- | --- |
| §1 Platform | `week_1_platform/` | `learn.md` + glossary + runnable code |
| §2 Ingestion | `week_2_ingestion/` | `learn_lakeflow_connect.md` + `learn_deep_dive.md` + glossary + code |
| §3 Transformation | `week_3_transformation/` | `learn_data_transformation.md` + glossary + code |
| §4 Pipelines + Jobs | `week_4_pipelines_and_jobs/` | `learn_jobs.md` + `learn_pipelines.md` + glossary + code |
| §5 + §6 CI/CD + Troubleshooting | `week_5_cicd_and_troubleshooting/` | `learn_devops.md` + `learn_cicd.md` + `learn_troubleshooting.md` + glossary + code |
| §7 Governance | `week_6_governance/` | `learn.md` + glossary + code |

Each week folder contains: `learn*.md` (theory), `glossary.md` (key terms), and `code/` (runnable notebooks/SQL/Python you can import into a Databricks workspace).

## Step-by-step study plan (6 weeks, ~6–8 h/week)

### Week 1 — Platform foundation (§1, 6%)
1. Read `week_1_platform/learn.md` and skim `week_1_platform/glossary.md`.
2. Self-paced Academy course: *Get Started with the Databricks Data Intelligence Platform* (or first hour of *Data Engineering with Databricks*).
3. Spin up a free Databricks Free Edition workspace (serverless-only; successor to the retired Community Edition); explore serverless compute, create a serverless SQL warehouse, and read [Compute concepts](https://docs.databricks.com/aws/en/compute/). Note: creating an all-purpose cluster requires a full workspace or free trial — Free Edition doesn't allow it.
4. Run the notebooks in `week_1_platform/code/` against your workspace.

### Week 2 — Ingestion (§2, 21%)
1. Read `week_2_ingestion/learn_lakeflow_connect.md` then `learn_deep_dive.md`.
2. Academy: **Data Ingestion with Lakeflow Connect** (full course).
3. Hands-on with `week_2_ingestion/code/`:
   - `COPY INTO` from a volume.
   - Auto Loader directory-listing AND file-notification modes — observe schema evolution behavior.
   - Use the existing `docker-compose.yml` + `ngrok` setup (see `README.md`) to ingest from SQL Server via Lakeflow Connect Managed Connector (CDC).
4. Practice JSON ingestion with nested structures — `from_json`, struct/array flattening.

### Week 3 — Transformation & modeling (§3, 22%)
1. Read `week_3_transformation/learn_data_transformation.md` and glossary.
2. Work through Udemy notebooks `2.1`–`2.4` and `3.1`–`3.3` (in `udemy_…/`).
3. Academy: **Data Analysis with Databricks** OR sections on PySpark/SQL in *Data Engineering with Databricks*.
4. Run `week_3_transformation/code/` — drill all join types and Gold-object choices.

### Week 4 — Pipelines + Jobs (§4, 16%)
1. Read `week_4_pipelines_and_jobs/learn_jobs.md` and `learn_pipelines.md`.
2. Academy:
   - **Build Data Pipelines with Lakeflow Spark Declarative Pipeline**
   - **Deploy Workloads with Lakeflow Jobs**
3. Run notebooks in `week_4_pipelines_and_jobs/code/`: a 3-task job (notebook → SQL → pipeline), CDC + SCD Type 1 vs 2 with `AUTO CDC INTO`.

### Week 5 — CI/CD + Troubleshooting (§5 + §6, 20%)
1. Read `week_5_cicd_and_troubleshooting/learn_devops.md` (concepts), then `learn_cicd.md` (DAB hands-on), then `learn_troubleshooting.md` (Spark UI / optimization).
2. Academy: **DevOps Essentials for Data Engineering**.
3. Run `week_5_cicd_and_troubleshooting/code/` to scaffold a DAB and deploy across targets, induce + diagnose Spark skew/spill.

### Week 6 — Governance + final prep (§7, 15%)
1. Read `week_6_governance/learn.md`.
2. Academy: **Get Started with Data Governance on Databricks** AND **Data Interoperability with Unity Catalog**.
3. Run `week_6_governance/code/` — managed/external tables, GRANT/REVOKE (see `02_grant_revoke.sql`; DENY is legacy Hive metastore only, not supported in Unity Catalog), row filters and column masks, Delta Sharing.
4. Practice exams:
   - Local: `practice_exam/practice_exam_1.md` — 45 questions matching the May-2026 outline weights, with answer key and per-question repo references.
   - Udemy practice exam pack already linked from `udemy_databricks_certified_data_engineer_associate/README.md`.
   - Aim for ≥85% on 2 consecutive practice runs before booking.
5. Re-read the official sample questions in the exam guide PDF (5 worked examples at the end).
6. Book the exam via [WebAssessor](https://webassessor.com/databricks).

## High-leverage drill list (last week before exam)

Memorize until automatic:
- The Auto Loader Python snippet end-to-end (`cloudFiles`, schemaLocation, checkpoint, trigger).
- `COPY INTO` syntax with `FILEFORMAT` + `FORMAT_OPTIONS` + `COPY_OPTIONS`.
- `CREATE OR REFRESH STREAMING TABLE … FROM STREAM read_files(…)`.
- `CREATE FLOW … AUTO CDC INTO … KEYS (…) APPLY AS DELETE WHEN … SEQUENCE BY … STORED AS SCD TYPE 2`.
- Pipeline expectations (Spark Declarative Pipelines): Python decorators `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail`; SQL form `CONSTRAINT <name> EXPECT (<cond>) ON VIOLATION DROP ROW / FAIL UPDATE`.
- `GRANT … ON CATALOG/SCHEMA/TABLE TO …`.
- Cluster type → workload mapping (be ready for trick questions on high-concurrency (legacy; superseded by shared access mode / serverless) vs SQL warehouse vs job).

## Resource index

- **Official exam guide PDF** — [May 2026 version](https://www.databricks.com/sites/default/files/2026-05/databricks-certified-data-engineer-associate-exam-guide-may-2026-000.pdf)
- **Certification page** — [databricks.com/learn/certification/data-engineer-associate](https://www.databricks.com/learn/certification/data-engineer-associate)
- **Databricks Academy** — [customer-academy.databricks.com](https://customer-academy.databricks.com) (free self-paced courses listed above)
- **Free workspace** — [Databricks Free Edition](https://www.databricks.com/try-databricks) (serverless-only; replaced the retired Community Edition)
- **Docs hubs** — [Lakeflow Connect](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/), [Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/), [Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/), [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/)
- **Udemy course in this repo** — see `udemy_databricks_certified_data_engineer_associate/README.md`
- **Registration** — [WebAssessor / Databricks](https://webassessor.com/databricks)