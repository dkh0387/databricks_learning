# Databricks Platform Fundamentals

> See also: `../LEARNING_PATH.md` for the full study plan, `../week_6_governance/learn.md` for the UC deep dive.

- **Delta Lake:** open source storage framework enables building Lakehouse
    - Parquet files: data file format which is linked to tables
    - Delta tables: tables stored in Delta Lake with full SQL support
    - Delta log: ordered records of all changes to the table (JSON)
    - Writing: write a new parquet file
    - Reading: read delta log first for last versions, then read parquet files
- **Database / schema:** collection of tables. `DATABASE` is an alias for `SCHEMA` in SQL; in Unity Catalog the
  canonical term is **schema**, and a schema lives inside a catalog (three-level namespace: `catalog.schema.object`).
- **Hive Metastore (legacy):** workspace-scoped metastore that stored table/database metadata before Unity Catalog.
  Still available as the `hive_metastore` catalog, but **deprecated for new workspaces** — prefer Unity Catalog.
- **Unity Catalog (UC):** account-level governance for tables, files, ML models, etc. Default for all new content.
- **Storage:**
    - HMS managed tables landed under `dbfs:/user/hive/warehouse/<tbl_name>`.
    - UC managed tables land under the catalog's or schema's **managed storage location** (cloud-bucket path
      configured in UC), *not* `dbfs:/user/hive/warehouse/`.
    - Inspecting a table:
        - `DESCRIBE EXTENDED <tbl_name>` — table-level metadata: **Type** (`MANAGED` / `EXTERNAL`), **Owner**,
          catalog, schema, comments, table properties.
        - `DESCRIBE DETAIL <tbl_name>` — Delta-specific physical detail: **location**, `numFiles`, `sizeInBytes`,
          `format`, `clusteringColumns`.
- **Tables:** *managed vs. external* is orthogonal to *Delta vs. other formats*.
    - **Managed table** — metastore owns both metadata and underlying files. `DROP TABLE` deletes the files.
      `CREATE TABLE tbl_name (col_name data_type, ...)` (no `LOCATION`).
    - **External table** — metastore owns metadata only; files live at a caller-provided path.
      `DROP TABLE` does **not** delete the files.
      `CREATE TABLE tbl_name (...) USING DELTA LOCATION '<path>'`.
    - **`USING <format>`** — picks the data source; **without it the default is Delta**. Valid formats per the
      [official reference](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using):
      `AVRO, BINARYFILE, CSV, DELTA, ICEBERG, JSON, ORC, PARQUET, TEXT`. Clause order:
      column list → `USING` → table clauses (`OPTIONS`, `LOCATION`). Exam favorite — external table over raw CSV files:
      ```sql
      CREATE TABLE my_table
      (col1 STRING, col2 STRING)
      USING CSV
      OPTIONS (header = "true", delimiter = ";")
      LOCATION '/path/input';
      ```
      `USING CSV` is mandatory here (a Delta table cannot point at raw CSV files); `OPTIONS` configures parsing.
      Result is a non-Delta external table: no ACID, no time travel, no MERGE — just a table mapping over the files.
- **Delta Tables:** any table (managed or external) stored in the Delta Lake format. Adds ACID transactions, time
  travel, schema evolution, etc.
    - Creating:
        - CTAS: `CREATE TABLE tbl_name AS SELECT ... FROM ...`
    - Cloning:
        - DEEP CLONE (full data copy, slow, independent of source after clone):
          `CREATE TABLE tbl_clone_name DEEP CLONE source_tbl_name`
        - SHALLOW CLONE (metadata-only, fast, references source data files — dependent on source's lifecycle):
          `CREATE TABLE tbl_clone_name SHALLOW CLONE source_tbl_name`
        - Pin to a specific source version: `... CLONE source_tbl_name VERSION AS OF 12` or
          `... CLONE source_tbl_name TIMESTAMP AS OF '2026-06-01T00:00:00'`
- **Table Constraints:**
    - NOT NULL: column cannot contain null values
    - CHECK: column value must satisfy a given condition
    - `ALTER TABLE tbl_name ADD CONSTRAINT constraint_name CHECK (constraint_details)`
- **Views:**
    - Permanent view (just "view"): SQL query stored in a schema (no data stored; recomputed at query time).
      `CREATE VIEW view_name AS SELECT ... FROM ...`
    - Temporary view: attached to the current Spark session (gone when the notebook/job session ends).
      `CREATE TEMP VIEW view_name AS SELECT ... FROM ...`
    - Global temporary view: attached to a cluster across sessions; restart clears it.
      Stored in the `global_temp` database. Not supported on serverless compute.
      `CREATE GLOBAL TEMP VIEW view_name AS SELECT ... FROM ...`
    - Materialized view (covered in Week 3/4): stored, refresh-on-demand, BI-friendly. Cannot be a streaming source.

# Architecture: control plane vs. compute plane

Databricks splits every deployment into two layers — the exam tests *where data processing happens*:

| Layer | Runs in | Contains |
| --- | --- | --- |
| **Control plane** | Databricks' cloud account | Web UI, notebooks frontend, job scheduler, cluster manager, Unity Catalog metadata — management only, no Spark data processing |
| **Compute plane (classic)** | **Customer's cloud account** | The actual clusters (driver + executors as VMs in the customer's VPC/subscription) — all Spark data processing |
| **Compute plane (serverless)** | Databricks' cloud account | Databricks-managed compute inside a secured environment; still reads/writes the customer's data in the customer's storage |

- With **classic compute**, the entire cluster — driver *and* executors — lives in the customer's
  account. There is no split where the driver sits in the control plane (a common distractor).
- **Data at rest** stays in the customer's cloud storage in both models; serverless moves only the
  *compute* into Databricks' account, not the data.
- Exam heuristic: "classic (non-serverless) compute — where does processing happen?" →
  **compute plane in the customer's cloud account**. The control plane never processes data; it
  orchestrates (UI, scheduler, metadata).

### Network consequence: reaching on-premises systems

Where the compute runs decides which network paths it can use:

- **Classic compute** is deployed into your own VNet/VPC ("VNet injection") — the cluster VMs sit in
  your corporate network and can reuse its existing routes: Site-to-Site VPN, AWS Direct Connect,
  Azure ExpressRoute. An on-prem database (e.g. PostgreSQL) is reachable directly via JDBC
  (see `../week_2_ingestion/learn_deep_dive.md` §6).
- **Serverless compute** runs in Databricks' serverless compute plane — outside your network. Your
  VPN / Direct Connect routes simply don't exist there. NCCs (Network Connectivity Configurations)
  solve a different problem: private connectivity to *cloud* services (storage private endpoints),
  not a bridge into an on-prem data center.
- Exam heuristic: "on-premises" + "VPN / Direct Connect / ExpressRoute" in the question → serverless
  is out; the answer is classic compute in the customer network — for a scheduled job specifically
  the **classic job cluster**. This is a third hard dependency that rules out serverless, next to
  specific instance types and init scripts.

# Compute services

Exam §1 explicitly tests choosing the right compute for a workload. Four options:

| Compute | Best for | Startup | DBU rate | Notes |
| --- | --- | --- | --- | --- |
| **All-purpose cluster** | Interactive dev, notebooks, ad-hoc analysis | 3–6 min (classic) | Highest | Shared across users; longer-lived. Pick this for development. |
| **Job cluster** | Production scheduled jobs | 3–6 min (classic) | Lower (Jobs Compute rate) | Created per job run, terminated after. Cheaper than all-purpose for the same workload. |
| **Serverless compute** (jobs / notebooks / pipelines) | Both dev and prod | Seconds | Bundled (compute + infra in one DBU rate) | Fully managed by Databricks. No cluster config. Use unless you have a hard dependency on instance type / init scripts. |
| **SQL warehouse** (classic, pro, serverless) | SQL queries, BI dashboards | Classic: minutes · Pro/Serverless: seconds | Varies by tier | Optimized for SQL workloads. Pick **serverless SQL warehouse** for BI by default. |

> Legacy note: **High Concurrency clusters** (multi-user clusters with process isolation) are a legacy option,
> superseded by clusters with **shared access mode** and by **SQL warehouses** for concurrent SQL workloads.

### Access modes & Unity Catalog

To read/write UC-governed tables, classic clusters must run a UC-capable **access mode**
([reference](https://docs.databricks.com/aws/en/compute/access-mode-limitations)); SQL warehouses
support UC by default, serverless is always UC-capable.

| Access mode | Formerly | Users | Unity Catalog |
| --- | --- | --- | --- |
| **Standard** | "Shared" | Multiple, isolated from each other | ✅ recommended default for all workloads |
| **Dedicated** | "Single user" | One user or group | ✅ (see caveat below) |
| **No isolation shared** | — | Multiple, no isolation | ❌ **cannot access UC data at all** — classic exam distractor |

- Exam pattern: "cluster can't read a UC table despite correct grants" → check the access mode
  (No isolation shared / legacy credential passthrough don't support UC).
- Dedicated caveat: on DBR ≤ 15.3 dedicated compute **cannot read tables with row filters / column
  masks or dynamic views** (fine-grained access control needs DBR 15.4+). Ties into week 6 security.

### Alternative names you'll meet in exam questions

| Exam/legacy term | Means |
| --- | --- |
| **Interactive compute / interactive cluster** | = all-purpose compute (older name; "automated cluster" was the old name for job clusters) |
| **Serverless job compute** | = serverless compute *for jobs* — Databricks names serverless per workload type: for jobs / for notebooks / for pipelines, plus serverless SQL warehouses |

### Instance pools

A pool keeps **pre-warmed, ready-to-run cloud VMs** that classic clusters (all-purpose *and* job) draw
from at startup and when autoscaling. Effect: the 3–6 min cluster start shrinks drastically because the
cloud-provider VM provisioning step disappears.

- Cost mechanics (favorite exam angle): **idle instances in the pool incur no DBUs** — only the cloud
  VM cost keeps running. You trade cloud infrastructure cost for start speed.
- Serverless vs pools: both attack slow cluster starts — serverless by letting Databricks own the
  compute entirely, pools by pre-warming *your own* VMs. Pools only make sense for classic compute;
  with serverless there is nothing to pre-warm.
- Exam pattern: "classic job clusters start too slowly, serverless is not an option" → **instance pool**.

Decision shortcuts the exam likes:
- "Need fast startup for an ad-hoc query by an analyst" → **Serverless SQL warehouse**.
- "Scheduled nightly ETL job, cost-sensitive" → **Job cluster** (or serverless jobs if available).
- "Notebook development, lots of iteration on one dataset" → **All-purpose cluster** (or serverless notebooks).
- "Lakeflow Declarative Pipeline" → defaults to **serverless** — recommended.
- "Many concurrent users running short SQL queries" → **Serverless SQL warehouse with autoscaling**.
- "Classic clusters must start faster; serverless not possible" → **Instance pool**.

# Delta Lake Basics

- **Optimizing techniques:**
    - Partitioning:
        - grouping data by column(s)
        - for each partition there is a separate .parquet file dir
        - only for low-cardinality columns (year, etc.)
        - `CREATE TABLE tbl_name (col1 TYPE, ...) PARTITIONED BY (col_name, ...)`
    - Z-Order Index:
        - grouping data by range of values (id) without creating subfolders
        - well for high-cardinality columns
        - `OPTIMIZE tbl_name ZORDER BY col_name`
        - adding new data requires recomputing Z-Order Index and recreating parquet files
    - Liquid Clustering:
        - improved, incremental successor to Z-Order. Files already clustered are skipped on subsequent OPTIMIZE runs.
        - **Explicit keys** — pick columns frequently used in `WHERE` / join predicates:
          `CREATE TABLE tbl_name CLUSTER BY (col_name, col_name, ...)`
        - **Automatic Liquid Clustering** (`CLUSTER BY AUTO`) — **requires Predictive Optimization**, which
          observes the table's query workload and selects clustering keys for you. UC managed tables only,
          DBR 15.4 LTS+:
          `CREATE TABLE tbl_name CLUSTER BY AUTO`
        - Change keys later with `ALTER TABLE tbl_name CLUSTER BY (...)` followed by `OPTIMIZE tbl_name FULL`
          to rewrite the layout.
- **Maintenance — `OPTIMIZE` vs `VACUUM`:**
    - `OPTIMIZE tbl_name`: compacts small files into larger ones (~1 GB target). The replaced files are only
      *tombstoned* (dereferenced in the Delta log), **not deleted** — history and time travel stay intact.
    - `VACUUM tbl_name [RETAIN n HOURS]`: **physically deletes** the data files of earlier table versions once
      they are older than the retention window (default 7 days). After a VACUUM, **time travel to those earlier
      versions is gone** — `VERSION AS OF` / `TIMESTAMP AS OF` beyond the retention fails with a
      file-not-found style error. The version *entries* may still appear in `DESCRIBE HISTORY`, but their data
      files no longer exist.
    - Exam heuristic: "time travel to last month suddenly fails" → `VACUUM`, never `OPTIMIZE`.
      Deep dive: `../week_5_cicd_and_troubleshooting/learn_troubleshooting.md` (VACUUM section).