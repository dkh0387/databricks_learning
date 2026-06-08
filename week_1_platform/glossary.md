# Week 1 Glossary — Platform Fundamentals

| Term | Definition |
| --- | --- |
| **Lakehouse** | Architecture combining the openness of a data lake with the management and ACID guarantees of a warehouse — sits on cloud object storage. |
| **Delta Lake** | Open source storage layer that adds ACID transactions, schema evolution, time travel, and versioning on top of Parquet. |
| **Delta table** | Table stored in Delta Lake format (Parquet data files + `_delta_log/` JSON transaction log). |
| **Delta log** | Ordered JSON records of every change to a Delta table — enables time travel and concurrent reads/writes. |
| **Parquet** | Columnar file format Delta tables use under the hood. |
| **Unity Catalog (UC)** | Account-level governance layer for tables, files, ML models, AI assets. Default for new workspaces. |
| **Hive Metastore (HMS)** | Legacy workspace-scoped metastore. Still available as the `hive_metastore` catalog for backwards compatibility, but not recommended for new content — use Unity Catalog. |
| **Catalog** | Top level of the UC three-level namespace (`catalog.schema.object`). |
| **Schema (database)** | Container of tables/views/functions/volumes inside a catalog. SQL keywords `SCHEMA` and `DATABASE` are aliases. |
| **Three-level namespace** | `catalog.schema.object` — replaces the HMS two-level `database.table`. |
| **Managed table** | Metastore owns metadata AND files. `DROP TABLE` deletes the data. |
| **External table** | Metastore owns metadata only; files live at a caller-given path. `DROP TABLE` keeps the data. |
| **Volume** | UC securable for non-tabular files (managed or external). |
| **DBFS** | Databricks File System abstraction. The **DBFS root** (`dbfs:/`) is deprecated for new content — use UC Volumes (`/Volumes/...`). DBFS mounts to external storage still work but are also superseded by UC external locations. |
| **DBR** | Databricks Runtime — the Spark/Photon version on a cluster. |
| **DBU** | Databricks Unit — the billing unit for compute (rate varies by workload). |
| **All-purpose cluster** | Interactive cluster for ad-hoc work; higher DBU rate. |
| **Job cluster** | Ephemeral cluster created per job run; lower Jobs Compute DBU rate. |
| **SQL warehouse** | Compute for SQL/BI workloads; serverless option available. |
| **Serverless compute** | Fully managed compute — no cluster startup, infra cost bundled into DBU. |
| **Photon** | Vectorized C++ engine; toggle at cluster level for SQL/DataFrame speedups. Free on serverless. |
| **CTAS** | `CREATE TABLE AS SELECT` — create + populate in one statement. |
| **Deep clone** | Full data copy of a Delta table (slow, independent). |
| **Shallow clone** | Metadata-only clone that references source files (fast, dependent on source lifecycle). |
| **Time travel** | Querying a Delta table at a prior version (`VERSION AS OF n`) or timestamp (`TIMESTAMP AS OF '…'`). |
| **OPTIMIZE** | Compacts small Delta files into larger ones (default ~1 GB target). |
| **Z-Order** | Multi-dim file layout on high-cardinality cols. Legacy — Liquid Clustering replaces it. |
| **Liquid Clustering** | Incremental, adaptive clustering (`CLUSTER BY (cols)` or `CLUSTER BY AUTO`). |
| **Predictive Optimization** | Auto runs `OPTIMIZE`, `VACUUM`, `ANALYZE` on UC managed Delta tables on serverless. |
| **NOT NULL / CHECK** | Delta table constraints enforced on write. |
| **View** | SQL query stored in the catalog — no data, recomputed each query. |
| **Temporary view** | Bound to the current Spark session; gone when session ends. |
| **Global temporary view** | Bound to a cluster across sessions; stored in `global_temp`. Not on serverless. |