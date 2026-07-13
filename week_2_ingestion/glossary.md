# Week 2 Glossary — Data Ingestion

| Term | Definition |
| --- | --- |
| **Lakeflow Connect** | Databricks' suite of connectors for ingesting from cloud storage, databases, SaaS, message buses, files. |
| **Standard connector** | Lakeflow Connect connector for cloud storage / Kafka / Kinesis / Event Hubs. |
| **Managed connector** | Lakeflow Connect connector for enterprise SaaS / DBs (Salesforce, Workday, SQL Server CDC, etc.). |
| **Partner connector** | Third-party connector surfaced via Partner Connect when no native option exists. |
| **Medallion architecture** | Bronze (raw) → Silver (cleaned) → Gold (business-ready) progression. |
| **Bronze** | Raw, append-only ingest layer — "what arrived". |
| **Silver** | Cleaned, conformed, deduplicated layer — "what is true". |
| **Gold** | Business / BI / ML feature-ready aggregates — "what's useful". |
| **`read_files()`** | Table-valued SQL function that reads files from a cloud path. Used by `CREATE TABLE AS …`. |
| **`COPY INTO`** | Idempotent, incremental SQL command for loading files into an existing Delta table. |
| **`FILEFORMAT`** | `COPY INTO` clause specifying source format (PARQUET, JSON, CSV, AVRO, etc.). |
| **`FORMAT_OPTIONS`** | Per-format reader options inside `COPY INTO` (e.g., `header`, `inferSchema`). |
| **`COPY_OPTIONS`** | `COPY INTO` behavior knobs — `mergeSchema`, `force`. |
| **Auto Loader** | Streaming/incremental file-ingest source identified by `format("cloudFiles")`. Scales to billions of files. |
| **`cloudFiles.schemaLocation`** | Durable path Auto Loader uses to version the inferred schema. |
| **`checkpointLocation`** | Durable path Spark Structured Streaming uses to track committed offsets. May be the same directory as `schemaLocation` (Auto Loader writes schemas to a `_schemas/` subdir). |
| **Directory listing mode** | Auto Loader default — lists the source directory each microbatch. Simple; fine into the thousands-to-tens-of-thousands of files (switch to file notification when counts reach the millions or listing gets slow/expensive). |
| **File notification mode** | Auto Loader scaled mode — uses cloud event service (SNS/SQS, Event Grid, Pub/Sub) to discover new files. |
| **Schema inference** | Auto Loader sampling the first 50 GB or 1,000 files to infer schema. JSON/CSV/XML default to STRING types. |
| **Schema hints** | `cloudFiles.schemaHints` override inferred types per column. |
| **`addNewColumns`** | Default evolution mode. New column → stream throws `UnknownFieldException` once, schema updated, restart picks up. |
| **`addNewColumnsWithTypeWidening`** | Like `addNewColumns` plus widens compatible types (INT→BIGINT); incompatible diffs go to `_rescued_data`. |
| **`rescue`** | Evolution mode that never updates schema — unmatched fields land in `_rescued_data`. |
| **`failOnNewColumns`** | Strict mode — stream fails permanently on new column until schema is manually updated. |
| **`none`** | Schema-supplied mode — ignore new columns silently. |
| **`_rescued_data`** | JSON STRING column holding fields/values that didn't fit the schema (missing col, type/case mismatch). |
| **`_metadata`** | Hidden struct on file sources exposing `file_path`, `file_name`, `file_size`, `file_modification_time`, … |
| **`availableNow=True`** | Spark stream trigger that processes all pending data once and stops. Used for incremental batch. |
| **`processingTime="N"`** | Periodic micro-batch trigger (e.g. every 1 hour). |
| **Streaming table** | UC-governed append-only physical Delta table fed by a stream; tracked by a checkpoint. |
| **`CREATE OR REFRESH STREAMING TABLE`** | SQL form to define a streaming table inside a Spark Declarative Pipeline. |
| **`STREAM read_files()`** | SQL streaming source — wraps Auto Loader. |
| **Ingestion gateway** | Lakeflow Connect component that holds source credentials and runs near the source network. |
| **Zerobus** | Direct event-write API for high-throughput ingestion straight into the lakehouse. |
| **VARIANT** | DBR 15.3+ semi-structured type — schema-less JSON storage, query with `:` operator. |
| **CDC** | Change Data Capture — propagating inserts/updates/deletes from source DB to target. |
| **SCD Type 1 / Type 2** | Slowly Changing Dimension patterns — overwrite vs preserve history with validity periods. |
| **Lakehouse Federation** | Query external data sources (other DBs, lakehouses) without copying data into Databricks. Useful for POC / virtualization. |
| **Delta Sharing** | Open protocol for read-only data sharing across orgs, clouds, regions, without copy. Covered in Week 6. |
| **Databricks Marketplace** | Open exchange for data products: datasets, notebooks, ML models, dashboards. |