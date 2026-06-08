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
    - `DESCRIBE DETAIL <tbl_name>` shows the actual physical location for any table.
- **Tables:** *managed vs. external* is orthogonal to *Delta vs. other formats*.
    - **Managed table** — metastore owns both metadata and underlying files. `DROP TABLE` deletes the files.
      `CREATE TABLE tbl_name (col_name data_type, ...)` (no `LOCATION`).
    - **External table** — metastore owns metadata only; files live at a caller-provided path.
      `DROP TABLE` does **not** delete the files.
      `CREATE TABLE tbl_name (...) USING DELTA LOCATION '<path>'`.
- **Delta Tables:** any table (managed or external) stored in the Delta Lake format. Adds ACID transactions, time
  travel, schema evolution, etc.
    - Creating:
        - CTAS: `CREATE TABLE tbl_name AS SELECT ... FROM ...`
    - Cloning:
        - DEEP CLONE (full data copy, slow): `CREATE TABLE tbl_clone_name DEEP CLONE source_tbl_name`
        - SHALLOW CLONE (metadata-only copy, fast, references source files):
          `CREATE TABLE tbl_clone_name SHALLOW CLONE source_tbl_name`
- **Table Constraints:**
    - NOT NULL: column cannot contain null values
    - CHECK: column value must satisfy a given condition
    - `ALTER TABLE tbl_name ADD CONSTRAINT constraint_name CHECK (constraint_details)`
- **Views:**
    - Stored View: SQL query stored in a database (no physical data stored)
      `CREATE VIEW view_name AS SELECT ... FROM ...`
    - Temporary View: attached to a Spark session (open new notebook, start job run, etc.)
      `CREATE TEMP VIEW view_name AS SELECT ... FROM ...`
    - Global Temporary View: attached to a cluster within multiple sessions (restart cluster, etc.).
      Those views are stored in `global_temp` database. Cannot be used on serverless clusters.
      `CREATE GLOBAL TEMP VIEW view_name AS SELECT ... FROM ...`

# Delta Lake Basics

- **Optimizing techniques:**
    - Partitioning:
        - grouping data by column(s)
        - for each partition there is a separate .parquet file dir
        - only for low-cardinality columns (year, etc.)
        - `CREATE TABLE tbl_name PARTITIONED BY (col_name, col_name, ...)`
    - Z-Order Index:
        - grouping data by range of values (id) without creating subfolders
        - well for high-cardinality columns
        - `OPTIMIZE tbl_name ZORDER BY col_name`
        - adding new data requires recomputing Z-Order Index and recreating parquet files
    - Liquid Clustering:
        - improved version of Z-Order Index
        - Incremental optimization: files already clustered are being ignored
        - Hint: use frequently used columns in WHERE for clustering; otherwise use automatic clustering ("Predictive
          Optimization" on Unity Catalog is ON): `CREATE TABLE tbl_name CLUSTER BY AUTO`
        - `CREATE TABLE tbl_name CLUSTER BY (col_name, col_name, ...)`