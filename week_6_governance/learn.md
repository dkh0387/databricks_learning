# Data Governance with Unity Catalog

Covers exam **§7 — Governance and Security (15%)** plus the *Data Interoperability with Unity Catalog* and *Get Started with Data Governance on Databricks* Academy modules.

## Why Unity Catalog (UC)

Single governance layer across all workspaces in an account: tables, files, ML models, notebooks, dashboards, AI assets. Replaces the legacy per-workspace Hive Metastore. Centralises **access control, lineage, audit, discovery, and sharing**.

## Object hierarchy

```
Account
└── Metastore                (one per region, attached to N workspaces)
    ├── Catalog              (top of the 3-level namespace)
    │   └── Schema           (synonym: database)
    │       ├── Table        (managed | external)
    │       ├── View
    │       ├── Materialized View
    │       ├── Streaming Table
    │       ├── Volume       (managed | external — for non-tabular files)
    │       ├── Function     (SQL UDF, Python UDF)
    │       └── Model
    ├── Storage Credential   (IAM role / SP / managed identity)
    ├── External Location    (cloud path + credential)
    ├── Connection           (to external DBs / lakehouses)
    └── Share                (Delta Sharing outbound)
```

Three-level namespace: `catalog.schema.object`. Replaces the two-level `database.table` from Hive.

## Securable object types

`METASTORE`, `CATALOG`, `SCHEMA`, `TABLE`, `VIEW`, `MATERIALIZED VIEW`, `VOLUME`, `FUNCTION`, `MODEL`, `EXTERNAL LOCATION`, `STORAGE CREDENTIAL`, `CONNECTION`, `SHARE`, `PROVIDER`, `RECIPIENT`, `CLEAN ROOM`.

## Principals

- **Users** — email-identified humans.
- **Groups** — account-level groups (preferred for permissions — never grant to users directly in production).
- **Service Principals** — non-human identities for jobs, CI/CD, external apps.

## Managed vs. external tables

| | Managed | External |
| --- | --- | --- |
| Storage location | UC-managed default location (catalog or schema) | Caller-provided cloud path |
| Lifecycle | UC owns files; `DROP TABLE` deletes data | UC owns metadata only; `DROP TABLE` keeps data |
| Format | Delta or Iceberg | Delta, Iceberg, Parquet, CSV, JSON, etc. |
| Predictive Optimization | Auto | Manual |
| Best for | All new tables, default choice | Reading existing data, sharing with non-Databricks engines |

### Creating

```sql
-- Managed (no LOCATION)
CREATE TABLE dea_learning.silver.silver_orders (id INT, total DOUBLE);

-- External (LOCATION required)
CREATE TABLE dea_learning.silver.orders_archive_ext (id INT, total DOUBLE)
USING DELTA
LOCATION 's3://my-bucket/sales/orders/';
```

### Convert between types (DBR 17.0+ or serverless, Delta only)

```sql
-- external -> managed (recommended over CTAS/DEEP CLONE: no downtime, keeps history, perms, name)
ALTER TABLE dea_learning.silver.orders_archive_ext SET MANAGED;

-- rollback: no location clause — only reverts a prior SET MANAGED (within 14 days)
-- back to the original external location
ALTER TABLE dea_learning.silver.orders_archive_ext UNSET MANAGED;
```

Why `SET MANAGED` beats DEEP CLONE for migrations
([official docs](https://docs.databricks.com/aws/en/tables/convert-external-managed)): it converts
**in place** with a two-phase process —

1. **Copy phase, no downtime:** table data + Delta transaction log are copied to the managed location
   while "active readers and writers to the external table run without interruption".
2. **Switch phase, brief writer downtime:** commits that landed on the external location during the
   copy are **caught up** and moved over, then the metadata switches to the managed location. Writes
   are blocked only during this step (~1–5 min even for 10 TB tables). Readers on **DBR 16.4 LTS+**
   experience no downtime at all.

Name/settings/permissions/views/history are retained throughout. A DEEP CLONE creates a *separate*
table: cutover needed (re-point consumers, re-grant, re-create views) and writes landing on the source
during the copy require manual re-syncs — exactly what the two-phase catch-up automates.

Note: a dropped UC **managed** table is not immediately lost — `UNDROP TABLE` recovers it within 7 days (underlying files are cleaned up after ~30 days).

## Storage credentials and external locations

Required to read/write external data through UC.

```sql
-- 1. Storage credential (admin only) wraps an IAM role / managed identity.
-- There is no CREATE STORAGE CREDENTIAL SQL — create it via Catalog Explorer,
-- the REST API, the Databricks CLI, or Terraform.

-- 2. External location binds a path to a credential
CREATE EXTERNAL LOCATION sales_landing
  URL 's3://my-bucket/sales/'
  WITH (STORAGE CREDENTIAL prod_s3_cred);

-- 3. Grant access to use it
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION sales_landing TO data_engineers;
```

## Volumes

Non-tabular file storage governed by UC. Use for ingestion landing zones, ML artifacts, libraries.

```sql
CREATE VOLUME dea_learning.raw.landing;                 -- managed
CREATE EXTERNAL VOLUME dea_learning.raw.legacy          -- external
  LOCATION 's3://my-bucket/legacy/';
```

Access via path: `/Volumes/dea_learning/raw/landing/file.csv`.

## Privilege model

### Core privileges

| Privilege | Applies to | Allows |
| --- | --- | --- |
| `USE CATALOG` | Catalog | Traverse into the catalog (required to see anything inside) |
| `USE SCHEMA` | Schema | Traverse into the schema |
| `SELECT` | Table, View | Read |
| `MODIFY` | Table | INSERT / UPDATE / DELETE / MERGE |
| `CREATE TABLE` / `CREATE SCHEMA` / `CREATE VOLUME` / `CREATE FUNCTION` / `CREATE MATERIALIZED VIEW` | Schema / Catalog | Create child object |
| `EXECUTE` | Function, Model | Invoke |
| `READ VOLUME` / `WRITE VOLUME` | Volume | Read / write files |
| `READ FILES` / `WRITE FILES` | External location | Read / write raw paths |
| `BROWSE` | Catalog, Schema | See in UI without `USE` privilege (metadata discovery) |
| `APPLY TAG` | Any securable | Add governed tags |
| `ALL PRIVILEGES` | Any | All of the above |

### Traversal rule

To read `dea_learning.silver.silver_orders` a user needs:
`USE CATALOG` on `dea_learning` **and** `USE SCHEMA` on `dea_learning.silver` **and** `SELECT` on the table.
Privileges granted on a parent **inherit downward** — `SELECT` on a schema applies to all current *and future* tables in it — but the `USE CATALOG` / `USE SCHEMA` traversal privileges are still required to reach the object.

### Privileges for `SELECT * FROM read_files('<path>')`

`read_files()` reads **raw files by path**, not a table — so `SELECT` (a table/view privilege) plays no role.
Which privileges apply depends on *where the path lives*:

| Path | Required privileges |
| --- | --- |
| UC volume: `/Volumes/dea_learning/raw/landing/...` | `USE CATALOG` on `dea_learning` + `USE SCHEMA` on `raw` + **`READ VOLUME`** on the volume |
| External location: `s3://corp-data/sales/...` | **`READ FILES`** on the external location covering the path |

```sql
-- Files in a volume — traversal + READ VOLUME
GRANT USE CATALOG  ON CATALOG dea_learning     TO `analysts`;
GRANT USE SCHEMA   ON SCHEMA  dea_learning.raw TO `analysts`;
GRANT READ VOLUME  ON VOLUME  dea_learning.raw.landing TO `analysts`;
SELECT * FROM read_files('/Volumes/dea_learning/raw/landing/orders/', format => 'csv');

-- Files under an external location — no USE CATALOG / USE SCHEMA needed:
-- external locations are metastore-level securables, they don't live inside a catalog/schema
GRANT READ FILES ON EXTERNAL LOCATION sales_landing TO `analysts`;
SELECT * FROM read_files('s3://corp-data/sales/2026/', format => 'json');
```

Same pairing for writes: `WRITE VOLUME` on volumes, `WRITE FILES` on external locations.
Exam pattern: "user has `SELECT` on every table but `read_files` on the volume fails" → the missing
privilege is `READ VOLUME` (files ≠ tables; each securable has its own read privilege).

### `GRANT` / `REVOKE`

```sql
GRANT USE CATALOG ON CATALOG dea_learning TO `analysts`;
GRANT USE SCHEMA  ON SCHEMA dea_learning.silver TO `analysts`;
GRANT SELECT       ON TABLE  dea_learning.silver.silver_orders TO `analysts`;

GRANT MODIFY ON TABLE dea_learning.silver.silver_orders TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA dea_learning.silver TO `data_engineers`;

REVOKE SELECT ON TABLE dea_learning.silver.silver_orders FROM `analysts`;

-- UC does NOT support DENY — that is legacy Hive metastore table ACLs.
-- To restrict access, REVOKE or simply don't grant (classic exam distractor).

SHOW GRANTS ON TABLE dea_learning.silver.silver_orders;
SHOW GRANTS `analysts` ON CATALOG dea_learning;   -- grants held by analysts inside this catalog

-- For "every grant a principal holds across the metastore", query the privilege views:
-- SELECT * FROM system.information_schema.table_privileges WHERE grantee = 'analysts';
```

Ownership transfers the management right (cannot be revoked):

```sql
ALTER TABLE dea_learning.silver.silver_orders OWNER TO `data_platform_admins`;
```

## Row filters and column masks (manual, per-table)

Both are SQL UDFs attached to a table. The examples below use week 3's plain Delta table `dea_learning.silver.customers_silver`. On pipeline-managed streaming tables / materialized views (like week 4's `silver_customers`), filters and masks must be declared in the pipeline definition (`WITH ROW FILTER`), not via `ALTER TABLE`.

### Row filter — drops rows based on UDF returning `FALSE`

```sql
CREATE FUNCTION dea_learning.sec.region_filter(region STRING)
RETURNS BOOLEAN
RETURN
  is_account_group_member('admins')
  OR (is_account_group_member('eu_team') AND region = 'EU')
  OR (is_account_group_member('us_team') AND region = 'US');

ALTER TABLE dea_learning.silver.customers_silver
  SET ROW FILTER dea_learning.sec.region_filter ON (region);

-- Remove
ALTER TABLE dea_learning.silver.customers_silver DROP ROW FILTER;
```

### Column mask — transforms a value at read time

```sql
CREATE FUNCTION dea_learning.sec.mask_email(email STRING)
RETURNS STRING
RETURN
  CASE WHEN is_account_group_member('pii_readers') THEN email
       ELSE regexp_replace(email, '(^.)(.*)(@.*$)', '$1***$3') END;

ALTER TABLE dea_learning.silver.customers_silver
  ALTER COLUMN email SET MASK dea_learning.sec.mask_email;

ALTER TABLE dea_learning.silver.customers_silver
  ALTER COLUMN email DROP MASK;
```

Built-in helpers: `current_user()`, `is_account_group_member(group)`, `session_user()`.

### Dynamic views

Row/column security without filters or masks: put `is_member()` / `is_account_group_member()` directly in a view definition, then grant on the view only.

```sql
CREATE OR REPLACE VIEW dea_learning.gold.customers_secure AS
SELECT customer_id,
       CASE WHEN is_account_group_member('pii_readers') THEN email ELSE '****' END AS email,
       region
FROM   dea_learning.silver.customers_silver
WHERE  is_account_group_member('admins') OR region <> 'EU';
```

## ABAC — Attribute-Based Access Control (GA 2026)

Apply one policy across many tables/columns by **tagging** them instead of altering each table individually. Recommended over manual row filters / column masks at scale.

### Governed tags

Account-level vocabulary of `key` or `key:value` pairs. Attach to catalogs, schemas, tables, columns.

```sql
-- (APPLY TAG is the *privilege* name; the DDL is ALTER ... SET TAGS)
ALTER TABLE dea_learning.silver.customers_silver
  ALTER COLUMN email SET TAGS ('pii' = 'true');
ALTER TABLE dea_learning.silver.customers_silver
  SET TAGS ('classification' = 'restricted');
```

### Policy components

1. **Tag condition** — what objects the policy targets (e.g. `column has tag pii=true`).
2. **Principal** — who is exempt / restricted.
3. **Action** — column mask UDF or row filter UDF to apply.

### Policies in SQL — using the tags set above

`CREATE POLICY` closes the loop: the tag from the previous section is what the policy matches on.

```sql
-- Column mask: every column tagged pii=true in the schema gets masked,
-- for everyone except pii_readers. New tagged columns are covered automatically.
CREATE OR REPLACE POLICY mask_pii
ON SCHEMA dea_learning.silver
COMMENT 'Mask every pii-tagged column for non-privileged users'
COLUMN MASK dea_learning.sec.mask_email
TO `account users` EXCEPT `pii_readers`
FOR TABLES
MATCH COLUMNS has_tag_value('pii', 'true') AS pii_col
ON COLUMN pii_col;

-- Row filter: applies the filter UDF to tables tagged as restricted,
-- binding the UDF argument to whichever column carries the region tag.
CREATE OR REPLACE POLICY hide_regions
ON SCHEMA dea_learning.silver
COMMENT 'Region-filter all restricted tables'
ROW FILTER dea_learning.sec.region_filter
TO `account users` EXCEPT `admins`
FOR TABLES
WHEN has_tag_value('classification', 'restricted')
MATCH COLUMNS has_tag('region_col') AS r
USING COLUMNS (r);
```

- `MATCH COLUMNS <tag condition> AS <alias>` finds the target column(s) by tag; `ON COLUMN` (mask) points at the alias, `USING COLUMNS` passes it as the UDF argument.
- `WHEN` optionally gates the whole policy on a table-level tag.
- `TO ... EXCEPT ...` does the audience gating: for exempt principals the policy does not apply and the UDF is
  never invoked. A simple on/off mask UDF therefore shrinks to a pure transformation (value in, masked value
  out — no `is_account_group_member()`). Group checks *inside* the UDF remain only when the logic itself varies
  by group (e.g. `region_filter`: which team sees which region is per-row logic that `TO`/`EXCEPT` cannot express).
- Alternative without SQL: UC UI under *Catalog Explorer → Policies*, or the Policies API.

Once created, every existing and future column with `pii=true` automatically gets the mask — no per-table `ALTER` needed.

### Mental model

- **Dynamic view** → `is_member()` / `is_account_group_member()` logic written *inside the view definition*;
  grant on the view, base table stays locked down. Recognize it by `CASE WHEN is_member(...)` in a `CREATE VIEW`.
- **Manual** row filters / column masks → UDF attached *to the table itself* (`SET ROW FILTER` / `SET MASK`);
  use for one-off tables, no extra view object.
- **ABAC** → central policies matching on **governed tags** — use for "all PII columns across the lakehouse"
  type rules; no per-table or per-view configuration.

## Audit logs

System catalog (always on):

```sql
SELECT * FROM system.access.audit
WHERE action_name = 'getTable'
  AND request_params.full_name_arg = 'dea_learning.silver.silver_orders'
ORDER BY event_time DESC;
```

Key tables in `system.access`: `audit`, `table_lineage`, `column_lineage`.

## Data lineage

Automatic for any query run on UC tables via a SQL warehouse or notebook on **DBR 11.3 LTS+**.

- UI: open table in *Catalog Explorer* → *Lineage* tab → upstream / downstream graph at table and column level.
- API: `system.access.table_lineage`, `system.access.column_lineage`.
- Captured from **actual query runs** — a pipeline that has never executed produces no lineage. Covers
  tables/views, notebooks, jobs, and pipelines as lineage nodes.

### What lineage does NOT capture (exam nuances)

- Queries on **non-UC compute** (old DBR, legacy Hive-metastore-only clusters) — nothing is recorded.
- Reads/writes **by storage path** (`spark.read.load('s3://...')`) — only accesses via **table names** are
  tracked; path-based access bypasses the catalog and therefore the lineage graph.
- Column lineage is lost when a column is produced in a way the parser can't attribute (e.g., through a UDF
  whose internals are opaque).
- Lineage entries are **retained for 1 year** — older history ages out.
- Visibility is permission-filtered: users only see lineage nodes for objects they have privileges on;
  other nodes are masked.

## Delta Sharing

Open protocol for read-only data sharing across orgs, clouds, regions — no data copy.

```sql
-- Outbound (provider side creates share container and to whom to be shared)
CREATE SHARE finance_share;
ALTER SHARE finance_share ADD TABLE  dea_learning.silver.silver_orders;
ALTER SHARE finance_share ADD SCHEMA dea_learning.gold;   -- schema-level: future objects too
-- Views and materialized views have dedicated clauses:
-- ALTER SHARE finance_share ADD VIEW dea_learning.gold.eu_daily_revenue;
-- ALTER SHARE finance_share ADD MATERIALIZED VIEW dea_learning.gold.gold_daily_revenue;
CREATE RECIPIENT partner_acme USING ID 'azure:eastus:abc-123';
GRANT SELECT ON SHARE finance_share TO RECIPIENT partner_acme;

-- Inbound (recipient side, if recipient is also UC, creates from who share is coming)
CREATE PROVIDER acme USING JSON '<credentials>';
CREATE CATALOG acme_data USING SHARE acme.finance_share;
```

Two modes:
- **Databricks-to-Databricks** — recipient is UC, gets full feature set (notebooks, dashboards, AI/BI).
- **Open sharing** — recipient uses any tool implementing the open Delta Sharing protocol (pandas, Spark, BI tool).

Advantages: no ETL, governed, audit-logged, live data.
Limits: read-only, Delta/Parquet only; tables with row filters or column masks **cannot be added to a share** at all — share a view that applies the same logic instead.

## Common SQL drills

```sql
-- Inspect
DESCRIBE TABLE EXTENDED dea_learning.silver.silver_orders;     -- type, location, owner
DESCRIBE DETAIL  dea_learning.silver.silver_orders;             -- format, num files, size
SHOW GRANTS ON TABLE dea_learning.silver.silver_orders;
SHOW TABLES IN dea_learning.silver;

-- Discover via system tables
SELECT * FROM system.information_schema.tables WHERE table_catalog = 'dea_learning';
SELECT * FROM system.information_schema.table_privileges;
```

## Hive Metastore → UC migration cheat sheet

- Use `UCX` toolkit (Databricks-provided) for bulk migration.
- Tables in `hive_metastore.<db>.<tbl>` → re-create as managed UC tables, ideally Delta.
- `CONVERT TO DELTA` for parquet tables before migrating.
- Workspace-local groups → upgrade to account-level groups.

## Exam-day quick-reference

- Three-level namespace: `catalog.schema.object`. Hive's two-level is **out**.
- Traversal: need `USE CATALOG` + `USE SCHEMA` + leaf privilege (`SELECT`/`MODIFY`/…).
- UC has **no `DENY`** — GRANT/REVOKE only. `DENY` exists only in legacy HMS table ACLs (exam trap).
- Managed table dropped → data gone (but `UNDROP TABLE` recovers it within 7 days). External dropped → data stays.
- `SET MANAGED` converts external → managed without downtime (DBR 17.0+). `UNSET MANAGED` (no location clause) rolls it back within 14 days.
- Row filter = UDF returns `BOOLEAN`. Column mask = UDF returns same/castable type.
- Manual filters/masks = per table. ABAC = tag once, applies everywhere.
- `is_account_group_member('grp')` is the canonical check inside filter/mask UDFs.
- Audit, lineage, table_lineage, column_lineage all live in `system.access.*`.
- Delta Sharing is read-only, supports open and D2D modes.

## References

- [UC overview](https://docs.databricks.com/aws/en/data-governance/unity-catalog/)
- [Managed vs external assets](https://docs.databricks.com/aws/en/data-governance/unity-catalog/managed-versus-external)
- [Convert external → managed](https://docs.databricks.com/aws/en/tables/convert-external-managed)
- [Row filters & column masks](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks/)
- [ABAC core concepts](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/core-concepts)
- [ABAC policy management](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies)
- [Delta Sharing](https://docs.databricks.com/aws/en/delta-sharing/)
- [Audit log reference](https://docs.databricks.com/aws/en/admin/account-settings/audit-logs)
- [System tables](https://docs.databricks.com/aws/en/admin/system-tables/)