# Data Governance with Unity Catalog

Covers exam **§7 — Governance and Security (15%)** plus the *Data Interoperability with Unity Catalog* and *Get Started with Data Governance on Databricks* Academy modules.

## Why Unity Catalog (UC)

Single governance layer across all workspaces in an account: tables, files, ML models, notebooks, dashboards, AI assets. Replaces the legacy per-workspace Hive Metastore. Centralises **access control, lineage, audit, discovery, and sharing**.

## Object hierarchy

```
Account
└── Metastore                (one per region, attached to N workspaces)
    ├── Catalog              (top of the 3-level namespace)
    │   └── Schema           (= database)
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
CREATE TABLE main.sales.orders (id INT, total DOUBLE);

-- External (LOCATION required)
CREATE TABLE main.sales.orders_ext (id INT, total DOUBLE)
USING DELTA
LOCATION 's3://my-bucket/sales/orders/';
```

### Convert between types (DBR 17.0+ or serverless, Delta only)

```sql
-- external -> managed (recommended over CTAS: no downtime, keeps history, perms, name)
ALTER TABLE main.sales.orders_ext SET MANAGED;

-- managed -> external (rollback)
ALTER TABLE main.sales.orders UNSET MANAGED LOCATION 's3://bucket/path/';
```

## Storage credentials and external locations

Required to read/write external data through UC.

```sql
-- 1. Storage credential (admin only) wraps an IAM role / managed identity
CREATE STORAGE CREDENTIAL prod_s3_cred
  WITH (AWS_ROLE = 'arn:aws:iam::123456789012:role/databricks-uc');

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
CREATE VOLUME main.raw.landing;                 -- managed
CREATE EXTERNAL VOLUME main.raw.legacy          -- external
  LOCATION 's3://my-bucket/legacy/';
```

Access via path: `/Volumes/main/raw/landing/file.csv`.

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

To read `main.sales.orders` a user needs:
`USE CATALOG` on `main` **and** `USE SCHEMA` on `main.sales` **and** `SELECT` on the table.
Grants on the parent **do not cascade** by default — you must grant at the correct level (or use ABAC, see below).

### `GRANT` / `REVOKE` / `DENY`

```sql
GRANT USE CATALOG ON CATALOG main TO `analysts`;
GRANT USE SCHEMA  ON SCHEMA main.sales TO `analysts`;
GRANT SELECT       ON TABLE  main.sales.orders TO `analysts`;

GRANT MODIFY ON TABLE main.sales.orders TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA main.sales TO `data_engineers`;

REVOKE SELECT ON TABLE main.sales.orders FROM `analysts`;

-- DENY overrides any GRANT, including inherited ones via groups
DENY SELECT ON TABLE main.sales.orders TO `contractors`;

SHOW GRANTS ON TABLE main.sales.orders;
SHOW GRANTS `analysts`;                        -- everything granted to a principal
```

Ownership transfers the management right (cannot be revoked):

```sql
ALTER TABLE main.sales.orders OWNER TO `data_platform_admins`;
```

## Row filters and column masks (manual, per-table)

Both are SQL UDFs attached to a table.

### Row filter — drops rows based on UDF returning `FALSE`

```sql
CREATE FUNCTION main.sec.region_filter(region STRING)
RETURNS BOOLEAN
RETURN
  IS_ACCOUNT_GROUP_MEMBER('eu_team') AND region = 'EU'
  OR IS_ACCOUNT_GROUP_MEMBER('us_team') AND region = 'US'
  OR IS_ACCOUNT_GROUP_MEMBER('admins');

ALTER TABLE main.sales.orders
  SET ROW FILTER main.sec.region_filter ON (region);

-- Remove
ALTER TABLE main.sales.orders DROP ROW FILTER;
```

### Column mask — transforms a value at read time

```sql
CREATE FUNCTION main.sec.mask_email(email STRING)
RETURNS STRING
RETURN
  CASE WHEN IS_ACCOUNT_GROUP_MEMBER('pii_readers') THEN email
       ELSE regexp_replace(email, '(^.)(.*)(@.*$)', '$1***$3') END;

ALTER TABLE main.users.profiles
  ALTER COLUMN email SET MASK main.sec.mask_email;

ALTER TABLE main.users.profiles
  ALTER COLUMN email DROP MASK;
```

Built-in helpers: `current_user()`, `is_account_group_member(group)`, `session_user()`.

## ABAC — Attribute-Based Access Control (GA 2026)

Apply one policy across many tables/columns by **tagging** them instead of altering each table individually. Recommended over manual row filters / column masks at scale.

### Governed tags

Account-level vocabulary of `key` or `key:value` pairs. Attach to catalogs, schemas, tables, columns.

```sql
APPLY TAG ('pii' = 'true')        ON COLUMN main.users.profiles.email;
APPLY TAG ('classification' = 'restricted') ON TABLE main.sales.contracts;
```

### Policy components

1. **Tag condition** — what objects the policy targets (e.g. `column has tag pii=true`).
2. **Principal** — who is exempt / restricted.
3. **Action** — column mask UDF or row filter UDF to apply.

Created in the UC UI under *Catalog Explorer → Policies* or via Policies API. Once created, every existing and future column with `pii=true` automatically gets the mask — no per-table `ALTER` needed.

### Mental model

- **Manual** filters/masks → use for one-off tables.
- **ABAC** → use for "all PII columns across the lakehouse" type rules.

## Audit logs

System catalog (always on):

```sql
SELECT * FROM system.access.audit
WHERE action_name = 'getTable'
  AND request_params.full_name_arg = 'main.sales.orders'
ORDER BY event_time DESC;
```

Key tables in `system.access`: `audit`, `table_lineage`, `column_lineage`.

## Data lineage

Automatic for any query run on UC tables via a SQL warehouse or notebook (Spark 3.5+).

- UI: open table in *Catalog Explorer* → *Lineage* tab → upstream / downstream graph at table and column level.
- API: `system.access.table_lineage`, `system.access.column_lineage`.

## Delta Sharing

Open protocol for read-only data sharing across orgs, clouds, regions — no data copy.

```sql
-- Outbound (provider side)
CREATE SHARE finance_share;
ALTER SHARE finance_share ADD TABLE main.sales.orders;
ALTER SHARE finance_share ADD SCHEMA main.public;
CREATE RECIPIENT partner_acme USING ID 'azure:eastus:abc-123';
GRANT SELECT ON SHARE finance_share TO RECIPIENT partner_acme;

-- Inbound (recipient side, if recipient is also UC)
CREATE PROVIDER acme USING JSON '<credentials>';
CREATE CATALOG acme_data USING SHARE acme.finance_share;
```

Two modes:
- **Databricks-to-Databricks** — recipient is UC, gets full feature set (notebooks, dashboards, AI/BI).
- **Open sharing** — recipient uses any tool implementing the open Delta Sharing protocol (pandas, Spark, BI tool).

Advantages: no ETL, governed, audit-logged, live data.
Limits: read-only, Delta/Parquet only, no row-filter/column-mask enforcement on shared tables (apply at source view).

## Common SQL drills

```sql
-- Inspect
DESCRIBE TABLE EXTENDED main.sales.orders;     -- type, location, owner
DESCRIBE DETAIL  main.sales.orders;             -- format, num files, size
SHOW GRANTS ON TABLE main.sales.orders;
SHOW TABLES IN main.sales;

-- Discover via system tables
SELECT * FROM system.information_schema.tables WHERE table_catalog = 'main';
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
- `DENY` beats `GRANT`. Group membership inherits, but `DENY` to a member overrides.
- Managed table dropped → data gone. External dropped → data stays.
- `SET MANAGED` converts external → managed without downtime (DBR 17+).
- Row filter = UDF returns `BOOLEAN`. Column mask = UDF returns same/castable type.
- Manual filters/masks = per table. ABAC = tag once, applies everywhere.
- `IS_ACCOUNT_GROUP_MEMBER('grp')` is the canonical check inside filter/mask UDFs.
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