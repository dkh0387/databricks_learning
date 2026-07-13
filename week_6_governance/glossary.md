# Week 6 Glossary — Governance and Security

| Term | Definition |
| --- | --- |
| **Unity Catalog (UC)** | Account-level governance for tables, files, ML models, AI assets across workspaces. |
| **Metastore** | Top-level UC container (one per region) holding catalogs, credentials, external locations, shares. |
| **Catalog** | First level of the UC three-level namespace. |
| **Schema** | Second level — holds tables, views, volumes, functions, models. |
| **Three-level namespace** | `catalog.schema.object` — replaces the HMS two-level convention. |
| **Securable** | Any UC object that can have grants: catalog, schema, table, view, volume, function, model, external location, storage credential, connection, share. |
| **Managed table** | UC owns metadata AND files. `DROP TABLE` removes data. Stored in catalog/schema managed location. |
| **External table** | UC owns metadata only; data at caller-given path. `DROP TABLE` leaves data. |
| **`SET MANAGED`** | DBR 17.3 LTS+ command converting an external Delta table to managed without downtime. |
| **`UNSET MANAGED`** | Rollback only — reverts a prior `SET MANAGED` (within 14 days) to the original external location; takes no location clause. |
| **Volume** | UC securable for non-tabular files. Path `/Volumes/<catalog>/<schema>/<volume>/…`. |
| **Storage credential** | UC-wrapped IAM role / managed identity used to access cloud storage. |
| **External location** | UC binding of a cloud path to a storage credential. |
| **Principal** | Entity that can be granted privileges — user, group, or service principal. |
| **User** | Email-identified human. |
| **Group** | Account-level group (preferred for grants over direct user grants). |
| **Service principal** | Non-human identity (CI/CD, jobs, apps). |
| **`USE CATALOG`** | Required to traverse into a catalog before any leaf privilege applies. |
| **`USE SCHEMA`** | Required to traverse into a schema. |
| **`SELECT`** | Read privilege on tables/views. |
| **`MODIFY`** | `INSERT` / `UPDATE` / `DELETE` / `MERGE` on a table. |
| **`CREATE TABLE` / `CREATE SCHEMA` / `CREATE VOLUME` / …** | Privileges to create child objects. |
| **`EXECUTE`** | Privilege to invoke a function or model. |
| **`READ FILES` / `WRITE FILES`** | Privileges on external locations only (volumes use `READ VOLUME` / `WRITE VOLUME`). |
| **`BROWSE`** | Metadata-discovery privilege — see object exists without traverse rights. |
| **`APPLY TAG`** | Privilege to attach governed tags. |
| **`ALL PRIVILEGES`** | All privileges on a securable. |
| **`GRANT` / `REVOKE`** | Standard SQL grant/revoke of UC privileges. |
| **`DENY`** | **Not supported in UC** (GRANT/REVOKE only) — exists only in legacy Hive-metastore table ACLs. Classic exam distractor. |
| **Ownership** | Securable owner has implicit management rights. Transfer with `ALTER … OWNER TO`. |
| **Row filter** | SQL UDF returning BOOLEAN — rows where false are dropped. Attached via `ALTER TABLE … SET ROW FILTER`. |
| **Column mask** | SQL UDF transforming a value at read time. Attached via `ALTER TABLE … ALTER COLUMN … SET MASK`. |
| **`IS_ACCOUNT_GROUP_MEMBER`** | Built-in function used inside filter/mask UDFs to gate by group. |
| **`current_user()`** | Built-in identifying the running principal. |
| **ABAC** | Attribute-Based Access Control — tag-driven policies that mass-apply row filters/column masks. |
| **Governed tag** | Account-level key or key:value vocabulary attached to catalogs/schemas/tables/columns. |
| **ABAC policy** | Tag-condition → principal → action (apply mask / filter). |
| **Lineage** | Auto-tracked upstream/downstream relationships, visible in Catalog Explorer; queryable in `system.access.table_lineage` / `column_lineage`. |
| **Audit log** | `system.access.audit` — every UC API call recorded. |
| **Delta Sharing** | Open protocol for read-only cross-org data sharing — no copy. |
| **Share** | UC securable representing an outbound Delta Sharing share. |
| **Recipient** | External party that receives a share. |
| **Provider** | External party that publishes a share you consume. |
| **D2D sharing** | Databricks-to-Databricks Delta Sharing — full UC integration on recipient side. |
| **Open sharing** | Open-protocol Delta Sharing — any client (pandas, Spark, BI tool). |
| **`information_schema`** | Standard SQL metadata views (e.g., `tables`, `table_privileges`) under the `system` catalog. |