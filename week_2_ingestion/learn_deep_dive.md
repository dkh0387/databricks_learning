# Data Ingestion — Deep Dive

Supplements `learn_lakeflow_connect.md` with the exam-specific edges of **§2 (21%)**: Auto Loader internals, `COPY INTO` mechanics, JSON/nested handling, JDBC/REST patterns, and the **picking-the-right-ingestion-method** decision matrix.

## 1. Ingestion method decision matrix

The exam tests this in scenario form ("which ingestion approach should the engineer use?"). Memorize the row that matches each scenario.

| Method | Mode | Use when | Avoid when |
| --- | --- | --- | --- |
| **Manual upload** (UI) | One-shot | Tiny CSV/Parquet, demos, onboarding | Anything recurring |
| **`CREATE TABLE AS read_files()`** | Batch | One-time backfill from cloud storage | Continuous arrival |
| **`COPY INTO`** | Incremental batch | < ~1M new files between runs; idempotent retries needed | Billions of files, streaming SLAs |
| **Auto Loader (`cloudFiles`)** | Incremental batch **or** streaming | Files arriving continuously; need to scale to billions; schema may evolve | One-shot loads (overkill) |
| **Lakeflow Connect — standard** | Streaming / incremental | Kafka, Kinesis, Event Hubs, cloud object storage | Enterprise SaaS (use managed) |
| **Lakeflow Connect — managed** | Streaming (CDC) | Salesforce, Workday, SQL Server, ServiceNow, Google Analytics, etc. | One-off file loads |
| **Partner Connect** | Various | Vendor with no Databricks-native connector but a third-party one | If a managed connector exists |
| **JDBC/ODBC/REST in notebook** | Batch | Niche source (custom HTTP API, on-prem DB) with no connector | When connector exists — connector is governed and resilient |
| **Zerobus** | Streaming | High-throughput event streams written directly via API | Standard file/DB sources |

### Choice criteria (exam phrasing)

- **Data volume** → tiny: manual / CTAS; massive: Auto Loader.
- **Frequency** → one-shot: `read_files`; continuous arrival: Auto Loader / streaming.
- **Data type** → files: Auto Loader; SaaS: managed connector; DB: managed connector or CDC.
- **Governance need** → must land in UC: any of the above EXCEPT manual upload to DBFS.
- **Schema evolution expected** → Auto Loader (only option with built-in evolution modes).

## 2. `COPY INTO` — mechanics worth remembering

```sql
-- 1. Empty target must exist
CREATE TABLE dea_learning.bronze.orders (id BIGINT, amount DOUBLE, ts TIMESTAMP, region STRING)
USING DELTA;

-- 2. Load
COPY INTO dea_learning.bronze.orders
FROM 's3://landing/orders/'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS  ('mergeSchema' = 'true', 'force' = 'false');
```

### Key behaviors

- **Idempotent**: tracks already-loaded files in the table's commit log. Re-running skips them.
- **`force = true`** → reprocesses every file (use for full refresh).
- **`mergeSchema = true`** in `COPY_OPTIONS` → table evolves to accept new columns.
- Subset selection at load time:
  ```sql
  COPY INTO dea_learning.bronze.orders
  FROM (SELECT id, amount, ts, region FROM 's3://landing/orders/')
  FILEFORMAT = PARQUET;
  ```
- File pattern / subset:
  ```sql
  COPY INTO dea_learning.bronze.orders
  FROM 's3://landing/orders/'
  FILEFORMAT = JSON
  PATTERN = '*2026-*.json'
  FILES = ('a.json', 'b.json');
  ```
- Cannot consume schema evolution as flexibly as Auto Loader; it merges only when `mergeSchema` is set.

### When `COPY INTO` beats Auto Loader

- File count is small and bounded.
- You need plain SQL (no Python).
- Tight idempotency with exactly-once-per-file semantics, no checkpoint plumbing.
- Source isn't expected to scale to billions of objects.

## 3. Auto Loader (`cloudFiles`) internals

### Two file-discovery modes

| Mode | How files are found | When to use |
| --- | --- | --- |
| **Directory listing** (default) | `LIST` the source path every microbatch | Few thousand files; simple to operate; works without cloud event setup |
| **File notification** | Cloud event service (SNS/SQS on AWS, Event Grid+Queue on Azure, Pub/Sub on GCP) pushes new-file events to a queue Auto Loader reads | Millions of files; LIST too slow / expensive; lowest latency |

Switch with `cloudFiles.useNotifications = true` (Auto Loader auto-creates queues if you grant the IAM permissions).

### Required options

```python
(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", "/Volumes/dea_learning/raw/checkpoints/orders/schema")
   .load("/Volumes/dea_learning/raw/landing/orders")
   .writeStream
   .option("checkpointLocation", "/Volumes/dea_learning/raw/checkpoints/orders/checkpoint")
   .trigger(availableNow=True)         # one-shot drain (incremental batch)
   .toTable("dea_learning.bronze.orders"))
```

Triggers:
- `processingTime="1 hour"` — micro-batch every hour (streaming).
- `availableNow=True` — process all pending and stop (incremental batch, ideal for jobs).
- Continuous trigger — not used in practice on Databricks.

### Schema inference

Samples the **first 50 GB or 1,000 files** (whichever first). Untyped formats (JSON, CSV, XML) default all columns to **STRING** to avoid type drift; Parquet/Avro keep file-level types.

Override via:
- `cloudFiles.schemaHints = "amount DOUBLE, ts TIMESTAMP"` — force types per column.
- Explicit schema via `.schema(StructType(...))` — disables inference entirely.

### Schema evolution modes (`cloudFiles.schemaEvolutionMode`)

| Mode | What it does | Default when |
| --- | --- | --- |
| `addNewColumns` | New column → stream throws `UnknownFieldException`, schema location updated, restart picks up new schema | No schema provided |
| `addNewColumnsWithTypeWidening` | Like above, plus widens compatible types (INT→BIGINT) | Opt-in |
| `rescue` | Never evolve — unmatched fields land in `_rescued_data` JSON column | Opt-in |
| `failOnNewColumns` | Stream fails permanently until schema manually updated | Opt-in (strict mode) |
| `none` | Ignore new columns silently; if `rescuedDataColumn` is set they land there | Schema explicitly provided |

Exam trap: with `addNewColumns` the stream **does fail once**, by design — but it restarts cleanly. That's not a bug, it's the protocol.

### Rescued data column

Auto Loader always adds `_rescued_data` (STRING containing JSON) unless you disable it. It catches:
- Fields present in source but absent from schema.
- Type mismatches (`"abc"` arriving for an INT column).
- Case mismatches.

Inspect in silver to catch upstream changes:

```sql
SELECT _metadata.file_path, _rescued_data
FROM dea_learning.bronze.orders
WHERE _rescued_data IS NOT NULL
LIMIT 100;
```

### Metadata columns

`_metadata` is a hidden struct on file sources:

```sql
SELECT
  _metadata.file_path,
  _metadata.file_name,
  _metadata.file_size,
  _metadata.file_modification_time,
  _metadata.file_block_start,
  _metadata.file_block_length
FROM bronze.orders;
```

Use `file_modification_time` for late-arriving partition tagging; `file_path` for lineage debug.

### Checkpoints

`checkpointLocation` stores offsets — which files have been processed. Move it and you lose state (everything reprocesses). Auto Loader actually stores **two** checkpoints:
- The Spark `checkpointLocation` (offsets).
- The Auto Loader `schemaLocation` (`_schemas/` versioned schema).

Both must be durable (cloud storage / UC volume), not local disk.

## 4. Streaming Table syntax in Spark Declarative Pipelines

The SQL form wraps Auto Loader for you — no Python plumbing:

```sql
CREATE OR REFRESH STREAMING TABLE dea_learning.bronze.orders
COMMENT "Raw orders from landing zone"
AS SELECT *, _metadata.file_path AS source_file
   FROM STREAM read_files(
     '/Volumes/dea_learning/raw/landing/orders',
     format        => 'json',
     schemaHints   => 'amount DOUBLE, ts TIMESTAMP',
     schemaEvolutionMode => 'addNewColumns'
   );
```

Equivalent Python:

```python
import dlt

@dlt.table(name="bronze_orders")
def bronze_orders():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaHints", "amount DOUBLE, ts TIMESTAMP")
        .load("/Volumes/dea_learning/raw/landing/orders"))
```

## 5. JSON and nested data

### Reading JSON

Two modes:

```sql
-- 1. Schema-on-read (Auto Loader infers, sees nested struct)
SELECT * FROM read_files('/Volumes/dea_learning/raw/landing/events', format => 'json');

-- 2. Parse a stringified JSON column inside an already-loaded table
SELECT from_json(payload_json, 'STRUCT<id: BIGINT, items: ARRAY<STRUCT<sku: STRING, qty: INT>>>') AS payload
FROM bronze.events_raw;
```

### Flattening nested structs

```sql
SELECT
  payload.id,
  payload.customer.email,                  -- dot notation
  payload.customer.address.country
FROM events
LATERAL VIEW explode(payload.items) AS item;     -- one row per item

-- Easier path: bring all struct fields up one level
SELECT payload.*, customer.* FROM events;
```

### Useful JSON functions

| Function | Use |
| --- | --- |
| `from_json(str, schema)` | String → struct |
| `to_json(struct)` | Struct → string |
| `get_json_object(str, '$.path')` | Pluck a single value from a JSON string without parsing all of it |
| `json_tuple(str, 'k1', 'k2')` | Multiple top-level keys at once |
| `schema_of_json(str)` | Infer a schema string from a sample |
| `parse_json(str)` (VARIANT, DBR 15.3+) | Schema-less binary JSON; query with `:` operator |

### VARIANT (schema-less)

```sql
CREATE TABLE bronze.events (id BIGINT, raw VARIANT);
INSERT INTO bronze.events VALUES (1, parse_json('{"a":1,"b":[10,20]}'));

SELECT id, raw:a::INT, raw:b[0]::INT FROM bronze.events;
```

Use VARIANT for highly heterogeneous JSON where you don't want to pre-define a schema.

## 6. JDBC / ODBC / REST ingestion

When no managed connector exists.

### JDBC read

```python
df = (spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://host:5432/mydb")
  .option("dbtable", "(SELECT * FROM orders WHERE updated_at > '2026-06-01') AS t")
  .option("user", dbutils.secrets.get("ops", "pg_user"))
  .option("password", dbutils.secrets.get("ops", "pg_pwd"))
  .option("partitionColumn", "id")
  .option("lowerBound", 1)
  .option("upperBound", 10_000_000)
  .option("numPartitions", 8)
  .load())

df.write.mode("append").saveAsTable("dea_learning.bronze.orders_pg")
```

Key options for parallelism: `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`. Without them you get one executor pulling the whole table — slow.

### REST API ingestion

No first-class connector; use `requests` (Python) or `dbutils` plus secrets:

```python
import requests, json, time
from pyspark.sql.types import StringType

token = dbutils.secrets.get("ops", "vendor_token")
resp  = requests.get("https://api.vendor.com/v1/events?since=2026-06-01",
                     headers={"Authorization": f"Bearer {token}"}, timeout=60)
resp.raise_for_status()

# Write raw payload to a Volume so Auto Loader can later pick it up
with open(f"/Volumes/dea_learning/raw/landing/vendor/events_{int(time.time())}.json", "w") as f:
    json.dump(resp.json(), f)
```

Pattern: REST → land file → Auto Loader → bronze. Avoid creating a DataFrame directly from the response; you lose retry/idempotency.

Schedule with Lakeflow Jobs (notebook task + cron trigger).

### Secrets

Never hardcode credentials. Use:

```python
dbutils.secrets.get(scope="ops", key="pg_pwd")
```

Scope creation requires admin: `databricks secrets create-scope ops`.

## 7. Lakeflow Connect — managed connectors recap

| Source | Mode |
| --- | --- |
| Salesforce | CDC (managed pipeline) |
| Workday | Full + incremental |
| ServiceNow | Incremental |
| SQL Server / PostgreSQL / MySQL / Oracle | CDC via gateway |
| Google Analytics 4 | Incremental |
| NetSuite | Incremental |
| SharePoint | Incremental |

Architecture (recap from `01_…lakeflow_connect.md`):

```
Source DB / SaaS
  → Ingestion Gateway     (collects creds from UC, network-aware)
  → UC Volume (staging)
  → Managed ingestion pipeline (serverless)
  → Streaming Delta table in UC
```

You configure once; Databricks runs the pipeline, handles schema drift, retries, watermarks, and exactly-once delivery.

## 8. Common gotchas (exam-bait)

- **Auto Loader requires `cloudFiles.schemaLocation`** — leaving it out throws.
- **Checkpoint and schema location are separate** — both must be durable.
- **`addNewColumns` mode causes one failure on new column** — by design; the next run picks up the schema.
- **`_rescued_data` is added automatically** — to disable: `cloudFiles.rescuedDataColumn = ""`.
- **`COPY INTO` is idempotent by default** — `force=true` is the override.
- **`CREATE TABLE AS read_files()` is one-shot** — not incremental; for that use `COPY INTO` or Auto Loader.
- **Manual file uploads land in DBFS**, not UC — for governance, ingest to a UC Volume instead.
- **JDBC without partitioning options = single executor** — always set them on big tables.
- **Auto Loader `directory listing` becomes slow above ~100k files** — switch to `file notification` mode.
- **Lakeflow Connect managed connectors are CDC by default** — schema drift handled, but source needs CDC enabled (e.g., SQL Server: enable CDC on the DB; see `../week_1_platform/learn.md` / `../README.md`).

## 9. Exam-day quick reference

- One-shot SQL load: `CREATE TABLE AS SELECT * FROM read_files(path, format=>...)`.
- Idempotent batch SQL load: `COPY INTO target FROM '<path>' FILEFORMAT = ...`.
- Scaling streaming/incremental: Auto Loader (`format = cloudFiles`).
- Schema evolution modes: `addNewColumns` (default, fails once on new col), `rescue` (capture in `_rescued_data`), `failOnNewColumns` (strict), `none` (ignore).
- File discovery: `directory listing` (default, simple) vs `file notification` (scales to billions).
- Auto Loader needs **`schemaLocation`** AND **`checkpointLocation`**, both durable.
- Triggers: `availableNow=True` for batch drain; `processingTime="N min"` for periodic stream.
- Metadata columns live under `_metadata.*` — use `file_path`, `file_modification_time`.
- Managed connector → enterprise SaaS / DBs (CDC). Standard connector → cloud storage / Kafka. Partner Connect → niche.
- JDBC: always set `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` for parallelism.
- Secrets: `dbutils.secrets.get(scope, key)` — never hardcode.

## References

- [Auto Loader overview](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
- [Auto Loader schema inference and evolution](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)
- [Auto Loader file notification mode](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/file-notification-mode)
- [`COPY INTO` SQL reference](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into)
- [`read_files` table-valued function](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files)
- [Lakeflow Connect — managed connectors](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/)
- [VARIANT type](https://docs.databricks.com/aws/en/semi-structured/variant)
- [JDBC connections](https://docs.databricks.com/aws/en/connect/external-systems/jdbc)
- [Databricks secrets](https://docs.databricks.com/aws/en/security/secrets/)