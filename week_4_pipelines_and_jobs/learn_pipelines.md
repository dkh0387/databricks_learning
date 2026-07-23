# Data Pipelines with LakeFlow Spark Declarative Pipelines

## Key definitions

- **Spark Declarative Pipelines (SDP):** Databricks' declarative framework for building batch or streaming ETL
  pipelines in SQL or Python. The pipeline runtime handles orchestration, dependency tracking, retries, and
  incremental refresh. Integrates with Lakeflow Connect (ingestion) and downstream dashboards/BI but is itself
  scoped to the **ETL layer** of the medallion.
- **Dataset types:**
    - Streaming Table: table with support for streaming or incremental data processing (only new data).
      SQL: `CREATE OR REFRESH STREAMING TABLE xyz AS SELECT * FROM STREAM read_files(...)` (`FROM STREAM` enables
      autoloading with checkpoints)
    - Materialized View: records are processed once and stored in a table (current data). Useful for aggregations and
      complex queries. Unlike a view, it is physically stored.
      SQL: `CREATE OR REFRESH MATERIALIZED VIEW xyz AS SELECT * FROM table_xyz` (`REFRESH` guarantees the view stays
      up to date). Incremental refresh requires **serverless** compute.
      An MV **cannot be a streaming source**: its refresh reconciles a query result, so existing rows are updated or
      deleted — the table is not append-only, which streaming requires. Consequence: an MV ends the streaming path;
      keep intermediate layers as streaming tables and place MVs at the end of the chain (gold/serving).
      Why in depth: `../week_3_transformation/learn_data_transformation.md` §8.
    - View: a saved query. Any time the query is executed, the latest data is returned.
      Views cannot be used as a streaming source.
      There are two kinds: temporary (lifetime within the pipeline, not exposed to any catalog) and normal (exposed to
      the catalog).
      SQL: `CREATE TEMPORARY VIEW xyz AS SELECT * FROM table_xyz` for the pipeline-scoped form (not published),
      or `CREATE VIEW xyz AS SELECT * FROM table_xyz` to publish the view to the catalog

## Advantages

- **Simple pipeline authoring:** just SQL or Python for ingestion and transformation
- **Easily scalable:** scales automatically if needed
- **Batch or streaming:** ingest data at once or stream continuously
- **Auto Loader:** when the pipeline reads cloud files via Auto Loader, checkpoints track which files have already
  been ingested and processed

## Structure

- **Ingestion Pipeline:**
    - Select Connector
    - Select Ingestion type (batch, incremental batch or streaming)
    - Write SQL or Python code to create the streaming table (bronze, silver) or materialized view (gold)
- **ETL Pipeline:**
    - Write SQL or Python code to clean/join/change data
    - Write SQL or Python code to create the incremental table (silver)
    - Write SQL or Python code to create the materialized view (gold)
- **Job:**
    - Add each pipeline to the job as a task
    - Add dependencies between tasks
    - Configure job parameters
    - Configure job scheduling
    - Configure failure handling

## Pipeline settings

- You can reach them in the pipeline editing UI just by clicking on the settings icon in the top left corner
- Settings for: compute, code assets, parameters, etc.
- Recommended compute option: serverless
- Root- and source folder: all relevant files for the pipeline project (can be a Git folder)
- Parameters: can be defined and used in SQL and Python code like `${key-name}`

## Creating data quality expectations

- We can add constraints to the SQLs to ensure data quality or apply filters (example on the unified orders domain):
  ```sql
  CREATE OR REFRESH STREAMING TABLE silver_orders
  (CONSTRAINT positive_amount EXPECT (amount > 0)                  ON VIOLATION DROP ROW,
   CONSTRAINT valid_status    EXPECT (status IN ('placed','shipped','delivered','cancelled')),
   CONSTRAINT valid_currency  EXPECT (length(currency) = 3)        ON VIOLATION FAIL UPDATE) AS
  SELECT * FROM STREAM(bronze_orders)
  ```

  ```python
  @dlt.table()
  @dlt.expect("positive_amount", "amount > 0")
  def silver_orders():
    # dlt.read / dlt.read_stream are legacy; the current recommendation for sibling
    # pipeline tables is spark.read.table / spark.readStream.table.
    return spark.readStream.table("bronze_orders")
  ```
- As we see in the code, we can use:
    - warnings to notice violations but let data through
    - drops row to remove the offending row
    - fails to stop the pipeline and notify the user about the violation

### The complete decorator list ([official reference](https://docs.databricks.com/aws/en/dlt/expectations))

| Action | Single constraint | Multiple constraints (dict) | SQL equivalent |
| --- | --- | --- | --- |
| **Warn** (default) | `expect` | `expect_all` | `EXPECT (...)` — no `ON VIOLATION` clause |
| **Drop** | `expect_or_drop` | `expect_all_or_drop` | `ON VIOLATION DROP ROW` |
| **Fail** | `expect_or_fail` | `expect_all_or_fail` | `ON VIOLATION FAIL UPDATE` |

- **There is no `expect_or_warn` and no `ON VIOLATION WARN`** — warn is the default of the plain
  `expect`, so it has no suffix/clause. Popular invented distractor.
- Current docs write the decorators with the **`@dp.` prefix** (new declarative-pipelines module);
  the classic `@dlt.` prefix used in this repo's examples still works — recognize both in questions.

## Streaming joins

- **Stream-Snapshot Join:** join a streaming table with a static table (f.e. a lookup mapping table for country codes).
  _Note:_ since streaming is incremental, it joins only new rows of streaming table with the whole static table.
  Useful for enriching new data with static context.
- **Stream-Stream Join inside a Materialized View:** join two streaming tables in the definition of a materialized
  view. Useful when both datasets change frequently and you want an up-to-date combined result.
  The MV refreshes incrementally, picking up new rows from either source.

## Deployment to production

- **Schedule:** ensure automatic execution
    - Triggered: refreshes selected tables at the start of the execution, ideal for batch processing
    - Continuous: keeps all tables and views up to date in near real-time, ideal for streaming processing
- **E-mail notifications:** send an e-mail when the pipeline fails. Three events are available: on failure, on success,
  and on start
- **Monitor:** use the Event Log to check execution status, runtime information, errors and metrics.
    - Captures information about:
        * Audit Log: who did what and when
        * Data quality checks: constraint violations
        * Pipeline progress: status of each run
        * Data lineage: how data flows through the pipeline
    - Event Log is a Delta Table and can be queried using SQL:
        * Via the table-valued function: `SELECT * FROM event_log("<pipeline-id>")`
        * Or via the published table in the pipeline's default catalog/schema, named
          `event_log_<pipeline_id_with_underscores>`

## Change Data Capture (CDC)

- **CDC:** technique used to track changes in data sources (database, lakehouse, etc.) and apply them to a target system.
  It is a **general pattern**, not tied to managed connectors — it shows up in three places on Databricks:
    1. **Source-database CDC via managed connectors** (SQL Server etc.): the *database's own* CDC/Change-Tracking
       feature produces the change feed, Lakeflow Connect consumes and applies it fully managed.
       _Note:_ CDC must be configured in the source system, for a database see `../init/04_configure_cdc_ct_support.sql`
    2. **Your own change events + `AUTO CDC INTO`**: any source works (files, Kafka, …) as long as it delivers events
       with an operation column and a sequence column — the repo's `pipeline_auto_cdc_scd2.sql` feeds a plain JSON
       event file, no database involved.
    3. **Delta Change Data Feed (CDF)**: a mutating Delta table publishes its own changes as a readable change feed
       (see below).
- **Why CDC does not "break" the append-only rule** — it works around it:
  streaming always requires an append-only log; CDC does not change that. The trick is **re-encoding**: instead of
  mutating rows in place, the mutations themselves are written as *new appended event rows* ("customer 6 was deleted"
  is an INSERT into the event log). The stream still reads forward, append-only, exactly-once — and only at the
  **target** does `AUTO CDC INTO` (or `foreachBatch` + MERGE) apply the events as real updates/deletes. That is why
  the target streaming table may mutate while every streaming *source* stays append-only.
  Chain: mutating source → encode changes as append-only event log (DB CDC, own events, or CDF) → stream reads the
  log → apply at the target.
- **SCD (Slowly Changing Dimension):** defines how historical changes are tracked and stored in a target system
  Two types:
    - SCD Type 1: overwrites existing data (old data is gone).
      Use when: only current data matters, no need to track historical changes
    - SCD Type 2: tracks historical changes by storing previous versions of data (old data is kept with a validity
      period).
      Use when: historical data is important
- Usage in Pipeline: `AUTO CDC INTO` statement

  ```sql
  CREATE FLOW <flow_name> AS AUTO CDC INTO <target_stream_table>
  FROM STREAM <source_table>
  KEYS (<pk_key_name>) -- primary key to recognize records
  APPLY AS DELETE WHEN operation = 'DELETE'
  SEQUENCE BY <sequence_column> -- when did the change happen?
  COLUMNS <column_list>
  STORED AS SCD TYPE 1
  ```

### Delta Change Data Feed (CDF) — streaming FROM a mutating table

The reverse direction: you want to stream **out of** a Delta table that receives updates/deletes — which normally
fails on exactly the append-only rule. CDF makes the table publish its own changes as an append-only feed of change
events:

```sql
-- enable on the table (or set at creation)
ALTER TABLE dea_learning.silver.customers_silver
  SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- batch: read changes between versions/timestamps
SELECT * FROM table_changes('dea_learning.silver.customers_silver', 2, 5);
```

```python
# streaming: consume the change feed instead of the table rows
(spark.readStream
   .option("readChangeFeed", "true")
   .table("dea_learning.silver.customers_silver"))
```

Each change row carries `_change_type` (`insert`, `update_preimage`, `update_postimage`, `delete`) plus
`_commit_version` / `_commit_timestamp` — i.e. the mutations arrive re-encoded as appended event rows, same principle
as above. Downstream you apply them with `AUTO CDC INTO` or `foreachBatch` + MERGE.

Cheap alternative without CDF: `.option("skipChangeCommits", "true")` lets a stream read such a table by **ignoring**
update/delete commits entirely — changes are silently lost downstream; only acceptable when downstream truly only
needs appended rows.

**When you actually hit this in a pipeline:** the target of an `AUTO CDC` flow (e.g. `customers_scd2`) mutates and
therefore **cannot be consumed with `STREAM(...)`**. Downstream options:
- **MV** — downstream only needs current/aggregated state: `... FROM customers_scd2 WHERE __END_AT IS NULL`.
  MV refresh reconciles and copes with mutations; no CDF needed (see week 3's MV-vs-streaming-source explanation).
- **CDF** — downstream must *stream* (custom incremental logic, external systems): enable
  `delta.enableChangeDataFeed` on the AUTO CDC target and consume the change feed with `foreachBatch` + MERGE.
  Note the **composite merge key** for SCD2 replicas (`customer_id` + `__START_AT`) — every version is its own row.

Worked example in this repo: `code/pipeline_auto_cdc_scd2.sql` (enables CDF on `customers_scd2`) +
`code/08_cdf_downstream_consumer.py` (readChangeFeed → `foreachBatch` MERGE into `gold.customers_cdf`) — wired into
the job as the `cdf_consumer` task downstream of `cdc_pipeline` (`code/03_lakeflow_job_definition.json`).

## Additional features (out of scope for this course)

- **Flows:** it allows blending multiple pipelines into a single output table.
  Data ingestion from multiple systems into a single table
- **Sinks:** API allows writing data to external systems (f.e. Kafka, Azure Event Hub, etc.)
- **Full native Delta Lake support:** includes:
    - Liquid clustering: automatically optimizes files for better performance during pipeline execution
    - Row Level Security & Column Masking: enforce fine-grained access control for streaming tables and views
    - Change Data Feed: covered above — captures a table's changes as a readable feed (also usable towards external systems)
- **Full Unity Catalog support:** publish to multiple catalogs and schemas; read streaming tables and views in Dedicated
  Access Mode
- **Performance tuning:**
    - Optimization mode for tasks
    - Serverless cluster size
    - Incremental refresh for materialized views
    - Photon
- **Declarative Automation Bundles (DAB, formerly Databricks Asset Bundles):** enables programmatic deploy, validate
  and run of CI/CD production workloads from a Git repository (full coverage in Week 5)

## Takeaways

- Streaming tables
- Materialized views
- Temporary views
- Pipeline settings: scheduling, trigger, compute, code assets, parameters, advanced like event logs, etc.
- Developing Lakeflow declarative pipelines using Pipeline Editor with SQL and Python code
- Data quality expectations
- Event Logs and pipeline metrics
- Change Data Capture (CDC) using `AUTO CDC INTO` (the 2026 replacement of the old `APPLY CHANGES INTO` DLT syntax) to
  handle slowly changing dimensions (SCD)
- CDC is a general pattern (DB CDC via managed connectors, own change events, Delta CDF) — it re-encodes mutations as
  an append-only event log; streaming sources always stay append-only, changes are applied at the target