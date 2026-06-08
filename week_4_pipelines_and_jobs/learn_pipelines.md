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
    - View: a saved query. Any time the query is executed, the latest data is returned.
      Views cannot be used as a streaming source.
      There are two kinds: temporary (lifetime within the pipeline, not exposed to any catalog) and normal (exposed to
      the catalog).
      SQL: `CREATE OR REPLACE VIEW xyz AS SELECT * FROM table_xyz` (add the optional `TEMPORARY` keyword between
      `REPLACE` and `VIEW` to get the pipeline-scoped form)

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
  (CONSTRAINT positive_amount CHECK (amount > 0)                  ON VIOLATION DROP ROW,
   CONSTRAINT valid_status    CHECK (status IN ('placed','shipped','delivered','cancelled')),
   CONSTRAINT valid_currency  CHECK (length(currency) = 3)        ON VIOLATION FAIL UPDATE) AS
  SELECT * FROM STREAM(bronze_orders)
  ```

  ```python
  @dlt.table()
  @dlt.expect("positive_amount", "amount > 0")
  def silver_orders():
    # For sibling pipeline tables, prefer dlt.read_stream over spark.readStream.table
    # so the dependency is registered in the pipeline graph.
    return dlt.read_stream("bronze_orders")
  ```
- As we see in the code, we can use:
    - warnings to notice violations but let data throw
    - drops row to remove the offending row
    - fails to stop the pipeline and notify the user about the violation

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
        * Publish: select "publish Event Log to metastore" in the advanced section of the pipeline configuration
        * Query: `SELECT * FROM <catalog>.<schema>.<event_log_table_name>`

## Change Data Capture (CDC)

- **CDC:** technique used to track changes in data sources (database, lakehouse, etc.) and apply them to a target system
  _Note:_ CDC must be configured in the source system, for a database see `../init/04_configure_cdc_ct_support.sql`
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

## Additional features (out of scope for this course)

- **Flows:** it allows blending multiple pipelines into a single output table.
  Data ingestion from multiple systems into a single table
- **Sinks:** API allows writing data to external systems (f.e. Kafka, Azure Event Hub, etc.)
- **Full native Delta Lake support:** includes:
    - Liquid clustering: automatically optimizes files for better performance during pipeline execution
    - Row Level Security & Column Masking: enforce fine-grained access control for streaming tables and views
    - Change Data Feed: captures changes in streaming tables and sends them to external systems
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