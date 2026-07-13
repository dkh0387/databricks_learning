# Week 4 Glossary — Pipelines and Jobs

| Term | Definition |
| --- | --- |
| **Lakeflow Jobs** | Databricks workflow orchestrator — schedules and runs DAGs of tasks. (Formerly "Databricks Workflows".) |
| **Job** | Top-level orchestratable unit; contains one or more tasks. |
| **Task** | A unit of work inside a job — notebook, SQL query, dashboard, pipeline, JAR, Python wheel. |
| **DAG** | Directed acyclic graph of task dependencies. |
| **Job cluster** | Ephemeral cluster created per job run; billed at Jobs Compute DBU. |
| **Shared cluster across tasks** | One job cluster reused by multiple tasks in the same job. |
| **Run-if conditional task** | Task that runs based on upstream outcome: All succeeded, At least one succeeded, None failed, All done, At least one failed, All failed (API: `ALL_SUCCESS`, `AT_LEAST_ONE_SUCCESS`, `NONE_FAILED`, `ALL_DONE`, `AT_LEAST_ONE_FAILED`, `ALL_FAILED`). |
| **If/Else task** | Boolean branch in the DAG (comparison operators on parameters or task outputs). |
| **For-each task** | Iterates over an input array, runs an inner task per element; iterations can be parallel. |
| **Trigger** | Rule that fires a job: scheduled (cron), file arrival, table update, continuous, manual. |
| **File arrival trigger** | Job fires when new files appear in a watched cloud location/volume (up to 10K files). |
| **Table update trigger** | Job fires when monitored UC tables (up to 10) receive a new commit. |
| **Continuous trigger** | Job runs forever with built-in retry; ideal for streaming. |
| **Scheduled trigger** | Cron-based recurrence. |
| **Repair feature** | Re-runs only failed tasks of a job (targeted recovery), optionally with new parameter values. |
| **Repair history** | UI record of what/when/who repaired a run. |
| **`system.lakeflow`** | Built-in UC schema with `jobs`, `job_tasks`, `job_run_timeline`, `job_task_run_timeline`, `pipelines`. |
| **Spark UI** | Per-cluster UI exposing stages, tasks, executors, storage, SQL plans. |
| **Lakeflow Spark Declarative Pipelines (SDP)** | Framework for declarative batch/streaming ETL in SQL or Python. (Formerly Delta Live Tables / DLT.) |
| **Streaming table** | Delta table inside an SDP, fed incrementally from an append-only streaming source; can be updated by AUTO CDC flows. |
| **Materialized view** | Persisted query result inside an SDP; recomputed on refresh. |
| **Pipeline view** | Saved query inside an SDP — not stored, recomputed each query. |
| **Triggered pipeline** | Pipeline runs once and stops — ideal for scheduled batch refreshes. |
| **Continuous pipeline** | Pipeline runs forever — ideal for streaming. |
| **Event log** | Delta table of pipeline lifecycle events (`flow_definition`, `flow_progress`, `update_progress`, `cluster_resources`). Expectation violations are recorded inside `flow_progress` events under `details:flow_progress.data_quality`. |
| **CDC (Change Data Capture)** | General pattern: mutations are re-encoded as an append-only event log (source-DB CDC, own change events, or Delta CDF) and applied at the target — streaming sources themselves always stay append-only. |
| **Auto CDC INTO** | Current SQL syntax for applying CDC changes into a target streaming table (replaces old DLT `APPLY CHANGES INTO`). |
| **`KEYS`** | `AUTO CDC INTO` clause — primary keys identifying records. |
| **`SEQUENCE BY`** | `AUTO CDC INTO` clause — column that orders changes (e.g., commit timestamp). |
| **`APPLY AS DELETE WHEN`** | `AUTO CDC INTO` clause — predicate marking a row as deleted. |
| **Change Data Feed (CDF)** | Delta feature (`delta.enableChangeDataFeed = true`): a mutating table publishes its changes (`_change_type` insert/update_preimage/update_postimage/delete) as a readable feed — `table_changes()` (batch) or `readChangeFeed` (stream). Enables streaming FROM non-append-only tables. |
| **`skipChangeCommits`** | Stream-read option to ignore update/delete commits in a Delta source — changes are lost downstream; the no-CDF workaround. |
| **`STORED AS SCD TYPE 1`** | Overwrite-only target — current state only. |
| **`STORED AS SCD TYPE 2`** | History-preserving target — adds validity windows per record. |
| **Stream-snapshot join** | Streaming source joined against a static/snapshot table. New rows × full snapshot. |
| **Stream-MV join** | Two streams joined and persisted in a materialized view. |
| **Sink** | API for writing pipeline output to external systems (Kafka, Event Hub, …). |
| **Flow** | Pipeline primitive blending multiple sources into one target table. |
| **Declarative Automation Bundle (DAB)** | Bundled pipeline+job definitions used to deploy declaratively. Formerly "Databricks Asset Bundles"; CLI command name unchanged. Covered in Week 5. |