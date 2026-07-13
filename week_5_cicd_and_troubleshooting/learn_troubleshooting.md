# Troubleshooting, Monitoring, and Optimization

Covers exam **§6 (10%)**. Read after `../week_4_pipelines_and_jobs/learn_jobs.md` (Jobs UI) and `../week_4_pipelines_and_jobs/learn_pipelines.md` (event logs).

## 1. Monitoring with Lakeflow Jobs UI

### Run history view

For each job, the **Runs** tab shows a time series of past runs. Use it to:
- Compare current run duration against historical baseline (median of last N runs).
- Spot regressions after code/data/config changes — sudden jump from 5 min to 30 min = investigate the last commit.
- Identify flaky tasks via failure rate over time.

### DAG view

Each task in the job graph is colour-coded:
- Green — succeeded
- Red — failed
- Yellow — running
- Grey — skipped (run-if condition not met)
- Blue — waiting on upstream

Click any task → opens the task run page with:
- Start/end time, total duration.
- Cluster used + driver/executor logs.
- Output (notebook cells, SQL results).
- Link to the Spark UI.

**Upstream blocker** = the slowest predecessor in the DAG that other tasks wait on. Optimize that first; downstream parallelism doesn't help.

### system.lakeflow tables (programmatic monitoring)

```sql
-- Slowest tasks in the last 7 days
SELECT job_id, task_key, 
       avg(unix_timestamp(period_end_time) - unix_timestamp(period_start_time)) AS avg_seconds
FROM system.lakeflow.job_task_run_timeline
WHERE period_start_time > current_date() - INTERVAL 7 DAYS
GROUP BY job_id, task_key
ORDER BY avg_seconds DESC;

-- Failure rate per job
SELECT job_id,
       sum(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) * 1.0 / count(*) AS fail_rate
FROM system.lakeflow.job_run_timeline
WHERE period_start_time > current_date() - INTERVAL 30 DAYS
GROUP BY job_id
HAVING fail_rate > 0;
```

Key tables: `jobs`, `job_tasks`, `job_run_timeline`, `job_task_run_timeline`, `pipelines`.

### Pipeline event log (Spark Declarative Pipelines)

Enable *publish event log to metastore* in pipeline settings, then:

```sql
SELECT timestamp, event_type, message, level, details
FROM dea_learning.observability.pipeline_event_log   -- the publish target you set in pipeline config
WHERE level IN ('ERROR', 'WARN')
ORDER BY timestamp DESC;
```

Event types you must recognise:
- `flow_progress` — start/complete/fail of a flow; expectation metrics live inside these events (`details:flow_progress.data_quality`).
- `cluster_resources` — cluster scaling events.
- `update_progress` — pipeline lifecycle.

## 2. Spark UI — diagnosing slow stages

Open from any task run → *Spark UI* link → *Stages* tab → click the long-running stage.

### Stage page checklist (in order)

1. **Spill present?** → memory pressure during shuffle.
2. **Skew present?** → uneven partition sizes.
3. **High I/O?** → input read time dominates.
4. **High GC time?** → executor memory undersized.

### Detecting skew

In *Summary Metrics for Tasks*, compare **Max** vs **75th percentile** (and **Median**) for:

| Metric | Skew flag |
| --- | --- |
| Duration | `Max > 1.5 × 75th percentile` → likely skew |
| Shuffle Read Size | `Max ≫ Median` (e.g. 5 GB vs 50 MB) → one partition holds most data |
| Shuffle Write Size | Same — upstream skew that will hit downstream stages |

Healthy stage: median ≈ 75th ≈ max.

### Detecting spill

Stage page shows two metrics:
- **Shuffle Spill (Memory)** — bytes that overflowed in memory before serialization.
- **Shuffle Spill (Disk)** — bytes written to disk.

Any non-zero spill = the executor ran low on memory and paged data to disk. Slow. Common causes: too few shuffle partitions (huge partitions), undersized executor memory, expensive aggregation.

### Remediation playbook

| Symptom | Fix |
| --- | --- |
| Skew on join key | Enable AQE skew join: `spark.sql.adaptive.skewJoin.enabled = true` (default true on DBR 13+). Or salt the key: `key || floor(rand()*N)`. |
| Skew on aggregation | Two-stage aggregation: salt → partial aggregate → unsalt → final aggregate. |
| Spill on shuffle | Increase `spark.sql.shuffle.partitions` (default 200) so each partition is smaller. Target ~100–200 MB per partition. |
| Many small files / overhead | `OPTIMIZE table` (bin-packing) or enable Liquid Clustering. |
| Broadcast join not happening on a small table | Raise `spark.sql.autoBroadcastJoinThreshold` (default 10 MB) or hint `/*+ BROADCAST(t) */`. |
| Broadcast join failing OOM | Lower the threshold or convert to sort-merge join (set threshold to `-1`). |
| Long GC time | Increase executor memory; consider Photon. |
| Long planning time | Cache table stats with `ANALYZE TABLE … COMPUTE STATISTICS`, or break giant queries into temp views. |

### AQE — Adaptive Query Execution

Runtime re-optimization based on observed shuffle stats. Default-on in DBR 7.3+. Key knobs (Python, runtime-settable):

```python
spark.conf.set("spark.sql.adaptive.enabled", True)                       # master switch
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)    # merge tiny post-shuffle partitions
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)              # split skewed partitions
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", True)    # avoids unnecessary network IO
```

## 3. Cluster diagnosis

### Cluster startup failures

Cluster *Event log* tab shows the lifecycle. Common termination reasons:

| Event | Meaning | Fix |
| --- | --- | --- |
| `CLOUD_PROVIDER_LAUNCH_FAILURE` | Cloud refused VM (quota / capacity / IAM). | Check quota, switch instance type, retry. |
| `INSTANCE_UNREACHABLE` | Driver couldn't contact workers. | VPC / security group / route table issue. |
| `INIT_SCRIPT_FAILURE` | Init script returned non-zero. | Read init log under `dbfs:/cluster-logs/<cluster-id>/init_scripts/`. |
| `DBFS_COMPONENT_UNHEALTHY` | Storage mount failed. | Re-mount or use UC volumes. |
| `WORKSPACE_CONFIG_PROBLEM` | Workspace setting changed (PrivateLink, etc.). | Admin needed. |

### Library conflicts

Symptom: `NoSuchMethodError`, `ClassNotFoundException`, `IncompatibleClassChangeError`, or driver crash on startup.

Causes:
- Two versions of the same JAR (cluster-scoped + notebook-scoped library).
- Library version incompatible with DBR.
- Conflict with Photon's bundled deps.

Fix:
1. Check the *Libraries* tab → are both versions listed?
2. Pin to the version that matches the DBR's bundled libraries ([release notes](https://docs.databricks.com/aws/en/release-notes/runtime/)).
3. Use notebook-scoped libraries via `%pip install pkg==x.y.z` so they don't leak to the cluster.
4. Prefer cluster-scoped only for libraries every notebook needs.

### Out-of-memory (OOM)

Two distinct flavours — diagnose by **where**:

| OOM on | Symptom | Fix |
| --- | --- | --- |
| Driver | Notebook hangs, then `java.lang.OutOfMemoryError: GC overhead limit` in driver log; `collect()` / `toPandas()` triggered it. | Stop pulling large data to driver. Use `display(df)` not `collect()`. Or scale driver memory. |
| Executor | Stage retries 4 times then fails; *Failed Tasks* list shows `ExecutorLostFailure` / `OutOfMemoryError`. | Larger instance type, more memory per executor, or more shuffle partitions to shrink each partition. |

Tip: in the Spark UI → *Executors* tab, look at **Storage Memory** and **On Heap Memory** columns to confirm the executor was saturated.

## 4. Storage layout optimization

### `OPTIMIZE` — bin-packing

Compacts small files into larger ones (~1 GB target). Run periodically on tables with frequent small writes (streaming sinks, MERGE-heavy tables).

```sql
OPTIMIZE dea_learning.silver.silver_orders;
OPTIMIZE dea_learning.silver.silver_orders WHERE event_date >= '2026-01-01';   -- partitioned subset
```

### Z-Order (legacy, prefer Liquid Clustering)

Co-locates rows with similar values on disk. Useful for high-cardinality filter columns.

```sql
OPTIMIZE dea_learning.silver.silver_orders ZORDER BY (customer_id);
```

Drawbacks: not incremental (full rewrite each run), partitioning must be planned upfront.

### Liquid Clustering (recommended)

Replaces partitioning + Z-Order. Incremental, predictive-optimization aware, lets clustering keys evolve.

```sql
-- Define keys explicitly
CREATE TABLE dea_learning.silver.silver_orders (id BIGINT, customer_id BIGINT, ts TIMESTAMP)
CLUSTER BY (customer_id, ts);

-- Or let Databricks pick keys based on query workload
CREATE TABLE dea_learning.silver.silver_orders (id BIGINT, customer_id BIGINT, ts TIMESTAMP)
CLUSTER BY AUTO;

-- Change keys later
ALTER TABLE dea_learning.silver.silver_orders CLUSTER BY (ts);
ALTER TABLE dea_learning.silver.silver_orders CLUSTER BY NONE;     -- disable
```

Trigger clustering work:
- `OPTIMIZE dea_learning.silver.silver_orders` — incremental, processes only new/modified data.
- `OPTIMIZE dea_learning.silver.silver_orders FULL` — full rewrite (use after `CLUSTER BY` change).

Constraints:
- Delta tables only.
- DBR 13.3+ for explicit keys, **DBR 15.4 LTS+** for `CLUSTER BY AUTO`.
- Max 4 clustering keys (documented limit).
- Cannot combine with `PARTITIONED BY`.

### Predictive Optimization (UC managed tables)

Databricks runs `OPTIMIZE`, `VACUUM`, and `ANALYZE` automatically on UC managed tables — no scheduling, no DBU costs on your clusters (runs on serverless, billed separately).

```sql
-- Enable at any level (inheritance: account → catalog → schema → table)
ALTER CATALOG dea_learning ENABLE PREDICTIVE OPTIMIZATION;
ALTER SCHEMA  dea_learning.silver DISABLE PREDICTIVE OPTIMIZATION;
ALTER SCHEMA  dea_learning.silver INHERIT PREDICTIVE OPTIMIZATION;

-- Check status
DESCRIBE EXTENDED dea_learning.silver.silver_orders;
```

Excluded: external tables, Delta Sharing recipient tables, Hive metastore tables.
Required: Premium plan, UC managed Delta table, account-level PO enablement.
PO uses its own serverless compute to run the work — the runtime that wrote the table is not the constraint.
Note: predictive `OPTIMIZE` does **not** run `ZORDER` — use Liquid Clustering instead.

### `VACUUM` — remove tombstoned files

```sql
VACUUM dea_learning.silver.silver_orders;                       -- default 7 day retention
VACUUM dea_learning.silver.silver_orders RETAIN 168 HOURS;      -- explicit
```

Lower retention than 7 days requires `spark.databricks.delta.retentionDurationCheck.enabled = false` and breaks time travel beyond that point.

## 5. Performance tuning cheat sheet

```python
# Shuffle partition count — default 200; aim for partitions ≈ 100–200 MB
spark.conf.set("spark.sql.shuffle.partitions", 400)

# Broadcast join threshold (bytes) — default 10 MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)   # disable broadcast joins
```

Executor / driver memory and `spark.default.parallelism` (used by RDD operations without
partition hints) cannot be set at runtime — configure them in the cluster UI under
**Compute → cluster → Advanced options → Spark → Spark config**, one `key value` per line:

```
spark.executor.memory     14g
spark.driver.memory       14g
spark.default.parallelism 200
```

When to tune what:

| Knob | Raise when | Lower when |
| --- | --- | --- |
| `spark.sql.shuffle.partitions` | Partitions too large → spill, slow tasks | Too many tiny tasks → scheduler overhead |
| `spark.sql.autoBroadcastJoinThreshold` | Small dim tables not being broadcast | Broadcast OOMing the driver |
| executor memory | OOM / GC time / spill | Cost too high relative to runtime gain |
| Photon (cluster flag) | CPU-bound SQL/DataFrame workload | Pure Python UDFs (no Photon benefit) |

## 6. Exam-day quick reference

- **Skew flag**: `max duration > 1.5 × p75`, or shuffle read max ≫ median.
- **Spill flag**: any non-zero "Shuffle Spill (Disk)".
- **Driver OOM**: caused by `collect()` / `toPandas()` / huge broadcast.
- **Executor OOM**: fix with bigger instance, more shuffle partitions, or skew remediation.
- **Liquid Clustering**: replaces partitioning + Z-Order; `CLUSTER BY (cols)` or `CLUSTER BY AUTO` (DBR 15.4 LTS+).
- **Predictive Optimization**: UC managed Delta only; auto `OPTIMIZE` + `VACUUM` + `ANALYZE`; **no Z-Order**.
- **AQE** handles dynamic partition coalescing and skew-join splitting automatically.
- **`system.lakeflow.*`** tables = source of truth for run-history trend analysis.
- Init script failures land in `dbfs:/cluster-logs/<cluster-id>/init_scripts/`.

## References

- [Spark UI — skew and spill](https://docs.databricks.com/aws/en/optimizations/spark-ui-guide/long-spark-stage-page)
- [Spark UI overview](https://docs.databricks.com/aws/en/optimizations/spark-ui-guide/)
- [Liquid Clustering](https://docs.databricks.com/aws/en/delta/clustering)
- [Predictive Optimization](https://docs.databricks.com/aws/en/optimizations/predictive-optimization)
- [Adaptive Query Execution](https://docs.databricks.com/aws/en/optimizations/aqe)
- [DBR release notes (library versions)](https://docs.databricks.com/aws/en/release-notes/runtime/)
- [`system.lakeflow` schema](https://docs.databricks.com/aws/en/admin/system-tables/jobs)
- [Cluster event log reference](https://docs.databricks.com/aws/en/compute/clusters-manage#event-log)