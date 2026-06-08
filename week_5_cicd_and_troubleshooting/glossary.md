# Week 5 Glossary — CI/CD and Troubleshooting

## CI/CD

| Term | Definition |
| --- | --- |
| **CI** | Continuous Integration — automated build/test on every commit. |
| **CD** | Continuous Delivery/Deployment — automated promotion to environments. |
| **Trunk-based development** | Short-lived branches frequently merged to `main`. Recommended for DAB. |
| **Feature branching** | Longer-lived branches per feature. Heavier process. |
| **PAT** | Personal Access Token used for Git ↔ Databricks auth. |
| **Service Principal** | Non-human identity for CI/CD and prod jobs. |
| **Databricks Git Folder** | Workspace folder backed by a remote Git repo (formerly "Databricks Repos"). |
| **Declarative Automation Bundle (DAB)** | YAML-defined Databricks infrastructure: jobs, pipelines, notebooks, etc. (Formerly "Databricks Asset Bundles"; CLI command name unchanged.) |
| **`databricks.yml`** | Root bundle config — `bundle`, `include`, `variables`, `resources`, `targets`, … |
| **Target** | Environment-scoped override block in `databricks.yml` (e.g., `dev`, `staging`, `prod`). |
| **`mode: development`** | Target mode that prefixes resource names with `[dev <user>]` and pauses schedules. |
| **`mode: production`** | Target mode requiring `run_as`, with strict resource naming, schedules active. |
| **Variable** | Reusable value in DAB referenced as `${var.<name>}`. |
| **Substitution** | DAB template expression like `${bundle.target}` or `${workspace.current_user.userName}`. |
| **`databricks bundle validate`** | Schema + reference check on the bundle. |
| **`databricks bundle deploy -t <target>`** | Upload files + create/update resources for that target. |
| **`databricks bundle run <resource> -t <target>`** | Trigger a deployed job/pipeline. |
| **`databricks bundle destroy -t <target>`** | Tear down everything deployed to that target. |
| **`databricks bundle generate`** | Reverse-engineer an existing UI resource into YAML. |
| **`databricks bundle deployment bind`** | Adopt an existing resource into bundle management without recreating. |
| **DataOps** | DevOps applied to data pipelines and data products. |
| **MLOps** | DevOps applied to ML model lifecycle. |
| **Testing pyramid** | Many unit tests → fewer integration → fewer end-to-end. |
| **PySpark unit tests** | `pyspark.testing.utils` helpers like `assertDataFrameEqual`, `assertSchemaEqual`. |

## Troubleshooting / Monitoring / Optimization

| Term | Definition |
| --- | --- |
| **Spark UI** | Per-cluster web UI with Jobs, Stages, Tasks, Executors, SQL views. |
| **Stage** | Set of tasks executable without a shuffle. |
| **Task** | Smallest unit of execution — one partition handled by one executor. |
| **Skew** | Uneven partition size causing one task to take much longer than the rest. |
| **Spill** | Data written from memory to disk during shuffle when out of memory. |
| **Straggler task** | The slowest task in a stage that holds up the whole stage — almost always caused by data skew on its partition. |
| **AQE (Adaptive Query Execution)** | Runtime re-optimization based on observed shuffle stats. Default-on in DBR 7.3+. Handles dynamic partition coalescing and skew-join splitting automatically. Knobs: `spark.sql.adaptive.{enabled, coalescePartitions.enabled, skewJoin.enabled, localShuffleReader.enabled}`. |
| **GC time** | Time the JVM spent garbage collecting — high = memory pressure. |
| **Salt key** | Random suffix appended to a join/group key to redistribute skewed data. |
| **Broadcast threshold** | `spark.sql.autoBroadcastJoinThreshold` — max table size for broadcast eligibility. |
| **Driver OOM** | Out-of-memory on the driver, often from `collect()` / `toPandas()`. |
| **Executor OOM** | Out-of-memory on a worker — fix with bigger executor or more shuffle partitions. |
| **`CLUSTER BY`** | Liquid Clustering declaration — explicit columns or `AUTO`. |
| **`CLUSTER BY AUTO`** | Predictive Optimization chooses keys from workload. Requires DBR 15.4 LTS+. |
| **`OPTIMIZE`** | Compact small Delta files; with Liquid Clustering, also performs clustering work. |
| **`OPTIMIZE … FULL`** | Full rewrite — used after changing clustering keys. |
| **`VACUUM`** | Remove tombstoned (deleted-but-not-yet-removed) Delta files. Default retention 7 days. |
| **Predictive Optimization** | Auto runs `OPTIMIZE`, `VACUUM`, `ANALYZE` on UC managed tables on serverless. Does NOT run Z-Order. |
| **Init script** | Bash script run during cluster startup. Failures logged under `dbfs:/cluster-logs/<cluster-id>/init_scripts/`. |
| **`CLOUD_PROVIDER_LAUNCH_FAILURE`** | Cluster termination reason — cloud refused VM (quota/IAM/capacity). |
| **`INSTANCE_UNREACHABLE`** | Driver can't reach workers (network/security group). |