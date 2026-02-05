# Deploy Workloads with Lakeflow Jobs

## Core definitions

1. **Lakeflow Connect:** A set of efficient ingestion connectors that simplify data ingestions from popular enterprise
   applications, databases, cloud storage, message buses and local files
2. **Lakeflow Declarative Pipeline:** A framework for building batch and streaming data pipelines using SQL and Python,
   designed to speed up ETL development
3. **Lakeflow Jobs:** A workflow automation tool that orchestrates data processing workflows. It enables coordination of
   multiple tasks within complex workflows, allowing for the scheduling, optimization and management of repeatable
   processes

## Lakeflow Jobs core concepts

- Connectors (Lakeflow Connect)
- Pipelines (Lakeflow Declarative Pipeline)
- Processing engine (Photon)
- Governance (Unity catalog)
- Storage (Delta Lake)

Everything is natively integrated in a Databricks platform. It eliminates integration challenges of external tools.

## Building Blocks of Lakeflow Jobs

- Task: A unit of work that can be executed by a processing engine (notebook, query, script, etc.)
- Control flow: A way to define the order in which tasks should be executed
- Job: A primary container for scheduling and orchestrating tasks such as data processing, ETL, analytics and ML
- Supported languages: Python, SQL, Scala, R, Java (JAR)
- Running environment:
    - **Interactive clusters:** development, testing, debugging
    - **Job clusters:** production (50% cheaper since only for job runs)
    - **Serverless clusters:** better overall performance, fully managed
    - **SQL warehouse:** for SQL, dashboards, BI, etc.
- Performance-optimized option: choose between lower cost and faster execution for serverless tasks:
    - Off: cost-efficiency, slow start-up (4–6 min, time-flexible tasks)
    - On: higher-cost, fast start-up (time-sensitive tasks)
- Tasks within the same job can share the same compute resource or use different ones

## Task orchestration

- **DAG:** directed acyclic graph, conceptual representation of a series of activities
- **Orchestration:** ability to run tasks as DAG using UI, API, SDK or Asset Bundles
- Examples: Sequence, Funnel (multipler inputs), Fan-out (multiple outputs)

## Job scheduling and triggers

- **Trigger:** rule engine that determines when a job should run
- Examples:
    - Schedule: daily, weekly, monthly, etc., cron expression support
    - Event: on file upload (Azure Storage, AWS S3, Google Cloud Storage, Databricks Volumes, up to 10K files), on table
      update, etc.
    - Continuous: build-in retry logic, ideal for streaming (Data Ingestion Gateway, Fraud Detection Engine, etc.)
    - Manual: user-initiated action can be started from UI, API, SDK or Asset Bundles (Testing and Debugging)
    - Table update: multiple tables can be monitored (up to 10), and a job will be triggered when any/all of them
      change (insert, update, delete, merge); sleep time between runs or waiting time can be configured

## Conditional and iterative tasks

- **Run-if conditional task:** task that runs based on the outcome of the upstream task.
  Dependency conditions: all succeeded, at least one succeeded, none failed (can skip but not fail), custom
- **If/Else task:** boolean conditional logic in the workflow, allows different branches of execution.
  Boolean operators: ==, =, >, >=, <, <=.
  Condition: "if none of the dependencies failed and at least one executed" ensures meaningful upstream results
- **For loop task:** the same logic is applied for multiple data partitions or parameter values.
  Loops over an input array and executes a task for each element as an input parameter.
  Iterations can run in parallel.
  Downstream tasks are attached to the whole loop container.

## Handling task failures

- **Repair feature:** approach to failure recovery
    - Targeted recovery: modify only failed tasks, run only what is necessary
    - Parameter overwrite capability: overwrite parameters of failed tasks with new values
- **Recovery scenarios:**
    - Configuration fixes: correct parameter values
    - Resource adjustments: increase cluster size, increase parallelism, etc.
    - Code updates: fix logical code errors
    - Data quality issues: adjust rules for data cleaning
- **Repair history:** history what, when and by whom was the repair applied is available in the UI

## Monitoring job performance

- **System tables:** `system.lakeflow` is a build-in read-only catalog that logs all job activities across workspaces
  within a region
    - **Key tables:**
      `jobs`: job basic info
      `job_tasks`: task basic definition
      `job_run_timeline`: each job run over time
      `job_task_run_timeline`: each task run over time
      `pipeline`: pipeline basic info
- **Spark UI:** detailed performance insights
    - Timeline analysis: task duration, overlap, bottlenecks
    - Task-Level details: run time, cluster, I/O, logs, etc.
    - Query performance details: execution plan, metrics, etc.
    - Actionable insights: high planning time, high execution time, resource bottleneck

## Production considerations

- **Selecting compute:**
    - Interactive clusters: for add-hoc analysis and development (fast start-up, costly, low scalability)
    - Job clusters: ideal for jobs (terminates if the job is finished, but start-up time is longer)
    - Serverless clusters: best overall performance (higher cost, lower start-up time, scalability)
- **Pricing components:**
    - DBUs: compute resources (CPU, memory, storage) to Databricks
    - Infrastructure costs: costs to cloud providers (AWS, Azure, GCP)
    - Operational costs: work time, etc.
      In the case of serverless, only DBUs are charged.
- **Modular design:** break complex DAGs into smaller, more manageable units (each represents a business context, not
  a technical task)
    - Parent-Child relationship: a parent job can have multiple child jobs
    - Reuse: tasks can be shared between jobs
    - Testability: each child job can be run independently
- **Version control:** jobs can run notebooks from a Git repository
    - CI/CD support: jobs can be triggered automatically on Git push
    - Support for multiple Git providers: GitHub, GitLab, etc.
    - Git connection can be selected per workspace
