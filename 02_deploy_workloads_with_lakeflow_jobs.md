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
- Trigger: Schedule, event, manual, continuous, REST, etc.
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