# DevOps Essentials for Data Engineering

> Hands-on CI/CD with Git Folders and Declarative Automation Bundles is in `learn_cicd.md`.
> This note covers the conceptual foundations.

## Software Engineering best practices, DevOps, and CI/CD fundamentals

### Key concepts:

- **Clean code:** readability, naming, maintainability, and reusability (usage of linting tools)
- **Documentation:** comments, docstrings, and READMEs
- **Automated testing:** unit tests (single unit), integration tests (multiple units), and end-to-end tests (integration
  with external systems)
- **Version Control & code review:** tracking changes, collaborating with others, history, knowledge sharing and early
  bug detection
- **Continuous Integration (CI):** continuous integration of code changes (commit, build, test to a repository)
- **Continuous Deployment (CD):** automated deployment to production (release to production after successful CI)
- **Isolated environments:** multiple workspaces and catalogs for development, staging, and production

### Databricks SE tools:

- **Databricks workspaces:** develop and run
- **Databricks Git folders:** version control
- **Unity Catalog:** leverages data for multiple workspaces
- **Databricks deployment tools:** compile, test, deploy

## Modularizing of PySpark code

- Split the code into multiple modules for better modifiability, testability, and reusability

## DevOps fundamentals

- **Definition:** process of continuously integrating, testing and deploying of code (bridge between development and
  delivery)
- **Benefits:**
    - Faster deployment cycles
    - Improved collaboration between teams
    - Enhanced system reliability
    - Better scalability and efficiency
- **Phases:**
    - _Dev & CI:_
        - Planning
        - Coding
        - Building (compilation)
        - Testing
    - _Ops & CD:_
        - Release (packaging)
        - Deployment (delivery)
        - Operation (monitoring and maintenance)
        - Monitoring
- **DataOps:** DevOps for data engineering
    - Management of data pipelines and workflows
    - Reliance on data flows from collection to processing to consumption
- **MLOps:** applying DataOps to Machine Learning
    - Management and deployment of ML models
    - Short time to production (TTP)

## CI/CD in DevOps

- **Overview:** automates and streamlines the entire software development lifecycle, improves the code quality,
  automates prod deployment
- **CI/CD process:** development, delivery and deployment in short cycles
- **CI/CD adaption:** standard in software development, highly adapted in data engineering
- **CI:**
    - merging code changes into a repository and running automated tests
    - branch strategy: feature branching (longer living feature branches), trunk-based development (frequent merges to
      the main branch)
    - _Benefits:_
        - Early detection of bugs
        - Faster delivery
        - Better collaboration
        - Automated testing
    - _Testing pyramid:_
        - Unit: small, isolated units testing
        - Integration: integration testing between units
        - System: end-to-end testing for production
- **CD:** continuous pushing of code changes to target environments

## Unit testing for PySpark

- `pyspark.testing.utils`: provides helper functions for testing PySpark (`assertDataFrameEqual(...)`,
  `assertSchemaEqual(...)`, etc.)

## Integration testing

- **SDP Expectations:** validate results of tables within Spark Declarative Pipelines (constraints on row counts,
  column ranges, business rules) — see `../week_4_pipelines_and_jobs/learn_pipelines.md`
- **Job Tasks:** integration tests as separate notebooks, integrated in Lakeflow Jobs as tasks

## Version control with Git

- **PAT:** personal access token used for authentication between Databricks and the Git provider
- **Add PAT to Databricks:** `Settings > Linked accounts > Git integration`
- **Databricks Git Folders** (formerly Repos): `Workspace > Users > <username> > Create > Git Folder`
- **Asset deployment options:**
    - **Declarative Automation Bundles (DAB, formerly Databricks Asset Bundles):** the recommended way to deploy
      jobs, pipelines, notebooks, and other resources as code. CLI: `databricks bundle init / validate / deploy / run`.
      Bundle configuration in `databricks.yml`. Full hands-on coverage in
      `learn_cicd.md`.
    - **Databricks CLI** (`databricks` binary): wraps the REST API. Used for ad-hoc operations and inside CI runners.
    - **Databricks SDKs** (`databricks-sdk` for Python / Java / Go): embed Databricks operations in applications.
    - **REST API** (`/api/2.0/...`, `/api/2.1/...`): lowest level, used by everything above. Most flexible, most code.
    - Ease of use: **DAB > CLI > SDK > REST API** (DAB is declarative; the rest are imperative).
    - Flexibility: **REST API > SDK > CLI > DAB** (DAB targets the common case).
- **What DAB gives you:**
    - Version control of infrastructure alongside source code
    - Code review of resource changes via PRs
    - Repeatable deployments across dev / staging / prod
    - Continuous integration via `databricks bundle deploy -t <target>` in CI
    - Infrastructure as code: jobs, pipelines, notebooks declared as YAML in `databricks.yml`