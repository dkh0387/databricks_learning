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
- **Databricks deployment tools:** compile, test , deploy

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
- Example: `test_spark_dataframe_equality.py`

## Integration testing

- **SPD Expectations:** validate results of tables within pipelines (like MVs for row counts, column counts, etc.)
- **Job Tasks:** integration tests as separate notebooks, integrated in jobs as tasks

### Version control with Git

- **PAT:** personal access token to be used for authentication in Databricks
- **Add PAT to Databricks:** `Settings > Developer Settings > Personal access token`
- **Databricks Git Folders:** `Workspace > Users > <username> > Create Git Folder`
- **Assets Deployment:**
    - REST API: `POST /2.0/git/repositories/<repo-id>/deploy` (best for custom integrations)
    - CLI: `databricks git deploy <repo-id>` (best for local development)
    - SDK: `workspace.deploy_git_repo(...)` (best for embedded Databricks functionality in applications)
    - Easer to use: (easy) SDK > CLI > REST API (hard)
    - Flexibility: (flexibel) REST API > SDK > CLI (unflexibel)
    - **Databricks Asset Bundles (DAB):** the most recommended way to deploy assets to Databricks
        - Version control
        - Code review
        - Testing
        - Continuous integration
        - Infrastructure as code: assets like jobs, pipelines, and notebooks are defined as source files and metadata in
          YAML format (`databricks.yml`)