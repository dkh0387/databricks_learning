# CI/CD with Git Folders and Declarative Automation Bundles

Covers exam **§5 (10%)**. Supersedes the DevOps fundamentals in `learn_devops.md` with the up-to-date terminology and concrete hands-on syntax.

## 0. Terminology update (May 2026 rename)

| Old name | Current name (exam) |
| --- | --- |
| Databricks Repos | **Databricks Git Folders** |
| Databricks Asset Bundles (DAB) | **Declarative Automation Bundles** (still abbreviated DAB) |
| Delta Live Tables (DLT) | **Lakeflow Spark Declarative Pipelines** |

Source files and docs still say "Asset Bundles" and `databricks bundle …` — the CLI command name did **not** change.

## 1. Databricks Git Folders

Per-user or shared folders inside the workspace, backed by a remote Git repo. Lets you treat notebooks/files as code, branch, commit, push, and open PRs without leaving Databricks.

### Setup

1. Generate a PAT on the Git provider (GitHub / GitLab / Bitbucket / Azure DevOps).
2. `Settings → Linked accounts → Git integration` → paste PAT + Git provider + username.
3. `Workspace → Users → <you> → Create → Git Folder` → repo URL + branch.

### Day-to-day operations

All from the Git Folder UI (top of the folder in workspace):

| Action | Where |
| --- | --- |
| Switch / create branch | Branch dropdown |
| Pull latest | "Pull" button |
| Stage + commit + push | Side panel — pick files, write message, push |
| Open PR | "Create pull request" → opens the provider's PR page |
| Resolve conflicts | Use side panel diff, or pull on remote and re-clone |

### Best practices

- Each developer has their own Git Folder under `Workspace/Users/<email>/`.
- Production schedules **never run from a personal Git Folder** — production runs deployed assets (notebooks/jars/wheels) from the bundle's deploy root (default `/Workspace/Users/<principal>/.bundle/<bundle>/<target>/files/...`), written by a DAB deploy from CI.
- Keep notebooks small and modular — easier to diff and review.
- Use the bundle's `sync.exclude` (or a regular `.gitignore` in Git Folders) to keep junk out of deploys.

### Bypassing the UI — Git CLI

For local development:

```bash
git clone https://github.com/<org>/<repo>.git
# edit locally in your IDE (VS Code Databricks extension or PyCharm plugin)
git push
# in Databricks UI: Git Folder → Pull
```

## 2. Declarative Automation Bundles (DABs)

Source-controlled YAML definition of every Databricks resource you ship: Jobs, Lakeflow Spark Declarative Pipelines, dashboards, model endpoints, MLflow experiments, clusters. **Infrastructure as code for Databricks.**

### Why it exists

| Without DAB | With DAB |
| --- | --- |
| Hand-clicked jobs/pipelines in UI per env | Single `databricks.yml`, promote dev → staging → prod via `-t <target>` |
| Configuration drift between environments | One codebase, env-specific overrides |
| No PR review for infra changes | Every change goes through Git |
| Manual restoration after workspace loss | `databricks bundle deploy` rebuilds everything |

### Project structure

```
my_bundle/
├── databricks.yml              # root bundle config
├── resources/                  # optional: split per-resource YAMLs
│   ├── orders_job.yml
│   └── orders_pipeline.yml
├── src/                        # notebooks, .py, SQL
│   ├── ingest.py
│   └── transform.sql
├── tests/
│   └── test_transform.py
└── README.md
```

### Bootstrap from a template

```bash
databricks bundle init                       # interactive: pick "default-python" / "default-sql" / etc.
databricks bundle init mlops-stacks          # MLOps reference template
databricks bundle init /path/to/custom       # your own template
```

### `databricks.yml` anatomy

```yaml
# 1. Identity
bundle:
  name: orders_pipeline
  databricks_cli_version: ">=0.230.0"

# 2. Optional split-file includes
include:
  - resources/*.yml

# 3. Reusable variables
variables:
  catalog:
    description: "Target Unity Catalog"
    default: "dev_catalog"
  notification_email:
    description: "Where alerts go"
    default: "team@example.com"
  cluster_node_type:
    default: "i3.xlarge"

# 4. Files synced to the workspace
sync:
  include:
    - "src/**"
  exclude:
    - "**/__pycache__/**"
    - "tests/**"

# 5. Resource definitions (jobs, pipelines, clusters, …)
resources:
  jobs:
    orders_etl:
      name: "orders_etl_${bundle.target}"
      email_notifications:
        on_failure: ["${var.notification_email}"]
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ./src/ingest.py
          job_cluster_key: main
        - task_key: transform
          depends_on:
            - task_key: ingest
          pipeline_task:
            pipeline_id: ${resources.pipelines.orders_silver.id}
      job_clusters:
        - job_cluster_key: main
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: ${var.cluster_node_type}
            num_workers: 2

  pipelines:
    orders_silver:
      name: "orders_silver_${bundle.target}"
      catalog: ${var.catalog}
      target: silver
      libraries:
        - notebook:
            path: ./src/transform.sql

# 6. Environment-specific overrides
targets:
  dev:
    default: true
    mode: development              # auto-prefixes resources with [dev <user>], pauses schedules
    workspace:
      host: https://dev.cloud.databricks.com
    variables:
      catalog: dev_catalog

  staging:
    workspace:
      host: https://staging.cloud.databricks.com
    variables:
      catalog: staging_catalog
      cluster_node_type: i3.2xlarge

  prod:
    mode: production               # locks "run as" identity, no [dev] prefix, schedules enabled
    workspace:
      host: https://prod.cloud.databricks.com
    run_as:
      service_principal_name: "prod-deployer-sp"
    variables:
      catalog: prod_catalog
      cluster_node_type: i3.4xlarge
      notification_email: oncall@example.com
    permissions:
      - level: CAN_VIEW
        group_name: data_consumers
      - level: CAN_MANAGE
        group_name: data_platform_admins
```

### Modes

| `mode` | Effect |
| --- | --- |
| `development` (dev default) | Prefixes all resource names with `[dev <user>]`, pauses job schedules, marks pipelines as `development=true`. Safe to share a workspace. |
| `production` | No prefix, schedules active, requires `run_as` and explicit permissions, validates that resources won't collide. |

### Variable layers (precedence high → low)

1. CLI flag: `databricks bundle deploy -t prod --var="catalog=foo"`
2. Env var: `BUNDLE_VAR_catalog=foo`
3. Target's `variables` block
4. Top-level `variables.<name>.default`

Variables referenced with `${var.<name>}`. Bundle metadata with `${bundle.target}`, `${bundle.name}`, `${workspace.current_user.userName}`. Cross-resource refs with `${resources.pipelines.orders_silver.id}`.

### Complex variables

```yaml
variables:
  cluster_spec:
    type: complex
    default:
      spark_version: "15.4.x-scala2.12"
      node_type_id: "i3.xlarge"
      num_workers: 2
```

Then `new_cluster: ${var.cluster_spec}`.

## 3. CLI workflow

Install once:

```bash
brew tap databricks/tap && brew install databricks    # macOS
databricks auth login --host https://<workspace>      # OAuth, recommended
databricks auth profiles                              # list configured profiles
```

Day-to-day:

```bash
databricks bundle init                       # scaffold
databricks bundle validate                   # schema + reference check, no deploy
databricks bundle validate -t prod           # validate against a specific target
databricks bundle deploy -t dev              # upload files + create/update resources
databricks bundle run orders_etl -t dev      # trigger a deployed job/pipeline
databricks bundle summary -t dev             # show URLs of deployed resources
databricks bundle destroy -t dev             # tear down everything from that target
databricks bundle sync                       # one-way file sync for local-IDE dev loop
databricks bundle generate job --existing-job-id 123   # reverse-engineer YAML from UI-created job
```

### Typical local loop

```bash
# 1. Validate
databricks bundle validate -t dev

# 2. Deploy to your dev target
databricks bundle deploy -t dev

# 3. Run the job and tail it
databricks bundle run orders_etl -t dev

# 4. Iterate on src/transform.sql, re-deploy
databricks bundle deploy -t dev
```

## 4. CI/CD pipeline example (GitHub Actions)

```yaml
# .github/workflows/deploy.yml
name: Deploy bundle
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - name: Validate (staging target)
        env:
          DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST_STAGING }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_STAGING }}
        run: databricks bundle validate -t staging

  deploy_staging:
    needs: validate
    if: github.event_name == 'push'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - env:
          DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST_STAGING }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_STAGING }}
        run: |
          databricks bundle deploy -t staging
          databricks bundle run orders_etl -t staging   # smoke-test the deployed job

  deploy_prod:
    needs: deploy_staging
    runs-on: ubuntu-latest
    environment: production       # GitHub manual approval gate
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - env:
          DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST_PROD }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_PROD }}
        run: databricks bundle deploy -t prod
```

Equivalent shapes exist for GitLab CI, Azure DevOps Pipelines, Jenkins — only auth and runner syntax differ.

## 5. Tests in a bundle

Two patterns:

### a) Unit tests as a separate job task

```yaml
resources:
  jobs:
    orders_etl:
      tasks:
        - task_key: unit_tests
          spark_python_task:
            python_file: ./tests/run_pytest.py
        - task_key: transform
          depends_on: [{task_key: unit_tests}]
          notebook_task:
            notebook_path: ./src/transform.sql
```

Pre-commit: `pytest tests/` locally before `bundle deploy`.

### b) Pipeline expectations (Spark Declarative Pipelines)

Already covered in `../week_3_transformation/learn.md` §8 and `../week_4_pipelines_and_jobs/learn_pipelines.md`. These run as part of the pipeline update — failed `expect_or_fail` aborts CD.

## 6. Bind to existing UI-built resources

If a job was originally created in the UI and someone wants to bring it under bundle control:

```bash
# 1. Generate YAML from existing
databricks bundle generate job --existing-job-id 12345 > resources/imported_job.yml

# 2. Adopt it (bundle takes ownership without recreating)
databricks bundle deployment bind imported_job 12345 -t prod

# 3. To release control later
databricks bundle deployment unbind imported_job -t prod
```

## 7. Auth options

| Method | Use when |
| --- | --- |
| OAuth user-to-machine (`databricks auth login`) | Local development |
| OAuth machine-to-machine (Service Principal + secret) | CI/CD, recommended for prod |
| PAT (`DATABRICKS_TOKEN`) | Quick start, lower security |
| Azure CLI / AAD (`az login`) | Azure Databricks with AAD identity |

CI config minimum:

```bash
DATABRICKS_HOST="https://<workspace>.cloud.databricks.com"
DATABRICKS_CLIENT_ID="<sp-app-id>"
DATABRICKS_CLIENT_SECRET="<sp-secret>"
```

## 8. Branch strategy that pairs with DAB

- **Trunk-based**: feature branch → PR to `main` → auto-deploy to `staging` → manual promote to `prod`. Recommended.
- **GitFlow**: heavier, more branches; prefer only if release cadence is monthly+.
- One `target` per long-lived environment. Short-lived "feature target" (`dev_alice`) is fine — `mode: development` makes them isolated.

## 9. Exam-day quick reference

- DAB root file: `databricks.yml`, one per project, at the root.
- Top-level keys: `bundle`, `include`, `variables`, `sync`, `resources`, `targets`, `workspace`, `artifacts`, `run_as`, `permissions`.
- Variable reference: `${var.foo}`. Override precedence: CLI > env var > target > default.
- One target must be `default: true`.
- `mode: development` → adds `[dev <user>]` prefix, pauses schedules. `mode: production` → strict, needs `run_as`.
- Resource types in DAB: `apps`, `clusters`, `dashboards`, `experiments`, `jobs`, `models`, `model_serving_endpoints`, `pipelines`, `quality_monitors`, `registered_models` (UC), `schemas`, `secret_scopes`, `synced_database_tables`, `volumes`.
- Core CLI: `init`, `validate`, `deploy`, `run`, `summary`, `destroy`, `sync`, `generate`, `deployment bind/unbind`.
- Git Folders are for **development**; bundle-deployed paths default to `/Workspace/Users/<principal>/.bundle/<bundle>/<target>/…` (a `Shared/…` root requires an explicit `workspace.root_path` override).
- `databricks bundle generate` reverse-engineers UI-built resources to YAML.
- `databricks bundle deployment bind` adopts an existing resource without recreating it.

## References

- [Declarative Automation Bundles overview](https://docs.databricks.com/aws/en/dev-tools/bundles/)
- [databricks.yml configuration schema](https://docs.databricks.com/aws/en/dev-tools/bundles/settings)
- [Variables and substitutions](https://docs.databricks.com/aws/en/dev-tools/bundles/variables)
- [Resource types reference](https://docs.databricks.com/aws/en/dev-tools/bundles/resources)
- [Bundle templates](https://docs.databricks.com/aws/en/dev-tools/bundles/templates)
- [Databricks CLI reference](https://docs.databricks.com/aws/en/dev-tools/cli/)
- [Git Folders](https://docs.databricks.com/aws/en/repos/)
- [setup-cli GitHub Action](https://github.com/databricks/setup-cli)