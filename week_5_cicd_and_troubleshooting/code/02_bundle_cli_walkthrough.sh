#!/usr/bin/env bash
# Week 5 · Declarative Automation Bundles — CLI walkthrough.
# Runs against THIS repo's real bundle: week_5_cicd_and_troubleshooting/ (databricks.yml + src/ + tests/).

set -euo pipefail

# 0. One-time prerequisites --------------------------------------------------
# CLI version matters: old versions (< 1.x) fail with
#   "error downloading Terraform: unable to verify checksums signature: openpgp: key expired"
brew upgrade databricks                              # → v1.x
databricks --version

# OAuth login, bound to a NAMED profile. A major CLI upgrade invalidates the old token cache
# ("stored credentials from older CLI versions are no longer used") → just log in again.
# If several ~/.databrickscfg profiles point at the same host, the CLI shows an interactive
# picker — that's why our databricks.yml pins `workspace.profile: adb-185960349365378`.
databricks auth login \
  --host https://adb-185960349365378.18.azuredatabricks.net \
  --profile adb-185960349365378

# 0.5 One-time: create the per-env catalogs BEFORE the first deploy ----------
# The pipelines API validates `catalog` at resource creation — deploying against a missing
# catalog fails with 404 CATALOG_DOES_NOT_EXIST. Catalogs are long-lived infra; schemas,
# volume and seed data INSIDE them are created later by the job's ingest_setup task.
for env in dev staging prod; do
  databricks catalogs create "dea_learning_${env}" \
    --storage-root "abfss://unity-catalog-storage@dbstorageghdo4vkcqfmqq.dfs.core.windows.net/185960349365378/dea_learning_${env}" \
    -p adb-185960349365378
done

# 1. Work from the bundle root (the folder holding databricks.yml) -----------
cd "$(git rev-parse --show-toplevel)/week_5_cicd_and_troubleshooting"

# (For a greenfield project you would scaffold instead:  databricks bundle init)

# 2. Run the unit tests locally — tests/ is excluded from sync, it runs in CI --
python3 -m pytest tests/ -q

# 3. Validate the bundle against a target ------------------------------------
databricks bundle validate -t dev

# 4. Deploy to dev (uploads src/**, creates/updates pipeline + job) ----------
# mode:development prefixes all resources with "[dev <user>]" and pauses schedules.
databricks bundle deploy -t dev

# 5. Trigger the deployed job and tail its progress --------------------------
# First run: the ingest_setup task creates catalog dea_learning_dev + schemas + volume + seed data,
# then the medallion pipeline publishes into dea_learning_dev.bronze/silver/gold.
databricks bundle run orders_etl -t dev

# 6. Show deployed-resource URLs ---------------------------------------------
databricks bundle summary -t dev

# 7. Promote to staging, then prod (same workspace, separate catalogs) -------
databricks bundle validate -t staging && databricks bundle deploy -t staging   # → dea_learning_staging
databricks bundle validate -t prod    && databricks bundle deploy -t prod      # → dea_learning_prod

# 8. Adopt an existing UI-built job into the bundle --------------------------
# generate writes the YAML file itself (-d sets the resources dir, -s the src dir)
databricks bundle generate job --existing-job-id 12345 -d resources
databricks bundle deployment bind imported_job 12345 -t dev

# 9. Tear down a target (destructive! removes the deployed job + pipeline) ---
# databricks bundle destroy -t dev