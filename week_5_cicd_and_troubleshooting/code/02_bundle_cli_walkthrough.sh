#!/usr/bin/env bash
# Week 5 · Declarative Automation Bundles — CLI walkthrough.
# Run on your local machine after `databricks auth login --host https://<workspace>`.

set -euo pipefail

# 1. Scaffold a new bundle in an empty directory ----------------------------
mkdir -p /tmp/bundle_demo && cd /tmp/bundle_demo
databricks bundle init                              # pick e.g. "default-python"

# 2. Inspect what was generated --------------------------------------------
ls -la
cat databricks.yml | head -50

# 3. Validate the bundle against a target ----------------------------------
databricks bundle validate -t dev

# 4. Deploy to dev (uploads files + creates/updates resources) -------------
databricks bundle deploy -t dev

# 5. Trigger a deployed job/pipeline and tail its progress -----------------
databricks bundle run orders_etl -t dev    # use the resource key from your databricks.yml

# 6. Show deployed-resource URLs -------------------------------------------
databricks bundle summary -t dev

# 7. Promote to staging then production (manual approval in CI) ------------
databricks bundle validate -t staging
databricks bundle deploy   -t staging

databricks bundle validate -t prod
databricks bundle deploy   -t prod

# 8. Adopt an existing UI-built job into the bundle ------------------------
databricks bundle generate job --existing-job-id 12345 > resources/imported_job.yml
databricks bundle deployment bind imported_job 12345 -t dev

# 9. Tear down a target (destructive!) -------------------------------------
# databricks bundle destroy -t dev