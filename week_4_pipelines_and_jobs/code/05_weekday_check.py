# Databricks notebook source
# MAGIC %md
# MAGIC # Week 4 · weekday_check
# MAGIC Tiny helper task referenced by `03_lakeflow_job_definition.json`. Computes the current day-of-week (1=Mon … 7=Sun)
# MAGIC and emits it as a **task value** so a downstream `condition_task` can branch on it.
# MAGIC
# MAGIC Why a separate notebook: Databricks `condition_task` cannot read `{{job.trigger.day_of_week}}` directly
# MAGIC (no such dynamic value reference exists). Task values, set by an upstream notebook, are the supported way to
# MAGIC parameterize branches.

# COMMAND ----------

from datetime import datetime, timezone

# 1..7 with Monday=1, Sunday=7 — matches ISO 8601
day_of_week = datetime.now(timezone.utc).isoweekday()

dbutils.jobs.taskValues.set(key="day_of_week", value=day_of_week)
print(f"day_of_week emitted: {day_of_week}")
