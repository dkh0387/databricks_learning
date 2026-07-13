# Databricks notebook source
# MAGIC %md
# MAGIC # Week 4 · weekday_check
# MAGIC Tiny helper task referenced by `03_lakeflow_job_definition.json`. Computes the current day-of-week (1=Mon … 7=Sun)
# MAGIC and emits it as a **task value** so a downstream `condition_task` can branch on it.
# MAGIC
# MAGIC Why a separate notebook: this demonstrates the **task values** pattern — `dbutils.jobs.taskValues.set` in an
# MAGIC upstream notebook, referenced downstream via `{{tasks.<task_key>.values.<key>}}`. In production the simpler
# MAGIC direct approach is the dynamic value reference `{{job.start_time.iso_weekday}}` (or `{{job.start_time.is_weekday}}`,
# MAGIC `{{job.trigger.time.iso_weekday}}`), which a `condition_task` can consume without any helper task.

# COMMAND ----------

from datetime import datetime, timezone

# 1..7 with Monday=1, Sunday=7 — matches ISO 8601
day_of_week = datetime.now(timezone.utc).isoweekday()

dbutils.jobs.taskValues.set(key="day_of_week", value=day_of_week)
print(f"day_of_week emitted: {day_of_week}")
