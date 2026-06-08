-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 4 · Querying `system.lakeflow` for job/pipeline observability
-- MAGIC These tables are the source of truth for run-history trend analysis.

-- COMMAND ----------

-- All jobs in your account
SELECT job_id, name, creator_id, change_time
FROM   system.lakeflow.jobs
ORDER BY change_time DESC
LIMIT 20;

-- COMMAND ----------

-- Recent runs of a specific job (replace job_id)
SELECT run_id, result_state, period_start_time, period_end_time,
       (unix_timestamp(period_end_time) - unix_timestamp(period_start_time)) AS duration_s
FROM   system.lakeflow.job_run_timeline
WHERE  job_id = REPLACE_WITH_JOB_ID
  AND  period_start_time > current_date() - INTERVAL 14 DAYS
ORDER BY period_start_time DESC;

-- COMMAND ----------

-- Slowest tasks last 7 days
SELECT job_id, task_key,
       count(*)                                                                AS runs,
       avg(unix_timestamp(period_end_time) - unix_timestamp(period_start_time)) AS avg_s,
       max(unix_timestamp(period_end_time) - unix_timestamp(period_start_time)) AS max_s
FROM   system.lakeflow.job_task_run_timeline
WHERE  period_start_time > current_date() - INTERVAL 7 DAYS
GROUP BY job_id, task_key
ORDER BY avg_s DESC
LIMIT 20;

-- COMMAND ----------

-- Failure rate per job last 30 days
SELECT job_id,
       count(*)                                                                  AS total,
       sum(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END)                  AS failed,
       sum(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) * 1.0 / count(*) AS fail_rate
FROM   system.lakeflow.job_run_timeline
WHERE  period_start_time > current_date() - INTERVAL 30 DAYS
GROUP BY job_id
HAVING fail_rate > 0
ORDER BY fail_rate DESC;