-- Gold quality audit — save this as a workspace saved query ("gold_quality_audit"),
-- then paste its query ID into 03_lakeflow_job_definition.json → quality_audit.sql_task.query.query_id.
--
-- Pattern: compute violation counts per check; raise_error() makes the query — and therefore
-- the job task — FAIL when any check is violated. Zero rows returned = audit passed.
--
-- The pipeline publishes its gold MVs fully qualified into dea_learning.gold (see PIPELINE.md §9).

WITH violations AS (
  SELECT 'gold_daily_revenue: NULL/negative revenue or bad date' AS check, count(*) AS n
  FROM dea_learning.gold.gold_daily_revenue
  WHERE revenue IS NULL OR revenue < 0
     OR order_date IS NULL OR order_date > current_date()

  UNION ALL
  SELECT 'gold_daily_revenue: table empty after refresh', CASE WHEN count(*) = 0 THEN 1 ELSE 0 END
  FROM dea_learning.gold.gold_daily_revenue

  UNION ALL
  SELECT 'gold_top_10_customers: rank > 10, NULL key, or non-positive spend', count(*)
  FROM dea_learning.gold.gold_top_10_customers
  WHERE rank > 10 OR customer_id IS NULL OR lifetime_spend <= 0

  UNION ALL
  SELECT 'gold_top_10_items: rank > 10 or non-positive units/revenue', count(*)
  FROM dea_learning.gold.gold_top_10_items
  WHERE rank_revenue > 10 OR units_sold <= 0 OR revenue <= 0
)
SELECT
  check,
  n AS violation_count,
  raise_error(concat('Gold quality audit failed: ', check, ' (', n, ' rows)')) AS status
FROM violations
WHERE n > 0;