# `dea_learning` — End-to-End Medallion Pipeline

The reference pipeline that ties every week's code together. Bronze → Silver → Gold over the
`customers × orders × items` domain (see `DATA_MODEL.md`), orchestrated by a Lakeflow Job, deployed via a
Declarative Automation Bundle, governed by Unity Catalog.

## 1. Data-flow diagram

```
                       SOURCE FILES (week_2_ingestion/data, week_4_…/data)
                       ┌─────────────────────────────────────────────────────┐
                       │ customers_seed.csv          (one-shot CSV)          │
                       │ items.csv                   (one-shot CSV)          │
                       │ orders_2026-06-0{1,2,3}.json  (continuous JSON)     │
                       │ orders_drift.json           (drift demo)            │
                       │ customers_cdc_events.json   (CDC INSERT/UPD/DEL)    │
                       └────────────────────────┬────────────────────────────┘
                                                │  one-time upload by
                                                │  week_2 / code / 00_setup_catalog_and_seed.py
                                                ▼
                       /Volumes/dea_learning/raw/landing
                       ├── customers/  ├── items/  ├── orders/
                       ├── orders_drift/           └── cdc/
                                                │
                                                ▼
   ╔════════════════════════════ BRONZE (raw ingest) ═════════════════════════════╗
   ║                                                                              ║
   ║  ┌─ bronze_customers ──────────┐   STREAM read_files(...csv)                 ║
   ║  ├─ bronze_items ──────────────┤   MATERIALIZED VIEW read_files(...csv)      ║
   ║  ├─ bronze_orders ─────────────┤   STREAM read_files(...json)                ║
   ║  └─ cdc_customer_events ───────┘   STREAM read_files(.../cdc, json)          ║
   ║                                                                              ║
   ╚════════════════════════════════════╤═════════════════════════════════════════╝
                                        │  expectations enforced from this layer on
                                        ▼
   ╔════════════════════════════ SILVER (cleansed) ═══════════════════════════════╗
   ║                                                                              ║
   ║  ┌─ silver_customers ──────────┐   trim, lower-case email, derive region     ║
   ║  ├─ silver_items ──────────────┤   trim, lower-case category                 ║
   ║  ├─ silver_orders ─────────────┤   status / currency validation, dates       ║
   ║  ├─ silver_order_items ────────┤   EXPLODE bronze_orders.items               ║
   ║  ├─ customers_scd1 ────────────┤   AUTO CDC INTO … STORED AS SCD TYPE 1      ║
   ║  └─ customers_scd2 ────────────┘   AUTO CDC INTO … STORED AS SCD TYPE 2      ║
   ║                                                                              ║
   ╚════════════════════════════════════╤═════════════════════════════════════════╝
                                        │  joins, aggregations
                                        ▼
   ╔════════════════════════════  GOLD (BI surfaces) ═════════════════════════════╗
   ║                                                                              ║
   ║  ┌─ gold_daily_revenue ───────┐   GROUP BY order_date, region                ║
   ║  ├─ gold_top_customers ───────┤   GROUP BY customer_id (LTV ranking)         ║
   ║  └─ gold_top_items ───────────┘   GROUP BY item_id, category                 ║
   ║                                                                              ║
   ╚════════════════════════════════════╤═════════════════════════════════════════╝
                                        │
                                        ▼
                       ┌──────────────────────────────────────────────┐
                       │  Delta Share  →  partners / external BI      │
                       │  Dashboards / SQL warehouse  →  internal BI  │
                       └──────────────────────────────────────────────┘
```

## 2. Layer-by-layer responsibilities

### Bronze — "what arrived"
- **Append-only**, raw schema preserved.
- Streaming tables for files that keep arriving (`orders`, `customers`, CDC events).
- Materialized view for the tiny static catalog (`items`).
- Adds `_metadata.file_path` so every row is traceable to its source file.
- No expectations — quality is enforced downstream.

### Silver — "what is true"
- One-row-per-key, conformed types, normalized strings.
- Joins to `bronze_items` happen here when enriching line items.
- **Expectations** enforced (`CHECK … ON VIOLATION DROP ROW` / `FAIL UPDATE`):
  - `silver_customers`: `customer_id IS NOT NULL`, `email` regex valid
  - `silver_orders`: `amount > 0`, `status` enumerated, `currency` length 3
  - `silver_order_items`: `quantity > 0`, `unit_price >= 0`
- `EXPLODE` flattens the nested `orders.items` array into a row-per-line-item table.
- CDC arrives separately and produces `customers_scd1` (current state) and `customers_scd2` (history).

### Gold — "what's useful"
- Materialized views (refresh-on-demand, cheap to query, BI-friendly).
- `gold_daily_revenue` — daily revenue per region, primary BI dashboard surface.
- `gold_top_customers` — LTV ranking for marketing.
- `gold_top_items` — bestsellers by category for merchandising.

## 3. Source → Sink summary

| Source file | Bronze | Silver | Gold |
| --- | --- | --- | --- |
| `customers_seed.csv` | `bronze_customers` | `silver_customers` | `gold_top_customers` |
| `items.csv` | `bronze_items` | `silver_items` | `gold_top_items` |
| `orders_*.json` | `bronze_orders` | `silver_orders`, `silver_order_items` | `gold_daily_revenue`, `gold_top_items` |
| `cdc/customers_cdc_events.json` | `cdc_customer_events` | `customers_scd1`, `customers_scd2` | — |

## 4. Notebook & resource map

| File | Role |
| --- | --- |
| `week_2_ingestion/code/00_setup_catalog_and_seed.py` | One-time: create catalog/schemas/volume, upload data files |
| `week_4_pipelines_and_jobs/code/01_pipeline_bronze_silver_gold.sql` | The whole declarative pipeline — bronze + silver + gold for all entities |
| `week_4_pipelines_and_jobs/code/02_pipeline_auto_cdc_scd2.sql` | CDC pipeline — produces `customers_scd1` + `customers_scd2` |
| `week_4_pipelines_and_jobs/code/03_lakeflow_job_definition.json` | Lakeflow Job orchestrating both pipelines, with conditional and for-each tasks |
| `week_4_pipelines_and_jobs/code/04_query_lakeflow_system_tables.sql` | Observability queries over `system.lakeflow.*` |
| `week_5_cicd_and_troubleshooting/code/databricks.yml` | DAB packaging the pipeline + job for dev / staging / prod |

## 5. Orchestration (Lakeflow Job)

```
┌────────────────────┐
│   ingest_setup     │  (notebook task: 00_setup_catalog_and_seed.py)
│   refresh landing  │
└──────────┬─────────┘
           │
   ┌───────┴────────────────────────────┐
   │                                    │
   ▼                                    ▼
┌─────────────────┐               ┌────────────────────┐
│ medallion_pipe  │               │  cdc_pipeline      │
│ bronze→silver→  │               │  AUTO CDC INTO     │
│ gold            │               │  scd1 / scd2       │
└──────┬──────────┘               └────────────────────┘
       │
       ├──────────────────────────────────────────────────┐
       │                                                  │
       ▼                                                  ▼
┌──────────────┐  (if/else)                  ┌────────────────────────┐
│ weekday_audit│  day_of_week < 6 ?          │  for_each_region       │
└──────┬───────┘                              │  region in [EU,NA,…]   │
       │ true                                 │  → region_report (×N)  │
       ▼                                      └────────────────────────┘
┌──────────────────┐
│  quality_audit   │  SQL task on gold tables
└──────────────────┘
```

Trigger: scheduled at 04:00 Europe/Berlin daily. Email on failure.

## 6. Data quality expectations

Aggregated in one place for the exam-day drill:

| Where | Constraint | Mode |
| --- | --- | --- |
| `silver_customers` | `customer_id IS NOT NULL` | `ON VIOLATION DROP ROW` |
| `silver_customers` | `email RLIKE '.+@.+\\..+'` | `ON VIOLATION DROP ROW` |
| `silver_orders` | `amount > 0` | `ON VIOLATION DROP ROW` |
| `silver_orders` | `status IN ('placed','shipped','delivered','cancelled')` | WARN (default) |
| `silver_orders` | `length(currency) = 3` | WARN (default) |
| `silver_order_items` | `quantity > 0` | `ON VIOLATION DROP ROW` |
| `silver_order_items` | `unit_price >= 0` | WARN |

Violations land in the pipeline event log. Inspect via `event_log("<pipeline_id>")`.

## 7. CI/CD deployment (DAB)

`week_5_cicd_and_troubleshooting/code/databricks.yml` declares the same pipeline + job for three targets:

```
            ┌─────────────────┐
            │   developer's   │
            │   Git Folder    │
            └────────┬────────┘
                     │ commit + push
                     ▼
            ┌─────────────────┐
            │   feature PR    │── validate -t dev → deploy -t dev  (auto on push to feature branch)
            └────────┬────────┘
                     │ merge
                     ▼
            ┌─────────────────┐
            │      main       │── deploy -t staging  (auto, run integration tests)
            └────────┬────────┘
                     │ manual approval
                     ▼
            ┌─────────────────┐
            │   production    │── deploy -t prod
            └─────────────────┘
```

Per-target catalog: `dea_learning_dev`, `dea_learning_staging`, `dea_learning` (prod uses the canonical name).

## 8. Governance overlay (Week 6)

Applied on top of the deployed silver/gold tables:

- **Grants**: analysts → `SELECT` on gold; engineers → full control on bronze + silver; prod SP → `MODIFY` on bronze + silver; marketing user → `SELECT` on `gold.eu_daily_revenue` view only.
- **Row filter**: `region_filter()` on `silver_customers.region` — regional teams see only their rows.
- **Column mask**: `mask_email()` on `silver_customers.email` — only `pii_readers` see the real value.
- **ABAC tags**: `pii=true` tagged on PII columns across the medallion so a single ABAC policy enforces masking everywhere.
- **Delta Share**: `dea_revenue_share` publishes `gold_daily_revenue` + `gold_top_items` to partner workspaces. `silver_customers` is **never** in a share (PII).

## 9. Running it end-to-end

1. `week_2_ingestion/code/00_setup_catalog_and_seed.py` — bootstrap catalog and upload data files.
2. **Week 4 pipeline 1** — create a Spark Declarative Pipeline pointed at `01_pipeline_bronze_silver_gold.sql`. Target catalog `dea_learning`, target schema `bronze`. Run an update.
3. **Week 4 pipeline 2** — create a second pipeline pointed at `02_pipeline_auto_cdc_scd2.sql`. Target catalog `dea_learning`, target schema `silver`. Run an update.
4. **Week 4 job** — import `03_lakeflow_job_definition.json`, replace pipeline IDs, run it manually once.
5. **Week 5 DAB** — copy `databricks.yml` into a bundle scaffold (`databricks bundle init`), then promote `dev → staging → prod`.
6. **Week 6 governance** — apply the row filter + column mask + grants + ABAC tags from `week_6_governance/code/`.
7. Inspect:
   ```sql
   SELECT * FROM dea_learning.gold.gold_daily_revenue ORDER BY order_date, region;
   SELECT * FROM dea_learning.gold.gold_top_customers LIMIT 10;
   SELECT * FROM dea_learning.gold.gold_top_items     LIMIT 10;
   ```

## 10. What this teaches for the exam

Touching this pipeline forces you through every exam section in context:

| Exam § | Where it lives in this pipeline |
| --- | --- |
| §1 Platform | Catalog/schema/volume creation; managed vs external; DBU rate per cluster |
| §2 Ingestion | `read_files` + Auto Loader for orders, `COPY INTO`/CTAS for static, CDC for CDC |
| §3 Transformation | Silver cleansing, joins, `EXPLODE`, aggregations in gold |
| §4 Pipelines + Jobs | Both declarative pipelines, the Lakeflow Job, conditional + for-each tasks |
| §5 CI/CD | DAB with dev/staging/prod targets |
| §6 Troubleshooting | Spark UI / Liquid Clustering / `system.lakeflow` over these tables |
| §7 Governance | Grants, row filter, column mask, ABAC tags, Delta Share over silver/gold |