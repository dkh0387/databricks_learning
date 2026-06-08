# Worked Data Project — `dea_learning`

A single small e-commerce domain threads through every week's code samples. By Week 6 you've built a complete medallion pipeline over it.

## Domain

```
┌──────────────────┐         ┌──────────────────┐
│    customers     │         │      items       │
│  customer_id PK  │         │  item_id     PK  │
│  name             │         │  name             │
│  email            │         │  category         │
│  country          │         │  price            │
│  region (derived) │         │  in_stock         │
│  signup_date      │         └──────────────────┘
│  tier             │                  ▲
└──────────────────┘                   │
         ▲                             │
         │ 1:N                         │ N:M (via nested array)
         │                             │
         │           ┌─────────────────┴───────────────┐
         │           │              orders             │
         │           │  order_id     PK                │
         └───────────┤  customer_id  FK → customers    │
                     │  order_ts     TIMESTAMP         │
                     │  status                         │
                     │  currency                       │
                     │  amount                         │
                     │  items: ARRAY<STRUCT<           │
                     │    item_id     FK → items,      │
                     │    quantity,                    │
                     │    unit_price                   │
                     │  >>                             │
                     └─────────────────────────────────┘
```

`orders.items` is a nested array — natural for an ingestion-from-JSON exercise. In silver it gets exploded to a flat `order_items` table.

## Layout in Unity Catalog

Catalog `dea_learning` with five schemas:

| Schema | Contents |
| --- | --- |
| `raw` | Volume holding the source files (`/Volumes/dea_learning/raw/landing/...`) |
| `bronze` | Raw ingest tables (`customers_bronze`, `items_bronze`, `orders_bronze`) |
| `silver` | Cleansed + conformed (`customers_silver`, `items_silver`, `orders_silver`, `order_items_silver`) |
| `gold` | Business-ready aggregates (`gold_daily_revenue`, `gold_top_customers`, `gold_top_items`) |
| `sec` | Security UDFs for row filters / column masks |

Replace `dea_learning` with any catalog name you can write to in your workspace. The sample code uses this name everywhere.

## Source files

Lives in `week_2_ingestion/data/`:

| File | Format | Used by |
| --- | --- | --- |
| `customers/customers_seed.csv` | CSV | `COPY INTO` demo |
| `items/items.csv` | CSV | One-shot `CREATE TABLE AS read_files()` |
| `orders/orders_2026-06-01.json` | JSON-lines | Auto Loader (clean schema) |
| `orders/orders_2026-06-02.json` | JSON-lines | Auto Loader (clean schema) |
| `orders/orders_2026-06-03.json` | JSON-lines | Auto Loader (introduces `discount_amount` → schema evolution) |
| `orders/orders_drift.json` | JSON-lines | Auto Loader `rescue` mode (type/case mismatches) |

CDC events live in `week_4_pipelines_and_jobs/data/cdc/`:

| File | Format | Used by |
| --- | --- | --- |
| `customers_cdc_events.json` | JSON-lines | `AUTO CDC INTO` SCD1 / SCD2 demos |

## Week-by-week thread

| Week | What you do with the domain |
| --- | --- |
| 1 — Platform | Use a tiny inline `events` table to drill Delta basics. Domain not yet introduced. |
| 2 — Ingestion | Load all three source datasets into `bronze` via `COPY INTO`, Auto Loader, and `read_files()`. |
| 3 — Transformation | Build `silver` from `bronze`: clean nulls/types, dedup customers, explode `orders.items` → `order_items_silver`, join to `items` for price enrichment. |
| 4 — Pipelines + Jobs | Wire the whole thing as a Spark Declarative Pipeline (bronze → silver → gold), add CDC into `customers_silver` (SCD1 + SCD2), orchestrate with a Lakeflow Job. |
| 5 — CI/CD + Troubleshooting | Deploy the pipeline as a DAB across `dev` / `staging` / `prod` targets. Induce skew on a heavy customer for the Spark UI drill. |
| 6 — Governance | Grants on the three silver/gold tables, row filter by `region`, column mask on `email`, Delta Share `gold_daily_revenue` outbound. |

## Seed table previews

### `customers_seed.csv` (20 rows)

```
customer_id,name,email,country,signup_date,tier
1,Anna Müller,anna.muller@example.com,DE,2025-01-15,gold
2,Bob Johnson,bob.johnson@example.com,US,2025-02-20,silver
…
```

### `items.csv` (15 rows)

```
item_id,name,category,price,in_stock
SKU-A001,Wireless Mouse,electronics,29.99,true
SKU-A002,Bluetooth Keyboard,electronics,79.99,true
SKU-B001,Office Chair,furniture,249.99,false
…
```

### `orders_2026-06-01.json` (JSON-lines)

```json
{"order_id":1001,"customer_id":1,"order_ts":"2026-06-01T09:23:00Z","status":"placed","currency":"EUR","amount":139.97,"items":[{"item_id":"SKU-A001","quantity":2,"unit_price":29.99},{"item_id":"SKU-A002","quantity":1,"unit_price":79.99}]}
```

## Setup once

Before running any code, in your workspace:

```sql
CREATE CATALOG IF NOT EXISTS dea_learning;
CREATE SCHEMA IF NOT EXISTS dea_learning.raw;
CREATE SCHEMA IF NOT EXISTS dea_learning.bronze;
CREATE SCHEMA IF NOT EXISTS dea_learning.silver;
CREATE SCHEMA IF NOT EXISTS dea_learning.gold;
CREATE SCHEMA IF NOT EXISTS dea_learning.sec;
CREATE VOLUME IF NOT EXISTS dea_learning.raw.landing;
```

Then upload the `week_2_ingestion/data/` and `week_4_pipelines_and_jobs/data/` files into the volume — each week's first notebook contains a `dbutils.fs.cp(...)` cell that does this for you.