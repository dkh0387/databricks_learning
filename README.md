# Databricks Learning

Study materials and demos for the **Databricks Certified Data Engineer Associate** exam (May 2026 version).

- Start with `LEARNING_PATH.md` for the full 6-week study plan.
- See `DATA_MODEL.md` for the unified `customers × orders × items` example that threads through every week.
- See `PIPELINE.md` for the end-to-end medallion pipeline diagram, orchestration, expectations, and run instructions.
- Content lives under `week_1_platform/` → `week_6_governance/`. Each folder has `learn*.md` (theory), `glossary.md` (key terms), `code/` (runnable Databricks notebooks / SQL / Python), and where relevant `data/` (source files).
- All code samples use the `dea_learning` catalog. Run `week_2_ingestion/code/00_setup_catalog_and_seed.py` once to create the catalog/schemas/volume and upload the source files.
- The full Udemy course materials are kept separate under `udemy_databricks_certified_data_engineer_associate/`.
- This README documents the local **SQL Server CDC demo** used in `week_2_ingestion/learn_lakeflow_connect.md`.

# Docker:

- Start:

```bash
  docker compose up -d
```

- Check logs:

```bash
  docker logs lakeflow-sqlserver
```

- Activate SQL Server Agent for CDC logs:

```bash
  docker exec -it lakeflow-sqlserver bash
  /opt/mssql/bin/mssql-conf set sqlagent.enabled true
  exit
  docker restart lakeflow-sqlserver
```

- Prove whether SQL Server Agent is running:

```sql
  SELECT servicename, status_desc
  FROM sys.dm_server_services;
```

# CDC and CT Tracking:

## Prepare SQL Server for ingestion using the utility objects script

- See: https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-utility

## Set up DDL capture and schema evolution

- See: https://docs.databricks.com/aws/en/archive/connectors/sql-server-ddl-legacy

# Serve:

- Expose localhost (SQL Server):

```bash
  ngrok tcp 1433
```

- You get something like: `tcp://0.tcp.ngrok.io:1234 -> localhost:1433`
- Tunnel check (SQL Server client — `psql` is PostgreSQL only and will not work here):

```bash
sqlcmd \
  -S tcp:7.tcp.eu.ngrok.io,11356 \
  -U lakeflow \
  -P lakeflow \
  -d demo \
  -Q "SELECT @@VERSION"
```

- CDC configuration for Databricks:

  | Field    | Value               |
  | -------- | ------------------- |
  | Host     | `2.tcp.eu.ngrok.io` |
  | Port     | `12345`             |
  | Database | `demo`              |
  | User     | `lakeflow`          |
  | Password | `lakeflow`          |
  | Tables   | `orders`            |
  | Mode     | CDC                 |
