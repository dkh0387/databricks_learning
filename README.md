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
- Tunnel check:

```bash
psql \
  -h 7.tcp.eu.ngrok.io \
  -p 11356 \
  -U lakeflow \
  demo
```

- CDC configuration for databricks:

  | Feld     | Wert                |
  | -------- | ------------------- |
  | Host     | `2.tcp.eu.ngrok.io` |
  | Port     | `12345`             |
  | Database | `demo`              |
  | User     | `lakeflow`          |
  | Password | `lakeflow`          |
  | Tables   | `orders`            |
  | Mode     | CDC                 |
