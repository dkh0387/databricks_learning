# Docker-Compose:

- Start:

```bash
  docker compose up -d
```

- Check logs:

```bash
  docker logs lakeflow-postgres
```

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
