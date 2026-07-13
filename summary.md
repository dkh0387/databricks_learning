# Topics

## Tables

### External tables

- INSERT OVERWRITE
- File storage and tables
- Data ingestion

### Delta tables

- File storage and tables
- Data ingestion (CTAS, STREAMING, Autoloader)

## Views

### Temp view

- Definition: a named stored query (no data persisted), computed at query time
- Scope: current Spark session only; dropped when the session ends; not registered in the catalog
- Purpose: intermediate results within a notebook/session without cataloging anything

### Global temp view

- Definition: a temp view registered in the `global_temp` schema (query as `global_temp.<name>`)
- Scope: all sessions on the same cluster; dropped when the cluster terminates
- Purpose: share intermediate results across notebooks/sessions on one cluster

### Materialized view

- Definition: a view whose results are precomputed and stored physically, refreshed (incrementally where possible) rather than recomputed per query
- Scope: persistent Unity Catalog object, available across sessions and clusters
- Purpose: cheap, fast repeated reads — typical gold-layer/BI surface

## Delta Lakehouse features

- DESCRIBE (DATABASE) EXTENDED
- VERSION and RESTORE
- VACUUM
- read_files()
- _metadata
- _rescued_data
- MERGE INTO USING
- JOIN
- UNION
- INTERSECT
- MINUS
- HOF (Higher Order Function) and UDF (User Defined Function):
  - FILTER

- Working with JSON:

  - Native access using `:` (e.g. `raw:items[0].id`; `::` is casting)
  - from_json(jsonStr, schema)
  - STRUCT field access

- Transformations:
  - explode()
  - collect_set()
  - flatten()
  - array_distinct()