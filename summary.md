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

- Definition
- Scope
- Purpose

### Global temp view

- Definition
- Scope
- Purpose

### Materialized view

- Definition
- Scope
- Purpose

## Delta Lakehouse features

- DESCRIBE (DATABASE) EXTENDED
- VERSION and RESTORE
- VACUUM
- read_file()
- _metadata
- _rescue_columns
- MERGE INTO USING
- JOIN
- UNION
- INTERSECT
- MINUS
- HOF (Higher Order Function) and UDF (User Defined Function):
  - FILTER

- Working with JSON:

  - Native access using ::
  - from_json(, data, schema)
  - STRUCT field access

- Transformations:
  - expose()
  - collect_set()
  - flatten()
  - array_distinct()