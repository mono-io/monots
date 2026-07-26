# MonoTS SQL Reference

This reference covers Data Definition (DDL), Data Manipulation (DML), Supported Types, and Query syntax (`SELECT`, `SHOW`).

For CDC streaming SQL syntax, refer to **[streams.md](streams.md)**.

---

## The Golden Rule: The `time` Column

MonoTS is a strict time-series engine. Every table **must** contain a column named exactly **`time`**.

- **Type Constraints:** Must be `BIGINT` or `TIMESTAMP` (any precision/timezone).
- **Nullability:** Automatically enforced as `NOT NULL`.
- **Ordering:** The engine relies on this column to sort MemTables and prune Parquet files during queries.

---

## Data Types

MonoTS maps SQL types directly to Apache Arrow memory formats.

### Numeric & Boolean

| SQL Type                                     | Arrow Type                   | Notes                                                     |
|----------------------------------------------|------------------------------|-----------------------------------------------------------|
| `TINYINT` / `SMALLINT` / `INT` / `BIGINT`    | Int8 / Int16 / Int32 / Int64 | Standard signed integers.                                 |
| `TINYINT UNSIGNED` (up to `BIGINT UNSIGNED`) | UInt8 to UInt64              | Unsigned variants.                                        |
| `FLOAT` / `REAL` / `DOUBLE`                  | Float32 / Float64            |                                                           |
| `BOOLEAN`                                    | Boolean                      |                                                           |
| `DECIMAL(p, s)` / `NUMERIC(p, s)`            | Decimal128                   | Precision `1..=38`. Bare `DECIMAL` defaults to `(38,10)`. |

### Strings & Binary

| SQL Type                                  | Arrow Type  |
|-------------------------------------------|-------------|
| `VARCHAR` / `CHAR` / `TEXT`               | Utf8        |
| `LARGETEXT` / `LONGTEXT` / `LARGEUTF8`    | LargeUtf8   |
| `BLOB` / `BINARY` / `VARBINARY` / `BYTES` | Binary      |
| `LARGEBLOB` / `LONGBLOB` / `LARGEBINARY`  | LargeBinary |

### Date & Timestamp

| SQL Type                         | Arrow Type | Notes                                  |
|----------------------------------|------------|----------------------------------------|
| `DATE`                           | Date32     | Inserted as `'YYYY-MM-DD'` strings.    |
| `TIMESTAMP`                      | Timestamp  | Default: Milliseconds, no TZ (Legacy). |
| `TIMESTAMP(0)` to `TIMESTAMP(3)` | Timestamp  | Seconds to Milliseconds.               |
| `TIMESTAMP(4)` to `TIMESTAMP(9)` | Timestamp  | Microseconds to Nanoseconds.           |
| `TIMESTAMP … WITH TIME ZONE`     | Timestamp  | Always normalized and stored as UTC.   |

> **CRITICAL: Timestamp Inserts**  
> When inserting into `TIMESTAMP` columns via SQL, you must provide the **raw integer epoch** matching the column's precision (e.g., `1718000000000` for milliseconds). ISO-8601 strings (`'2024-01-01T00:00:00Z'`) are not currently supported in `INSERT VALUES`.

### Advanced & Nested Types

| SQL Type             | Notes                                                     |
|----------------------|-----------------------------------------------------------|
| `ENUM('a','b',…)`    | Dictionary-backed enum for high-cardinality optimization. |
| `ARRAY<T>` / `T[]`   | Lists. e.g., `ARRAY<VARCHAR>`.                            |
| `STRUCT<field T, …>` | Nested structs. e.g., `STRUCT<name VARCHAR, level INT>`.  |

*(Note: Unlisted types like `UUID`, `JSON`, or `INTERVAL` will return an `unsupported SQL type` error.)*

---

## Data Definition Language (DDL)

### CREATE TABLE

```sql
CREATE TABLE metrics (
  time BIGINT,
  device_id VARCHAR,
  region VARCHAR,
  value DOUBLE
);
```

**Nullable by Default:** All columns except `time` are implicitly nullable. Explicit column-level `NOT NULL` constraints (other than on `time`) are currently ignored.

**Unsupported syntax:** `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY`, `CREATE TABLE IF NOT EXISTS`, and `WITH (...)` partitioning options.

### ALTER TABLE

Only `ADD COLUMN` is supported. Newly added columns are strictly nullable to pad historical Parquet files safely.

```sql
ALTER TABLE metrics ADD COLUMN tags ARRAY<INT>;
ALTER TABLE metrics ADD COLUMN meta STRUCT<region VARCHAR, level INT>;
```

(Not supported: `DROP COLUMN`, `RENAME`, `ALTER COLUMN`, type changes).

### DROP TABLE

```sql
DROP TABLE IF EXISTS staging;
```

---

## Data Manipulation Language (DML)

### INSERT

Only `INSERT INTO … VALUES` is supported (`INSERT … SELECT` is not permitted).

```sql
INSERT INTO metrics (time, device_id, value) VALUES
  (1000, 'sensor-1', 21.5),
  (2000, 'sensor-2', 22.0);
```

**Literal Formatting Rules:**

- **Arrays:** Native `ARRAY['a','b']` or JSON string `'["a","b"]'`.
- **Structs:** Tuples `('alice', 90)` or JSON object string `'{"name": "alice", "score": 90}'`.
- **Enums:** Must strictly match a declared variant.

> **Pro Tip:** For high-volume ingestion, bypass SQL and use the Rust SDK's `write_batch` APIs. The SDK natively packs Arrow memory and auto-sorts rows by `time` before sending over gRPC.

### Bulk Ingestion (`LOAD PARQUET`)

Directly ingest Parquet files into a table. Skips the MemTable and registers the files directly to the catalog.

```sql
LOAD PARQUET '/data/import/part-000.parquet' INTO metrics;
LOAD PARQUET '/data/batch_folder/' INTO TABLE sensor_readings;
```

(Requires exact schema alignment. Mismatched schemas or missing `time` columns will reject the load).

### FLUSH

Force the active MemTable to flush into an immutable Parquet SST file immediately.

```sql
FLUSH TABLE metrics;  -- Target specific table
FLUSH TABLES;         -- Flush all tables globally
```

---

## Queries (`SELECT` & `SHOW`)

Powered by Apache DataFusion, MonoTS supports robust analytical queries.

```sql
SELECT device_id, COUNT(*) AS n, AVG(value) AS avg_v
FROM metrics
WHERE time >= 1000 AND region = 'east'
GROUP BY device_id
HAVING COUNT(*) >= 1
ORDER BY n DESC
LIMIT 10;
```

### Supported Capabilities

- **Filtering & Math:** `WHERE` (`AND`/`OR`/`IN`/`BETWEEN`), `DISTINCT`.
- **Aggregations:** `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`.
- **Operations:** `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`.
- **Joins:** `INNER JOIN` (Optimized for time-aligned merges).

### Architecture Constraints

- **Time Pruning:** Always include a time range (e.g., `WHERE time >= X AND time <= Y`) in your queries. The engine uses this to aggressively prune Parquet files before scanning.
- **Single-Threaded:** To respect edge device memory constraints, query execution parallelism is locked (`target_partitions = 1`).
- **No Mutations:** `UPDATE` and `DELETE` are not supported. Time-series data is immutable.
- **No Information Schema:** The standard `information_schema` is disabled. Use the `SHOW` commands below instead.

### Introspection (`SHOW`)

```sql
SHOW TABLES;
SHOW CREATE TABLE metrics;
```

- `SHOW TABLES` returns `table_name`, `column_count`, and `parquet_files`.
- `SHOW CREATE TABLE` returns the original DDL alongside total row counts and file metadata.
