# MonoTS Streams (CDC) Reference

MonoTS Change Data Capture (CDC) Streams allow you to continuously push table changes to downstream systems like Delta Lake, Kafka, or local filesystems.

Delivery is **server-driven** (push model) with **at-least-once** guarantees. There is no client-side pull API.

---

## Core Concepts

- **One-to-One Binding:** A stream captures data from exactly **one** source table.
- **At-Least-Once Guarantee:** Streams guarantee no data loss, but in rare network partition events, duplicates may be delivered. Downstream systems should be designed to handle idempotency.
- **Automated Checkpointing:** MonoTS manages stream offsets and recovery checkpoints automatically.

---

## Stream Management Syntax

```sql
-- Create a stream
CREATE STREAM [IF NOT EXISTS] <name> WITH (
  'key' = 'value',
  ...
);

-- Introspection
SHOW STREAMS;
SHOW STREAM <name>;
SHOW STREAM STATUS FOR <name>;

-- Removal
DROP STREAM <name>;
DROP STREAM <name> WITH CHECKPOINT; -- Also permanently deletes recovery state
```

---

## Configuration Properties

Every stream requires a `WITH` clause to define its sink and capture behavior.

### Global Properties

| Property       | Required | Description                                                                                                                               |
|----------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `sink.type`    | Yes      | Destination type: `filesystem`, `delta`, or `kafka`.                                                                                      |
| `source.table` | Yes      | The name of the table to capture.                                                                                                         |
| `cdc.mode`     | No       | `batch` (historical Parquet files only) or `hybrid` (historical files + live WAL tailing). Defaults depend on the sink.                   |
| `cdc.auto_end` | No       | `true` or `false` (default). If `true`, the stream automatically terminates after the current historical export finishes (one-shot mode). |

*(Note: Legacy flat keys like `sink.path` are still accepted for backward compatibility, but prefixed keys like `sink.delta.path` are highly recommended.)*

---

## Sink: Delta Lake (`sink.type = 'delta'`)

Exports data as standard Delta Lake tables. Supports Optimistic Concurrency Control (OCC) for safe multi-writer scenarios.

- **Default `cdc.mode`:** `batch`
- **Default `sink.format`:** `parquet`

| Property              | Description                                                                                                              |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------|
| `sink.delta.path`     | The Delta table root URI. Supports local paths, `file://`, `s3://`, and `s3a://`. (GCS and Azure are not yet supported). |
| `sink.delta.endpoint` | (Optional) Custom S3-compatible API endpoint (e.g., a MinIO URL).                                                        |

**Cloud Object Storage (S3):** When writing to S3, MonoTS automatically picks up credentials from standard environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`).

```sql
CREATE STREAM metrics_delta WITH (
  'sink.type' = 'delta',
  'sink.delta.path' = 's3://my-bucket/metrics',
  'sink.delta.endpoint' = 'http://127.0.0.1:9000',
  'source.table' = 'metrics'
);
```

---

## Sink: Kafka (`sink.type = 'kafka'`)

Pushes change events as row-level messages to a Kafka topic.

- **Default `cdc.mode`:** `hybrid` (Streams both historical Parquet rows and live WAL inserts).
- **Default `sink.format`:** `json` (Currently the only supported format).

| Property             | Description                                                     |
|----------------------|-----------------------------------------------------------------|
| `sink.kafka.brokers` | Comma-separated list of Kafka brokers (e.g., `127.0.0.1:9092`). |
| `sink.kafka.topic`   | The destination topic name.                                     |

```sql
CREATE STREAM metrics_kafka WITH (
  'sink.type' = 'kafka',
  'sink.kafka.brokers' = '127.0.0.1:9092',
  'sink.kafka.topic' = 'metrics-cdc',
  'source.table' = 'metrics'
);
```

---

## Sink: Filesystem (`sink.type = 'filesystem'` or `'fs'`)

Exports raw Parquet files to a local directory. Guarantees file integrity via atomic renames.

- **Default `cdc.mode`:** `batch`
- **Default `sink.format`:** `parquet`

| Property               | Description                                             |
|------------------------|---------------------------------------------------------|
| `sink.filesystem.path` | Local output directory for the generated Parquet files. |

```sql
CREATE STREAM metrics_fs WITH (
  'sink.type' = 'filesystem',
  'sink.filesystem.path' = '/data/export/metrics',
  'source.table' = 'metrics'
);
```

---

## Operations Guide

### The Role of `FLUSH TABLE` (For Batch Modes)

For sinks operating in `batch` mode (default for Filesystem and Delta), MonoTS only exports immutable Parquet SST files. Data residing in live MemTables will not be captured until it is flushed.

To force a capture immediately, run:

```sql
INSERT INTO metrics (time, value) VALUES (1000, 1.0);
FLUSH TABLE metrics; -- Flushes MemTable to disk, triggering the CDC batch pipeline
```

### Monitoring Streams

Use `SHOW STREAM STATUS FOR <name>` to monitor pipeline health and progression.

Key output columns:

- **`phase`:** Current lifecycle state (`inactive`, `syncingbatch`, `syncinglog`, `active`, `completed`, `failed`).
- **`batch_files_done` / `total`:** Progress of historical backfill.
- **`acked_lsn`:** The highest Log Sequence Number successfully committed to the sink.
