# MonoTS Streams (CDC) Reference

MonoTS Change Data Capture (CDC) Streams allow you to continuously push table changes to downstream systems like Delta Lake, Kafka, Pulsar, Iceberg, or local filesystems.

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

-- Removal (also deletes recovery / checkpoint state)
DROP STREAM <name>;
```

---

## Configuration Properties

Every stream requires a `WITH` clause to define its sink and capture behavior.

### Global Properties

| Property       | Required | Description                                                                                                                               |
|----------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `sink.type`    | Yes      | Destination type: `filesystem`, `delta`, `kafka`, `iceberg`, or `pulsar`.                                                                    |
| `source.table` | Yes      | The name of the table to capture.                                                                                                         |
| `cdc.mode`     | No       | `batch` (historical Parquet files only) or `hybrid` (historical files + live WAL tailing). Defaults depend on the sink.                   |
| `cdc.auto_end` | No       | `true` or `false` (default). If `true`, the stream automatically terminates after the current historical export finishes (one-shot mode). |

---

## Sink: Delta Lake (`sink.type = 'delta'`)

Exports data as standard Delta Lake tables. Supports Optimistic Concurrency Control (OCC) for safe multi-writer scenarios.

- **Default `cdc.mode`:** `batch`
- **Default `sink.format`:** `parquet`

| Property | Default | Description |
|----------|---------|-------------|
| `sink.delta.path` | *(required)* | Delta table root URI. Supports local paths, `file://`, `s3://`, and `s3a://`. (GCS and Azure are not yet supported). |
| `sink.delta.endpoint` | *(optional)* | Custom S3-compatible API endpoint (e.g., a MinIO URL). |
| `sink.delta.access.key` / `sink.delta.secret.key` | *(env)* | Optional explicit credentials; otherwise use `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`. |
| `sink.delta.region` | `us-east-1` | S3 region (`AWS_REGION`). |
| `sink.delta.path.style.access` | auto | Path-style addressing; defaults to `true` when `sink.delta.endpoint` is set (MinIO / OSS), else `false`. |
| `sink.delta.connection.maximum` | `500` | Object-store concurrency limit. |
| `sink.delta.connection.timeout` | `200s` | Connect / request timeout. SQL accepts durations (`200s`, `3 min`, `200000ms`) or bare ms integers. |
| `sink.delta.attempts.maximum` | `20` | Object-store request retry budget. |

Omitting any of the optional keys above still **fills and persists the defaults**. `SHOW CREATE` / reconstructed DDL always materializes the full set. These options are passed into the object-store client at runtime.

**Cloud Object Storage (S3):** Credentials prefer DDL keys when set; otherwise MonoTS picks up `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION` from the environment.

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

Pushes change events as row-level JSON messages to a Kafka topic (librdkafka producer).

- **Default `cdc.mode`:** `hybrid` (historical Parquet + live WAL inserts).
- **Default `sink.format`:** `json` (only supported value format).
- **Default delivery:** `at-least-once` (`acks=all` + idempotent producer). Set `exactly-once` to enable Kafka transactions.

| Property | Default | Description |
|----------|---------|-------------|
| `sink.kafka.brokers` | *(required)* | Comma-separated brokers (`bootstrap.servers`). |
| `sink.kafka.topic` | *(required)* | Destination topic. |
| `sink.kafka.key.format` | — | Key serialization; only `json` when keys are enabled. |
| `sink.kafka.key.fields` | — | Comma-separated columns used as the Kafka message key. |
| `sink.kafka.key.fields-prefix` | `""` | Prefix applied to key JSON field names (e.g. `k_`). |
| `sink.kafka.partitioner` | `default` | `default` (key hash) \| `round-robin` \| `fixed` (partition 0). |
| `sink.kafka.delivery-guarantee` | `at-least-once` | `at-least-once` \| `exactly-once`. |
| `sink.kafka.transactional.id` | `monots-stream-<name>` | Required identity for EOS (auto-derived if omitted). |
| `sink.kafka.transaction.timeout.ms` | `900000` | Must be ≤ broker `transaction.max.timeout.ms`. |
| `sink.kafka.compression.type` | librdkafka default | e.g. `lz4`, `zstd`, `snappy`, `gzip`, `none`. |
| `sink.kafka.batch.size` | librdkafka default | Producer batch size (bytes). |
| `sink.kafka.linger.ms` | librdkafka default | Max wait before sending a batch. |
| `sink.kafka.acks` | `all` | Forced to `all` under exactly-once. |
| `sink.kafka.retries` | librdkafka default | Producer retries. |
| `sink.kafka.security.protocol` | — | `PLAINTEXT` / `SSL` / `SASL_PLAINTEXT` / `SASL_SSL`. |
| `sink.kafka.sasl.mechanism` | — | e.g. `PLAIN`, `SCRAM-SHA-256`. |
| `sink.kafka.sasl.username` / `password` | — | Preferred over JAAS when set. |
| `sink.kafka.sasl.jaas.config` | — | Flink-style JAAS; parsed into username/password for librdkafka. |
| `sink.kafka.ssl.ca.location` | — | PEM CA bundle (also accepts `ssl.truststore.location` as alias). |
| `sink.kafka.ssl.certificate.location` / `ssl.key.location` | — | Client PEM cert/key (mTLS). |
| `sink.kafka.ssl.keystore.location` / `password` | — | PKCS#12 client keystore (librdkafka). |

> **Note:** MonoTS uses **librdkafka**, not the JVM client. Java JKS truststores are not loaded as JKS — point `ssl.ca.location` / `ssl.truststore.location` at a **PEM** CA file. Prefer `sasl.username` / `sasl.password` over JAAS when possible.

```sql
CREATE STREAM metrics_kafka WITH (
  'sink.type' = 'kafka',
  'sink.kafka.brokers' = '127.0.0.1:9092',
  'sink.kafka.topic' = 'metrics-cdc',
  'sink.kafka.key.format' = 'json',
  'sink.kafka.key.fields' = 'order_id',
  'sink.kafka.key.fields-prefix' = 'k_',
  'sink.kafka.partitioner' = 'default',
  'sink.kafka.delivery-guarantee' = 'exactly-once',
  'sink.kafka.transaction.timeout.ms' = '900000',
  'sink.kafka.compression.type' = 'lz4',
  'sink.kafka.batch.size' = '65536',
  'sink.kafka.linger.ms' = '20',
  'sink.kafka.acks' = 'all',
  'sink.kafka.retries' = '10',
  'source.table' = 'metrics'
);
```

---

## Sink: Filesystem (`sink.type = 'filesystem'` or `'fs'`)

Exports raw Parquet files (no `_delta_log`). Supports the same storage URIs as Delta:

- local paths (e.g. `/data/export/metrics`)
- `file://…`
- `s3://` / `s3a://` (MinIO via `sink.filesystem.endpoint`)

Local commits use atomic rename (`.tmp` → `.parquet`). Object-store commits stage locally then stream-upload finalized Parquet keys.

- **Default `cdc.mode`:** `batch`
- **Default `sink.format`:** `parquet`

| Property | Default | Description |
|----------|---------|-------------|
| `sink.filesystem.path` | *(required)* | Output directory / URI. Supports local paths, `file://`, `s3://`, and `s3a://`. |
| `sink.filesystem.endpoint` | *(optional)* | Custom S3-compatible API endpoint (e.g. MinIO). |
| `sink.filesystem.access.key` / `secret.key` | *(optional)* | Explicit credentials; otherwise default AWS chain / env. |
| `sink.filesystem.region` | `us-east-1` | S3 region. |
| `sink.filesystem.path.style.access` | auto (`true` if endpoint set) | Path-style addressing. |
| `sink.filesystem.connection.maximum` | `500` | Max concurrent object-store connections (upload concurrency is capped). |
| `sink.filesystem.connection.timeout` | `200s` | Connect / request timeout (SQL duration). |
| `sink.filesystem.attempts.maximum` | `20` | Request retry budget. |

```sql
CREATE STREAM metrics_fs WITH (
  'sink.type' = 'filesystem',
  'sink.filesystem.path' = '/data/export/metrics',
  'source.table' = 'metrics'
);
```

```sql
CREATE STREAM metrics_fs_s3 WITH (
  'sink.type' = 'filesystem',
  'sink.filesystem.path' = 's3://lake/export/metrics',
  'sink.filesystem.endpoint' = 'http://127.0.0.1:9000',
  'sink.filesystem.access.key' = 'minioadmin',
  'sink.filesystem.secret.key' = 'minioadmin',
  'source.table' = 'metrics'
);
```

---

## Sink: Iceberg (`sink.type = 'iceberg'`)

Writes Apache Iceberg tables through a Catalog (Flink-compatible options). Warehouse may be a local path or `s3://` / `s3a://` (with optional MinIO endpoint).

- **Default `cdc.mode`:** `batch`
- **Default `sink.format`:** `parquet`

| Property | Required | Description |
|----------|----------|-------------|
| `sink.iceberg.catalog-type` | Yes | `hive` \| `hadoop` \| `rest` \| `glue`. |
| `sink.iceberg.catalog-name` | Yes | Catalog name registered inside MonoTS / Flink-style identity. |
| `sink.iceberg.uri` | hive / rest | Metastore thrift URI (`thrift://host:9083`) or REST catalog endpoint. |
| `sink.iceberg.warehouse` | hadoop (required); others optional | Physical warehouse root (`/data/iceberg`, `s3://bucket/wh`, …). **Required for glue** writes. |
| `sink.iceberg.namespace` | Yes | Iceberg namespace / database (dot or `/` separators allowed). |
| `sink.iceberg.table` | Yes | Table name. |
| `sink.iceberg.create-table-if-not-exists` | No (`true`) | Create the table from the first Parquet schema when missing. |
| `sink.iceberg.endpoint` | No | S3-compatible endpoint for warehouse object storage. |
| `sink.iceberg.access.key` / `secret.key` / `region` / … | No | Same industrial S3 client knobs as Delta (`path.style.access`, `connection.*`, `attempts.maximum`). |

**Catalog write support**

| Type | Connect | Append commits |
|------|---------|----------------|
| `hadoop` | Yes (warehouse + `version-hint.text`) | Yes |
| `rest` | Yes | Yes |
| `glue` | Yes | No (`update_table` missing in iceberg-catalog-glue 0.4) |
| `hive` | DDL accepted | Not available in this build (HMS thrift client incompatible with current rustc); use `hadoop` or `rest` |

```sql
CREATE STREAM metrics_iceberg WITH (
  'sink.type' = 'iceberg',
  'sink.iceberg.catalog-type' = 'hadoop',
  'sink.iceberg.catalog-name' = 'my_hive_catalog',
  'sink.iceberg.warehouse' = '/tmp/iceberg-warehouse',
  'sink.iceberg.namespace' = 'db',
  'sink.iceberg.table' = 'metrics',
  'source.table' = 'metrics'
);
```

```sql
CREATE STREAM metrics_iceberg_s3 WITH (
  'sink.type' = 'iceberg',
  'sink.iceberg.catalog-type' = 'hadoop',
  'sink.iceberg.catalog-name' = 'lake',
  'sink.iceberg.warehouse' = 's3://monots/iceberg',
  'sink.iceberg.endpoint' = 'http://127.0.0.1:9000',
  'sink.iceberg.access.key' = 'minioadmin',
  'sink.iceberg.secret.key' = 'minioadmin',
  'sink.iceberg.namespace' = 'db',
  'sink.iceberg.table' = 'metrics',
  'source.table' = 'metrics'
);
```

```sql
CREATE STREAM metrics_iceberg_rest WITH (
  'sink.type' = 'iceberg',
  'sink.iceberg.catalog-type' = 'rest',
  'sink.iceberg.catalog-name' = 'rest',
  'sink.iceberg.uri' = 'http://127.0.0.1:18181',
  'sink.iceberg.warehouse' = 's3://monots/iceberg-rest-warehouse/',
  'sink.iceberg.namespace' = 'db',
  'sink.iceberg.table' = 'metrics',
  'sink.iceberg.endpoint' = 'http://127.0.0.1:19000',
  'sink.iceberg.access.key' = 'minioadmin',
  'sink.iceberg.secret.key' = 'minioadmin',
  'source.table' = 'metrics'
);
```

---

## Sink: Pulsar (`sink.type = 'pulsar'`)

Pushes change events as row-level JSON messages to an Apache Pulsar topic (pure-Rust `pulsar` client). Options mirror the Flink Pulsar connector under the `sink.pulsar.*` prefix.

- **Default `cdc.mode`:** `hybrid`
- **Default `sink.format`:** `json`
- **Default delivery:** `at-least-once` (await broker send receipt). `none` enqueues without waiting. `exactly-once` is rejected for now — the Rust client has no Transaction API (Flink EOS needs `transactionCoordinatorEnabled=true` on the broker).

| Property | Default | Description |
|----------|---------|-------------|
| `sink.pulsar.topic` | *(required)* | Full topic name, e.g. `persistent://public/default/my-topic`. |
| `sink.pulsar.service-url` | *(required)* | Broker URL (`pulsar://host:6650` or `pulsar+ssl://…`). |
| `sink.pulsar.admin-url` | *(required)* | Admin HTTP API (`http://host:8080`); used for health/metadata probes. |
| `sink.pulsar.delivery-guarantee` | `at-least-once` | `none` \| `at-least-once` \| `exactly-once` (EOS not available yet). |
| `sink.pulsar.transaction-timeout` | — | Flink-style duration (`3 h`, `15m`) or milliseconds; stored for EOS when available. |
| `sink.pulsar.message-router` | `round-robin` | `round-robin` \| `single` \| `key-hash` (requires `key.fields`). Java `custom` routers are not supported. |
| `sink.pulsar.key.format` | — | Key serialization; only `json` when keys are enabled. |
| `sink.pulsar.key.fields` | — | Comma-separated columns used as the Pulsar partition key. |
| `sink.pulsar.key.fields-prefix` | `""` | Prefix applied to key JSON field names. |
| `sink.pulsar.auth-plugin` | — | e.g. `org.apache.pulsar.client.impl.auth.AuthenticationToken`. |
| `sink.pulsar.auth-params` | — | Token auth params (`token:<jwt>` or raw token / `{"token":"…"}`). |

```sql
CREATE STREAM metrics_pulsar WITH (
  'sink.type' = 'pulsar',
  'sink.pulsar.topic' = 'persistent://public/default/metrics-cdc',
  'sink.pulsar.service-url' = 'pulsar://127.0.0.1:6650',
  'sink.pulsar.admin-url' = 'http://127.0.0.1:8080',
  'sink.pulsar.delivery-guarantee' = 'at-least-once',
  'sink.pulsar.message-router' = 'key-hash',
  'sink.pulsar.key.format' = 'json',
  'sink.pulsar.key.fields' = 'order_id',
  'source.table' = 'metrics'
);
```

---

## Operations Guide

### The Role of `FLUSH TABLE` (For Batch Modes)

For sinks operating in `batch` mode (default for Filesystem, Delta, and Iceberg), MonoTS only exports immutable Parquet SST files. Data residing in live MemTables will not be captured until it is flushed.

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
