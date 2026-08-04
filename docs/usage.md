# MonoTS Usage Guide

MonoTS is an edge time-series database designed for high-performance ingestion, SQL querying, and continuous CDC streaming to downstream lakes or queues.

- **Default Endpoint:** `http://127.0.0.1:50051`
- **Default Credentials:** `admin` / `admin`
- **The Golden Rule:** Every table **must** contain a `time` column (`BIGINT` or `TIMESTAMP`).

| Deep Dive                    | Contents                                                               |
|------------------------------|------------------------------------------------------------------------|
| **[sql.md](sql.md)**         | Data types, DDL, `INSERT` / `LOAD PARQUET` / `FLUSH`, `SELECT`, `SHOW` |
| **[streams.md](streams.md)** | CDC streams: protocols, sink properties, filesystem / delta / kafka    |

Applications communicate with **`monots-server`** over gRPC via the **`monots` CLI** or the native **Rust SDK**.

---

## Quick Start

Compile the project and start the server:

```bash
make build-host
make run-server
```

Open a second terminal to launch the interactive SQL CLI:

```bash
make run-cli
```

(Or run one-shot SQL directly: `monots --sql "SHOW TABLES"`)

---

## SQL at a Glance

For comprehensive syntax, see **[sql.md](sql.md)**.

```sql
-- DDL & Ingestion
CREATE TABLE metrics (time BIGINT, device_id VARCHAR, value DOUBLE);
ALTER TABLE metrics ADD COLUMN region VARCHAR;

INSERT INTO metrics (time, device_id, value) VALUES (1000, 'sensor-1', 21.5);
LOAD PARQUET '/data/import/part-000.parquet' INTO metrics;
FLUSH TABLES;

-- Querying
SELECT device_id, COUNT(*) AS n, AVG(value) AS avg_v
FROM metrics
WHERE time >= 1000 AND region = 'east'
GROUP BY device_id
ORDER BY time;
```

---

## Streams (CDC) at a Glance

Streams continuously push data to external systems. For full sink configurations, see **[streams.md](streams.md)**.

```sql
CREATE STREAM metrics_out WITH (
  'sink.type' = 'delta',
  'sink.delta.path' = 's3://my-bucket/metrics',
  'sink.delta.endpoint' = 'http://127.0.0.1:9000',
  'source.table' = 'metrics',
  'cdc.mode' = 'batch'
);

SHOW STREAMS;
SHOW STREAM STATUS FOR metrics_out;
DROP STREAM metrics_out;
```

### Essential Sink Properties

| Sink Type  | Key Properties                           |
|------------|------------------------------------------|
| filesystem | `sink.filesystem.path`                   |
| delta      | `sink.delta.path`, `sink.delta.endpoint`, plus S3 client defaults (`region`, path-style, retries — see streams.md) |
| kafka      | `sink.kafka.brokers`, `sink.kafka.topic` |

**Note:** For S3/MinIO integrations, credentials prefer `sink.delta.access.key` / `sink.delta.secret.key` when set; otherwise use `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION`. Omitting Delta optional keys still fills and persists the defaults.

---

## Rust SDK

Add the MonoTS SDK to your `Cargo.toml`, then connect programmatically. The SDK natively supports sorting Arrow batches by `time` before ingestion.

```rust
use sdk::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect and Authenticate
    let mut client = Client::connect("http://127.0.0.1:50051").await?;
    client.login("admin", "admin").await?;

    // Execute DDL
    client
        .no_query("CREATE TABLE points (time BIGINT, value DOUBLE)")
        .await?;

    // Query Data
    let results = client.query("SELECT * FROM points LIMIT 10").await?;
    println!("{results:?}");

    Ok(())
}
```

**Tip:** Use `write_batch` or `write_batches` for high-throughput Arrow ingestions.

---

## CLI & Server Configuration

### Config Resolution Order

`--config` → `MONOTS_CONF` → `$MONOTS_HOME/conf/config.yaml` → `./conf/config.yaml`

### CLI Usage

```bash
# Connect interactively
monots -H http://127.0.0.1:50051 -u admin -p admin

# Execute one-shot commands
monots --sql "SHOW TABLES"
```

| Flag             | Default                  | Description                         |
|------------------|--------------------------|-------------------------------------|
| `-H, --host`     | `http://127.0.0.1:50051` | Server URL                          |
| `-u, --user`     | `admin`                  | Username                            |
| `-p, --password` | `admin`                  | Password                            |
| `--sql <STRING>` | —                        | Execute a single statement and exit |

### Key `config.yaml` Settings

| Key                                 | Default             | Description                                                 |
|-------------------------------------|---------------------|-------------------------------------------------------------|
| `service.host` / `.port`            | `0.0.0.0` / `50051` | gRPC bind address and port.                                 |
| `service.data_dir`                  | `data`              | Root directory for local storage.                           |
| `storage.memtable_max_bytes`        | 64 MiB              | Size threshold to freeze and flush MemTable to Parquet.     |
| `storage.global_memory_limit_bytes` | 512 MiB             | Hard process-wide memory cap for MemTables.                 |
| `sync.queue_size`                   | 1024                | Event buffer size between CDC capture and downstream sinks. |

### Helper Scripts

```bash
./scripts/start-server.sh       # Start foreground
./scripts/start-server.sh -d    # Start background daemon
make dist-host                  # Package binary distribution
```
