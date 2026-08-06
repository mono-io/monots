# Integration Tests

Rust end-to-end tests against a real `monots-server` process, using the [`sdk`](../../sdk) client.

## Layout

```
tests/integration/
├── Makefile
├── Cargo.toml              # each tests/<category>/*.rs is a [[test]] target
├── docker-compose.yml      # Kafka (KRaft) + MinIO for stream sink ITs
├── src/
│   ├── framework/          # harness (process, workspace, instance, docker)
│   ├── all_types_arrow.rs  # shared full-schema DDL / Arrow batch builders
│   ├── parquet_util.rs     # write_i64_parquet / list_sst_files / corrupt_file_mid
│   ├── helpers.rs
│   └── test_context.rs
└── tests/                  # categorized ITs (see below)
    ├── auth/               # login / credentials
    ├── ddl/                # CREATE / ALTER / catalog / time column / concurrency
    ├── write/              # INSERT, Arrow IPC, types, dedup, memory
    ├── query/              # SELECT syntax and advanced analytics
    ├── load/               # LOAD PARQUET
    ├── stream/             # CDC sinks + stream SQL DDL
    └── recovery/           # graceful restart + hard-kill WAL + corrupt SST
```

> Note: MonoTS already uses a categorized layout (not a flat `tests/*.rs`). Prefer adding cases
> under the existing `auth|ddl|write|query|load|stream|recovery` buckets and shared helpers in
> `src/`, rather than inventing a parallel `sql/types/storage` tree.

| Category | Cargo `--test` prefix | Add new cases under |
|----------|----------------------|---------------------|
| Auth | `auth_*` | `tests/auth/` |
| DDL | `ddl_*` | `tests/ddl/` |
| Write | `write_*` | `tests/write/` |
| Query | `query_*` | `tests/query/` (`basic` / `joins` / `subqueries` / `expressions` / `aggregations` / `scalar_types` / `case_sensitivity` / `scale`) |
| Load | `load_*` | `tests/load/` |
| Stream | `stream_*` | `tests/stream/` |
| Recovery | `recovery_*` | `tests/recovery/` |

**Adding a test:** create `tests/<category>/<short_name>.rs`, then register a matching `[[test]]` in `Cargo.toml` (`name = "<category>_<short_name>"`, `path = "tests/..."`). Nested paths are not auto-discovered.

## Shared helpers (dedupe)

| Helper | Location | Use for |
|--------|----------|---------|
| `full_types_ddl` / `full_types_batch` / `enum_value_at` | `src/all_types_arrow.rs` | Wide-schema roundtrip |
| `write_i64_parquet` / `list_sst_files` / `corrupt_file_mid` | `src/parquet_util.rs` | LOAD / flush / corruption ITs |
| `TestContext` / `assert_time_scan_non_decreasing` | `src/test_context.rs` | Assert helpers |
| `MonotsInstance::restart` / `restart_after_hard_kill` | `src/framework/instance.rs` | Recovery |

## Prerequisites

| Dependency | Note |
|------------|------|
| Rust (see `rust-toolchain.toml`) | `cargo` on PATH |
| Docker | Required for Kafka / MinIO stream ITs |

## Run

```bash
# From repo root — all ITs (Docker-backed stream sinks require Docker)
make integration-test

# Or directly
cd tests/integration && make test

# One category
cd tests/integration && make test-write
cd tests/integration && make test-query
cd tests/integration && make test-load
cd tests/integration && make test-ddl
cd tests/integration && make test-recovery

# One binary
cd tests/integration && make test CARGO_TEST_ARGS="--test write_dedup"
```

Docker-backed Kafka / MinIO stream tests **fail** if Docker is unavailable (they call `docker compose` and wait for ports). Start the stack with `make docker-up` or let tests bring it up themselves.

## Stream sink coverage

| Test binary | Sink | Data plan |
|-------------|------|-----------|
| `stream_filesystem_sink` | local Parquet dir | 10k rows, flush every 1k, LOAD + compare |
| `stream_delta_sink` (local) | Delta `_delta_log` on disk | flush every 1k, then LOAD + aggregate verify |
| `stream_delta_sink` (local SHOW CREATE) | defaults materialization | asserts S3 client defaults + rejects removed keys |
| `stream_delta_sink` (MinIO) | `s3://` + `sink.delta.endpoint` | Docker MinIO; env credentials; download then LOAD |
| `stream_delta_sink` (MinIO full SQL) | full `sink.delta.*` DDL knobs | DDL credentials + `3 min` timeout; download then LOAD |
| `stream_iceberg_sink` (Hadoop+MinIO) | Iceberg Hadoop Catalog | Docker MinIO warehouse; download parquet → LOAD; COUNT/SUM/DISTINCT |
| `stream_iceberg_sink` (REST+MinIO) | Iceberg REST fixture | Docker `iceberg-rest` + MinIO; same integrity checks |
| `stream_kafka_sink` | Kafka JSON | Docker Kafka; content-verify JSON rows |
| `stream_sql_ddl` | — | CREATE / SHOW / DROP STREAM SQL surface |

## Framework

| Module | Role |
|--------|------|
| `framework/path.rs` | Locate `monots-server` binary |
| `framework/workspace.rs` | Per-test isolated data/logs directory |
| `framework/process.rs` | Spawn / SIGTERM / SIGKILL server process |
| `framework/instance.rs` | Facade: `MonotsInstance::new(test_name).start().await` |
| `framework/utils.rs` | Free port allocation, TCP health check |
| `framework/docker.rs` | Compose up/wait for Kafka `:19092` + MinIO `:19000` |

Each test gets its own port and workspace under `tests/integration/target/<test_name>/`.

## Known remaining gaps

- Background physical compaction (no public `COMPACT` SQL yet) — covered only indirectly via multi-SST query/dedup.
- Extreme multi-GB single-batch OOM stress — opt-in via env vars where present (`MONOTS_IT_BULK_ROWS`).

## Extra args

```bash
make integration-test CARGO_TEST_ARGS="-- --nocapture"
cd tests/integration && make test CARGO_TEST_ARGS="--test stream_delta_sink"
```
