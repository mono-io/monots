# Integration test layout

Put new cases under the matching category. Register each file in `../Cargo.toml` as `[[test]]`.

| Directory | Covers |
|-----------|--------|
| `auth/` | Login / credentials |
| `ddl/` | CREATE TABLE, catalog, time-column rules |
| `write/` | INSERT / Arrow write / types / dedup / memory |
| `query/` | SELECT syntax and advanced queries |
| `load/` | `LOAD PARQUET` |
| `stream/` | CDC sinks + stream SQL DDL |
| `recovery/` | Restart / metadata recovery |

Naming: prefer `<what>_…` so `cargo test --test write_dedup` / `--test stream_kafka_sink` reads clearly.
