// Copyright 2026 MonoTS Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Unit tests for proto metadata store (v1): durability, degrade, memory, performance.

use super::*;
use common::TIMESTAMP_COLUMN;
use monots_storage::sst::SstMeta;
use monots_storage::SstIdentity;
use std::fs;
use std::path::{Path, PathBuf};

struct TempMetaDir(PathBuf);

impl TempMetaDir {
    fn new(label: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("monots_meta_{label}_{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempMetaDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn table_schema(name: &str, dir: &Path) -> proto::meta::TableSchema {
    proto::meta::TableSchema {
        table_name: name.to_string(),
        columns: vec![proto::meta::ColumnDef {
            name: TIMESTAMP_COLUMN.to_string(),
            data_type: "BIGINT".to_string(),
            nullable: false,
        }],
        data_dir: dir.join(name).to_string_lossy().to_string(),
    }
}

fn sample_file(version: u64, min: i64, max: i64) -> SstMeta {
    let identity =
        SstIdentity::from_parts(1_575_028_885_956 + version as i64, version, version, 0, 0);
    SstMeta::from_identity(
        identity,
        format!("/data/metrics/{}", identity.filename()),
        min,
        max,
        100,
        4096,
    )
}

fn manifest_files(table: &str, n: usize) -> Vec<SstMeta> {
    (0..n)
        .map(|i| {
            let mut meta = sample_file(i as u64 + 1, i as i64, i as i64 + 1);
            meta.file_path = format!("/data/{table}/{}", meta.identity().filename());
            meta
        })
        .collect()
}

#[test]
fn durable_wal_replay_without_snapshot() {
    let dir = TempMetaDir::new("wal_replay");
    {
        let store = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
        store
            .put_schema(table_schema("metrics", dir.path()))
            .unwrap();
        store
            .set_manifest("metrics", vec![sample_file(1, 1, 10)])
            .unwrap();
        assert!(store.wal_bytes() > 0);
        assert!(!store.snapshot_exists());
    }
    let reopened = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
    let meta = reopened.get_table_meta("metrics").unwrap();
    assert_eq!(meta.runtime.parquet_files.len(), 1);
    assert_eq!(meta.runtime.parquet_files[0].min_ts, 1);
}

#[test]
fn snapshot_plus_wal_replay_merges_state() {
    let dir = TempMetaDir::new("snap_wal");
    {
        let store = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
        store.put_schema(table_schema("t", dir.path())).unwrap();
        store.compact().unwrap();
        assert!(store.snapshot_exists());
        store.set_manifest("t", vec![sample_file(2, 5, 6)]).unwrap();
    }
    let reopened = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
    assert_eq!(
        reopened
            .get_table_meta("t")
            .unwrap()
            .runtime
            .parquet_files
            .len(),
        1
    );
}

#[test]
fn memory_only_buffers_pending_and_recovers_to_wal() {
    let dir = TempMetaDir::new("degrade");
    let store = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
    store.put_schema(table_schema("s", dir.path())).unwrap();
    store.set_persist_mode_for_test(PersistMode::MemoryOnly);
    store.set_manifest("s", vec![sample_file(3, 0, 1)]).unwrap();
    assert_eq!(store.persist_mode(), PersistMode::MemoryOnly);
    assert_eq!(store.pending_len(), 1);

    store.try_recover_durable().unwrap();
    assert_eq!(store.persist_mode(), PersistMode::Durable);
    assert_eq!(store.pending_len(), 0);
    assert!(store.wal_bytes() > 0);

    let reopened = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
    assert_eq!(
        reopened
            .get_table_meta("s")
            .unwrap()
            .runtime
            .parquet_files
            .len(),
        1
    );
}

#[test]
fn memory_budget_rejects_new_table() {
    let dir = TempMetaDir::new("budget");
    let store = MetaStore::open(dir.path(), 64).unwrap();
    let err = store
        .put_schema(table_schema("big", dir.path()))
        .unwrap_err();
    assert!(err.to_string().contains("metadata memory budget"));
}

#[test]
fn set_manifest_unknown_table_fails() {
    let dir = TempMetaDir::new("no_table");
    let store = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
    let err = store
        .set_manifest("missing", vec![sample_file(4, 0, 1)])
        .unwrap_err();
    assert!(matches!(err, TsdbError::TableNotFound(_)));
}

#[test]
fn corrupt_wal_crc_stops_replay_at_last_valid_record() {
    let dir = TempMetaDir::new("wal_crc");
    {
        let store = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
        store.put_schema(table_schema("a", dir.path())).unwrap();
    }
    let wal_path = dir.path().join("meta/wal/metadata.wal");
    let mut data = fs::read(&wal_path).unwrap();
    if let Some(last) = data.last_mut() {
        *last ^= 0xFF;
    }
    fs::write(&wal_path, &data).unwrap();

    let store = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
    assert!(store.list_tables().is_empty());
}

#[test]
fn unsupported_store_version_rejected() {
    let dir = TempMetaDir::new("bad_ver");
    fs::create_dir_all(dir.path().join("meta/snapshots")).unwrap();
    let bad = proto::meta::StoreSnapshot {
        store_version: STORE_VERSION + 99,
        seq: 1,
        schemas: Default::default(),
        manifests: Default::default(),
    };
    let frame = snapshot::encode_snapshot_frame(&bad.encode_to_vec());
    fs::write(dir.path().join("meta/snapshots/latest.pb"), frame).unwrap();

    assert!(MetaStore::open(dir.path(), 8 * 1024 * 1024).is_err());
}

#[test]
fn incremental_manifest_updates_append_wal_not_rewrite_snapshot() {
    let dir = TempMetaDir::new("perf");
    let store = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
    store.put_schema(table_schema("p", dir.path())).unwrap();
    store.compact().unwrap();

    let snap_path = dir.path().join("meta/snapshots/latest.pb");
    let snap_size_before = fs::metadata(&snap_path).unwrap().len();
    let wal_before = store.wal_bytes();

    for i in 1..=10 {
        store.set_manifest("p", manifest_files("p", i)).unwrap();
    }

    let snap_size_after = fs::metadata(&snap_path).unwrap().len();
    assert_eq!(
        snap_size_before, snap_size_after,
        "snapshot must not rewrite on each flush"
    );
    assert!(
        store.wal_bytes() > wal_before,
        "wal should grow incrementally"
    );

    store.compact().unwrap();
    assert_eq!(store.wal_bytes(), 0);
    assert_eq!(
        store
            .get_table_meta("p")
            .unwrap()
            .runtime
            .parquet_files
            .len(),
        10
    );
}

#[test]
fn auto_compaction_triggers_at_wal_record_limit() {
    let dir = TempMetaDir::new("auto_compact");
    let store = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
    store.put_schema(table_schema("c", dir.path())).unwrap();

    for i in 1..=(wal::MAX_WAL_RECORDS - 1) {
        store.set_manifest("c", manifest_files("c", i)).unwrap();
    }

    assert!(
        store.snapshot_exists(),
        "WAL should compact at {} records",
        wal::MAX_WAL_RECORDS
    );
    assert_eq!(store.wal_bytes(), 0);
    assert_eq!(
        store
            .get_table_meta("c")
            .unwrap()
            .runtime
            .parquet_files
            .len(),
        wal::MAX_WAL_RECORDS - 1
    );
}

#[test]
fn header_persist_mode_survives_reopen() {
    let dir = TempMetaDir::new("header");
    {
        let store = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
        store.put_schema(table_schema("h", dir.path())).unwrap();
        store.set_persist_mode_for_test(PersistMode::MemoryOnly);
        store
            .write_header(PersistMode::MemoryOnly, store.current_seq())
            .unwrap();
    }
    let reopened = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
    assert_eq!(reopened.persist_mode(), PersistMode::MemoryOnly);
}

#[test]
fn drop_table_replay_and_budget_release() {
    let dir = TempMetaDir::new("drop");
    {
        let store = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
        store.put_schema(table_schema("d", dir.path())).unwrap();
        store.set_manifest("d", vec![sample_file(5, 1, 2)]).unwrap();
        let used = store.memory_stats().used_bytes;
        assert!(used > 0);
        store.drop_table("d").unwrap();
        assert!(store.list_tables().is_empty());
        assert!(store.memory_stats().used_bytes < used);
    }
    let reopened = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
    assert!(reopened.list_tables().is_empty());
}

#[test]
fn create_schema_rejects_duplicate() {
    let dir = TempMetaDir::new("create_dup");
    let store = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
    store.create_schema(table_schema("t", dir.path())).unwrap();
    let err = store
        .create_schema(table_schema("t", dir.path()))
        .unwrap_err();
    assert!(err.to_string().contains("already exists"), "got: {err}");
}

#[test]
fn concurrent_create_schema_only_one_wins() {
    let dir = TempMetaDir::new("create_conc");
    let store = std::sync::Arc::new(MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap());
    let mut handles = Vec::new();
    for _ in 0..16 {
        let store = std::sync::Arc::clone(&store);
        let path = dir.path().to_path_buf();
        handles.push(std::thread::spawn(move || {
            store.create_schema(table_schema("race", &path))
        }));
    }

    let mut oks = 0usize;
    let mut errs = 0usize;
    for h in handles {
        match h.join().expect("worker thread panicked") {
            Ok(()) => oks += 1,
            Err(e) => {
                assert!(
                    e.to_string().contains("already exists"),
                    "unexpected error: {e}"
                );
                errs += 1;
            }
        }
    }
    assert_eq!(
        oks, 1,
        "exactly one create_schema must win, got ok={oks} err={errs}"
    );
    assert_eq!(errs, 15);
    assert_eq!(store.list_tables(), vec!["race".to_string()]);
}

#[test]
fn add_column_updates_schema_via_put_schema() {
    let dir = TempMetaDir::new("add_col");
    let store = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
    store.put_schema(table_schema("t", dir.path())).unwrap();
    let mut schema = table_schema("t", dir.path());
    schema.columns.push(proto::meta::ColumnDef {
        name: "extra".into(),
        data_type: "Utf8".into(),
        nullable: true,
    });
    store.put_schema(schema).unwrap();
    let meta = store.get_table_meta("t").unwrap();
    assert_eq!(meta.columns.len(), 2);
    assert!(meta.columns.iter().any(|c| c.name == "extra"));
}

#[test]
fn drop_unknown_table_fails() {
    let dir = TempMetaDir::new("drop_missing");
    let store = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
    assert!(store.drop_table("nope").is_err());
}

#[test]
fn rejects_invalid_type_in_schema() {
    let dir = TempMetaDir::new("invalid_type");
    let store = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
    let mut schema = table_schema("t", dir.path());
    schema.columns.push(proto::meta::ColumnDef {
        name: "bad".into(),
        data_type: "NotARealType".into(),
        nullable: true,
    });
    assert!(store.put_schema(schema).is_err());
}

#[test]
fn cow_snapshot_reads_do_not_see_partial_writes() {
    let dir = TempMetaDir::new("cow_reads");
    let store = MetaStore::open(dir.path(), 8 * 1024 * 1024).unwrap();
    store.put_schema(table_schema("t1", dir.path())).unwrap();
    let before = store.list_tables();
    store.put_schema(table_schema("t2", dir.path())).unwrap();
    assert_eq!(before, vec!["t1".to_string()]);
    let after = store.list_tables();
    assert_eq!(after.len(), 2);
}
