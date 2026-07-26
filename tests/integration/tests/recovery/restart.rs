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

use monots_integration_tests::{
    scalar_i64_named, table_names_from_show, total_rows, unique_table, MonotsInstance,
};

#[tokio::test]
async fn restart_recovers_metadata_and_multiple_tables() {
    let mut inst = MonotsInstance::new("recovery_metadata").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let tables: Vec<String> = (0..6).map(|i| unique_table(&format!("cat_{i}"))).collect();

    for (i, table) in tables.iter().enumerate() {
        client
            .no_query(&format!(
                "CREATE TABLE {table} (time BIGINT NOT NULL, tag INT, value DOUBLE)"
            ))
            .await
            .unwrap();
        client
            .no_query(&format!(
                "INSERT INTO {table} (time, tag, value) VALUES ({}, {}, {})",
                1000 + i as i64,
                i,
                i as f64 + 0.5
            ))
            .await
            .unwrap();
    }
    drop(client);

    inst.restart().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let show = client.query("SHOW TABLES").await.unwrap();
    let names = table_names_from_show(&show);
    for table in &tables {
        assert!(names.contains(table), "missing table {table} after restart");
    }
    assert_eq!(names.len(), tables.len());

    for (i, table) in tables.iter().enumerate() {
        let rows = client
            .query(&format!("SELECT tag, value FROM {table}"))
            .await
            .unwrap();
        assert_eq!(total_rows(&rows), 1);
        let tag = rows[0]
            .column_by_name("tag")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(tag.value(0), i as i32);
    }

    let ddl = client
        .query(&format!("SHOW CREATE TABLE {}", tables[0]))
        .await
        .unwrap();
    assert_eq!(total_rows(&ddl), 1);
}

#[tokio::test]
async fn restart_recovers_memtable_wal_data() {
    let table = unique_table("wal");
    let mut inst = MonotsInstance::new("recovery_wal").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, v INT)"
        ))
        .await
        .unwrap();

    for i in 0..50 {
        client
            .no_query(&format!(
                "INSERT INTO {table} (time, v) VALUES ({}, {})",
                10_000 + i,
                i
            ))
            .await
            .unwrap();
    }
    drop(client);

    inst.restart().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 50);

    let rows = client
        .query(&format!(
            "SELECT v FROM {table} WHERE time = 10025 ORDER BY time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    let v = rows[0]
        .column_by_name("v")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int32Array>()
        .unwrap();
    assert_eq!(v.value(0), 25);
}

#[tokio::test]
async fn restart_recovers_flushed_parquet_data() {
    let table = unique_table("parquet");
    let mut inst =
        MonotsInstance::with_memory_limits("recovery_parquet", 256 * 1024, 16 * 1024 * 1024)
            .unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, value DOUBLE)"
        ))
        .await
        .unwrap();

    let mut ts = 1_700_000_000_000_i64;
    for batch in 0..20 {
        let mut values = String::new();
        for i in 0..500 {
            if i > 0 {
                values.push(',');
            }
            values.push_str(&format!("({}, {})", ts + i, batch * 500 + i));
        }
        ts += 500;
        client
            .no_query(&format!(
                "INSERT INTO {table} (time, value) VALUES {values}"
            ))
            .await
            .unwrap();
    }

    let before = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&before, "c"), 10_000);
    drop(client);

    inst.restart().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let after = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&after, "c"), 10_000);

    let sum = client
        .query(&format!("SELECT SUM(value) AS s FROM {table}"))
        .await
        .unwrap();
    let s = sum[0]
        .column_by_name("s")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert!((s - 49_995_000.0).abs() < 1.0);
}

#[tokio::test]
async fn double_restart_preserves_catalog_and_data() {
    let table = unique_table("dbl");
    let mut inst = MonotsInstance::new("recovery_double").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, x INT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, x) VALUES (1, 10), (2, 20), (3, 30)"
        ))
        .await
        .unwrap();
    drop(client);

    inst.restart().await.unwrap();
    inst.restart().await.unwrap();

    let mut client = inst.authenticated_client().await.unwrap();
    let rows = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&rows, "c"), 3);
}

#[tokio::test]
async fn hard_kill_preserves_flushed_data_and_recovers_cleanly() {
    let table = unique_table("hard_kill");
    let mut inst = MonotsInstance::new("recovery_hard_kill").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, v INT)"
        ))
        .await
        .unwrap();

    // Durable baseline: FLUSH seals Parquet on disk.
    for i in 0..20 {
        client
            .no_query(&format!(
                "INSERT INTO {table} (time, v) VALUES ({}, {})",
                10_000 + i,
                i
            ))
            .await
            .unwrap();
    }
    client
        .no_query(&format!("FLUSH TABLE {table}"))
        .await
        .unwrap();

    // Extra memtable / async-WAL rows — may be lost under SIGKILL (default Async durability).
    for i in 0..30 {
        client
            .no_query(&format!(
                "INSERT INTO {table} (time, v) VALUES ({}, {})",
                20_000 + i,
                100 + i
            ))
            .await
            .unwrap();
    }
    drop(client);

    inst.restart_after_hard_kill().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    let c = scalar_i64_named(&count, "c");
    assert!(c >= 20, "flushed SST rows must survive hard kill, got {c}");
    assert!(
        c <= 50,
        "cannot recover more rows than were written, got {c}"
    );

    // Catalog + write path still healthy after crash recovery.
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, v) VALUES (30000, 999)"
        ))
        .await
        .unwrap();
    let after = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&after, "c"), c + 1);
}

#[tokio::test]
async fn query_rejects_corrupted_sealed_sst() {
    use monots_integration_tests::{corrupt_file_mid, list_sst_files};

    let table = unique_table("corrupt_sst");
    let mut inst = MonotsInstance::new("recovery_corrupt_sst").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, value) VALUES (1, 10), (2, 20), (3, 30)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!("FLUSH TABLE {table}"))
        .await
        .unwrap();

    let table_dir = inst.data_dir().join(&table);
    let ssts = list_sst_files(&table_dir);
    assert_eq!(ssts.len(), 1, "expected one sealed SST, got {ssts:?}");
    corrupt_file_mid(&table_dir.join(&ssts[0]));
    drop(client);

    // Restart so any open file handles are dropped before reading the damaged SST.
    inst.restart().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let err = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("parquet")
            || msg.contains("corrupt")
            || msg.contains("footer")
            || msg.contains("magic")
            || msg.contains("checksum")
            || msg.contains("invalid")
            || msg.contains("eof")
            || msg.contains("decode")
            || msg.contains("io"),
        "expected readable Parquet/IO error, got: {err}"
    );
}
