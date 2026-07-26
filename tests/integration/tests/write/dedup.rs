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
    list_sst_files, scalar_i64_named, unique_table, write_i64_parquet, MonotsInstance,
};
use std::path::Path;

fn write_parquet(path: &Path, timestamps: &[i64], values: &[i64]) {
    write_i64_parquet(path, timestamps, values);
}

#[tokio::test]
async fn flush_table_sql_persists_memtable_to_sst() {
    let table = unique_table("flush_sql");
    let mut inst = MonotsInstance::new("flush_sql").unwrap();
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
            "INSERT INTO {table} (time, value) VALUES (1, 10), (2, 20)"
        ))
        .await
        .unwrap();

    let rows = client
        .no_query(&format!("FLUSH TABLE {table}"))
        .await
        .unwrap();
    assert_eq!(rows, 2);

    let table_dir = inst.data_dir().join(&table);
    assert_eq!(list_sst_files(&table_dir).len(), 1);

    inst.restart().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();
    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 2);
}

#[tokio::test]
async fn flush_table_rejected_by_query_api() {
    let table = unique_table("flush_route");
    let mut inst = MonotsInstance::new("flush_route").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let err = client
        .query(&format!("FLUSH TABLE {table}"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("NoQuery"));
}

#[tokio::test]
async fn duplicate_timestamp_deduped_across_multiple_sst_files() {
    let table = unique_table("dedup_multi_sst");
    let mut inst = MonotsInstance::new("dedup_multi_sst").unwrap();
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
            "INSERT INTO {table} (time, value) VALUES (500, 1)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!("FLUSH TABLE {table}"))
        .await
        .unwrap();

    client
        .no_query(&format!(
            "INSERT INTO {table} (time, value) VALUES (500, 2)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!("FLUSH TABLE {table}"))
        .await
        .unwrap();

    let f3 = inst.data_dir().join("layer3.parquet");
    write_parquet(&f3, &[500], &[3]);
    client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", f3.display()))
        .await
        .unwrap();

    let table_dir = inst.data_dir().join(&table);
    assert!(
        list_sst_files(&table_dir).len() >= 3,
        "expected at least 3 SST files, got {:?}",
        list_sst_files(&table_dir)
    );

    let result = client
        .query(&format!("SELECT value FROM {table} WHERE time = 500"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&result, "value"), 3);

    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 1);
}

#[tokio::test]
async fn duplicate_timestamp_newest_bulk_file_wins_among_ssts() {
    let table = unique_table("dedup_bulk_sst");
    let mut inst = MonotsInstance::new("dedup_bulk_sst").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let f1 = inst.data_dir().join("a.parquet");
    let f2 = inst.data_dir().join("b.parquet");
    let f3 = inst.data_dir().join("c.parquet");
    write_parquet(&f1, &[600], &[10]);
    write_parquet(&f2, &[600], &[20]);
    write_parquet(&f3, &[600], &[30]);

    client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", f1.display()))
        .await
        .unwrap();
    client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", f2.display()))
        .await
        .unwrap();
    client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", f3.display()))
        .await
        .unwrap();

    assert_eq!(list_sst_files(&inst.data_dir().join(&table)).len(), 3);

    let result = client
        .query(&format!("SELECT value FROM {table} WHERE time = 600"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&result, "value"), 30);

    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 1);
}

#[tokio::test]
async fn duplicate_timestamp_newest_mem_wins_over_multiple_sst_files() {
    let table = unique_table("dedup_mem_over_sst");
    let mut inst = MonotsInstance::new("dedup_mem_over_sst").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    for (val, name) in [(10, "s1.parquet"), (20, "s2.parquet")] {
        let path = inst.data_dir().join(name);
        write_parquet(&path, &[700], &[val]);
        client
            .no_query(&format!("LOAD PARQUET '{}' INTO {table}", path.display()))
            .await
            .unwrap();
    }

    client
        .no_query(&format!(
            "INSERT INTO {table} (time, value) VALUES (700, 99)"
        ))
        .await
        .unwrap();

    let result = client
        .query(&format!("SELECT value FROM {table} WHERE time = 700"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&result, "value"), 99);
}

#[tokio::test]
async fn flush_dedupes_duplicates_in_sst_file() {
    let table = unique_table("flush_dedup");
    let mut inst = MonotsInstance::new("flush_dedup_sst").unwrap();
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
            "INSERT INTO {table} (time, value) VALUES (800, 1), (800, 9), (900, 2)"
        ))
        .await
        .unwrap();

    let rows = client
        .no_query(&format!("FLUSH TABLE {table}"))
        .await
        .unwrap();
    assert_eq!(rows, 2);

    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 2);

    let val = client
        .query(&format!("SELECT value FROM {table} WHERE time = 800"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&val, "value"), 9);
}

#[tokio::test]
async fn duplicate_timestamp_newest_write_wins_in_memtable() {
    let table = unique_table("dedup_mem");
    let mut inst = MonotsInstance::new("dedup_mem").unwrap();
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
            "INSERT INTO {table} (time, value) VALUES (100, 1)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, value) VALUES (100, 99)"
        ))
        .await
        .unwrap();

    let result = client
        .query(&format!("SELECT value FROM {table} WHERE time = 100"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&result, "value"), 99);

    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 1);
}

#[tokio::test]
async fn duplicate_timestamp_newer_layer_wins_after_bulk_load() {
    let table = unique_table("dedup_layer");
    let mut inst = MonotsInstance::new("dedup_layer").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let file = inst.data_dir().join("cold.parquet");
    write_parquet(&file, &[200], &[10]);

    client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", file.display()))
        .await
        .unwrap();

    client
        .no_query(&format!(
            "INSERT INTO {table} (time, value) VALUES (200, 20)"
        ))
        .await
        .unwrap();

    let result = client
        .query(&format!("SELECT value FROM {table} WHERE time = 200"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&result, "value"), 20);

    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 1);
}

#[tokio::test]
async fn duplicate_timestamp_count_distinct_timestamps() {
    let table = unique_table("dedup_count");
    let mut inst = MonotsInstance::new("dedup_count").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    for (ts, val) in [(1, 1), (1, 2), (2, 3), (2, 4), (3, 5)] {
        client
            .no_query(&format!(
                "INSERT INTO {table} (time, value) VALUES ({ts}, {val})"
            ))
            .await
            .unwrap();
    }

    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 3);

    let sum = client
        .query(&format!("SELECT SUM(value) AS s FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&sum, "s"), 11); // 2 + 4 + 5
}
