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

//! Query IT: larger datasets, flush/SST layers, LIKE on big tables.

#[path = "common.rs"]
mod common;

use arrow::array::StringArray;
use common::insert_numeric_series;
use monots_integration_tests::{
    scalar_f64_named, scalar_i64_named, total_rows, unique_table, MonotsInstance,
};

const INSERT_CHUNK: usize = 500;

async fn insert_metrics_series(client: &mut sdk::Client, table: &str, start_ts: i64, count: usize) {
    let regions = ["east", "west", "north", "south"];
    let mut ts = start_ts;
    let mut remaining = count;
    while remaining > 0 {
        let n = remaining.min(INSERT_CHUNK);
        let mut values = String::new();
        for i in 0..n {
            if i > 0 {
                values.push(',');
            }
            let idx = (ts + i as i64 - start_ts) as usize;
            let region = regions[idx % regions.len()];
            let value = (idx as f64) * 1.5;
            values.push_str(&format!("({}, '{region}', {value})", ts + i as i64));
        }
        client
            .no_query(&format!(
                "INSERT INTO {table} (time, region, value) VALUES {values}"
            ))
            .await
            .unwrap();
        ts += n as i64;
        remaining -= n;
    }
}

#[tokio::test]
async fn join_after_flush_with_sst_layers() {
    let a = unique_table("join_flush_a");
    let b = unique_table("join_flush_b");
    let mut inst = MonotsInstance::new("scale_join_flush").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {a} (time BIGINT NOT NULL, x BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "CREATE TABLE {b} (time BIGINT NOT NULL, y BIGINT)"
        ))
        .await
        .unwrap();

    insert_numeric_series(&mut client, &a, "x", 10_000, 2_000, 0).await;
    client.no_query(&format!("FLUSH TABLE {a}")).await.unwrap();
    insert_numeric_series(&mut client, &a, "x", 12_000, 500, 2_000).await;

    insert_numeric_series(&mut client, &b, "y", 10_000, 2_500, 100).await;
    client.no_query(&format!("FLUSH TABLE {b}")).await.unwrap();

    let count = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {a} INNER JOIN {b} ON {a}.time = {b}.time
             WHERE {a}.time >= 11000 AND {a}.time < 12000"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 1_000);
}

#[tokio::test]
async fn where_complex_and_or_not_null() {
    let table = unique_table("where_complex");
    let mut inst = MonotsInstance::new("scale_where_complex").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, region VARCHAR, value DOUBLE)"
        ))
        .await
        .unwrap();
    insert_metrics_series(&mut client, &table, 5_000, 40).await;

    let count = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {table}
             WHERE time >= 5010 AND time <= 5030
               AND ((region = 'east' AND value > 10) OR (region = 'west' AND value < 5))
               AND region IS NOT NULL"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 5);

    let sum = client
        .query(&format!(
            "SELECT SUM(value) AS s FROM {table}
             WHERE region NOT IN ('north', 'south') AND value BETWEEN 0 AND 100"
        ))
        .await
        .unwrap();
    assert!(scalar_f64_named(&sum, "s") > 0.0);
}

#[tokio::test]
async fn large_dataset_count_sum_and_time_range() {
    let table = unique_table("large_basic");
    let mut inst = MonotsInstance::new("scale_large_count_sum").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let total = 50_000usize;
    insert_numeric_series(&mut client, &table, "value", 1_700_000_000_000, total, 0).await;

    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), total as i64);

    let sum = client
        .query(&format!("SELECT SUM(value) AS s FROM {table}"))
        .await
        .unwrap();
    let expected_sum = (total as i64 - 1) * (total as i64) / 2;
    assert_eq!(scalar_i64_named(&sum, "s"), expected_sum);

    let range = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {table}
             WHERE time >= 1700000010000 AND time < 1700000020000"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&range, "c"), 10_000);
}

#[tokio::test]
async fn large_dataset_flush_then_where_and_group_by() {
    let table = unique_table("large_flush_grp");
    let mut inst = MonotsInstance::new("scale_large_flush_grp").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, region VARCHAR, value DOUBLE)"
        ))
        .await
        .unwrap();

    insert_metrics_series(&mut client, &table, 100_000, 20_000).await;
    client
        .no_query(&format!("FLUSH TABLE {table}"))
        .await
        .unwrap();
    insert_metrics_series(&mut client, &table, 120_000, 5_000).await;

    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 25_000);

    let filtered = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {table}
             WHERE time >= 105000 AND time < 115000 AND region = 'east'"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&filtered, "c"), 2_500);

    let groups = client
        .query(&format!(
            "SELECT region, COUNT(*) AS c, SUM(value) AS s FROM {table}
             GROUP BY region
             HAVING COUNT(*) >= 6000
             ORDER BY region"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&groups), 4);
    let east_count = scalar_i64_named(&groups, "c");
    assert_eq!(east_count, 6_250);
}

#[tokio::test]
async fn large_inner_join_two_tables() {
    let left = unique_table("join_large_l");
    let right = unique_table("join_large_r");
    let mut inst = MonotsInstance::new("scale_join_large").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {left} (time BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "CREATE TABLE {right} (time BIGINT NOT NULL, payload BIGINT)"
        ))
        .await
        .unwrap();

    let n = 10_000usize;
    insert_numeric_series(&mut client, &left, "value", 2_000_000, n, 0).await;
    insert_numeric_series(&mut client, &right, "payload", 2_000_000, n, 1_000).await;

    let count = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {left} l INNER JOIN {right} r ON l.time = r.time"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), n as i64);

    let filtered_join = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {left} l
             INNER JOIN {right} r ON l.time = r.time
             WHERE l.value >= 5000 AND l.value < 5100"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&filtered_join, "c"), 100);
}

#[tokio::test]
async fn join_with_lsm_dedup_newest_wins() {
    let facts = unique_table("join_dedup_f");
    let dims = unique_table("join_dedup_d");
    let mut inst = MonotsInstance::new("scale_join_dedup").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {facts} (time BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "CREATE TABLE {dims} (time BIGINT NOT NULL, label BIGINT)"
        ))
        .await
        .unwrap();

    client
        .no_query(&format!(
            "INSERT INTO {facts} (time, value) VALUES (900, 1)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!("FLUSH TABLE {facts}"))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {facts} (time, value) VALUES (900, 99)"
        ))
        .await
        .unwrap();

    client
        .no_query(&format!(
            "INSERT INTO {dims} (time, label) VALUES (900, 42)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT f.value, d.label FROM {facts} f
             INNER JOIN {dims} d ON f.time = d.time
             WHERE f.time = 900"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    assert_eq!(
        rows[0]
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0),
        99
    );
}

#[tokio::test]
async fn where_like_and_order_limit_on_large_table() {
    let table = unique_table("where_like");
    let mut inst = MonotsInstance::new("scale_where_like").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, region VARCHAR, value DOUBLE)"
        ))
        .await
        .unwrap();
    insert_metrics_series(&mut client, &table, 1_000, 5_000).await;
    client
        .no_query(&format!("FLUSH TABLE {table}"))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT time, region, value FROM {table}
             WHERE region LIKE 'e%'
               AND time >= 2000 AND time <= 2500
             ORDER BY time DESC
             LIMIT 5"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 5);
    let regions = rows[0]
        .column_by_name("region")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(regions
        .iter()
        .all(|r| r.map(|s| s.starts_with('e')).unwrap_or(false)));
}
