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

//! Query IT: aggregates, DISTINCT aggregates, and window functions.

#[path = "common.rs"]
mod common;

use common::boot;
use monots_integration_tests::{
    col_f64, col_i64, col_is_null, col_str, scalar_f64_named, scalar_i64_named, total_rows,
    unique_table,
};

async fn seed(client: &mut sdk::Client, table: &str) {
    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, region VARCHAR, value DOUBLE)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, region, value) VALUES
             (1000, 'east', 10.0),
             (2000, 'east', 20.0),
             (3000, 'west', 30.0),
             (4000, 'west', 40.0),
             (5000, 'north', 50.0),
             (6000, 'east', 10.0)"
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn count_sum_avg_min_max_aggregations() {
    let table = unique_table("agg_basic");
    let (_inst, mut client) = boot("agg_basic").await;
    seed(&mut client, &table).await;

    let stats = client
        .query(&format!(
            "SELECT COUNT(*) AS c, SUM(value) AS s, AVG(value) AS a,
                    MIN(value) AS mn, MAX(value) AS mx
             FROM {table}"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&stats, "c"), 6);
    assert_eq!(scalar_f64_named(&stats, "s"), 160.0);
    assert!((scalar_f64_named(&stats, "a") - 160.0 / 6.0).abs() < 1e-9);
    assert_eq!(scalar_f64_named(&stats, "mn"), 10.0);
    assert_eq!(scalar_f64_named(&stats, "mx"), 50.0);
}

#[tokio::test]
async fn group_by_with_having() {
    let table = unique_table("agg_grp");
    let (_inst, mut client) = boot("agg_group_having").await;
    seed(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "SELECT region, SUM(value) AS total FROM {table}
             GROUP BY region
             HAVING SUM(value) >= 50
             ORDER BY total DESC"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_str(&rows, "region", 0), "west");
}

#[tokio::test]
async fn agg_distinct_values() {
    let table = unique_table("agg_dist");
    let (_inst, mut client) = boot("agg_distinct").await;
    seed(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "SELECT COUNT(DISTINCT region) AS regions,
                    COUNT(DISTINCT value) AS values_d
             FROM {table}"
        ))
        .await
        .unwrap();
    assert_eq!(col_i64(&rows, "regions", 0), 3);
    assert_eq!(col_i64(&rows, "values_d", 0), 5);
}

#[tokio::test]
async fn window_function_row_number() {
    let table = unique_table("win_rn");
    let (_inst, mut client) = boot("win_row_number").await;
    seed(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "SELECT time, region, value,
                    CAST(ROW_NUMBER() OVER (PARTITION BY region ORDER BY time) AS BIGINT) AS rn
             FROM {table}
             WHERE region = 'east'
             ORDER BY time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 3, "schema={:?}", rows[0].schema());
    assert_eq!(col_i64(&rows, "rn", 0), 1);
    assert_eq!(col_i64(&rows, "rn", 1), 2);
    assert_eq!(col_i64(&rows, "rn", 2), 3);
}

#[tokio::test]
async fn window_function_rank() {
    let table = unique_table("win_rank");
    let (_inst, mut client) = boot("win_rank").await;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, score BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, score) VALUES (1, 100), (2, 90), (3, 90), (4, 80)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT time, score,
                    CAST(RANK() OVER (ORDER BY score DESC) AS BIGINT) AS rnk,
                    CAST(DENSE_RANK() OVER (ORDER BY score DESC) AS BIGINT) AS drnk
             FROM {table}
             ORDER BY time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 4, "schema={:?}", rows[0].schema());
    assert_eq!(col_i64(&rows, "rnk", 0), 1);
    assert_eq!(col_i64(&rows, "rnk", 1), 2);
    assert_eq!(col_i64(&rows, "rnk", 2), 2);
    assert_eq!(col_i64(&rows, "rnk", 3), 4);
    assert_eq!(col_i64(&rows, "drnk", 3), 3);
}

#[tokio::test]
async fn window_function_lead_lag() {
    let table = unique_table("win_ll");
    let (_inst, mut client) = boot("win_lead_lag").await;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, value) VALUES (100, 10), (200, 25), (300, 40)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT time, value,
                    LAG(value, 1) OVER (ORDER BY time) AS prev_v,
                    LEAD(value, 1) OVER (ORDER BY time) AS next_v
             FROM {table}
             ORDER BY time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 3);
    assert!(col_is_null(&rows, "prev_v", 0));
    assert_eq!(col_i64(&rows, "next_v", 0), 25);
    assert_eq!(col_i64(&rows, "prev_v", 1), 10);
    assert_eq!(col_i64(&rows, "next_v", 1), 40);
    assert_eq!(col_i64(&rows, "prev_v", 2), 25);
    assert!(col_is_null(&rows, "next_v", 2));
}

#[tokio::test]
async fn window_function_running_sum() {
    let table = unique_table("win_rs");
    let (_inst, mut client) = boot("win_running_sum").await;

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

    let rows = client
        .query(&format!(
            "SELECT time, value,
                    SUM(value) OVER (
                      ORDER BY time
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS running
             FROM {table}
             ORDER BY time"
        ))
        .await
        .unwrap();
    assert_eq!(col_i64(&rows, "running", 0), 10);
    assert_eq!(col_i64(&rows, "running", 1), 30);
    assert_eq!(col_i64(&rows, "running", 2), 60);
}

#[tokio::test]
async fn grouping_sets_basic() {
    let table = unique_table("agg_gs");
    let (_inst, mut client) = boot("agg_grouping_sets").await;
    seed(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "SELECT region, COUNT(*) AS c FROM {table}
             GROUP BY GROUPING SETS ((region), ())
             ORDER BY region NULLS LAST, c"
        ))
        .await
        .unwrap();
    // 3 regions + 1 grand total
    assert_eq!(total_rows(&rows), 4);

    // east / north / west then NULL grand total (NULLS LAST)
    assert_eq!(col_str(&rows, "region", 0), "east");
    assert_eq!(col_i64(&rows, "c", 0), 3);
    assert_eq!(col_str(&rows, "region", 1), "north");
    assert_eq!(col_i64(&rows, "c", 1), 1);
    assert_eq!(col_str(&rows, "region", 2), "west");
    assert_eq!(col_i64(&rows, "c", 2), 2);
    assert!(
        col_is_null(&rows, "region", 3),
        "grand total region should be NULL"
    );
    assert_eq!(col_i64(&rows, "c", 3), 6, "grand total count");
}

#[tokio::test]
async fn conditional_count_via_case() {
    // Parser does not accept COUNT(*) FILTER (WHERE ...); emulate with CASE.
    let table = unique_table("agg_case");
    let (_inst, mut client) = boot("agg_case_count").await;
    seed(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "SELECT
               COUNT(*) AS all_c,
               SUM(CASE WHEN region = 'east' THEN 1 ELSE 0 END) AS east_c
             FROM {table}"
        ))
        .await
        .unwrap();
    assert_eq!(col_i64(&rows, "all_c", 0), 6);
    assert_eq!(col_i64(&rows, "east_c", 0), 3);
}

#[tokio::test]
async fn agg_handle_nulls() {
    let table = unique_table("agg_nulls");
    let (_inst, mut client) = boot("agg_nulls").await;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, val DOUBLE)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, val) VALUES
             (1, 10.0), (2, NULL), (3, 20.0), (4, NULL)"
        ))
        .await
        .unwrap();

    let stats = client
        .query(&format!(
            "SELECT COUNT(*) AS c_all, COUNT(val) AS c_val,
                    SUM(val) AS s, AVG(val) AS a,
                    MIN(val) AS mn, MAX(val) AS mx
             FROM {table}"
        ))
        .await
        .unwrap();

    assert_eq!(scalar_i64_named(&stats, "c_all"), 4);
    assert_eq!(scalar_i64_named(&stats, "c_val"), 2);
    assert_eq!(scalar_f64_named(&stats, "s"), 30.0);
    assert!((scalar_f64_named(&stats, "a") - 15.0).abs() < 1e-9);
    assert_eq!(scalar_f64_named(&stats, "mn"), 10.0);
    assert_eq!(scalar_f64_named(&stats, "mx"), 20.0);
}

#[tokio::test]
async fn agg_empty_table() {
    let table = unique_table("agg_empty");
    let (_inst, mut client) = boot("agg_empty").await;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, val DOUBLE)"
        ))
        .await
        .unwrap();

    let stats = client
        .query(&format!(
            "SELECT COUNT(*) AS c, SUM(val) AS s, MIN(val) AS mn, MAX(val) AS mx, AVG(val) AS a
             FROM {table}"
        ))
        .await
        .unwrap();

    assert_eq!(scalar_i64_named(&stats, "c"), 0);
    assert!(
        col_is_null(&stats, "s", 0),
        "SUM over empty table should be NULL"
    );
    assert!(
        col_is_null(&stats, "mn", 0),
        "MIN over empty table should be NULL"
    );
    assert!(
        col_is_null(&stats, "mx", 0),
        "MAX over empty table should be NULL"
    );
    assert!(
        col_is_null(&stats, "a", 0),
        "AVG over empty table should be NULL"
    );
}

#[tokio::test]
async fn agg_where_filters_all_rows() {
    let table = unique_table("agg_nofit");
    let (_inst, mut client) = boot("agg_where_none").await;
    seed(&mut client, &table).await;

    let stats = client
        .query(&format!(
            "SELECT COUNT(*) AS c, SUM(value) AS s, AVG(value) AS a
             FROM {table}
             WHERE region = 'no-such-region'"
        ))
        .await
        .unwrap();

    assert_eq!(scalar_i64_named(&stats, "c"), 0);
    assert!(col_is_null(&stats, "s", 0));
    assert!(col_is_null(&stats, "a", 0));
}

#[tokio::test]
async fn group_by_multiple_columns() {
    let table = unique_table("agg_multi");
    let (_inst, mut client) = boot("agg_group_multi").await;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (
                time BIGINT NOT NULL,
                region VARCHAR,
                device_id VARCHAR,
                value BIGINT
            )"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, region, device_id, value) VALUES
             (1, 'east', 'a', 10),
             (2, 'east', 'a', 20),
             (3, 'east', 'b', 30),
             (4, 'west', 'a', 40)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT region, device_id, SUM(value) AS s, COUNT(*) AS c
             FROM {table}
             GROUP BY region, device_id
             ORDER BY region, device_id"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 3);
    assert_eq!(col_str(&rows, "region", 0), "east");
    assert_eq!(col_str(&rows, "device_id", 0), "a");
    assert_eq!(col_i64(&rows, "s", 0), 30);
    assert_eq!(col_i64(&rows, "c", 0), 2);
    assert_eq!(col_str(&rows, "device_id", 1), "b");
    assert_eq!(col_i64(&rows, "s", 1), 30);
    assert_eq!(col_str(&rows, "region", 2), "west");
    assert_eq!(col_i64(&rows, "s", 2), 40);
}

#[tokio::test]
async fn agg_distinct_with_group_by() {
    let table = unique_table("agg_dist_grp");
    let (_inst, mut client) = boot("agg_distinct_group").await;
    seed(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "SELECT region, COUNT(DISTINCT value) AS uniq_v
             FROM {table}
             GROUP BY region
             ORDER BY region"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 3);
    // east: 10, 20, 10 → 2 distinct; north: 50 → 1; west: 30, 40 → 2
    assert_eq!(col_str(&rows, "region", 0), "east");
    assert_eq!(col_i64(&rows, "uniq_v", 0), 2);
    assert_eq!(col_str(&rows, "region", 1), "north");
    assert_eq!(col_i64(&rows, "uniq_v", 1), 1);
    assert_eq!(col_str(&rows, "region", 2), "west");
    assert_eq!(col_i64(&rows, "uniq_v", 2), 2);
}

#[tokio::test]
async fn window_lag_with_default() {
    let table = unique_table("win_lag_def");
    let (_inst, mut client) = boot("win_lag_default").await;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, value) VALUES (100, 10), (200, 25)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT time, value,
                    LAG(value, 1, 0) OVER (ORDER BY time) AS prev_or_zero
             FROM {table}
             ORDER BY time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_i64(&rows, "prev_or_zero", 0), 0);
    assert_eq!(col_i64(&rows, "prev_or_zero", 1), 10);
}

#[tokio::test]
async fn window_range_frame_preceding() {
    let table = unique_table("win_range");
    let (_inst, mut client) = boot("win_range_frame").await;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, value) VALUES
             (1000, 1), (2000, 2), (3000, 4), (5000, 8)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT time, value,
                    SUM(value) OVER (
                      ORDER BY time
                      RANGE BETWEEN 1000 PRECEDING AND CURRENT ROW
                    ) AS win_sum
             FROM {table}
             ORDER BY time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 4);
    assert_eq!(col_i64(&rows, "win_sum", 0), 1); // [1000]
    assert_eq!(col_i64(&rows, "win_sum", 1), 3); // [1000,2000]
    assert_eq!(col_i64(&rows, "win_sum", 2), 6); // [2000,3000]
    assert_eq!(col_i64(&rows, "win_sum", 3), 8); // [5000] only (4000 gap)
}

#[tokio::test]
async fn window_mixed_definitions_same_select() {
    let table = unique_table("win_mix");
    let (_inst, mut client) = boot("win_mixed").await;
    seed(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "SELECT time, region, value,
                    CAST(ROW_NUMBER() OVER (PARTITION BY region ORDER BY time) AS BIGINT) AS rn,
                    SUM(value) OVER (PARTITION BY region) AS region_sum,
                    LAG(value, 1) OVER (ORDER BY time) AS prev_global
             FROM {table}
             WHERE region = 'east'
             ORDER BY time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 3);
    assert_eq!(col_i64(&rows, "rn", 0), 1);
    assert_eq!(col_i64(&rows, "rn", 2), 3);
    // east values 10+20+10 = 40
    assert!((col_f64(&rows, "region_sum", 0) - 40.0).abs() < 1e-9);
    assert!((col_f64(&rows, "region_sum", 1) - 40.0).abs() < 1e-9);
    assert!(col_is_null(&rows, "prev_global", 0));
    assert_eq!(col_f64(&rows, "prev_global", 1), 10.0);
}
