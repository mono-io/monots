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

//! Query IT: arithmetic, strings, CASE, CAST, COALESCE, time functions.

#[path = "common.rs"]
mod common;

use common::boot;
use monots_integration_tests::{
    col_f64, col_i64, col_str, scalar_i64_named, total_rows, unique_table,
};

async fn seed(client: &mut sdk::Client, table: &str) {
    client
        .no_query(&format!(
            "CREATE TABLE {table} (
                time BIGINT NOT NULL,
                region VARCHAR,
                device_id VARCHAR,
                value DOUBLE,
                optional_value DOUBLE
            )"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, region, device_id, value, optional_value) VALUES
             (1000, 'east', 'sn_1', 10.0, NULL),
             (2000, 'east', 'sn_2', 20.0, 2.0),
             (3000, 'west', 'sn_3', 30.0, NULL),
             (3600000, 'north', 'sn_4', 40.0, 4.0),
             (86400000, 'south', 'sn_5', 50.0, 5.0)"
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn arithmetic_expressions_in_select() {
    let table = unique_table("expr_arith_sel");
    let (_inst, mut client) = boot("expr_arith_select").await;
    seed(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "SELECT value * 100 AS scaled, (value + optional_value) / 2 AS mid
             FROM {table}
             WHERE time = 2000"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    assert_eq!(col_f64(&rows, "scaled", 0), 2000.0);
    assert_eq!(col_f64(&rows, "mid", 0), 11.0);
}

#[tokio::test]
async fn arithmetic_expressions_in_where() {
    let table = unique_table("expr_arith_wh");
    let (_inst, mut client) = boot("expr_arith_where").await;
    seed(&mut client, &table).await;

    let count = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {table} WHERE value + 10 > 35"
        ))
        .await
        .unwrap();
    // 30+10, 40+10, 50+10
    assert_eq!(scalar_i64_named(&count, "c"), 3);
}

#[tokio::test]
async fn string_function_concat() {
    let table = unique_table("expr_concat");
    let (_inst, mut client) = boot("expr_concat").await;
    seed(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "SELECT concat(region, ':', device_id) AS label
             FROM {table} WHERE time = 1000"
        ))
        .await
        .unwrap();
    assert_eq!(col_str(&rows, "label", 0), "east:sn_1");
}

#[tokio::test]
async fn string_function_u_l_case() {
    let table = unique_table("expr_case_fn");
    let (_inst, mut client) = boot("expr_upper_lower").await;
    seed(&mut client, &table).await;

    let count = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {table} WHERE upper(region) = 'EAST'"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 2);

    let rows = client
        .query(&format!(
            "SELECT lower(region) AS r FROM {table} WHERE time = 3000"
        ))
        .await
        .unwrap();
    assert_eq!(col_str(&rows, "r", 0), "west");
}

#[tokio::test]
async fn string_function_substr() {
    let table = unique_table("expr_substr");
    let (_inst, mut client) = boot("expr_substr").await;
    seed(&mut client, &table).await;

    let count = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {table} WHERE substr(device_id, 1, 3) = 'sn_'"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 5);
}

#[tokio::test]
async fn date_trunc_hour() {
    let table = unique_table("expr_dth");
    let (_inst, mut client) = boot("expr_date_trunc_hour").await;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time TIMESTAMP(3) NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();
    // epoch millis: 0 and 1h+1ms
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, value) VALUES (0, 1), (3600001, 2), (7200000, 3)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT date_trunc('hour', time) AS hour_bucket, SUM(value) AS s
             FROM {table}
             GROUP BY date_trunc('hour', time)
             ORDER BY hour_bucket"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 3);
    assert_eq!(col_i64(&rows, "s", 0), 1);
    assert_eq!(col_i64(&rows, "s", 1), 2);
    assert_eq!(col_i64(&rows, "s", 2), 3);
}

#[tokio::test]
async fn date_trunc_day() {
    let table = unique_table("expr_dtd");
    let (_inst, mut client) = boot("expr_date_trunc_day").await;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time TIMESTAMP(3) NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, value) VALUES
             (0, 1), (3600000, 2), (86400000, 3), (90000000, 4)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT date_trunc('day', time) AS day_bucket, COUNT(*) AS c
             FROM {table}
             GROUP BY date_trunc('day', time)
             ORDER BY day_bucket"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_i64(&rows, "c", 0), 2);
    assert_eq!(col_i64(&rows, "c", 1), 2);
}

#[tokio::test]
async fn case_when_simple() {
    let table = unique_table("expr_case_s");
    let (_inst, mut client) = boot("expr_case_simple").await;
    seed(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "SELECT CASE region
               WHEN 'east' THEN 1
               WHEN 'west' THEN 2
               ELSE 0
             END AS code
             FROM {table}
             WHERE region IN ('east', 'west', 'north')
             ORDER BY time
             LIMIT 3"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 3);
    // times 1000 east, 2000 east, 3000 west — LIMIT 3 takes first three by time
    assert_eq!(col_i64(&rows, "code", 0), 1);
    assert_eq!(col_i64(&rows, "code", 1), 1);
    assert_eq!(col_i64(&rows, "code", 2), 2);
}

#[tokio::test]
async fn case_when_searched() {
    let table = unique_table("expr_case_w");
    let (_inst, mut client) = boot("expr_case_searched").await;
    seed(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "SELECT CASE
               WHEN value >= 40 THEN 'high'
               WHEN value >= 20 THEN 'mid'
               ELSE 'low'
             END AS band
             FROM {table}
             WHERE value IN (10.0, 20.0, 50.0)
             ORDER BY value"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 3);
    assert_eq!(col_str(&rows, "band", 0), "low");
    assert_eq!(col_str(&rows, "band", 1), "mid");
    assert_eq!(col_str(&rows, "band", 2), "high");
}

#[tokio::test]
async fn cast_types() {
    let table = unique_table("expr_cast");
    let (_inst, mut client) = boot("expr_cast").await;
    seed(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "SELECT CAST(value AS BIGINT) AS v_int FROM {table} WHERE time = 3000"
        ))
        .await
        .unwrap();
    assert_eq!(col_i64(&rows, "v_int", 0), 30);
}

#[tokio::test]
async fn null_handling_coalesce() {
    let table = unique_table("expr_coal");
    let (_inst, mut client) = boot("expr_coalesce").await;
    seed(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "SELECT COALESCE(optional_value, 0) AS filled
             FROM {table}
             WHERE time IN (1000, 2000)
             ORDER BY time"
        ))
        .await
        .unwrap();
    assert_eq!(col_f64(&rows, "filled", 0), 0.0);
    assert_eq!(col_f64(&rows, "filled", 1), 2.0);
}

#[tokio::test]
async fn abs_and_round_math() {
    let table = unique_table("expr_math");
    let (_inst, mut client) = boot("expr_math").await;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, value DOUBLE)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, value) VALUES (1, -12.6), (2, 3.4)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT abs(value) AS a, round(value) AS r FROM {table} ORDER BY time"
        ))
        .await
        .unwrap();
    assert_eq!(col_f64(&rows, "a", 0), 12.6);
    assert_eq!(col_f64(&rows, "r", 0), -13.0);
    assert_eq!(col_f64(&rows, "r", 1), 3.0);
}

#[tokio::test]
async fn like_and_ilike_patterns() {
    let table = unique_table("expr_like");
    let (_inst, mut client) = boot("expr_like").await;
    seed(&mut client, &table).await;

    let c1 = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {table} WHERE region LIKE 'e%'"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&c1, "c"), 2);

    // DataFusion may support ILIKE; if not, this test will fail and we adjust.
    let c2 = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {table} WHERE region ILIKE 'EAST'"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&c2, "c"), 2);
}
