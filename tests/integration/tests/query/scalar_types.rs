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

//! Query IT: scalar type filtering / projection / aggregates.

#[path = "common.rs"]
mod common;

use common::boot;
use monots_integration_tests::{
    col_i64, col_str, scalar_bool_named, scalar_f64_named, scalar_i64_named, total_rows,
    unique_table,
};

#[tokio::test]
async fn query_bigint_filters_and_aggs() {
    let table = unique_table("st_bi");
    let (_inst, mut client) = boot("scalar_bigint").await;

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
            "SELECT SUM(value) AS s FROM {table} WHERE value >= 20"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&rows, "s"), 50);
}

#[tokio::test]
async fn query_double_filters_and_aggs() {
    let table = unique_table("st_dbl");
    let (_inst, mut client) = boot("scalar_double").await;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, value DOUBLE)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, value) VALUES (1, 1.5), (2, 2.5), (3, 3.5)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT AVG(value) AS a FROM {table} WHERE value < 3.0"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_f64_named(&rows, "a"), 2.0);
}

#[tokio::test]
async fn query_varchar_equality_and_order() {
    let table = unique_table("st_str");
    let (_inst, mut client) = boot("scalar_varchar").await;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, name VARCHAR)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, name) VALUES (1, 'bob'), (2, 'alice'), (3, 'carol')"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT name FROM {table} WHERE name <> 'bob' ORDER BY name"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_str(&rows, "name", 0), "alice");
    assert_eq!(col_str(&rows, "name", 1), "carol");
}

#[tokio::test]
async fn query_boolean_filters() {
    let table = unique_table("st_bool");
    let (_inst, mut client) = boot("scalar_bool").await;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, ok BOOLEAN)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, ok) VALUES (1, TRUE), (2, FALSE), (3, TRUE)"
        ))
        .await
        .unwrap();

    let count = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {table} WHERE ok = TRUE"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 2);

    let rows = client
        .query(&format!("SELECT ok FROM {table} WHERE time = 2"))
        .await
        .unwrap();
    assert!(!scalar_bool_named(&rows, "ok"));
}

#[tokio::test]
async fn query_int_family_mixed() {
    let table = unique_table("st_ints");
    let (_inst, mut client) = boot("scalar_ints").await;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (
                time BIGINT NOT NULL,
                i8 TINYINT,
                i16 SMALLINT,
                i32 INT,
                u32 INT UNSIGNED
            )"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, i8, i16, i32, u32) VALUES
             (1, 1, 10, 100, 1000),
             (2, 2, 20, 200, 2000)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT CAST(i8 AS BIGINT) + CAST(i16 AS BIGINT) + CAST(i32 AS BIGINT) AS s
             FROM {table} WHERE u32 > 1500"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    assert_eq!(col_i64(&rows, "s", 0), 222);
}

#[tokio::test]
async fn query_timestamp_column_range() {
    let table = unique_table("st_ts");
    let (_inst, mut client) = boot("scalar_timestamp").await;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time TIMESTAMP(3) NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, value) VALUES (1000, 1), (2000, 2), (3000, 3)"
        ))
        .await
        .unwrap();

    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 3);

    let rows = client
        .query(&format!(
            "SELECT value FROM {table} ORDER BY time DESC LIMIT 1"
        ))
        .await
        .unwrap();
    assert_eq!(col_i64(&rows, "value", 0), 3);
}

#[tokio::test]
async fn query_decimal_compare_and_sum() {
    let table = unique_table("st_dec");
    let (_inst, mut client) = boot("scalar_decimal").await;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, price DECIMAL(10, 2))"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, price) VALUES (1, 10.50), (2, 20.25), (3, 5.00)"
        ))
        .await
        .unwrap();

    let count = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {table} WHERE price >= 10.50"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 2);
}
