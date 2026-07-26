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

//! `time` column as Arrow Timestamp(s/ms/us/ns): DDL, write, query, restart, bounds.

use monots_integration_tests::{
    assert_err_contains, col_i64, scalar_i64_named, show_create_statement, total_rows,
    unique_table, TestContext, TIME_COL, VALUE_COL,
};
use pretty_assertions::assert_eq;

#[tokio::test]
async fn time_as_timestamp_millisecond_roundtrip() {
    let table = unique_table("time_ms");
    let mut ctx = TestContext::new("time_col_ms").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} TIMESTAMP NOT NULL, {VALUE_COL} DOUBLE)"
        ))
        .await
        .unwrap();
    // TIMESTAMP INSERT requires epoch integers matching column precision (docs/sql.md).
    ctx.client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, {VALUE_COL}) VALUES \
             (1717236000000, 1.5), (1717236001000, 2.5)"
        ))
        .await
        .unwrap();

    let rows = ctx
        .client
        .query(&format!(
            "SELECT {TIME_COL}, {VALUE_COL} FROM {table} ORDER BY {TIME_COL}"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    ctx.assert_ms_timestamp_col(&rows, TIME_COL, &[1717236000000, 1717236001000]);
}

#[tokio::test]
async fn time_as_timestamp_microsecond_roundtrip() {
    let table = unique_table("time_us");
    let mut ctx = TestContext::new("time_col_us").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} TIMESTAMP(6) NOT NULL, payload INT)"
        ))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, payload) VALUES (1717236000000123, 42)"
        ))
        .await
        .unwrap();

    let rows = ctx
        .client
        .query(&format!("SELECT {TIME_COL} FROM {table}"))
        .await
        .unwrap();
    ctx.assert_us_timestamp_col(&rows, TIME_COL, &[1717236000000123]);
}

#[tokio::test]
async fn time_as_timestamp_second_and_nanosecond() {
    let table = unique_table("time_sn");
    let mut ctx = TestContext::new("time_col_sn").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} TIMESTAMP(0) NOT NULL, tag VARCHAR)"
        ))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, tag) VALUES (1717236000, 'sec')"
        ))
        .await
        .unwrap();

    let rows = ctx
        .client
        .query(&format!("SELECT {TIME_COL} FROM {table}"))
        .await
        .unwrap();
    ctx.assert_s_timestamp_col(&rows, TIME_COL, &[1717236000]);

    let table_ns = unique_table("time_ns");
    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table_ns} ({TIME_COL} TIMESTAMP(9) NOT NULL, v INT)"
        ))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!(
            "INSERT INTO {table_ns} ({TIME_COL}, v) VALUES (1717236000000000000, 7)"
        ))
        .await
        .unwrap();
    let rows = ctx
        .client
        .query(&format!("SELECT {TIME_COL} FROM {table_ns}"))
        .await
        .unwrap();
    ctx.assert_ns_timestamp_col(&rows, TIME_COL, &[1717236000000000000]);
}

#[tokio::test]
async fn rejects_table_without_time_column() {
    let table = unique_table("no_time");
    let mut ctx = TestContext::new("time_col_required").await;

    let err = ctx
        .client
        .no_query(&format!(
            "CREATE TABLE {table} (event_at TIMESTAMP NOT NULL, v INT)"
        ))
        .await
        .unwrap_err();
    assert_err_contains(&err, &["time"]);
}

#[tokio::test]
async fn timestamp_time_column_survives_restart() {
    let table = unique_table("time_restart");
    let mut ctx = TestContext::new("time_col_restart").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} TIMESTAMP(3) NOT NULL, v INT)"
        ))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, v) VALUES (1717236000123, 99)"
        ))
        .await
        .unwrap();

    ctx.inst.restart().await.unwrap();
    ctx.refresh_client().await;

    let rows = ctx
        .client
        .query(&format!(
            "SELECT {TIME_COL}, v FROM {table} ORDER BY {TIME_COL}"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    ctx.assert_ms_timestamp_col(&rows, TIME_COL, &[1717236000123]);

    let ddl = ctx
        .client
        .query(&format!("SHOW CREATE TABLE {table}"))
        .await
        .unwrap();
    let stmt = show_create_statement(&ddl);
    assert!(
        stmt.contains("TIMESTAMP"),
        "expected TIMESTAMP in SHOW CREATE, got: {stmt}"
    );
}

#[tokio::test]
async fn rejects_iso8601_timestamp_string_literal() {
    let table = unique_table("time_iso");
    let mut ctx = TestContext::new("time_col_iso_reject").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} TIMESTAMP NOT NULL, v INT)"
        ))
        .await
        .unwrap();

    let err = ctx
        .client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, v) VALUES ('2024-06-01 10:00:00.000', 1)"
        ))
        .await
        .unwrap_err();
    assert_err_contains(
        &err,
        &["type", "timestamp", "mismatch", "cast", "parse", "int"],
    );
}

#[tokio::test]
async fn bigint_time_boundary_values_roundtrip_and_survive_restart() {
    let table = unique_table("time_bounds");
    let mut ctx = TestContext::new("time_col_bounds").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, tag VARCHAR)"
        ))
        .await
        .unwrap();

    // Unique times — MonoTS newest-wins collapses duplicate timestamps.
    // Note: raw SQL literal `i64::MIN` (-2^63) may fail parsers that negate after
    // parsing a positive magnitude; use MIN+1 as the lower edge.
    let cases = [
        (i64::MIN + 1, "near_min"),
        (-1_i64, "neg"),
        (0_i64, "epoch"),
        (1_i64, "one"),
        (i64::MAX, "max"),
    ];
    for (ts, tag) in cases {
        ctx.client
            .no_query(&format!(
                "INSERT INTO {table} ({TIME_COL}, tag) VALUES ({ts}, '{tag}')"
            ))
            .await
            .unwrap();
    }

    let rows = ctx
        .client
        .query(&format!(
            "SELECT {TIME_COL}, tag FROM {table} ORDER BY {TIME_COL}"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 5);
    assert_eq!(col_i64(&rows, TIME_COL, 0), i64::MIN + 1);
    assert_eq!(col_i64(&rows, TIME_COL, 2), 0);
    assert_eq!(col_i64(&rows, TIME_COL, 4), i64::MAX);

    ctx.inst.restart().await.unwrap();
    ctx.refresh_client().await;

    let count = ctx
        .client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 5);
    let rows = ctx
        .client
        .query(&format!(
            "SELECT {TIME_COL} FROM {table} WHERE {TIME_COL} = {}",
            i64::MIN + 1
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    assert_eq!(col_i64(&rows, TIME_COL, 0), i64::MIN + 1);
}

#[tokio::test]
async fn duplicate_time_newest_wins_keeps_last_payload() {
    let table = unique_table("time_dedup");
    let mut ctx = TestContext::new("time_col_dedup").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, region VARCHAR, v BIGINT)"
        ))
        .await
        .unwrap();

    // Same time, different tags — newest write wins (single row retained).
    ctx.client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, region, v) VALUES (1000, 'east', 1)"
        ))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, region, v) VALUES (1000, 'west', 2)"
        ))
        .await
        .unwrap();

    let rows = ctx
        .client
        .query(&format!(
            "SELECT region, v FROM {table} WHERE {TIME_COL} = 1000"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    assert_eq!(
        monots_integration_tests::col_str(&rows, "region", 0),
        "west"
    );
    assert_eq!(col_i64(&rows, "v", 0), 2);
}
