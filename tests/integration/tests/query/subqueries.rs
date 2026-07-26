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

//! Query IT: subqueries and CTEs.

#[path = "common.rs"]
mod common;

use common::boot;
use monots_integration_tests::{
    col_f64, col_i64, col_str, scalar_i64_named, total_rows, unique_table,
};

async fn seed_metrics(client: &mut sdk::Client, table: &str) {
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
             (5000, 'north', 50.0)"
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn subquery_in_predicate() {
    let table = unique_table("sq_in");
    let (_inst, mut client) = boot("subq_in").await;
    seed_metrics(&mut client, &table).await;

    let count = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {table}
             WHERE time IN (SELECT time FROM {table} WHERE region = 'west')"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 2);
}

#[tokio::test]
async fn subquery_not_in_predicate() {
    let table = unique_table("sq_nin");
    let (_inst, mut client) = boot("subq_not_in").await;
    seed_metrics(&mut client, &table).await;

    let count = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {table}
             WHERE region NOT IN (SELECT region FROM {table} WHERE value >= 40)"
        ))
        .await
        .unwrap();
    // west(40) and north(50) excluded → only east rows remain
    assert_eq!(scalar_i64_named(&count, "c"), 2);
}

#[tokio::test]
async fn subquery_in_from_clause() {
    let table = unique_table("sq_from");
    let (_inst, mut client) = boot("subq_from").await;
    seed_metrics(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "SELECT region, total FROM (
               SELECT region, SUM(value) AS total FROM {table} GROUP BY region
             ) s
             WHERE total >= 50
             ORDER BY total DESC"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_str(&rows, "region", 0), "west");
    assert_eq!(col_f64(&rows, "total", 0), 70.0);
}

#[tokio::test]
async fn subquery_exists_predicate() {
    let metrics = unique_table("sq_ex_m");
    let events = unique_table("sq_ex_e");
    let (_inst, mut client) = boot("subq_exists").await;

    client
        .no_query(&format!(
            "CREATE TABLE {metrics} (time BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "CREATE TABLE {events} (time BIGINT NOT NULL, kind VARCHAR)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {metrics} (time, value) VALUES (1, 10), (2, 20), (3, 30)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {events} (time, kind) VALUES (2, 'alert'), (4, 'info')"
        ))
        .await
        .unwrap();

    let count = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {metrics} m
             WHERE EXISTS (
               SELECT 1 FROM {events} e WHERE e.time = m.time AND e.kind = 'alert'
             )"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 1);
}

#[tokio::test]
async fn subquery_not_exists_predicate() {
    let metrics = unique_table("sq_nex_m");
    let events = unique_table("sq_nex_e");
    let (_inst, mut client) = boot("subq_not_exists").await;

    client
        .no_query(&format!(
            "CREATE TABLE {metrics} (time BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "CREATE TABLE {events} (time BIGINT NOT NULL, kind VARCHAR)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {metrics} (time, value) VALUES (1, 10), (2, 20), (3, 30)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {events} (time, kind) VALUES (2, 'alert')"
        ))
        .await
        .unwrap();

    let count = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {metrics} m
             WHERE NOT EXISTS (SELECT 1 FROM {events} e WHERE e.time = m.time)"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 2);
}

#[tokio::test]
async fn correlated_subquery_where() {
    let table = unique_table("sq_corr");
    let (_inst, mut client) = boot("subq_correlated").await;
    seed_metrics(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "SELECT time, value FROM {table} m
             WHERE value > (
               SELECT AVG(value) FROM {table} x WHERE x.region = m.region
             )
             ORDER BY time"
        ))
        .await
        .unwrap();
    // east avg=15 → 20; west avg=35 → 40; north avg=50 → none
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_i64(&rows, "time", 0), 2000);
    assert_eq!(col_i64(&rows, "time", 1), 4000);
}

#[tokio::test]
async fn cte_basic_scalar() {
    let table = unique_table("cte_basic");
    let (_inst, mut client) = boot("cte_basic").await;
    seed_metrics(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "WITH summary AS (
               SELECT region, SUM(value) AS total FROM {table} GROUP BY region
             )
             SELECT region, total FROM summary WHERE total >= 50 ORDER BY total DESC"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_str(&rows, "region", 0), "west");
    assert_eq!(col_f64(&rows, "total", 0), 70.0);
}

#[tokio::test]
async fn cte_multiple_definitions() {
    let table = unique_table("cte_multi");
    let (_inst, mut client) = boot("cte_multi").await;
    seed_metrics(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "WITH east_rows AS (
               SELECT time, value FROM {table} WHERE region = 'east'
             ),
             west_rows AS (
               SELECT time, value FROM {table} WHERE region = 'west'
             )
             SELECT e.time AS et, w.time AS wt, (w.value - e.value) AS delta
             FROM east_rows e
             INNER JOIN west_rows w ON w.time = e.time + 2000
             ORDER BY e.time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_f64(&rows, "delta", 0), 20.0);
    assert_eq!(col_f64(&rows, "delta", 1), 20.0);
}

#[tokio::test]
async fn subquery_scalar_in_select_list() {
    let table = unique_table("sq_sel");
    let (_inst, mut client) = boot("subq_select_scalar").await;
    seed_metrics(&mut client, &table).await;

    let rows = client
        .query(&format!(
            "SELECT time, value,
                    (SELECT MAX(value) FROM {table}) AS global_max
             FROM {table}
             WHERE region = 'east'
             ORDER BY time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_f64(&rows, "global_max", 0), 50.0);
    assert_eq!(col_f64(&rows, "global_max", 1), 50.0);
}
