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

//! Advanced Query IT: JOINs interacting with LSM levels, NULLs, and pushdown filters.
//!
//! Each test calls [`common::boot`], which starts an isolated MonoTS process + data dir.

#[path = "common.rs"]
mod common;

use common::boot;
use monots_integration_tests::{col_i64, col_is_null, col_str, total_rows, unique_table};

#[tokio::test]
async fn join_interacting_with_lsm_flush() {
    let left = unique_table("jlf_l");
    let right = unique_table("jlf_r");
    let (_inst, mut client) = boot("join_lsm_flush").await;

    client
        .no_query(&format!(
            "CREATE TABLE {left} (time BIGINT NOT NULL, k BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {left} (time, k) VALUES (1, 100), (2, 200)"
        ))
        .await
        .unwrap();

    // Push left into SST; right stays in memtable.
    client
        .no_query(&format!("FLUSH TABLE {left}"))
        .await
        .unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {right} (time BIGINT NOT NULL, k BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {right} (time, k) VALUES (1, 100), (3, 300)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT l.k FROM {left} l INNER JOIN {right} r ON l.k = r.k ORDER BY l.k"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    assert_eq!(col_i64(&rows, "k", 0), 100);
}

#[tokio::test]
async fn join_after_both_sides_flushed() {
    let left = unique_table("jbf_l");
    let right = unique_table("jbf_r");
    let (_inst, mut client) = boot("join_both_flushed").await;

    client
        .no_query(&format!(
            "CREATE TABLE {left} (time BIGINT NOT NULL, k BIGINT, v BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "CREATE TABLE {right} (time BIGINT NOT NULL, k BIGINT, tag VARCHAR)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {left} (time, k, v) VALUES (10, 1, 100), (20, 2, 200)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {right} (time, k, tag) VALUES (11, 1, 'A'), (21, 3, 'C')"
        ))
        .await
        .unwrap();

    client
        .no_query(&format!("FLUSH TABLE {left}"))
        .await
        .unwrap();
    client
        .no_query(&format!("FLUSH TABLE {right}"))
        .await
        .unwrap();

    // Extra memtable rows after flush (mixed SST + mem).
    client
        .no_query(&format!(
            "INSERT INTO {left} (time, k, v) VALUES (30, 3, 300)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT l.k, l.v, r.tag FROM {left} l
             INNER JOIN {right} r ON l.k = r.k
             ORDER BY l.k"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_i64(&rows, "k", 0), 1);
    assert_eq!(col_str(&rows, "tag", 0), "A");
    assert_eq!(col_i64(&rows, "k", 1), 3);
    assert_eq!(col_str(&rows, "tag", 1), "C");
}

#[tokio::test]
async fn join_null_keys_should_not_match() {
    let left = unique_table("jnk_l");
    let right = unique_table("jnk_r");
    let (_inst, mut client) = boot("join_null_keys").await;

    client
        .no_query(&format!(
            "CREATE TABLE {left} (time BIGINT NOT NULL, k BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {left} (time, k) VALUES (1, 10), (2, NULL)"
        ))
        .await
        .unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {right} (time BIGINT NOT NULL, k BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {right} (time, k) VALUES (1, 10), (3, NULL)"
        ))
        .await
        .unwrap();

    // INNER JOIN: NULL = NULL is unknown → no match
    let rows = client
        .query(&format!(
            "SELECT l.k AS l_k, r.k AS r_k FROM {left} l
             INNER JOIN {right} r ON l.k = r.k"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1, "Only non-NULL key 10 should match");
    assert_eq!(col_i64(&rows, "l_k", 0), 10);

    // LEFT JOIN: left NULL key kept, right padded NULL (not matched to right NULL)
    let rows = client
        .query(&format!(
            "SELECT l.time, l.k AS l_k, r.k AS r_k FROM {left} l
             LEFT OUTER JOIN {right} r ON l.k = r.k
             ORDER BY l.time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_i64(&rows, "l_k", 0), 10);
    assert_eq!(col_i64(&rows, "r_k", 0), 10);
    assert!(col_is_null(&rows, "l_k", 1));
    assert!(col_is_null(&rows, "r_k", 1));
}

#[tokio::test]
async fn join_inside_subquery_and_cte() {
    let a = unique_table("jsc_a");
    let b = unique_table("jsc_b");
    let (_inst, mut client) = boot("join_subquery_cte").await;

    client
        .no_query(&format!(
            "CREATE TABLE {a} (time BIGINT NOT NULL, k BIGINT, v BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "CREATE TABLE {b} (time BIGINT NOT NULL, k BIGINT, tag VARCHAR)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {a} (time, k, v) VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {b} (time, k, tag) VALUES (11, 10, 'X'), (22, 30, 'Z')"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT t1.v, b.tag FROM (
               SELECT k, v FROM {a} WHERE v >= 100
             ) t1
             INNER JOIN {b} b ON t1.k = b.k
             ORDER BY t1.v"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_i64(&rows, "v", 0), 100);
    assert_eq!(col_str(&rows, "tag", 0), "X");
    assert_eq!(col_i64(&rows, "v", 1), 300);
    assert_eq!(col_str(&rows, "tag", 1), "Z");

    let rows = client
        .query(&format!(
            "WITH a_filt AS (SELECT k, v FROM {a} WHERE k <> 20)
             SELECT a_filt.v, b.tag FROM a_filt
             INNER JOIN {b} b ON a_filt.k = b.k
             ORDER BY a_filt.v"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_i64(&rows, "v", 0), 100);
    assert_eq!(col_i64(&rows, "v", 1), 300);
}

#[tokio::test]
async fn join_time_range_predicate_pushdown() {
    let left = unique_table("jpp_l");
    let right = unique_table("jpp_r");
    let (_inst, mut client) = boot("join_pushdown").await;

    client
        .no_query(&format!(
            "CREATE TABLE {left} (time BIGINT NOT NULL, k BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "CREATE TABLE {right} (time BIGINT NOT NULL, k BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {left} (time, k) VALUES (100, 1), (200, 2), (300, 3)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {right} (time, k) VALUES (100, 1), (200, 2), (300, 3)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT l.time, l.k FROM {left} l
             INNER JOIN {right} r ON l.k = r.k
             WHERE l.time > 150 AND l.time < 250
             ORDER BY l.time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    assert_eq!(col_i64(&rows, "time", 0), 200);
    assert_eq!(col_i64(&rows, "k", 0), 2);
}

#[tokio::test]
async fn cross_join_with_empty_table() {
    let left = unique_table("jce_l");
    let right = unique_table("jce_r");
    let (_inst, mut client) = boot("join_cross_empty").await;

    client
        .no_query(&format!(
            "CREATE TABLE {left} (time BIGINT NOT NULL, v BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!("INSERT INTO {left} (time, v) VALUES (1, 10)"))
        .await
        .unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {right} (time BIGINT NOT NULL, v BIGINT)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT l.v AS lv, r.v AS rv FROM {left} l CROSS JOIN {right} r"
        ))
        .await
        .unwrap();
    assert_eq!(
        total_rows(&rows),
        0,
        "Cartesian product with empty set must be empty"
    );
}

#[tokio::test]
async fn join_output_type_coercion() {
    let a = unique_table("jot_a");
    let b = unique_table("jot_b");
    let (_inst, mut client) = boot("join_type_coercion").await;

    client
        .no_query(&format!("CREATE TABLE {a} (time BIGINT NOT NULL, v INT)"))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "CREATE TABLE {b} (time BIGINT NOT NULL, v BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!("INSERT INTO {a} (time, v) VALUES (1, 10)"))
        .await
        .unwrap();
    client
        .no_query(&format!("INSERT INTO {b} (time, v) VALUES (1, 20)"))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT (a.v + b.v) AS sum_v FROM {a} a
             INNER JOIN {b} b ON a.time = b.time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    assert_eq!(col_i64(&rows, "sum_v", 0), 30);
}
