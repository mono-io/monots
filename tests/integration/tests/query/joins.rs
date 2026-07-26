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

//! Query IT: JOIN variants (INNER / OUTER / CROSS / self / multi-table).

#[path = "common.rs"]
mod common;

use common::{boot, insert_numeric_series};
use monots_integration_tests::{
    col_i64, col_is_null, col_str, scalar_i64_named, total_rows, unique_table,
};

/// Two tables joined on `k` (not `time`).
///
/// MonoTS newest-wins collapses duplicate timestamps, so every physical row needs a
/// unique `time`. One-to-many matches are expressed via duplicate `k` values on the
/// right (e.g. k=400 → T40-1 and T40-2).
///
/// Left k: 100, 200, 300, 400
/// Right k: 200, 400, 400, 500  (300 missing on right; 100 missing on right; 500 missing on left)
async fn setup_join_tables(client: &mut sdk::Client, left: &str, right: &str) {
    client
        .no_query(&format!(
            "CREATE TABLE {left} (time BIGINT NOT NULL, k BIGINT, value BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {left} (time, k, value) VALUES
             (100, 100, 1), (200, 200, 2), (300, 300, 3), (400, 400, 4)"
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
            "INSERT INTO {right} (time, k, tag) VALUES
             (201, 200, 'T20'), (401, 400, 'T40-1'), (402, 400, 'T40-2'), (501, 500, 'T50')"
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn inner_join_basic() {
    let left = unique_table("join_in_l");
    let right = unique_table("join_in_r");
    let (_inst, mut client) = boot("join_inner").await;
    setup_join_tables(&mut client, &left, &right).await;

    // Matches: 200, 400×2 → 3 rows
    let rows = client
        .query(&format!(
            "SELECT l.k, l.value, r.tag FROM {left} l
             INNER JOIN {right} r ON l.k = r.k
             ORDER BY l.k, r.tag"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 3);
    assert_eq!(col_i64(&rows, "k", 0), 200);
    assert_eq!(col_str(&rows, "tag", 0), "T20");
    assert_eq!(col_i64(&rows, "k", 1), 400);
    assert_eq!(col_str(&rows, "tag", 1), "T40-1");
    assert_eq!(col_str(&rows, "tag", 2), "T40-2");
}

#[tokio::test]
async fn left_outer_join_basic() {
    let left = unique_table("join_left_l");
    let right = unique_table("join_left_r");
    let (_inst, mut client) = boot("join_left_outer").await;
    setup_join_tables(&mut client, &left, &right).await;

    // Left preserved: 100(NULL), 200(T20), 300(NULL), 400(T40-1), 400(T40-2) → 5 rows
    let rows = client
        .query(&format!(
            "SELECT l.k, l.value, r.tag FROM {left} l
             LEFT OUTER JOIN {right} r ON l.k = r.k
             ORDER BY l.k, r.tag"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 5);

    assert_eq!(col_i64(&rows, "k", 0), 100);
    assert!(col_is_null(&rows, "tag", 0));

    assert_eq!(col_i64(&rows, "k", 1), 200);
    assert_eq!(col_str(&rows, "tag", 1), "T20");

    assert_eq!(col_i64(&rows, "k", 2), 300);
    assert!(col_is_null(&rows, "tag", 2));

    assert_eq!(col_i64(&rows, "k", 3), 400);
    assert_eq!(col_str(&rows, "tag", 3), "T40-1");
    assert_eq!(col_str(&rows, "tag", 4), "T40-2");
}

#[tokio::test]
async fn right_outer_join_basic() {
    let left = unique_table("join_right_l");
    let right = unique_table("join_right_r");
    let (_inst, mut client) = boot("join_right_outer").await;
    setup_join_tables(&mut client, &left, &right).await;

    // Right preserved: 200, 400×2, 500(NULL) → 4 rows
    let rows = client
        .query(&format!(
            "SELECT l.k AS l_k, r.k AS r_k, l.value, r.tag FROM {left} l
             RIGHT OUTER JOIN {right} r ON l.k = r.k
             ORDER BY r.k, r.tag"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 4);

    assert_eq!(col_i64(&rows, "r_k", 0), 200);
    assert_eq!(col_i64(&rows, "value", 0), 2);

    assert_eq!(col_i64(&rows, "r_k", 3), 500);
    assert!(col_is_null(&rows, "l_k", 3));
    assert!(col_is_null(&rows, "value", 3));
}

#[tokio::test]
async fn full_outer_join_basic() {
    let left = unique_table("join_full_l");
    let right = unique_table("join_full_r");
    let (_inst, mut client) = boot("join_full_outer").await;
    setup_join_tables(&mut client, &left, &right).await;

    // Union: 100, 200, 300, 400×2, 500 → 6 rows
    let rows = client
        .query(&format!(
            "SELECT
               COALESCE(l.k, r.k) AS merged_k,
               l.value, r.tag
             FROM {left} l
             FULL OUTER JOIN {right} r ON l.k = r.k
             ORDER BY merged_k, r.tag"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 6);

    assert_eq!(col_i64(&rows, "merged_k", 0), 100);
    assert!(col_is_null(&rows, "tag", 0));

    assert_eq!(col_i64(&rows, "merged_k", 2), 300);
    assert!(col_is_null(&rows, "tag", 2));

    assert_eq!(col_i64(&rows, "merged_k", 5), 500);
    assert!(col_is_null(&rows, "value", 5));
}

#[tokio::test]
async fn cross_join_basic() {
    let left = unique_table("join_cross_l");
    let right = unique_table("join_cross_r");
    let (_inst, mut client) = boot("join_cross").await;

    client
        .no_query(&format!(
            "CREATE TABLE {left} (time BIGINT NOT NULL, v BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {left} (time, v) VALUES (1, 10), (2, 20)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "CREATE TABLE {right} (time BIGINT NOT NULL, tag VARCHAR)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {right} (time, tag) VALUES (1, 'A'), (3, 'B'), (5, 'C')"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT l.v, r.tag FROM {left} l CROSS JOIN {right} r"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 6);
}

#[tokio::test]
async fn join_on_multiple_columns() {
    let left = unique_table("join_multi_l");
    let right = unique_table("join_multi_r");
    let (_inst, mut client) = boot("join_multi_cols").await;

    // Unique `time` per row (MonoTS dedupes on timestamp).
    client
        .no_query(&format!(
            "CREATE TABLE {left} (time BIGINT NOT NULL, host VARCHAR, val BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {left} (time, host, val) VALUES
             (100, 'h1', 1), (101, 'h2', 2), (200, 'h1', 3)"
        ))
        .await
        .unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {right} (time BIGINT NOT NULL, host VARCHAR, status BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {right} (time, host, status) VALUES
             (100, 'h1', 200), (101, 'h3', 404), (200, 'h1', 500)"
        ))
        .await
        .unwrap();

    // Matches: (100,h1), (200,h1) → 2 rows
    let rows = client
        .query(&format!(
            "SELECT l.val, r.status FROM {left} l
             INNER JOIN {right} r ON l.time = r.time AND l.host = r.host
             ORDER BY l.time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_i64(&rows, "val", 0), 1);
    assert_eq!(col_i64(&rows, "status", 0), 200);
    assert_eq!(col_i64(&rows, "val", 1), 3);
    assert_eq!(col_i64(&rows, "status", 1), 500);
}

#[tokio::test]
async fn join_on_varchar_tag_column() {
    let metrics = unique_table("join_tag_m");
    let meta = unique_table("join_tag_meta");
    let (_inst, mut client) = boot("join_tag").await;

    client
        .no_query(&format!(
            "CREATE TABLE {metrics} (time BIGINT NOT NULL, device_id VARCHAR, v DOUBLE)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {metrics} (time, device_id, v) VALUES
             (1000, 'd1', 1.1), (2000, 'd2', 2.2), (3000, 'd1', 3.3)"
        ))
        .await
        .unwrap();

    // Unique time per dimension row.
    client
        .no_query(&format!(
            "CREATE TABLE {meta} (time BIGINT NOT NULL, device_id VARCHAR, model VARCHAR)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {meta} (time, device_id, model) VALUES
             (0, 'd1', 'ModA'), (1, 'd3', 'ModB')"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT m.time, m.v, meta.model FROM {metrics} m
             INNER JOIN {meta} meta ON m.device_id = meta.device_id
             ORDER BY m.time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_i64(&rows, "time", 0), 1000);
    assert_eq!(col_str(&rows, "model", 0), "ModA");
    assert_eq!(col_i64(&rows, "time", 1), 3000);
    assert_eq!(col_str(&rows, "model", 1), "ModA");
}

#[tokio::test]
async fn join_with_non_equi_condition() {
    let a = unique_table("jne");
    let b = unique_table("jneb");
    let (_inst, mut client) = boot("join_non_equi").await;

    client
        .no_query(&format!(
            "CREATE TABLE {a} (time BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "CREATE TABLE {b} (time BIGINT NOT NULL, threshold BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {a} (time, value) VALUES (1, 5), (2, 15), (3, 25)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {b} (time, threshold) VALUES (1, 10), (2, 10), (3, 10)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT a.value, b.threshold FROM {a} a
             INNER JOIN {b} b ON a.time = b.time AND a.value > b.threshold
             ORDER BY a.value"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_i64(&rows, "value", 0), 15);
    assert_eq!(col_i64(&rows, "value", 1), 25);
}

#[tokio::test]
async fn self_join_metrics() {
    let t = unique_table("selfj");
    let (_inst, mut client) = boot("join_self").await;

    client
        .no_query(&format!(
            "CREATE TABLE {t} (time BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {t} (time, value) VALUES (100, 10), (200, 25), (300, 40)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT a.time AS t0, b.time AS t1, (b.value - a.value) AS delta
             FROM {t} a
             INNER JOIN {t} b ON b.time = a.time + 100
             ORDER BY a.time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(col_i64(&rows, "delta", 0), 15);
    assert_eq!(col_i64(&rows, "delta", 1), 15);
}

#[tokio::test]
async fn multi_table_join_three_plus() {
    let a = unique_table("j3a");
    let b = unique_table("j3b");
    let c = unique_table("j3c");
    let (_inst, mut client) = boot("join_three").await;

    for (tbl, col) in [(&a, "va"), (&b, "vb"), (&c, "vc")] {
        client
            .no_query(&format!(
                "CREATE TABLE {tbl} (time BIGINT NOT NULL, {col} BIGINT)"
            ))
            .await
            .unwrap();
    }
    client
        .no_query(&format!(
            "INSERT INTO {a} (time, va) VALUES (1, 10), (2, 20)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {b} (time, vb) VALUES (1, 100), (2, 200)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {c} (time, vc) VALUES (1, 1000), (3, 3000)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT a.va, b.vb, c.vc FROM {a} a
             INNER JOIN {b} b ON a.time = b.time
             INNER JOIN {c} c ON a.time = c.time
             ORDER BY a.time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    assert_eq!(col_i64(&rows, "va", 0), 10);
    assert_eq!(col_i64(&rows, "vb", 0), 100);
    assert_eq!(col_i64(&rows, "vc", 0), 1000);
}

#[tokio::test]
async fn inner_join_with_where_on_both_sides() {
    let metrics = unique_table("jwm");
    let events = unique_table("jwe");
    let (_inst, mut client) = boot("join_where_both").await;

    client
        .no_query(&format!(
            "CREATE TABLE {metrics} (time BIGINT NOT NULL, region VARCHAR, value DOUBLE)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "CREATE TABLE {events} (time BIGINT NOT NULL, severity INT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {metrics} (time, region, value) VALUES
             (1008, 'east', 12.0), (1012, 'east', 18.0), (1016, 'west', 5.0)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {events} (time, severity) VALUES (1008, 2), (1012, 3), (1016, 1)"
        ))
        .await
        .unwrap();

    let count = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {metrics} m
             INNER JOIN {events} e ON m.time = e.time
             WHERE m.region = 'east' AND m.value >= 10.0 AND e.severity >= 2"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 2);
}

#[tokio::test]
async fn large_inner_join_table_scans() {
    let left = unique_table("join_large_l");
    let right = unique_table("join_large_r");
    let (_inst, mut client) = boot("join_large").await;

    client
        .no_query(&format!(
            "CREATE TABLE {left} (time BIGINT NOT NULL, v1 BIGINT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "CREATE TABLE {right} (time BIGINT NOT NULL, v2 BIGINT)"
        ))
        .await
        .unwrap();

    let n = 10_000usize;
    insert_numeric_series(&mut client, &left, "v1", 1_000_000, n, 0).await;
    insert_numeric_series(&mut client, &right, "v2", 1_000_000, n, 100).await;

    let rows = client
        .query(&format!(
            "SELECT COUNT(*) AS c, SUM(l.v1) AS s1, SUM(r.v2) AS s2
             FROM {left} l INNER JOIN {right} r ON l.time = r.time"
        ))
        .await
        .unwrap();
    assert_eq!(col_i64(&rows, "c", 0), n as i64);
    // SUM(0..9999) = 9999 * 10000 / 2
    assert_eq!(col_i64(&rows, "s1", 0), 49_995_000);
    // SUM(100..10099) = 100*10000 + 49995000
    assert_eq!(col_i64(&rows, "s2", 0), 50_995_000);
}
