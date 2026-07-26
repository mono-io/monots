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

//! DDL IT: large catalogs, identifier edge cases, ALTER negatives.

use monots_integration_tests::{
    assert_err_contains, show_create_statement, table_names_from_show, unique_table, TestContext,
    TIME_COL,
};
use pretty_assertions::assert_eq;

/// Large enough to stress metadata listing without making CI unbearably slow.
/// Override with `MONOTS_IT_CATALOG_SCALE` when running dedicated stress jobs.
fn catalog_scale() -> usize {
    std::env::var("MONOTS_IT_CATALOG_SCALE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500)
}

#[tokio::test]
async fn create_many_tables_show_tables_lists_all() {
    let n = catalog_scale();
    let mut ctx = TestContext::new("ddl_catalog_scale").await;

    let mut expected: Vec<String> = Vec::with_capacity(n);
    for i in 0..n {
        let table = unique_table(&format!("scale_{i}"));
        ctx.client
            .no_query(&format!(
                "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, v INT)"
            ))
            .await
            .unwrap();
        expected.push(table);
    }

    let show = ctx.client.query("SHOW TABLES").await.unwrap();
    let mut names: Vec<String> = table_names_from_show(&show).into_iter().collect();
    names.sort();
    expected.sort();
    assert_eq!(names, expected, "SHOW TABLES must list every created table");
}

#[tokio::test]
async fn rejects_empty_and_overlong_identifiers() {
    let mut ctx = TestContext::new("ddl_ident_invalid").await;

    let long_name = format!("t_{}", "x".repeat(512));
    let err = ctx
        .client
        .no_query(&format!(
            "CREATE TABLE {long_name} ({TIME_COL} BIGINT NOT NULL, v INT)"
        ))
        .await
        .unwrap_err();
    // Parser or catalog should reject; exact message varies.
    assert!(
        !err.to_string().is_empty(),
        "overlong identifier should fail"
    );

    let err = ctx
        .client
        .no_query(&format!(
            "CREATE TABLE bad-name ({TIME_COL} BIGINT NOT NULL, v INT)"
        ))
        .await
        .unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[tokio::test]
async fn quoted_keyword_as_column_name_works() {
    let table = unique_table("kw_col");
    let mut ctx = TestContext::new("ddl_ident_keyword").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, \"select\" BIGINT, \"table\" VARCHAR)"
        ))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, \"select\", \"table\") VALUES (1, 7, 'ok')"
        ))
        .await
        .unwrap();

    let rows = ctx
        .client
        .query(&format!(
            "SELECT \"select\" AS s, \"table\" AS t FROM {table}"
        ))
        .await
        .unwrap();
    assert_eq!(monots_integration_tests::col_i64(&rows, "s", 0), 7);
    assert_eq!(monots_integration_tests::col_str(&rows, "t", 0), "ok");
}

#[tokio::test]
async fn alter_add_duplicate_column_errors() {
    let table = unique_table("alter_dup");
    let mut ctx = TestContext::new("ddl_alter_dup_col").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, v INT)"
        ))
        .await
        .unwrap();

    let err = ctx
        .client
        .no_query(&format!("ALTER TABLE {table} ADD COLUMN v DOUBLE"))
        .await
        .unwrap_err();
    assert_err_contains(&err, &["exist", "duplicate", "already", "column"]);
}

#[tokio::test]
async fn alter_drop_column_unsupported() {
    let table = unique_table("alter_drop_col");
    let mut ctx = TestContext::new("ddl_alter_drop_col").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, v INT)"
        ))
        .await
        .unwrap();

    let err = ctx
        .client
        .no_query(&format!("ALTER TABLE {table} DROP COLUMN v"))
        .await
        .unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[tokio::test]
async fn alter_add_column_and_show_create() {
    let table = unique_table("alter_add_ok");
    let mut ctx = TestContext::new("ddl_alter_add_ok").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, v INT)"
        ))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!("ALTER TABLE {table} ADD COLUMN tags ARRAY<INT>"))
        .await
        .unwrap();

    let ddl = ctx
        .client
        .query(&format!("SHOW CREATE TABLE {table}"))
        .await
        .unwrap();
    let stmt = show_create_statement(&ddl);
    assert!(stmt.contains("tags"), "got: {stmt}");
    assert!(stmt.contains("ARRAY<INT>"), "got: {stmt}");
}

#[tokio::test]
async fn concurrent_create_same_table_name_is_safe() {
    let table = unique_table("conc_create");
    let mut ctx = TestContext::new("ddl_concurrent_create").await;

    let mut handles = Vec::new();
    for _ in 0..8 {
        let mut client = ctx.inst.authenticated_client().await.unwrap();
        let t = table.clone();
        handles.push(tokio::spawn(async move {
            client
                .no_query(&format!(
                    "CREATE TABLE {t} ({TIME_COL} BIGINT NOT NULL, v INT)"
                ))
                .await
        }));
    }

    let mut oks = 0usize;
    let mut errs = 0usize;
    for h in handles {
        match h.await.unwrap() {
            Ok(_) => oks += 1,
            Err(_) => errs += 1,
        }
    }
    assert_eq!(
        oks, 1,
        "exactly one CREATE should win, got ok={oks} err={errs}"
    );
    assert!(errs >= 1, "losers should see conflicts");

    let show = ctx.client.query("SHOW TABLES").await.unwrap();
    let names = table_names_from_show(&show);
    assert_eq!(
        names.iter().filter(|n| *n == &table).count(),
        1,
        "catalog must have a single entry: {names:?}"
    );

    ctx.client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, v) VALUES (1, 1)"
        ))
        .await
        .unwrap();
}
