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

//! DDL IT: DROP TABLE / IF EXISTS / recreate after drop.

use monots_integration_tests::{
    assert_err_contains, scalar_i64_named, table_names_from_show, unique_table, TestContext,
};
use pretty_assertions::assert_eq;

#[tokio::test]
async fn drop_table_removes_from_catalog_and_rejects_insert() {
    let table = unique_table("drop_basic");
    let mut ctx = TestContext::new("ddl_drop_basic").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, v BIGINT)"
        ))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!("INSERT INTO {table} (time, v) VALUES (1, 10)"))
        .await
        .unwrap();

    ctx.client
        .no_query(&format!("DROP TABLE {table}"))
        .await
        .unwrap();

    let show = ctx.client.query("SHOW TABLES").await.unwrap();
    let names = table_names_from_show(&show);
    assert!(
        !names.contains(&table),
        "dropped table still in SHOW TABLES: {names:?}"
    );

    let err = ctx
        .client
        .no_query(&format!("INSERT INTO {table} (time, v) VALUES (2, 20)"))
        .await
        .unwrap_err();
    assert_err_contains(
        &err,
        &["not found", "does not exist", "unknown table", "no such"],
    );
}

#[tokio::test]
async fn drop_table_if_exists_is_idempotent() {
    let table = unique_table("drop_ife");
    let mut ctx = TestContext::new("ddl_drop_if_exists").await;

    ctx.client
        .no_query(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .unwrap();

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, v INT)"
        ))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .unwrap();

    let show = ctx.client.query("SHOW TABLES").await.unwrap();
    assert!(!table_names_from_show(&show).contains(&table));
}

#[tokio::test]
async fn drop_missing_table_without_if_exists_errors() {
    let table = unique_table("drop_missing");
    let mut ctx = TestContext::new("ddl_drop_missing").await;

    let err = ctx
        .client
        .no_query(&format!("DROP TABLE {table}"))
        .await
        .unwrap_err();
    assert_err_contains(&err, &["not found", "does not exist", "unknown", "no such"]);
}

#[tokio::test]
async fn recreate_same_name_after_drop() {
    let table = unique_table("drop_recreate");
    let mut ctx = TestContext::new("ddl_drop_recreate").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, v BIGINT)"
        ))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!("INSERT INTO {table} (time, v) VALUES (1, 1)"))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!("DROP TABLE {table}"))
        .await
        .unwrap();

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, v BIGINT)"
        ))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!("INSERT INTO {table} (time, v) VALUES (2, 99)"))
        .await
        .unwrap();

    let rows = ctx
        .client
        .query(&format!("SELECT COUNT(*) AS c, SUM(v) AS s FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&rows, "c"), 1);
    assert_eq!(scalar_i64_named(&rows, "s"), 99);
}

#[tokio::test]
async fn concurrent_drop_and_create_same_table() {
    let table = unique_table("drop_race");
    let mut ctx = TestContext::new("ddl_drop_race").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, v INT)"
        ))
        .await
        .unwrap();

    let mut c_drop = ctx.inst.authenticated_client().await.unwrap();
    let mut c_create = ctx.inst.authenticated_client().await.unwrap();
    let t_drop = table.clone();
    let t_create = table.clone();

    let (drop_res, create_res) = tokio::join!(
        async move {
            c_drop
                .no_query(&format!("DROP TABLE IF EXISTS {t_drop}"))
                .await
        },
        async move {
            // May fail if drop wins first; may succeed if create wins on existing table.
            c_create
                .no_query(&format!(
                    "CREATE TABLE {t_create} (time BIGINT NOT NULL, v INT)"
                ))
                .await
        }
    );

    assert!(
        drop_res.is_ok(),
        "DROP IF EXISTS should not fail: {drop_res:?}"
    );
    let _ = create_res; // ok or already-exists / not-found race — both fine

    // Ensure catalog is usable: recreate cleanly and insert one row.
    let _ = ctx
        .client
        .no_query(&format!("DROP TABLE IF EXISTS {table}"))
        .await;
    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, v INT)"
        ))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!("INSERT INTO {table} (time, v) VALUES (1, 7)"))
        .await
        .unwrap();

    let show = ctx.client.query("SHOW TABLES").await.unwrap();
    let names = table_names_from_show(&show);
    assert_eq!(
        names.iter().filter(|n| *n == &table).count(),
        1,
        "expected exactly one catalog entry, got {names:?}"
    );
    let rows = ctx
        .client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&rows, "c"), 1);
}
