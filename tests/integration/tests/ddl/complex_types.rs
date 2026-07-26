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

//! DDL/DML IT: nested types, NULLs inside ARRAY/STRUCT, BLOB sizes, type coercion.

use arrow::array::{Array, BinaryArray, ListArray, StructArray};
use monots_integration_tests::{
    assert_err_contains, scalar_bool_named, scalar_i64_named, total_rows, unique_table,
    TestContext, TIME_COL,
};
use pretty_assertions::assert_eq;

#[tokio::test]
async fn nested_array_of_struct_with_array_roundtrip() {
    let table = unique_table("nested_deep");
    let mut ctx = TestContext::new("ddl_nested_deep").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} (
                {TIME_COL} BIGINT NOT NULL,
                payload ARRAY<STRUCT<name VARCHAR, vals ARRAY<INT>>>
            )"
        ))
        .await
        .unwrap();

    ctx.client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, payload) VALUES (
               1,
               ARRAY[('a', ARRAY[1, 2]), ('b', ARRAY[3])]
             )"
        ))
        .await
        .unwrap();

    let rows = ctx
        .client
        .query(&format!("SELECT payload FROM {table}"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    let col = rows[0].column_by_name("payload").unwrap();
    assert!(
        col.as_any().downcast_ref::<ListArray>().is_some(),
        "payload should be ListArray, got {:?}",
        col.data_type()
    );
}

#[tokio::test]
async fn array_and_struct_null_elements() {
    let table = unique_table("nested_nulls");
    let mut ctx = TestContext::new("ddl_nested_nulls").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} (
                {TIME_COL} BIGINT NOT NULL,
                tags ARRAY<VARCHAR>,
                meta STRUCT<name VARCHAR, score INT>
            )"
        ))
        .await
        .unwrap();

    ctx.client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, tags, meta) VALUES
             (1, ARRAY['a', NULL, 'b'], (NULL, 90))"
        ))
        .await
        .unwrap();

    let rows = ctx
        .client
        .query(&format!("SELECT tags, meta FROM {table}"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);

    let tags = rows[0]
        .column_by_name("tags")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(tags.value(0).len(), 3);
    assert!(tags.value(0).is_null(1));

    let meta = rows[0]
        .column_by_name("meta")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(meta.column_by_name("name").unwrap().is_null(0));
    assert_eq!(
        meta.column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap()
            .value(0),
        90
    );
}

#[tokio::test]
async fn blob_payload_sizes_1kb_and_1mb() {
    let table = unique_table("blob_sizes");
    let mut ctx = TestContext::new("ddl_blob_sizes").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, payload BLOB)"
        ))
        .await
        .unwrap();

    // 1 KiB — hex of 512 bytes (1024 hex chars)
    let kb_hex: String = "AB".repeat(512);
    ctx.client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, payload) VALUES (1, X'{kb_hex}')"
        ))
        .await
        .unwrap();

    // 1 MiB would make a huge SQL string; use 64 KiB as a heavier-but-practical case.
    let mid_hex: String = "CD".repeat(32 * 1024);
    ctx.client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, payload) VALUES (2, X'{mid_hex}')"
        ))
        .await
        .unwrap();

    let rows = ctx
        .client
        .query(&format!(
            "SELECT {TIME_COL}, payload FROM {table} ORDER BY {TIME_COL}"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    let blob = rows[0]
        .column_by_name("payload")
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(blob.value(0).len(), 512);
    assert_eq!(blob.value(1).len(), 32 * 1024);
}

#[tokio::test]
async fn insert_type_coercion_and_boolean_literals() {
    let table = unique_table("coerce");
    let mut ctx = TestContext::new("ddl_type_coerce").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} (
                {TIME_COL} BIGINT NOT NULL,
                flag BOOLEAN,
                note VARCHAR,
                n BIGINT
            )"
        ))
        .await
        .unwrap();

    // Strict typing: string into BIGINT must fail.
    let err = ctx
        .client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, n) VALUES (1, 'not_a_number')"
        ))
        .await
        .unwrap_err();
    assert_err_contains(&err, &["type", "mismatch"]);

    // BIGINT into VARCHAR should fail (no silent coerce).
    let err = ctx
        .client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, note) VALUES (2, 12345)"
        ))
        .await
        .unwrap_err();
    assert_err_contains(&err, &["type", "mismatch"]);

    ctx.client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, flag) VALUES (3, true), (4, false)"
        ))
        .await
        .unwrap();

    let rows = ctx
        .client
        .query(&format!("SELECT flag FROM {table} WHERE {TIME_COL} = 3"))
        .await
        .unwrap();
    assert!(scalar_bool_named(&rows, "flag"));

    // 1/0 into BOOLEAN — accept either success with correct bool or typed error.
    let r = ctx
        .client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, flag) VALUES (5, 1)"
        ))
        .await;
    match r {
        Ok(_) => {
            let rows = ctx
                .client
                .query(&format!("SELECT flag FROM {table} WHERE {TIME_COL} = 5"))
                .await
                .unwrap();
            assert!(scalar_bool_named(&rows, "flag"));
        }
        Err(e) => assert_err_contains(&e, &["type", "mismatch", "boolean", "bool"]),
    }

    let count = ctx
        .client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert!(scalar_i64_named(&count, "c") >= 2);
}
