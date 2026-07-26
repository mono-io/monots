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

use arrow::array::{BinaryArray, ListArray, StringArray, StructArray};
use monots_integration_tests::{total_rows, unique_table, MonotsInstance};

#[tokio::test]
async fn blob_binary_roundtrip_via_sql() {
    let table = unique_table("blob");
    let mut inst = MonotsInstance::new("types_blob").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, payload BLOB)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, payload) VALUES (1000, X'DEADBEEF'), (2000, X'0102')"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!("SELECT payload FROM {table} ORDER BY time"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    let payload = rows[0]
        .column_by_name("payload")
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(payload.value(0), &[0xDE, 0xAD, 0xBE, 0xEF]);
    assert_eq!(payload.value(1), &[0x01, 0x02]);
}

#[tokio::test]
async fn list_array_roundtrip_via_sql() {
    let table = unique_table("list");
    let mut inst = MonotsInstance::new("types_list").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, tags ARRAY<VARCHAR>)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, tags) VALUES (1000, '[\"a\",\"b\"]'), (2000, '[\"c\"]')"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!("SELECT tags FROM {table} ORDER BY time"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    let tags = rows[0]
        .column_by_name("tags")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let first = tags.value(0);
    let strings = first.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "a");
    assert_eq!(strings.value(1), "b");
}

#[tokio::test]
async fn struct_row_roundtrip_via_sql() {
    let table = unique_table("struct");
    let mut inst = MonotsInstance::new("types_struct").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (
                time BIGINT NOT NULL,
                meta STRUCT<name VARCHAR, score INT>
            )"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, meta) VALUES
            (1000, '{{\"name\":\"alice\",\"score\":90}}'),
            (2000, '{{\"name\":\"bob\",\"score\":80}}')"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!("SELECT meta FROM {table} ORDER BY time"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    let meta = rows[0]
        .column_by_name("meta")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let names = meta
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "alice");
    assert_eq!(names.value(1), "bob");
}

#[tokio::test]
async fn nested_types_survive_restart() {
    let table = unique_table("nested");
    let mut inst = MonotsInstance::new("types_nested_restart").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (
                time BIGINT NOT NULL,
                payload BLOB,
                tags ARRAY<INT>
            )"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, payload, tags) VALUES (1000, X'FF', '[1,2,3]')"
        ))
        .await
        .unwrap();

    inst.restart().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let rows = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    let count = rows[0]
        .column_by_name("c")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1);

    let ddl = client
        .query(&format!("SHOW CREATE TABLE {table}"))
        .await
        .unwrap();
    let stmt = ddl[0]
        .column_by_name("create_statement")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert!(stmt.contains("BLOB"));
    assert!(stmt.contains("ARRAY<INT>"));
}
