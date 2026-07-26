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

use monots_integration_tests::{total_rows, unique_table, MonotsInstance};

#[tokio::test]
async fn create_table_validates_types_and_starts_cleanly() {
    let table = unique_table("ddl");
    let mut inst = MonotsInstance::new("ddl_create_validate").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let err = client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, x NOT_A_TYPE)"
        ))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unsupported") || err.to_string().contains("type"));

    client
        .no_query(&format!(
            "CREATE TABLE {table} (
                time BIGINT NOT NULL,
                payload BLOB,
                tags ARRAY<VARCHAR>,
                meta STRUCT<name VARCHAR, score INT>,
                status ENUM('open', 'closed')
            )"
        ))
        .await
        .unwrap();

    inst.restart().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let rows = client.query("SHOW TABLES").await.unwrap();
    assert_eq!(total_rows(&rows), 1);

    let ddl = client
        .query(&format!("SHOW CREATE TABLE {table}"))
        .await
        .unwrap();
    let stmt = ddl[0]
        .column_by_name("create_statement")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0);
    assert!(stmt.contains("BLOB"));
    assert!(stmt.contains("ARRAY<VARCHAR>"));
    assert!(stmt.contains("STRUCT<"));
}

#[tokio::test]
async fn add_column_validates_type_and_applies() {
    let table = unique_table("ddl");
    let mut inst = MonotsInstance::new("ddl_add_column").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, v INT)"
        ))
        .await
        .unwrap();

    let err = client
        .no_query(&format!("ALTER TABLE {table} ADD COLUMN bad UNKNOWN_TYPE"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unsupported") || err.to_string().contains("type"));

    client
        .no_query(&format!("ALTER TABLE {table} ADD COLUMN tags ARRAY<INT>"))
        .await
        .unwrap();

    inst.restart().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let ddl = client
        .query(&format!("SHOW CREATE TABLE {table}"))
        .await
        .unwrap();
    let stmt = ddl[0]
        .column_by_name("create_statement")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0);
    assert!(stmt.contains("tags"));
    assert!(stmt.contains("ARRAY<INT>"));
}

#[tokio::test]
async fn insert_uses_sqlparser_array_and_struct_literals() {
    let table = unique_table("ddl");
    let mut inst = MonotsInstance::new("ddl_insert_parser").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (
                time BIGINT NOT NULL,
                tags ARRAY<VARCHAR>,
                meta STRUCT<name VARCHAR, score INT>,
                payload BLOB
            )"
        ))
        .await
        .unwrap();

    client
        .no_query(&format!(
            "INSERT INTO {table} (time, tags, meta, payload) VALUES \
             (1000, ARRAY['a','b'], ('alice', 90), X'DEAD')"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!("SELECT tags FROM {table}"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
}

#[tokio::test]
async fn insert_rejects_schema_mismatch_and_null_timestamp() {
    let table = unique_table("ddl");
    let mut inst = MonotsInstance::new("ddl_insert_validate").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, v INT)"
        ))
        .await
        .unwrap();

    let err = client
        .no_query(&format!("INSERT INTO {table} (time, v) VALUES (NULL, 1)"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("time"));

    let err = client
        .no_query(&format!(
            "INSERT INTO {table} (time, v) VALUES (1000, 'not_int')"
        ))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("type mismatch") || err.to_string().contains("mismatch"));
}

#[tokio::test]
async fn sdk_create_table_normalizes_types() {
    use monots_core::metadata::catalog::ColumnDef;
    use monots_integration_tests::ts_col;

    let table = unique_table("sdk");
    let mut inst = MonotsInstance::new("ddl_sdk_normalize").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .create_table(
            &table,
            vec![
                ts_col(),
                ColumnDef {
                    name: "tags".into(),
                    data_type: "List<Utf8>".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();

    let ddl = client
        .query(&format!("SHOW CREATE TABLE {table}"))
        .await
        .unwrap();
    let stmt = ddl[0]
        .column_by_name("create_statement")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0);
    assert!(stmt.contains("ARRAY<VARCHAR>"));
}
