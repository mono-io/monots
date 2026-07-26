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

//! Integration tests: write all column types, duplicate timestamps, ADD COLUMN, DROP TABLE.

use arrow::array::{
    BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int8Array, LargeBinaryArray, LargeStringArray, ListArray, StringArray, StructArray,
    TimestampMicrosecondArray, UInt64Array,
};
use monots_integration_tests::{
    enum_value_at, full_types_ddl, scalar_i64_named, table_names_from_show, total_rows,
    unique_table, MonotsInstance,
};

fn full_types_insert(
    table: &str,
    time: i64,
    tag: &str,
    status: &str,
    c_int32: i32,
    c_price: f64,
) -> String {
    format!(
        "INSERT INTO {table} (
            time, c_int8, c_int16, c_int32, c_int64,
            c_uint8, c_uint16, c_uint32, c_uint64,
            c_float32, c_float64, c_bool, c_str,
            c_day, c_price, c_note, c_blob, c_binary, c_ts,
            tags, meta, status
        ) VALUES (
            {time}, -1, -2, {c_int32}, -4,
            1, 2, 3, 4,
            1.25, 2.5, true, '{tag}',
            '2021-06-01', {c_price}, 'note-{tag}', X'DEAD', X'BEEF', 1717236000000123,
            '[\"{tag}\"]', '{{\"name\":\"{tag}\",\"score\":90}}', '{status}'
        )"
    )
}

#[tokio::test]
async fn write_all_column_types_and_read_back() {
    let table = unique_table("all_types");
    let mut inst = MonotsInstance::new("it_write_all_types").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(&full_types_ddl(&table)).await.unwrap();
    let inserted = client
        .no_query(&full_types_insert(&table, 1000, "alpha", "on", -3, 12.34))
        .await
        .unwrap();
    assert_eq!(inserted, 1);

    let rows = client
        .query(&format!("SELECT * FROM {table} ORDER BY time"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    let batch = &rows[0];

    assert_eq!(
        batch
            .column_by_name("c_int8")
            .unwrap()
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(0),
        -1
    );
    assert_eq!(
        batch
            .column_by_name("c_int32")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        -3
    );
    assert_eq!(
        batch
            .column_by_name("c_uint64")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        4
    );
    assert!(
        (batch
            .column_by_name("c_float32")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(0)
            - 1.25)
            .abs()
            < 0.001
    );
    assert_eq!(
        batch
            .column_by_name("c_float64")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        2.5
    );
    assert!(batch
        .column_by_name("c_bool")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(0));
    assert_eq!(
        batch
            .column_by_name("c_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "alpha"
    );
    assert_eq!(
        batch
            .column_by_name("c_day")
            .unwrap()
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(0),
        18779
    );
    let price = batch
        .column_by_name("c_price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(price.value(0), 1234);
    assert_eq!(
        batch
            .column_by_name("c_note")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(0),
        "note-alpha"
    );
    assert_eq!(
        batch
            .column_by_name("c_blob")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(0),
        &[0xDE, 0xAD]
    );
    assert_eq!(
        batch
            .column_by_name("c_binary")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        &[0xBE, 0xEF]
    );
    assert_eq!(
        batch
            .column_by_name("c_ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        1717236000000123
    );

    let tags = batch
        .column_by_name("tags")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let tag_values = tags.value(0);
    let tag0 = tag_values.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(tag0.value(0), "alpha");

    let meta = batch
        .column_by_name("meta")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        meta.column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "alpha"
    );
    assert_eq!(
        meta.column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        90
    );
    assert_eq!(enum_value_at(batch, "status", 0), "on");
}

#[tokio::test]
async fn duplicate_timestamp_newest_row_wins_for_typed_columns() {
    let table = unique_table("dedup_types");
    let mut inst = MonotsInstance::new("it_dedup_types_mem").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(&full_types_ddl(&table)).await.unwrap();
    client
        .no_query(&full_types_insert(&table, 5000, "old", "on", -3, 12.34))
        .await
        .unwrap();
    client
        .no_query(&full_types_insert(&table, 5000, "new", "off", -99, 12.34))
        .await
        .unwrap();

    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 1);

    let rows = client
        .query(&format!(
            "SELECT c_str, c_price, status FROM {table} WHERE time = 5000"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    assert_eq!(
        rows[0]
            .column_by_name("c_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "new"
    );
    let price = rows[0]
        .column_by_name("c_price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(price.value(0), 1234);
    assert_eq!(enum_value_at(&rows[0], "status", 0), "off");
}

#[tokio::test]
async fn duplicate_timestamp_dedup_after_flush_keeps_latest_typed_row() {
    let table = unique_table("dedup_flush");
    let mut inst = MonotsInstance::new("it_dedup_types_flush").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(&full_types_ddl(&table)).await.unwrap();
    client
        .no_query(&full_types_insert(&table, 6000, "v1", "on", -3, 12.34))
        .await
        .unwrap();
    client
        .no_query(&format!("FLUSH TABLE {table}"))
        .await
        .unwrap();

    client
        .no_query(&full_types_insert(&table, 6000, "v2", "off", -99, 99.99))
        .await
        .unwrap();
    client
        .no_query(&format!("FLUSH TABLE {table}"))
        .await
        .unwrap();

    inst.restart().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 1);

    let rows = client
        .query(&format!(
            "SELECT c_int32, c_str, c_price, status FROM {table} WHERE time = 6000"
        ))
        .await
        .unwrap();
    assert_eq!(
        rows[0]
            .column_by_name("c_int32")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        -99
    );
    assert_eq!(
        rows[0]
            .column_by_name("c_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "v2"
    );
    let price = rows[0]
        .column_by_name("c_price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(price.value(0), 9999);
    assert_eq!(enum_value_at(&rows[0], "status", 0), "off");
}

#[tokio::test]
async fn duplicate_timestamp_in_same_insert_batch_dedupes_to_last_value() {
    let table = unique_table("dedup_batch");
    let mut inst = MonotsInstance::new("it_dedup_same_batch").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, c_str VARCHAR, c_int32 INT)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, c_str, c_int32) VALUES
             (7000, 'first', 1),
             (7000, 'second', 2),
             (7000, 'third', 3),
             (8000, 'solo', 9)"
        ))
        .await
        .unwrap();

    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 2);

    let rows = client
        .query(&format!(
            "SELECT c_str, c_int32 FROM {table} WHERE time = 7000"
        ))
        .await
        .unwrap();
    assert_eq!(
        rows[0]
            .column_by_name("c_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "third"
    );
    assert_eq!(
        rows[0]
            .column_by_name("c_int32")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        3
    );
}

#[tokio::test]
async fn add_columns_of_various_types_null_pads_existing_rows() {
    let table = unique_table("add_col");
    let mut inst = MonotsInstance::new("it_add_column_types").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, c_str VARCHAR)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, c_str) VALUES (1000, 'before')"
        ))
        .await
        .unwrap();

    client
        .no_query(&format!(
            "ALTER TABLE {table} ADD COLUMN c_price DECIMAL(8, 2)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "ALTER TABLE {table} ADD COLUMN tags ARRAY<VARCHAR>"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "ALTER TABLE {table} ADD COLUMN meta STRUCT<region VARCHAR, level INT>"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!("ALTER TABLE {table} ADD COLUMN c_bool BOOLEAN"))
        .await
        .unwrap();

    client
        .no_query(&format!(
            "INSERT INTO {table} (time, c_str, c_price, tags, meta, c_bool) VALUES \
             (2000, 'after', 3.14, '[\"x\"]', '{{\"region\":\"east\",\"level\":7}}', true)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!("FLUSH TABLE {table}"))
        .await
        .unwrap();

    inst.restart().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let rows = client
        .query(&format!(
            "SELECT time, c_str, c_price, tags, meta, c_bool FROM {table} ORDER BY time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);

    assert_eq!(
        rows[0]
            .column_by_name("c_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "before"
    );
    assert!(rows[0].column_by_name("c_price").unwrap().is_null(0));
    assert!(rows[0].column_by_name("tags").unwrap().is_null(0));
    assert!(rows[0].column_by_name("meta").unwrap().is_null(0));
    assert!(rows[0].column_by_name("c_bool").unwrap().is_null(0));

    assert_eq!(
        rows[0]
            .column_by_name("c_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(1),
        "after"
    );
    let price = rows[0]
        .column_by_name("c_price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(price.value(1), 314);
    assert!(rows[0]
        .column_by_name("c_bool")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(1));

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
    assert!(stmt.contains("DECIMAL(8,2)"));
    assert!(stmt.contains("ARRAY<VARCHAR>"));
    assert!(stmt.contains("STRUCT<"));
    assert!(stmt.contains("BOOLEAN"));
}

#[tokio::test]
async fn drop_table_removes_catalog_and_data_after_typed_writes() {
    let table = unique_table("drop");
    let mut inst = MonotsInstance::new("it_drop_table").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(&full_types_ddl(&table)).await.unwrap();
    client
        .no_query(&full_types_insert(&table, 1000, "row", "on", -3, 12.34))
        .await
        .unwrap();
    client
        .no_query(&full_types_insert(&table, 2000, "row2", "off", -3, 12.34))
        .await
        .unwrap();

    let before = client.query("SHOW TABLES").await.unwrap();
    assert!(table_names_from_show(&before).contains(&table));

    client
        .no_query(&format!("DROP TABLE {table}"))
        .await
        .unwrap();

    let after = client.query("SHOW TABLES").await.unwrap();
    assert!(!table_names_from_show(&after).contains(&table));

    let err = client
        .query(&format!("SELECT * FROM {table}"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not found") || err.to_string().contains("Table"));

    let err = client
        .query(&format!("SHOW CREATE TABLE {table}"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not found") || err.to_string().contains("Table"));
}

#[tokio::test]
async fn drop_table_if_exists_then_recreate_with_new_schema() {
    let table = unique_table("drop_re");
    let mut inst = MonotsInstance::new("it_drop_if_exists").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .unwrap();

    client.no_query(&full_types_ddl(&table)).await.unwrap();
    client
        .no_query(&full_types_insert(&table, 3000, "old", "on", -3, 12.34))
        .await
        .unwrap();

    client
        .no_query(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .unwrap();
    client
        .no_query(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, c_int32 INT, c_str VARCHAR)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, c_int32, c_str) VALUES (4000, 42, 'fresh')"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!("SELECT c_int32, c_str FROM {table}"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    assert_eq!(
        rows[0]
            .column_by_name("c_int32")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        42
    );
    assert_eq!(
        rows[0]
            .column_by_name("c_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "fresh"
    );

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
    assert!(stmt.contains("c_int32 INT"));
    assert!(!stmt.contains("DECIMAL"));
}
