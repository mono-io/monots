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

//! SDK Arrow IPC write: all types, client/server time ordering.

use arrow::array::{Decimal128Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use monots_integration_tests::{
    enum_value_at, full_types_batch, full_types_ddl, is_sorted_by_time, scalar_i64_named,
    total_rows, unique_table, MonotsInstance,
};
use sdk::sort_batch_by_time;
use std::sync::Arc;

fn metrics_batch(times: &[i64], regions: &[&str], values: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(times.to_vec())),
            Arc::new(StringArray::from(regions.to_vec())),
            Arc::new(Float64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn sdk_write_all_column_types_roundtrip() {
    let table = unique_table("arrow_all");
    let mut inst = MonotsInstance::new("sdk_arrow_all_types").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(&full_types_ddl(&table)).await.unwrap();

    // Deliberately unsorted times; client always sorts before send.
    let batch = full_types_batch(&[3000, 1000, 2000], &["c", "a", "b"], &["on", "off", "on"]);
    assert!(!is_sorted_by_time(&batch).unwrap());

    let rows = client.write_batches(&table, vec![batch]).await.unwrap();
    assert_eq!(rows, 3);

    let result = client
        .query(&format!("SELECT * FROM {table} ORDER BY time"))
        .await
        .unwrap();
    assert_eq!(total_rows(&result), 3);
    let batch = &result[0];

    assert_eq!(
        batch
            .column_by_name("time")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1000, 2000, 3000]
    );
    assert_eq!(
        batch
            .column_by_name("c_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "a"
    );
    assert_eq!(
        batch
            .column_by_name("c_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(2),
        "c"
    );
    assert_eq!(enum_value_at(batch, "status", 0), "off");
    assert_eq!(enum_value_at(batch, "status", 1), "on");
    assert_eq!(enum_value_at(batch, "status", 2), "on");

    let price = batch
        .column_by_name("c_price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(price.value(0), 1334);
    assert_eq!(price.value(2), 1234);

    let meta = batch
        .column_by_name("meta")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .unwrap();
    assert_eq!(
        meta.column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(1),
        "b"
    );
}

#[tokio::test]
async fn sdk_write_batches_sorts_unsorted_input_before_send() {
    let table = unique_table("arrow_sort_client");
    let mut inst = MonotsInstance::new("sdk_arrow_client_sort").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, tag VARCHAR)"
        ))
        .await
        .unwrap();

    let unsorted = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("tag", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![3000, 1000, 2000])),
            Arc::new(StringArray::from(vec!["c", "a", "b"])),
        ],
    )
    .unwrap();
    assert!(!is_sorted_by_time(&unsorted).unwrap());

    client.write_batches(&table, vec![unsorted]).await.unwrap();

    let rows = client
        .query(&format!("SELECT tag FROM {table} ORDER BY time"))
        .await
        .unwrap();
    let tags = rows[0]
        .column_by_name("tag")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(tags.value(0), "a");
    assert_eq!(tags.value(1), "b");
    assert_eq!(tags.value(2), "c");
}

#[tokio::test]
async fn sdk_write_batch_dedupes_duplicate_timestamps_after_sort() {
    let table = unique_table("arrow_dedup");
    let mut inst = MonotsInstance::new("sdk_arrow_dedup").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    // Unsorted with duplicate time; after sort the last row for time=500 should win.
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![600, 500, 500])),
            Arc::new(Int64Array::from(vec![2, 1, 9])),
        ],
    )
    .unwrap();

    client.write_batch(&table, batch).await.unwrap();

    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 2);

    let val = client
        .query(&format!("SELECT value FROM {table} WHERE time = 500"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&val, "value"), 9);
}

#[tokio::test]
async fn sdk_write_all_types_survives_restart_after_unsorted_input() {
    let table = unique_table("arrow_restart");
    let mut inst = MonotsInstance::new("sdk_arrow_all_restart").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(&full_types_ddl(&table)).await.unwrap();

    let batch = full_types_batch(&[2000, 1000], &["y", "x"], &["on", "off"]);
    client.write_batch(&table, batch).await.unwrap();
    client
        .no_query(&format!("FLUSH TABLE {table}"))
        .await
        .unwrap();

    inst.restart().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let rows = client
        .query(&format!(
            "SELECT time, c_str, status FROM {table} ORDER BY time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    assert_eq!(
        rows[0]
            .column_by_name("time")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1000
    );
    assert_eq!(
        rows[0]
            .column_by_name("c_str")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "x"
    );
    assert_eq!(enum_value_at(&rows[0], "status", 1), "on");
}

#[test]
fn sort_batch_by_time_helper_reorders_rows() {
    let batch = metrics_batch(&[3000, 1000], &["b", "a"], &[2.0, 1.0]);
    let sorted = sort_batch_by_time(batch).unwrap();
    assert!(is_sorted_by_time(&sorted).unwrap());
    assert_eq!(
        sorted
            .column_by_name("region")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "a"
    );
}
