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

//! End-to-end coverage for the extended scalar types: DECIMAL, DATE, high-precision TIMESTAMP,
//! and LARGETEXT / LARGEBLOB. Each test creates a table, writes rows, and reads them back
//! (including a filtered query) to prove the write path and query path agree.

use arrow::array::{
    Date32Array, Decimal128Array, Int64Array, LargeBinaryArray, LargeStringArray,
    TimestampMicrosecondArray,
};
use monots_integration_tests::{total_rows, unique_table, MonotsInstance};

#[tokio::test]
async fn decimal_roundtrip_and_filter_via_sql() {
    let table = unique_table("decimal");
    let mut inst = MonotsInstance::new("types_decimal").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, price DECIMAL(10,2))"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, price) VALUES (1000, 12.34), (2000, 99.99), (3000, -5)"
        ))
        .await
        .unwrap();

    // Full projection, ordered.
    let rows = client
        .query(&format!("SELECT price FROM {table} ORDER BY time"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 3);
    let price = rows[0]
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(price.precision(), 10);
    assert_eq!(price.scale(), 2);
    assert_eq!(price.value(0), 1234); // 12.34
    assert_eq!(price.value(1), 9999); // 99.99
    assert_eq!(price.value(2), -500); // -5.00

    // Predicate query: literal coerced to DECIMAL.
    let filtered = client
        .query(&format!(
            "SELECT price FROM {table} WHERE price >= 50 ORDER BY time"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&filtered), 1);
    let hi = filtered[0]
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(hi.value(0), 9999);
}

#[tokio::test]
async fn date_roundtrip_and_filter_via_sql() {
    let table = unique_table("date");
    let mut inst = MonotsInstance::new("types_date").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, day DATE)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, day) VALUES (1000, '2021-06-01'), (2000, '2021-06-02')"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!("SELECT day FROM {table} ORDER BY time"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    let day = rows[0]
        .column_by_name("day")
        .unwrap()
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(day.value(0), 18779); // days from epoch to 2021-06-01
    assert_eq!(day.value(1), 18780);

    // Predicate query: string literal coerced to DATE.
    let filtered = client
        .query(&format!(
            "SELECT day FROM {table} WHERE day = DATE '2021-06-02'"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&filtered), 1);
    let d = filtered[0]
        .column_by_name("day")
        .unwrap()
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(d.value(0), 18780);
}

#[tokio::test]
async fn timestamp_microsecond_roundtrip_via_sql() {
    let table = unique_table("tsmicro");
    let mut inst = MonotsInstance::new("types_ts_micro").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, event_at TIMESTAMP(6))"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, event_at) VALUES \
             (1000, 1717236000000000), (2000, 1717236000000123)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!("SELECT event_at FROM {table} ORDER BY time"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    let event_at = rows[0]
        .column_by_name("event_at")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(event_at.value(0), 1717236000000000);
    assert_eq!(event_at.value(1), 1717236000000123);

    // Query with a filter on the ordering timestamp, still projecting the microsecond column.
    let filtered = client
        .query(&format!("SELECT event_at FROM {table} WHERE time >= 2000"))
        .await
        .unwrap();
    assert_eq!(total_rows(&filtered), 1);
    let one = filtered[0]
        .column_by_name("event_at")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(one.value(0), 1717236000000123);
}

#[tokio::test]
async fn large_text_and_binary_roundtrip_via_sql() {
    let table = unique_table("large");
    let mut inst = MonotsInstance::new("types_large").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, note LARGETEXT, blob LARGEBLOB)"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, note, blob) VALUES \
             (1000, 'hello world', X'DEADBEEF'), (2000, 'second', X'0102')"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!("SELECT note, blob FROM {table} ORDER BY time"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    let note = rows[0]
        .column_by_name("note")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(note.value(0), "hello world");
    assert_eq!(note.value(1), "second");
    let blob = rows[0]
        .column_by_name("blob")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(blob.value(0), &[0xDE, 0xAD, 0xBE, 0xEF]);
    assert_eq!(blob.value(1), &[0x01, 0x02]);

    // Aggregate query exercises the query path over LARGETEXT.
    let count = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {table} WHERE note = 'second'"
        ))
        .await
        .unwrap();
    let c = count[0]
        .column_by_name("c")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(c, 1);
}

#[tokio::test]
async fn extended_types_survive_restart() {
    let table = unique_table("ext_restart");
    let mut inst = MonotsInstance::new("types_ext_restart").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (
                time BIGINT NOT NULL,
                price DECIMAL(12,3),
                day DATE,
                note LARGETEXT
            )"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, price, day, note) VALUES (1000, 1.5, '2020-01-01', 'x')"
        ))
        .await
        .unwrap();

    inst.restart().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let rows = client
        .query(&format!("SELECT price, day FROM {table} ORDER BY time"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    let price = rows[0]
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(price.precision(), 12);
    assert_eq!(price.scale(), 3);
    assert_eq!(price.value(0), 1500); // 1.5 at scale 3

    // DDL type is preserved across restart.
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
    assert!(stmt.contains("DECIMAL(12,3)"), "got: {stmt}");
    assert!(stmt.contains("DATE"), "got: {stmt}");
}
