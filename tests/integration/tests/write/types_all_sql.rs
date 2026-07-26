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

use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray, TimestampMillisecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use monots_integration_tests::{total_rows, unique_table, MonotsInstance};

fn all_types_ddl(table: &str) -> String {
    format!(
        "CREATE TABLE {table} (
            time BIGINT NOT NULL,
            c_int8 TINYINT,
            c_int16 SMALLINT,
            c_int32 INT,
            c_int64 BIGINT,
            c_uint8 TINYINT UNSIGNED,
            c_uint16 SMALLINT UNSIGNED,
            c_uint32 INT UNSIGNED,
            c_uint64 BIGINT UNSIGNED,
            c_float32 FLOAT,
            c_float64 DOUBLE,
            c_bool BOOLEAN,
            c_str VARCHAR,
            c_ts TIMESTAMP
        )"
    )
}

fn all_types_insert(table: &str) -> String {
    format!(
        "INSERT INTO {table} (
            time, c_int8, c_int16, c_int32, c_int64,
            c_uint8, c_uint16, c_uint32, c_uint64,
            c_float32, c_float64, c_bool, c_str, c_ts
        ) VALUES (
            1000, -1, -2, -3, -4,
            1, 2, 3, 4,
            1.25, 2.5, true, 'hello', 2000
        ), (
            2000, 10, 20, 30, 40,
            10, 20, 30, 40,
            3.75, 4.5, false, 'world', 3000
        )"
    )
}

#[tokio::test]
async fn all_supported_types_roundtrip_via_sql() {
    let table = unique_table("types");
    let mut inst = MonotsInstance::new("types_all_sql").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(&all_types_ddl(&table)).await.unwrap();
    let inserted = client.no_query(&all_types_insert(&table)).await.unwrap();
    assert_eq!(inserted, 2);

    let rows = client
        .query(&format!("SELECT * FROM {table} ORDER BY time"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);

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
            .column_by_name("c_int16")
            .unwrap()
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        -2
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
            .column_by_name("c_int64")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        -4
    );
    assert_eq!(
        batch
            .column_by_name("c_uint8")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        batch
            .column_by_name("c_uint16")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .value(0),
        2
    );
    assert_eq!(
        batch
            .column_by_name("c_uint32")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(0),
        3
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
        "hello"
    );
    assert_eq!(
        batch
            .column_by_name("c_ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0),
        2000
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
    assert!(stmt.contains("TINYINT"));
    assert!(stmt.contains("TINYINT UNSIGNED"));
    assert!(stmt.contains("BOOLEAN"));
    assert!(stmt.contains("TIMESTAMP"));
}

#[tokio::test]
async fn types_survive_restart() {
    let table = unique_table("types");
    let mut inst = MonotsInstance::new("types_restart").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(&all_types_ddl(&table)).await.unwrap();
    client.no_query(&all_types_insert(&table)).await.unwrap();

    inst.restart().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let rows = client
        .query(&format!(
            "SELECT c_str, c_bool, c_float64 FROM {table} WHERE time = 2000"
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
        "world"
    );
    assert!(!rows[0]
        .column_by_name("c_bool")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(0));
    assert_eq!(
        rows[0]
            .column_by_name("c_float64")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        4.5
    );
}
