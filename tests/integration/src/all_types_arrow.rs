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

//! Build a wide-schema Arrow batch matching `full_types_ddl` in integration tests.

use arrow::array::{
    ArrayRef, AsArray, BinaryArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, ListArray, StringArray, StructArray, TimestampMicrosecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Int8Type, SchemaRef};
use arrow::record_batch::RecordBatch;
use monots_core::metadata::catalog::{CatalogManager, ColumnDef};
use std::sync::Arc;

pub fn full_types_ddl(table: &str) -> String {
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
            c_day DATE,
            c_price DECIMAL(10, 2),
            c_note LARGETEXT,
            c_blob LARGEBLOB,
            c_binary BLOB,
            c_ts TIMESTAMP(6),
            tags ARRAY<VARCHAR>,
            meta STRUCT<name VARCHAR, score INT>,
            status ENUM('on', 'off')
        )"
    )
}

pub fn full_types_columns() -> Vec<ColumnDef> {
    vec![
        ColumnDef {
            name: "time".into(),
            data_type: "Int64".into(),
            nullable: false,
        },
        ColumnDef {
            name: "c_int8".into(),
            data_type: "Int8".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_int16".into(),
            data_type: "Int16".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_int32".into(),
            data_type: "Int32".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_int64".into(),
            data_type: "Int64".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_uint8".into(),
            data_type: "UInt8".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_uint16".into(),
            data_type: "UInt16".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_uint32".into(),
            data_type: "UInt32".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_uint64".into(),
            data_type: "UInt64".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_float32".into(),
            data_type: "Float32".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_float64".into(),
            data_type: "Float64".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_bool".into(),
            data_type: "Boolean".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_str".into(),
            data_type: "Utf8".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_day".into(),
            data_type: "Date32".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_price".into(),
            data_type: "Decimal(10,2)".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_note".into(),
            data_type: "LargeUtf8".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_blob".into(),
            data_type: "LargeBinary".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_binary".into(),
            data_type: "Binary".into(),
            nullable: true,
        },
        ColumnDef {
            name: "c_ts".into(),
            data_type: "Timestamp(Microsecond)".into(),
            nullable: true,
        },
        ColumnDef {
            name: "tags".into(),
            data_type: "List<Utf8>".into(),
            nullable: true,
        },
        ColumnDef {
            name: "meta".into(),
            data_type: "Struct<name:Utf8,score:Int32>".into(),
            nullable: true,
        },
        ColumnDef {
            name: "status".into(),
            data_type: "Enum<on,off>".into(),
            nullable: true,
        },
    ]
}

pub fn full_types_schema() -> SchemaRef {
    CatalogManager::build_arrow_schema(&full_types_columns()).expect("full types schema")
}

/// Build rows; `times` may be unsorted — used to exercise sort-on-write.
pub fn full_types_batch(times: &[i64], tags: &[&str], statuses: &[&str]) -> RecordBatch {
    assert_eq!(times.len(), tags.len());
    assert_eq!(times.len(), statuses.len());
    let n = times.len();
    let schema = full_types_schema();

    let status_keys: Vec<i8> = statuses
        .iter()
        .map(|s| if *s == "on" { 0 } else { 1 })
        .collect();
    let status_array = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(status_keys),
        Arc::new(StringArray::from(vec!["on", "off"])),
    )
    .unwrap();

    let tag_values = StringArray::from(tags.to_vec());
    let tags_array = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::from_lengths(vec![1; n]),
        Arc::new(tag_values),
        None,
    )
    .unwrap();

    let meta_names = StringArray::from(tags.to_vec());
    let meta_scores = Int32Array::from(vec![90; n]);
    let meta_array = StructArray::from(vec![
        (
            Arc::new(Field::new("name", DataType::Utf8, true)),
            Arc::new(meta_names) as ArrayRef,
        ),
        (
            Arc::new(Field::new("score", DataType::Int32, true)),
            Arc::new(meta_scores) as ArrayRef,
        ),
    ]);

    let price_values: Vec<i128> = (0..n).map(|i| 1234 + i as i128 * 100).collect();
    let price_array = Decimal128Array::from(price_values)
        .with_precision_and_scale(10, 2)
        .unwrap();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(times.to_vec())),
            Arc::new(Int8Array::from(vec![-1_i8; n])),
            Arc::new(Int16Array::from(vec![-2_i16; n])),
            Arc::new(Int32Array::from(
                (0..n as i32).map(|i| -(3 + i)).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(vec![-4_i64; n])),
            Arc::new(UInt8Array::from(vec![1_u8; n])),
            Arc::new(UInt16Array::from(vec![2_u16; n])),
            Arc::new(UInt32Array::from(vec![3_u32; n])),
            Arc::new(UInt64Array::from(vec![4_u64; n])),
            Arc::new(Float32Array::from(vec![1.25_f32; n])),
            Arc::new(Float64Array::from(vec![2.5_f64; n])),
            Arc::new(BooleanArray::from(vec![true; n])),
            Arc::new(StringArray::from(tags.to_vec())),
            Arc::new(Date32Array::from(vec![18779; n])),
            Arc::new(price_array),
            Arc::new(LargeStringArray::from(
                tags.iter().map(|t| format!("note-{t}")).collect::<Vec<_>>(),
            )),
            Arc::new(LargeBinaryArray::from(
                (0..n).map(|_| &[0xDE_u8, 0xAD][..]).collect::<Vec<_>>(),
            )),
            Arc::new(BinaryArray::from(
                (0..n).map(|_| &[0xBE_u8, 0xEF][..]).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1717236000000123_i64;
                n
            ])),
            Arc::new(tags_array),
            Arc::new(meta_array),
            Arc::new(status_array),
        ],
    )
    .unwrap()
}

pub fn enum_value_at(batch: &RecordBatch, column: &str, row: usize) -> String {
    let array = batch.column_by_name(column).unwrap();
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return values.value(row).to_string();
    }
    let dict = array
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .expect("enum column");
    let values = dict.values().as_string::<i32>();
    values.value(dict.keys().value(row) as usize).to_string()
}
