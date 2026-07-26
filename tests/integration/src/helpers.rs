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

use std::collections::HashSet;

use arrow::array::{Array, BooleanArray, Float32Array, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;

pub fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

pub fn scalar_i64(batches: &[RecordBatch], col: usize) -> i64 {
    batches[0]
        .column(col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

pub fn scalar_i64_named(batches: &[RecordBatch], name: &str) -> i64 {
    batches[0]
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

pub fn scalar_f64_named(batches: &[RecordBatch], name: &str) -> f64 {
    batches[0]
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0)
}

pub fn scalar_f32_named(batches: &[RecordBatch], name: &str) -> f32 {
    batches[0]
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap()
        .value(0)
}

pub fn scalar_bool_named(batches: &[RecordBatch], name: &str) -> bool {
    batches[0]
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(0)
}

pub fn scalar_str_named(batches: &[RecordBatch], name: &str) -> String {
    batches[0]
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_string()
}

pub fn col_i64(batches: &[RecordBatch], name: &str, row: usize) -> i64 {
    let mut offset = 0usize;
    for batch in batches {
        if row < offset + batch.num_rows() {
            let local = row - offset;
            return batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("missing column {name}; schema={:?}", batch.schema()))
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| {
                    panic!("column {name} is not Int64; schema={:?}", batch.schema())
                })
                .value(local);
        }
        offset += batch.num_rows();
    }
    panic!(
        "row {row} out of range (total_rows={})",
        total_rows(batches)
    );
}

pub fn col_f64(batches: &[RecordBatch], name: &str, row: usize) -> f64 {
    let mut offset = 0usize;
    for batch in batches {
        if row < offset + batch.num_rows() {
            let local = row - offset;
            return batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("missing column {name}; schema={:?}", batch.schema()))
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap_or_else(|| {
                    panic!("column {name} is not Float64; schema={:?}", batch.schema())
                })
                .value(local);
        }
        offset += batch.num_rows();
    }
    panic!(
        "row {row} out of range (total_rows={})",
        total_rows(batches)
    );
}

pub fn col_str(batches: &[RecordBatch], name: &str, row: usize) -> String {
    let mut offset = 0usize;
    for batch in batches {
        if row < offset + batch.num_rows() {
            let local = row - offset;
            return batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("missing column {name}; schema={:?}", batch.schema()))
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("column {name} is not Utf8; schema={:?}", batch.schema()))
                .value(local)
                .to_string();
        }
        offset += batch.num_rows();
    }
    panic!(
        "row {row} out of range (total_rows={})",
        total_rows(batches)
    );
}

pub fn col_is_null(batches: &[RecordBatch], name: &str, row: usize) -> bool {
    let mut offset = 0usize;
    for batch in batches {
        if row < offset + batch.num_rows() {
            let local = row - offset;
            return batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("missing column {name}; schema={:?}", batch.schema()))
                .is_null(local);
        }
        offset += batch.num_rows();
    }
    panic!(
        "row {row} out of range (total_rows={})",
        total_rows(batches)
    );
}

pub fn table_names_from_show(batches: &[RecordBatch]) -> HashSet<String> {
    let mut names = HashSet::new();
    for batch in batches {
        let col = batch
            .column_by_name("table_name")
            .expect("SHOW TABLES missing table_name column");
        let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                names.insert(arr.value(i).to_string());
            }
        }
    }
    names
}
