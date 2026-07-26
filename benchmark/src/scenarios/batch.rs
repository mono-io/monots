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

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub fn make_write_batch(thread_id: usize, batch_idx: usize, rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));

    let base_ts = (thread_id as i64) * 1_000_000_000_000 + (batch_idx as i64) * rows as i64;
    let timestamps: Vec<i64> = (0..rows).map(|i| base_ts + i as i64).collect();
    let values: Vec<i64> = (0..rows).map(|i| (thread_id * 10_000 + i) as i64).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("valid benchmark batch")
}

#[cfg(test)]
mod tests {
    use super::make_write_batch;

    #[test]
    fn write_batch_has_unique_timestamps_per_thread() {
        let b1 = make_write_batch(0, 0, 10);
        let b2 = make_write_batch(1, 0, 10);
        assert_eq!(b1.num_rows(), 10);
        assert_ne!(b1.column(0), b2.column(0));
    }
}
