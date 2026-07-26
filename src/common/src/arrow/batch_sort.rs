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

//! Sort write batches by the mandatory `time` column (ascending).

use arrow::array::Array;
use arrow::compute::{lexsort_to_indices, take, SortColumn, SortOptions};
use arrow::record_batch::RecordBatch;

use crate::{time_column_index, time_value_at, Result, TsdbError};

/// Returns true when `time` values are non-decreasing row-by-row.
///
/// Fast path for dense `Int64` / timestamp columns uses a contiguous value slice (O(n),
/// no per-row dynamic dispatch). Prefer server-side checks in ITs for large tables.
pub fn is_sorted_by_time(batch: &RecordBatch) -> Result<bool> {
    let rows = batch.num_rows();
    if rows <= 1 {
        return Ok(true);
    }
    let ts_idx = time_column_index(batch.schema())?;
    let time_col = batch.column(ts_idx);

    if let Some(arr) = time_col.as_any().downcast_ref::<arrow::array::Int64Array>() {
        if arr.null_count() == 0 {
            let values = arr.values();
            return Ok(values.windows(2).all(|w| w[1] >= w[0]));
        }
    }

    for row in 1..rows {
        let prev = time_value_at(time_col.as_ref(), row - 1)?;
        let cur = time_value_at(time_col.as_ref(), row)?;
        if cur < prev {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Sort all columns by `time` ascending (stable lex sort).
pub fn sort_batch_by_time(batch: RecordBatch) -> Result<RecordBatch> {
    let ts_idx = time_column_index(batch.schema())?;

    let sort_column = SortColumn {
        values: batch.column(ts_idx).clone(),
        options: Some(SortOptions {
            descending: false,
            nulls_first: false,
        }),
    };
    let indices = lexsort_to_indices(&[sort_column], None)
        .map_err(|e| TsdbError::Storage(format!("sort by time: {e}")))?;

    let mut columns = Vec::with_capacity(batch.num_columns());
    for i in 0..batch.num_columns() {
        let sorted = take(batch.column(i).as_ref(), &indices, None)
            .map_err(|e| TsdbError::Storage(format!("sort by time take: {e}")))?;
        columns.push(sorted);
    }
    RecordBatch::try_new(batch.schema(), columns).map_err(TsdbError::from)
}

/// Sort only when the batch is not already ordered by `time`.
pub fn ensure_sorted_by_time(batch: RecordBatch) -> Result<RecordBatch> {
    if is_sorted_by_time(&batch)? {
        Ok(batch)
    } else {
        sort_batch_by_time(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn ts_value_batch(ts: &[i64], values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("time", DataType::Int64, false),
                Field::new("value", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(ts.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn detects_unsorted_and_sorts() {
        let batch = ts_value_batch(&[300, 100, 200], &[3, 1, 2]);
        assert!(!is_sorted_by_time(&batch).unwrap());
        let sorted = ensure_sorted_by_time(batch).unwrap();
        assert!(is_sorted_by_time(&sorted).unwrap());
        let ts = sorted
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let val = sorted
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.values(), &[100, 200, 300]);
        assert_eq!(val.values(), &[1, 2, 3]);
    }

    #[test]
    fn ensure_sorted_is_noop_when_already_sorted() {
        let batch = ts_value_batch(&[100, 200], &[1, 2]);
        assert!(is_sorted_by_time(&batch).unwrap());
        let sorted = ensure_sorted_by_time(batch.clone()).unwrap();
        assert!(is_sorted_by_time(&sorted).unwrap());
        assert_eq!(
            sorted
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }
}
