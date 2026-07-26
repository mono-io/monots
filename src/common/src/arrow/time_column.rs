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

//! Helpers for the mandatory `time` column (BIGINT or Arrow `Timestamp(unit)`).

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Int64Type, Schema, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};

use crate::{Result, TsdbError, TIMESTAMP_COLUMN};

/// Index of the `time` column in an Arrow schema.
pub fn time_column_index(schema: impl AsRef<Schema>) -> Result<usize> {
    schema
        .as_ref()
        .index_of(TIMESTAMP_COLUMN)
        .map_err(|_| TsdbError::Schema(format!("missing time column `{TIMESTAMP_COLUMN}`")))
}

/// Read one logical epoch value from the time column at `row`.
pub fn time_value_at(array: &dyn Array, row: usize) -> Result<i64> {
    match array.data_type() {
        DataType::Int64 => Ok(array.as_primitive::<Int64Type>().value(row)),
        DataType::Timestamp(TimeUnit::Second, _) => {
            Ok(array.as_primitive::<TimestampSecondType>().value(row))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Ok(array.as_primitive::<TimestampMillisecondType>().value(row))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Ok(array.as_primitive::<TimestampMicrosecondType>().value(row))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Ok(array.as_primitive::<TimestampNanosecondType>().value(row))
        }
        other => Err(TsdbError::Schema(format!(
            "time column must be BIGINT or TIMESTAMP, got {other:?}"
        ))),
    }
}

/// Borrow the underlying `i64` slice for the time column (valid for Int64 / Timestamp arrays).
pub fn time_values_slice(array: &dyn Array) -> Result<&[i64]> {
    match array.data_type() {
        DataType::Int64 => Ok(array.as_primitive::<Int64Type>().values()),
        DataType::Timestamp(TimeUnit::Second, _) => {
            Ok(array.as_primitive::<TimestampSecondType>().values())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Ok(array.as_primitive::<TimestampMillisecondType>().values())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Ok(array.as_primitive::<TimestampMicrosecondType>().values())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Ok(array.as_primitive::<TimestampNanosecondType>().values())
        }
        other => Err(TsdbError::Schema(format!(
            "time column must be BIGINT or TIMESTAMP, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, TimestampMicrosecondArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn reads_int64_and_timestamp_microsecond() {
        let int64 = Int64Array::from(vec![1_i64, 2]);
        assert_eq!(time_value_at(&int64, 0).unwrap(), 1);
        assert_eq!(time_values_slice(&int64).unwrap(), &[1, 2]);

        let ts = TimestampMicrosecondArray::from(vec![100_i64, 200]);
        assert_eq!(time_value_at(&ts, 1).unwrap(), 200);
        assert_eq!(time_values_slice(&ts).unwrap(), &[100, 200]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Int64,
            false,
        )]));
        assert_eq!(time_column_index(&schema).unwrap(), 0);
    }
}
