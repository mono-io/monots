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

use arrow::array::{new_null_array, RecordBatch};
use arrow::datatypes::SchemaRef;
use common::{Result, TsdbError};

pub struct BatchAligner;

impl BatchAligner {
    pub fn align(source_batch: RecordBatch, target_schema: SchemaRef) -> Result<RecordBatch> {
        let source_schema = source_batch.schema();
        if source_schema.as_ref() == target_schema.as_ref() {
            return Ok(source_batch);
        }

        let mut aligned_columns = Vec::with_capacity(target_schema.fields().len());
        for field in target_schema.fields() {
            match source_schema.index_of(field.name()) {
                Ok(idx) => {
                    let source_field = source_schema.field(idx);
                    if source_field.data_type() != field.data_type() {
                        return Err(TsdbError::Schema(format!(
                            "column `{}` type mismatch: expected {:?}, got {:?}",
                            field.name(),
                            field.data_type(),
                            source_field.data_type()
                        )));
                    }
                    aligned_columns.push(source_batch.column(idx).clone());
                }
                Err(_) => {
                    if !field.is_nullable() {
                        return Err(TsdbError::Schema(format!(
                            "cannot align batch: column `{}` is not nullable",
                            field.name()
                        )));
                    }
                    let null_array = new_null_array(field.data_type(), source_batch.num_rows());
                    aligned_columns.push(null_array);
                }
            }
        }

        RecordBatch::try_new(target_schema, aligned_columns)
            .map_err(|e| TsdbError::Storage(format!("batch alignment failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn rejects_type_mismatch_when_aligning() {
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();

        let err = BatchAligner::align(batch, target_schema).unwrap_err();
        assert!(err.to_string().contains("type mismatch"));
    }

    #[test]
    fn pads_missing_columns_with_nulls() {
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("temperature", DataType::Float64, true),
        ]));
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("temperature", DataType::Float64, true),
            Field::new("humidity", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(Int64Array::from(vec![100_i64, 200])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    Some(21.5),
                    Some(22.0),
                ])),
            ],
        )
        .unwrap();

        let aligned = BatchAligner::align(batch, target_schema).unwrap();
        assert_eq!(aligned.num_columns(), 3);
        assert_eq!(aligned.num_rows(), 2);
        let humidity = aligned
            .column(2)
            .as_primitive::<arrow::datatypes::Float64Type>();
        assert_eq!(humidity.null_count(), 2);
    }

    #[test]
    fn rejects_missing_non_nullable_column() {
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Int64,
            false,
        )]));
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("humidity", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int64Array::from(vec![100_i64]))],
        )
        .unwrap();

        let err = BatchAligner::align(batch, target_schema).unwrap_err();
        assert!(err.to_string().contains("not nullable"));
    }
}
