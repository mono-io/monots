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

use arrow::array::Array;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use common::{Result, TsdbError};

/// Validate a write batch against the catalog schema after alignment.
pub fn validate_write_batch(batch: &RecordBatch, catalog_schema: &Schema) -> Result<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }

    if batch.schema().fields().len() != catalog_schema.fields().len() {
        return Err(TsdbError::Schema(format!(
            "column count mismatch: batch has {}, catalog has {}",
            batch.schema().fields().len(),
            catalog_schema.fields().len()
        )));
    }

    for (idx, (batch_field, catalog_field)) in batch
        .schema()
        .fields()
        .iter()
        .zip(catalog_schema.fields())
        .enumerate()
    {
        if batch_field.name() != catalog_field.name() {
            return Err(TsdbError::Schema(format!(
                "column name mismatch at index {idx}: batch `{}` vs catalog `{}`",
                batch_field.name(),
                catalog_field.name()
            )));
        }
        if batch_field.data_type() != catalog_field.data_type() {
            return Err(TsdbError::Schema(format!(
                "column `{}` type mismatch: batch {:?} vs catalog {:?}",
                batch_field.name(),
                batch_field.data_type(),
                catalog_field.data_type()
            )));
        }
        if !catalog_field.is_nullable() {
            let col = batch.column(idx);
            if col.null_count() > 0 {
                return Err(TsdbError::Schema(format!(
                    "column `{}` cannot be null",
                    catalog_field.name()
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn rejects_column_count_mismatch() {
        let catalog_schema = Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "time",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let err = validate_write_batch(&batch, &catalog_schema).unwrap_err();
        assert!(err.to_string().contains("column count"));
    }
}
