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

//! Fixed-size memtable chunks via Arrow `slice` + `concat_batches`.

use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common::{Result, TsdbError};

/// Default rows per sealed chunk.
pub const DEFAULT_PRIMITIVE_ARRAY_SIZE: usize = 64;

/// Prefer [`crate::memtable::builders::DEFAULT_TARGET_BATCH_SIZE`] / `memtable_batch_max_rows`.
pub const DEFAULT_MEMTABLE_BATCH_MAX_ROWS: usize = 1024;
pub const DEFAULT_MEMTABLE_BATCH_MAX_BYTES: usize = 1024 * 1024;

/// Active write buffer: accumulates zero-copy slices until `array_size` rows, then seals.
pub struct ChunkBuffer {
    schema: SchemaRef,
    active_slices: Vec<RecordBatch>,
    active_rows: usize,
    ram_cost: usize,
    array_size: usize,
}

impl ChunkBuffer {
    pub fn new(schema: SchemaRef, array_size: usize) -> Self {
        Self {
            schema,
            active_slices: Vec::new(),
            active_rows: 0,
            ram_cost: 0,
            array_size: array_size.max(1),
        }
    }

    pub fn row_count(&self) -> usize {
        self.active_rows
    }

    pub fn ram_cost(&self) -> usize {
        self.ram_cost
    }

    pub fn is_empty(&self) -> bool {
        self.active_rows == 0
    }

    pub fn array_size(&self) -> usize {
        self.array_size
    }

    /// Slice at `array_size` boundaries; returns completed fixed-size chunks.
    pub fn append(&mut self, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(TsdbError::Schema(format!(
                "Memtable chunk schema mismatch. Expected: {:?}, Got: {:?}",
                self.schema.fields(),
                batch.schema().fields()
            )));
        }

        let mut completed = Vec::new();
        let mut offset = 0;
        let num_rows = batch.num_rows();

        while offset < num_rows {
            let space_left = self.array_size - self.active_rows;
            let take = (num_rows - offset).min(space_left);
            self.active_slices.push(batch.slice(offset, take));
            self.active_rows += take;
            offset += take;

            if self.active_rows == self.array_size {
                completed.push(self.materialize_active_block()?);
            }
        }

        self.update_ram_cost();
        Ok(completed)
    }

    /// Seal the partial tail chunk, if any.
    pub fn finish(&mut self) -> Result<Vec<RecordBatch>> {
        if self.active_rows == 0 {
            return Ok(Vec::new());
        }
        Ok(vec![self.materialize_active_block()?])
    }

    fn materialize_active_block(&mut self) -> Result<RecordBatch> {
        debug_assert!(!self.active_slices.is_empty());

        let batch = if self.active_slices.len() == 1 {
            self.active_slices.pop().unwrap()
        } else {
            concat_batches(&self.schema, &self.active_slices).map_err(TsdbError::from)?
        };

        self.active_slices.clear();
        self.active_rows = 0;
        self.ram_cost = 0;
        Ok(batch)
    }

    fn update_ram_cost(&mut self) {
        self.ram_cost = self
            .active_slices
            .iter()
            .map(|batch| {
                batch
                    .columns()
                    .iter()
                    .map(|array| array.get_array_memory_size())
                    .sum::<usize>()
            })
            .sum();
    }
}

pub fn coalesce_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    if batches.len() == 1 {
        return Ok(vec![batches[0].clone()]);
    }
    Ok(vec![
        concat_batches(schema, batches).map_err(TsdbError::from)?
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn row_batch(schema: &SchemaRef, ts: i64, value: Option<i64>) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![ts])),
                Arc::new(Int64Array::from(vec![value])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn emits_fixed_size_chunk_every_64_rows() {
        let schema = test_schema();
        let mut buf = ChunkBuffer::new(schema.clone(), 64);
        let mut chunks = Vec::new();
        for ts in 0..130 {
            chunks.extend(buf.append(&row_batch(&schema, ts, Some(ts))).unwrap());
        }
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].num_rows(), 64);
        assert_eq!(chunks[1].num_rows(), 64);
        assert_eq!(buf.row_count(), 2);
    }

    #[test]
    fn tail_sealed_on_finish() {
        let schema = test_schema();
        let mut buf = ChunkBuffer::new(schema.clone(), 64);
        for ts in 0..10 {
            buf.append(&row_batch(&schema, ts, Some(ts))).unwrap();
        }
        let tail = buf.finish().unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].num_rows(), 10);
        assert!(buf.is_empty());
    }

    #[test]
    fn oversized_batch_is_split_with_slice() {
        let schema = test_schema();
        let times: Vec<i64> = (0..100).collect();
        let values: Vec<Option<i64>> = times.iter().copied().map(Some).collect();
        let big = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(times)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap();

        let mut buf = ChunkBuffer::new(schema, 64);
        let chunks = buf.append(&big).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].num_rows(), 64);
        assert_eq!(buf.row_count(), 36);
    }
}
