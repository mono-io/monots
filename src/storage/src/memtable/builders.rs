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

//! Runtime Arrow [`BatchBuffer`] for partial memtable batches.
//!
//! Coalesces and splits incoming [`RecordBatch`]es into fixed-capacity chunks using
//! Arrow `slice` + `concat_batches` (avoiding per-row builder copies).

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use common::{Result, TsdbError};
use std::collections::VecDeque;

/// Default micro-batch row capacity before materializing a [`RecordBatch`].
///
/// Tunable at runtime via `storage.memtable_batch_max_rows` in `conf/config.yaml`
/// (and [`EngineConfig::memtable_batch_max_rows`]). Prefer 512–1024 for write latency;
/// larger values increase concatenation cost when sealing.
pub const DEFAULT_TARGET_BATCH_SIZE: usize = 1024;

/// Coalesce multiple batches with the same schema into one (WAL replay helper).
pub fn coalesce_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    if batches.len() == 1 {
        return Ok(vec![batches[0].clone()]);
    }
    let coalesced = concat_batches(schema, batches).map_err(TsdbError::from)?;
    Ok(vec![coalesced])
}

/// A zero-copy (where possible) buffer that accumulates incoming `RecordBatch`es
/// and yields correctly sized batches at the specified capacity.
pub struct BatchBuffer {
    schema: SchemaRef,
    /// Pending batches waiting to be emitted.
    buffer: VecDeque<RecordBatch>,
    /// Total number of rows currently in the buffer.
    buffered_rows: usize,
    capacity: usize,
}

/// Backward-compatible alias used by older call sites / docs.
pub type ActiveBuilders = BatchBuffer;

impl BatchBuffer {
    pub fn new(schema: SchemaRef, capacity: usize) -> Self {
        Self {
            schema,
            buffer: VecDeque::new(),
            buffered_rows: 0,
            capacity: capacity.max(1),
        }
    }

    pub fn row_count(&self) -> usize {
        self.buffered_rows
    }

    pub fn is_empty(&self) -> bool {
        self.buffered_rows == 0
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Approximate Arrow footprint of buffered slices (includes shared parent buffers).
    pub fn ram_cost(&self) -> usize {
        self.buffer
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum()
    }

    /// Appends a batch and returns completed micro-batches when `capacity` rows are reached.
    pub fn append_batch(&mut self, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }

        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(TsdbError::Schema(format!(
                "Memtable batch schema mismatch. Expected: {:?}, Got: {:?}",
                self.schema.fields(),
                batch.schema().fields()
            )));
        }

        self.buffer.push_back(batch.clone());
        self.buffered_rows += batch.num_rows();

        let mut completed = Vec::new();
        while self.buffered_rows >= self.capacity {
            completed.push(self.extract_capacity_batch()?);
        }
        Ok(completed)
    }

    /// Materialize the tail partial batch, if any.
    pub fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.is_empty() {
            return Ok(None);
        }

        let batches: Vec<&RecordBatch> = self.buffer.iter().collect();
        let tail_batch = concat_batches(&self.schema, batches).map_err(TsdbError::from)?;

        self.buffer.clear();
        self.buffered_rows = 0;

        Ok(Some(tail_batch))
    }

    /// Extracts exactly `self.capacity` rows from the front of the buffer.
    fn extract_capacity_batch(&mut self) -> Result<RecordBatch> {
        let mut chunks = Vec::new();
        let mut rows_collected = 0;

        while rows_collected < self.capacity {
            let needed = self.capacity - rows_collected;
            let Some(front) = self.buffer.pop_front() else {
                break;
            };

            let front_rows = front.num_rows();
            if front_rows <= needed {
                rows_collected += front_rows;
                chunks.push(front);
            } else {
                let chunk_to_take = front.slice(0, needed);
                rows_collected += needed;
                chunks.push(chunk_to_take);

                let remainder = front.slice(needed, front_rows - needed);
                self.buffer.push_front(remainder);
            }
        }

        debug_assert_eq!(
            rows_collected, self.capacity,
            "extract_capacity_batch called without enough buffered rows"
        );
        self.buffered_rows -= self.capacity;

        concat_batches(&self.schema, chunks.iter()).map_err(TsdbError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray, Int64Array, ListBuilder, StringBuilder};
    use arrow::datatypes::{DataType, Field, Int8Type, Schema};
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
    fn dynamic_builders_materialize_at_capacity() {
        let schema = test_schema();
        let mut active = BatchBuffer::new(schema.clone(), 64);
        let mut completed = Vec::new();
        for ts in 0..130 {
            completed.extend(
                active
                    .append_batch(&row_batch(&schema, ts, Some(ts)))
                    .unwrap(),
            );
        }
        assert_eq!(completed.len(), 2);
        assert_eq!(completed[0].num_rows(), 64);
        assert_eq!(completed[1].num_rows(), 64);
        assert_eq!(active.row_count(), 2);
    }

    #[test]
    fn list_utf8_roundtrips_through_builders() {
        use arrow::array::{ListArray, StringArray};

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]));

        // Row 0 -> ["a","b"], Row 1 -> ["c"].
        let mut lb = ListBuilder::new(StringBuilder::new());
        lb.values().append_value("a");
        lb.values().append_value("b");
        lb.append(true);
        lb.values().append_value("c");
        lb.append(true);
        let list = lb.finish();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2])), Arc::new(list)],
        )
        .unwrap();

        let mut active = BatchBuffer::new(schema, 64);
        active.append_batch(&batch).unwrap();
        let out = active.finish().unwrap().unwrap();

        let tags = out.column(1).as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(tags.len(), 2);
        let first = tags.value(0);
        let first = first.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(first.value(0), "a");
        assert_eq!(first.value(1), "b");
        let second = tags.value(1);
        let second = second.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second.value(0), "c");
    }

    #[test]
    fn enum_dictionary_roundtrips_through_builders() {
        use arrow::array::{DictionaryArray, Int8Array, StringArray};

        let dict_type = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("status", dict_type, true),
        ]));

        let variants = Arc::new(StringArray::from(vec!["open", "closed"]));
        let keys = Int8Array::from(vec![Some(0), Some(1), None]);
        let dict = DictionaryArray::<Int8Type>::try_new(keys, variants).unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
                Arc::new(dict),
            ],
        )
        .unwrap();

        let mut active = BatchBuffer::new(schema, 64);
        active.append_batch(&batch).unwrap();
        let out = active.finish().unwrap().unwrap();

        let status = out.column(1).as_dictionary::<Int8Type>();
        let values = status.values().as_string::<i32>();
        assert_eq!(status.len(), 3);
        assert_eq!(values.value(status.keys().value(0) as usize), "open");
        assert_eq!(values.value(status.keys().value(1) as usize), "closed");
        assert!(status.is_null(2));
    }

    #[test]
    fn finish_seals_tail() {
        let schema = test_schema();
        let mut active = BatchBuffer::new(schema.clone(), 64);
        for ts in 0..10 {
            active
                .append_batch(&row_batch(&schema, ts, Some(ts)))
                .unwrap();
        }
        let tail = active.finish().unwrap().unwrap();
        assert_eq!(tail.num_rows(), 10);
        assert!(active.is_empty());
    }

    #[test]
    fn oversized_input_is_sliced_without_row_copy_builders() {
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

        let mut buf = BatchBuffer::new(schema, 64);
        let completed = buf.append_batch(&big).unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].num_rows(), 64);
        assert_eq!(buf.row_count(), 36);
        let tail = buf.finish().unwrap().unwrap();
        assert_eq!(tail.num_rows(), 36);
    }
}
