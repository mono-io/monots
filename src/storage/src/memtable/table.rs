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

//! MemTable with O(1) incremental memory accounting.

use crate::memory::MemoryController;
use crate::memtable::builders::{coalesce_batches, BatchBuffer};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common::Result;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

pub use crate::memtable::builders::DEFAULT_TARGET_BATCH_SIZE as DEFAULT_MEMTABLE_BATCH_MAX_ROWS;
pub use crate::memtable::builders::DEFAULT_TARGET_BATCH_SIZE as DEFAULT_PRIMITIVE_ARRAY_SIZE;
pub const DEFAULT_MEMTABLE_BATCH_MAX_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemTableLayout {
    pub total_rows: usize,
    pub full_chunks: usize,
    pub tail_rows: usize,
    pub chunk_size: usize,
    pub ram_cost: usize,
    pub charged_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct ChunkSnapshot {
    pub chunks: Vec<RecordBatch>,
    pub layout: MemTableLayout,
}

/// Append-only write buffer: sealed batches + active [`BatchBuffer`].
///
/// Inclusive LSN span is tracked here for the live window; flush stamps it onto SST meta.
pub struct MemTable {
    pub id: u64,
    pub schema: SchemaRef,
    frozen_batches: Mutex<Vec<RecordBatch>>,
    active_buffer: Mutex<BatchBuffer>,
    frozen_ram_bytes: AtomicUsize,
    memory_charged: AtomicUsize,
    ram_cost: AtomicUsize,
    /// Inclusive first replicated LSN (`0` = none yet).
    base_lsn: AtomicU64,
    /// Inclusive last replicated LSN (`0` = none yet).
    max_lsn: AtomicU64,
    max_size: usize,
    target_batch_size: usize,
    memory_controller: Arc<MemoryController>,
}

impl MemTable {
    pub fn new(
        id: u64,
        schema: SchemaRef,
        max_size: usize,
        memory_controller: Arc<MemoryController>,
        batch_max_rows: usize,
        _batch_max_bytes: usize,
    ) -> Arc<Self> {
        Self::with_target_batch_size(
            id,
            schema,
            max_size,
            memory_controller,
            batch_max_rows.max(1),
        )
    }

    pub fn with_target_batch_size(
        id: u64,
        schema: SchemaRef,
        max_size: usize,
        memory_controller: Arc<MemoryController>,
        target_batch_size: usize,
    ) -> Arc<Self> {
        let target_batch_size = target_batch_size.max(1);
        Arc::new(Self {
            id,
            schema: schema.clone(),
            frozen_batches: Mutex::new(Vec::new()),
            active_buffer: Mutex::new(BatchBuffer::new(schema, target_batch_size)),
            frozen_ram_bytes: AtomicUsize::new(0),
            memory_charged: AtomicUsize::new(0),
            ram_cost: AtomicUsize::new(0),
            base_lsn: AtomicU64::new(0),
            max_lsn: AtomicU64::new(0),
            max_size,
            target_batch_size,
            memory_controller,
        })
    }

    /// Record a global LSN assigned to a write into this memtable.
    pub fn record_lsn(&self, lsn: u64) {
        if lsn == 0 {
            return;
        }
        self.max_lsn.fetch_max(lsn, Ordering::AcqRel);
        let _ = self
            .base_lsn
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                if cur == 0 || lsn < cur {
                    Some(lsn)
                } else {
                    None
                }
            });
    }

    /// Inclusive `(base_lsn, max_lsn)` for flush / SST naming. `(0, 0)` if never written.
    pub fn lsn_span(&self) -> (u64, u64) {
        let base = self.base_lsn.load(Ordering::Acquire);
        let max = self.max_lsn.load(Ordering::Acquire);
        if base == 0 && max == 0 {
            (0, 0)
        } else if max < base {
            (base, base)
        } else {
            (base, max)
        }
    }

    pub fn with_array_size(
        id: u64,
        schema: SchemaRef,
        max_size: usize,
        memory_controller: Arc<MemoryController>,
        array_size: usize,
    ) -> Arc<Self> {
        Self::with_target_batch_size(id, schema, max_size, memory_controller, array_size)
    }

    pub fn from_batches(
        id: u64,
        schema: SchemaRef,
        max_size: usize,
        memory_controller: Arc<MemoryController>,
        batch_max_rows: usize,
        batch_max_bytes: usize,
        batches: Vec<RecordBatch>,
    ) -> Result<Arc<Self>> {
        let table = Self::new(
            id,
            schema.clone(),
            max_size,
            memory_controller,
            batch_max_rows,
            batch_max_bytes,
        );
        if batches.is_empty() {
            return Ok(table);
        }

        let coalesced = coalesce_batches(&schema, &batches)?;
        for batch in coalesced {
            table.insert(Arc::new(batch))?;
        }
        Ok(table)
    }

    pub fn insert(&self, batch: Arc<RecordBatch>) -> Result<bool> {
        self.insert_for_table(batch, None)
    }

    pub fn insert_for_table(
        &self,
        batch: Arc<RecordBatch>,
        _writing_table: Option<&str>,
    ) -> Result<bool> {
        if batch.num_rows() == 0 {
            return Ok(false);
        }
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(common::TsdbError::Schema(
                "memtable batch schema mismatch with table schema".into(),
            ));
        }

        let (new_frozen, active_ram) = {
            let mut active = self.active_buffer.lock();
            let frozen = active.append_batch(batch.as_ref())?;
            (frozen, active.ram_cost())
        };

        if !new_frozen.is_empty() {
            let newly_frozen_ram: usize = new_frozen
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum();
            self.frozen_ram_bytes
                .fetch_add(newly_frozen_ram, Ordering::AcqRel);
            self.frozen_batches.lock().extend(new_frozen);
        }

        let frozen_ram = self.frozen_ram_bytes.load(Ordering::Acquire);
        self.ram_cost
            .store(frozen_ram + active_ram, Ordering::Release);
        self.sync_charged_to_ram(true)?;

        Ok(self.ram_cost.load(Ordering::Acquire) >= self.max_size)
    }

    /// Seal active tail. Accounting must not fail on the flush path (`allow_reserve = false`).
    pub fn seal(&self) -> Result<()> {
        self.flush_active_buffer()?;
        self.sync_charged_to_ram(false)?;
        Ok(())
    }

    pub fn flush_active_buffer(&self) -> Result<()> {
        let tail = {
            let mut active = self.active_buffer.lock();
            let res = active.finish()?;
            let frozen_ram = self.frozen_ram_bytes.load(Ordering::Acquire);
            self.ram_cost.store(frozen_ram, Ordering::Release);
            res
        };

        if let Some(batch) = tail {
            let tail_ram = batch.get_array_memory_size();
            self.frozen_ram_bytes.fetch_add(tail_ram, Ordering::AcqRel);
            self.ram_cost.fetch_add(tail_ram, Ordering::AcqRel);
            self.frozen_batches.lock().push(batch);
        }
        Ok(())
    }

    pub fn flush_active_builders(&self) -> Result<()> {
        self.flush_active_buffer()
    }

    pub fn layout(&self) -> MemTableLayout {
        let frozen = self.frozen_batches.lock();
        let active = self.active_buffer.lock();
        let chunk_size = self.target_batch_size;

        let mut full_chunks = 0;
        let mut frozen_rows = 0;
        let mut non_chunk_rows = 0;
        for batch in frozen.iter() {
            let rows = batch.num_rows();
            frozen_rows += rows;
            if rows == chunk_size {
                full_chunks += 1;
            } else {
                non_chunk_rows += rows;
            }
        }

        MemTableLayout {
            total_rows: frozen_rows + active.row_count(),
            full_chunks,
            tail_rows: active.row_count() + non_chunk_rows,
            chunk_size,
            ram_cost: self.ram_cost.load(Ordering::Relaxed),
            charged_bytes: self.charged_bytes(),
        }
    }

    pub fn footprint_bytes(&self) -> usize {
        self.ram_cost.load(Ordering::Relaxed)
    }

    pub fn size_bytes(&self) -> usize {
        self.footprint_bytes()
    }

    pub fn charged_bytes(&self) -> usize {
        self.memory_charged.load(Ordering::Relaxed)
    }

    pub fn chunk_count(&self) -> usize {
        self.frozen_batches.lock().len()
    }

    pub fn batch_count(&self) -> usize {
        self.chunk_count()
    }

    /// Point-in-time snapshot under both locks (after seal).
    pub fn snapshot_chunks(&self) -> Result<ChunkSnapshot> {
        self.seal()?;

        let frozen = self.frozen_batches.lock();
        let active = self.active_buffer.lock();
        let chunks = frozen.clone();
        let chunk_size = self.target_batch_size;

        let mut full_chunks = 0;
        let mut frozen_rows = 0;
        let mut non_chunk_rows = 0;
        for batch in frozen.iter() {
            let rows = batch.num_rows();
            frozen_rows += rows;
            if rows == chunk_size {
                full_chunks += 1;
            } else {
                non_chunk_rows += rows;
            }
        }

        let layout = MemTableLayout {
            total_rows: frozen_rows + active.row_count(),
            full_chunks,
            tail_rows: active.row_count() + non_chunk_rows,
            chunk_size,
            ram_cost: self.ram_cost.load(Ordering::Relaxed),
            charged_bytes: self.memory_charged.load(Ordering::Relaxed),
        };
        Ok(ChunkSnapshot { chunks, layout })
    }

    pub fn get_batches_snapshot(&self) -> Vec<RecordBatch> {
        self.snapshot_chunks()
            .map(|snapshot| snapshot.chunks)
            .unwrap_or_default()
    }

    pub fn release_memory(&self) {
        let charged = self.memory_charged.swap(0, Ordering::AcqRel);
        if charged > 0 {
            self.memory_controller.release(charged);
        }
        self.ram_cost.store(0, Ordering::Release);
        self.frozen_ram_bytes.store(0, Ordering::Release);
        let _ = self.active_buffer.lock().finish();
        self.frozen_batches.lock().clear();
    }

    fn sync_charged_to_ram(&self, allow_reserve: bool) -> Result<()> {
        let ram = self.ram_cost.load(Ordering::Acquire);
        let charged = self.memory_charged.load(Ordering::Acquire);

        match ram.cmp(&charged) {
            std::cmp::Ordering::Equal => Ok(()),
            std::cmp::Ordering::Greater => {
                let delta = ram - charged;
                if !allow_reserve {
                    self.memory_controller.reserve_unchecked(delta);
                    self.memory_charged.store(ram, Ordering::Release);
                    return Ok(());
                }
                if !self.memory_controller.try_reserve(delta) {
                    return Err(self.memory_controller.memory_limit_error());
                }
                self.memory_charged.store(ram, Ordering::Release);
                Ok(())
            }
            std::cmp::Ordering::Less => {
                self.memory_controller.release(charged - ram);
                self.memory_charged.store(ram, Ordering::Release);
                Ok(())
            }
        }
    }
}

impl Drop for MemTable {
    fn drop(&mut self) {
        let charged = self.memory_charged.load(Ordering::Relaxed);
        if charged > 0 {
            self.memory_controller.release(charged);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn sample_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn row_batch(schema: &SchemaRef, ts: i64, value: i64) -> RecordBatch {
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
    fn memtable_accepts_runtime_schema_batches() {
        let memory = Arc::new(MemoryController::new(64 * 1024 * 1024));
        let schema = sample_schema();
        let mem = MemTable::new(
            1,
            schema.clone(),
            64 * 1024 * 1024,
            memory.clone(),
            DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
            DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
        );

        for ts in 0..200 {
            mem.insert(Arc::new(row_batch(&schema, ts, ts))).unwrap();
        }

        let layout = mem.layout();
        assert_eq!(layout.total_rows, 200);
        assert_eq!(mem.batch_count(), 0);
        assert!(mem.charged_bytes() > 0);

        let snapshot = mem.snapshot_chunks().unwrap();
        assert_eq!(snapshot.chunks.len(), 1);
        assert_eq!(snapshot.chunks[0].num_rows(), 200);
    }

    #[test]
    fn memtable_supports_dynamic_string_and_float_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("sensor", DataType::Utf8, true),
            Field::new("reading", DataType::Float64, true),
        ]));
        let mem = MemTable::new(
            1,
            schema.clone(),
            1024 * 1024,
            Arc::new(MemoryController::new(1024 * 1024)),
            DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
            DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
        );

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
                Arc::new(Float64Array::from(vec![Some(1.5), None])),
            ],
        )
        .unwrap();
        mem.insert(Arc::new(batch)).unwrap();

        let snapshot = mem.snapshot_chunks().unwrap();
        assert_eq!(snapshot.chunks.len(), 1);
        assert_eq!(snapshot.chunks[0].num_rows(), 2);
    }

    #[test]
    fn seal_is_idempotent_when_active_is_empty() {
        let schema = sample_schema();
        let mem = MemTable::new(
            1,
            schema.clone(),
            1024 * 1024,
            Arc::new(MemoryController::new(1024 * 1024)),
            DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
            DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
        );
        mem.insert(Arc::new(row_batch(&schema, 1, 1))).unwrap();
        mem.seal().unwrap();
        assert_eq!(mem.batch_count(), 1);
    }

    #[test]
    fn records_inclusive_lsn_span() {
        let mem = MemTable::new(
            1,
            sample_schema(),
            1024 * 1024,
            Arc::new(MemoryController::new(1024 * 1024)),
            DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
            DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
        );
        assert_eq!(mem.lsn_span(), (0, 0));
        mem.record_lsn(100);
        mem.record_lsn(150);
        mem.record_lsn(140);
        assert_eq!(mem.lsn_span(), (100, 150));
    }
}
