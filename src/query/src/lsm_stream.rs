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

//! Priority-aware K-way merge stream with O(1) dedupe state and vectorized interleaving.
//!
//! Rows are never copied one at a time. The merge only records each surviving row's coordinate
//! `(source_batch_idx, row_idx)`; once a chunk reaches `OUTPUT_BATCH_SIZE` (or the inputs drain) a
//! single `arrow::compute::interleave` call gathers all coordinates per column in one contiguous,
//! kernel-level copy — orders of magnitude cheaper than per-row `slice` + `concat`.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::Array;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common::time_value_at;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_common::{DataFusionError, Result as DfResult};
use futures::{ready, Stream, StreamExt};

const OUTPUT_BATCH_SIZE: usize = 8192;

struct HeapCursor {
    ts: i64,
    layer_priority: usize,
    stream_idx: usize,
    row_idx: usize,
    batch: Arc<RecordBatch>,
}

impl HeapCursor {
    fn new(
        batch: Arc<RecordBatch>,
        row_idx: usize,
        stream_idx: usize,
        layer_priority: usize,
        ts_column_idx: usize,
    ) -> Self {
        let ts = time_value_at(batch.column(ts_column_idx), row_idx).unwrap_or(i64::MIN);
        Self {
            ts,
            layer_priority,
            stream_idx,
            row_idx,
            batch,
        }
    }

    fn refresh_ts(&mut self, ts_column_idx: usize) {
        self.ts = time_value_at(self.batch.column(ts_column_idx), self.row_idx).unwrap_or(i64::MIN);
    }
}

impl PartialEq for HeapCursor {
    fn eq(&self, other: &Self) -> bool {
        self.ts == other.ts && self.layer_priority == other.layer_priority
    }
}

impl Eq for HeapCursor {}

impl PartialOrd for HeapCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapCursor {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap pops the "greatest" element — invert so smallest ts wins.
        // On equal ts, smaller layer_priority (newer layer) wins.
        other
            .ts
            .cmp(&self.ts)
            .then_with(|| other.layer_priority.cmp(&self.layer_priority))
    }
}

/// Accumulates surviving row coordinates and materializes them with one interleave per column.
struct VectorizedMergeChunk {
    schema: SchemaRef,
    /// Ordered `(source_idx, row_idx)` coordinates handed to `interleave`.
    indices: Vec<(usize, usize)>,
    /// Distinct source batches referenced by `indices`.
    sources: Vec<Arc<RecordBatch>>,
    /// Batch pointer → index in `sources`, so each batch is referenced once (no Array re-clone).
    source_index_map: HashMap<usize, usize>,
}

impl VectorizedMergeChunk {
    fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            indices: Vec::with_capacity(OUTPUT_BATCH_SIZE),
            sources: Vec::new(),
            source_index_map: HashMap::new(),
        }
    }

    fn push(&mut self, batch: Arc<RecordBatch>, row_idx: usize) {
        let batch_ptr = Arc::as_ptr(&batch) as usize;
        let source_idx = *self.source_index_map.entry(batch_ptr).or_insert_with(|| {
            let idx = self.sources.len();
            self.sources.push(batch);
            idx
        });
        self.indices.push((source_idx, row_idx));
    }

    fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    fn len(&self) -> usize {
        self.indices.len()
    }

    fn build(self) -> DfResult<RecordBatch> {
        let num_cols = self.schema.fields().len();
        let mut final_columns = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let arrays: Vec<&dyn Array> = self
                .sources
                .iter()
                .map(|b| b.column(col_idx).as_ref())
                .collect();
            final_columns.push(arrow::compute::interleave(&arrays, &self.indices)?);
        }
        RecordBatch::try_new(self.schema.clone(), final_columns)
            .map_err(|e| DataFusionError::ArrowError(e, None))
    }
}

pub struct LsmPriorityMergeStream {
    streams: Vec<(SendableRecordBatchStream, usize)>,
    schema: SchemaRef,
    ts_column_idx: usize,
    heap: BinaryHeap<HeapCursor>,
    initialized: bool,
    stream_init_idx: usize,
    last_emitted_ts: Option<i64>,
    chunk: VectorizedMergeChunk,
    pending_stream_fetch: Option<usize>,
}

impl LsmPriorityMergeStream {
    pub fn new(
        streams: Vec<(SendableRecordBatchStream, usize)>,
        schema: SchemaRef,
        ts_column_idx: usize,
    ) -> Self {
        Self {
            streams,
            schema: schema.clone(),
            ts_column_idx,
            heap: BinaryHeap::new(),
            initialized: false,
            stream_init_idx: 0,
            last_emitted_ts: None,
            chunk: VectorizedMergeChunk::new(schema),
            pending_stream_fetch: None,
        }
    }

    fn continue_pending_fetch(&mut self, cx: &mut Context<'_>) -> Poll<DfResult<()>> {
        let idx = self.pending_stream_fetch.expect("pending_stream_fetch set");
        match ready!(self.streams[idx].0.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                self.pending_stream_fetch = None;
                if batch.num_rows() > 0 {
                    self.heap.push(HeapCursor::new(
                        Arc::new(batch),
                        0,
                        idx,
                        self.streams[idx].1,
                        self.ts_column_idx,
                    ));
                }
                Poll::Ready(Ok(()))
            }
            Some(Err(e)) => Poll::Ready(Err(e)),
            None => {
                self.pending_stream_fetch = None;
                Poll::Ready(Ok(()))
            }
        }
    }

    fn ensure_initialized(&mut self, cx: &mut Context<'_>) -> Poll<DfResult<()>> {
        if self.initialized {
            return Poll::Ready(Ok(()));
        }
        while self.stream_init_idx < self.streams.len() {
            let idx = self.stream_init_idx;
            match ready!(self.streams[idx].0.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    if batch.num_rows() > 0 {
                        let priority = self.streams[idx].1;
                        self.heap.push(HeapCursor::new(
                            Arc::new(batch),
                            0,
                            idx,
                            priority,
                            self.ts_column_idx,
                        ));
                    }
                    self.stream_init_idx += 1;
                }
                Some(Err(e)) => return Poll::Ready(Err(e)),
                None => {
                    self.stream_init_idx += 1;
                }
            }
        }
        self.initialized = true;
        Poll::Ready(Ok(()))
    }

    fn take_chunk(&mut self) -> DfResult<RecordBatch> {
        let chunk = std::mem::replace(
            &mut self.chunk,
            VectorizedMergeChunk::new(self.schema.clone()),
        );
        chunk.build()
    }

    fn advance_cursor(
        &mut self,
        mut cursor: HeapCursor,
        cx: &mut Context<'_>,
    ) -> Poll<DfResult<()>> {
        cursor.row_idx += 1;
        if cursor.row_idx < cursor.batch.num_rows() {
            cursor.refresh_ts(self.ts_column_idx);
            self.heap.push(cursor);
            return Poll::Ready(Ok(()));
        }

        let idx = cursor.stream_idx;
        match self.streams[idx].0.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                if batch.num_rows() > 0 {
                    self.heap.push(HeapCursor::new(
                        Arc::new(batch),
                        0,
                        idx,
                        self.streams[idx].1,
                        self.ts_column_idx,
                    ));
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => {
                self.pending_stream_fetch = Some(idx);
                Poll::Pending
            }
        }
    }
}

impl Stream for LsmPriorityMergeStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.pending_stream_fetch.is_some() {
            match self.continue_pending_fetch(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                Poll::Pending => return Poll::Pending,
            }
        }

        match ready!(self.ensure_initialized(cx)) {
            Ok(()) => {}
            Err(e) => return Poll::Ready(Some(Err(e))),
        }

        loop {
            while self.chunk.len() < OUTPUT_BATCH_SIZE {
                let Some(cursor) = self.heap.pop() else {
                    break;
                };

                let duplicate = Some(cursor.ts) == self.last_emitted_ts;
                if !duplicate {
                    self.last_emitted_ts = Some(cursor.ts);
                    self.chunk.push(cursor.batch.clone(), cursor.row_idx);
                    if self.chunk.len() >= OUTPUT_BATCH_SIZE {
                        let batch = match self.take_chunk() {
                            Ok(b) => b,
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        };
                        match self.advance_cursor(cursor, cx) {
                            Poll::Ready(Ok(())) => return Poll::Ready(Some(Ok(batch))),
                            Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                            Poll::Pending => return Poll::Ready(Some(Ok(batch))),
                        }
                    }
                }

                match self.advance_cursor(cursor, cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                    Poll::Pending => return Poll::Pending,
                }
            }

            if !self.chunk.is_empty() {
                return Poll::Ready(Some(self.take_chunk()));
            }

            if self.heap.is_empty() {
                return Poll::Ready(None);
            }
        }
    }
}

impl RecordBatchStream for LsmPriorityMergeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
