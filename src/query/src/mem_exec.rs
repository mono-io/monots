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

//! Lazy memtable scan — align / filter / sort run at execute time, not in `TableProvider::scan`.
//!
//! The common monotonic-ingest case streams one chunk at a time (align + time-filter on demand,
//! bounded to a single chunk of transient memory). Only when chunks are not strictly
//! timestamp-ordered do we fall back to materializing a single globally-sorted, deduped snapshot
//! (required so the LSM merge sees a sorted input stream).

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result as DfResult};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::ExecutionMode;
use futures::Stream;
use monots_storage::dedup::{chunks_strictly_ordered, prepare_scan_batches};
use monots_storage::memtable::MemTable;
use monots_storage::parquet_read::{filter_batch_by_time, ParquetReadOptions};
use monots_storage::reader::BatchAligner;

use crate::filter::TimeRange;

/// Physical plan: read one memtable layer at execution time (planner stays non-blocking).
#[derive(Clone)]
pub struct MemTableScanExec {
    memtable: Arc<MemTable>,
    schema: SchemaRef,
    time_range: TimeRange,
    cache: PlanProperties,
}

impl MemTableScanExec {
    pub fn new(memtable: Arc<MemTable>, schema: SchemaRef, time_range: TimeRange) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self {
            memtable,
            schema,
            time_range,
            cache,
        }
    }

    fn read_options(&self) -> ParquetReadOptions {
        ParquetReadOptions {
            min_ts: self.time_range.min_ts,
            max_ts: self.time_range.max_ts,
            projection: None,
        }
    }
}

/// Align one raw memtable chunk to the target schema and drop rows outside the time range.
/// Returns `None` when the chunk has no surviving rows.
fn align_and_filter_chunk(
    chunk: RecordBatch,
    schema: &SchemaRef,
    opts: &ParquetReadOptions,
) -> DfResult<Option<RecordBatch>> {
    let aligned = BatchAligner::align(chunk, schema.clone())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let filtered =
        filter_batch_by_time(&aligned, opts).map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(filtered.filter(|b| b.num_rows() > 0))
}

/// Streaming memtable scan output.
///
/// * `Streaming` — strictly ordered chunks: align + filter each chunk on demand and yield it
///   directly (already sorted). Peak transient memory is a single chunk.
/// * `Buffered` — disordered chunks: a single globally-sorted, deduped snapshot pre-materialized
///   in `execute`, then handed out batch by batch.
enum ScanState {
    Streaming {
        chunks: Vec<RecordBatch>,
        next: usize,
    },
    Buffered {
        batches: Vec<RecordBatch>,
        next: usize,
    },
}

struct MemTableScanStream {
    schema: SchemaRef,
    opts: ParquetReadOptions,
    state: ScanState,
}

impl Stream for MemTableScanStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match &mut this.state {
            ScanState::Streaming { chunks, next } => {
                while *next < chunks.len() {
                    let chunk = chunks[*next].clone();
                    *next += 1;
                    match align_and_filter_chunk(chunk, &this.schema, &this.opts) {
                        Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                        Ok(None) => continue,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
                Poll::Ready(None)
            }
            ScanState::Buffered { batches, next } => {
                if *next < batches.len() {
                    let batch = batches[*next].clone();
                    *next += 1;
                    Poll::Ready(Some(Ok(batch)))
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}

impl RecordBatchStream for MemTableScanStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl std::fmt::Debug for MemTableScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemTableScanExec")
            .field("memtable_id", &self.memtable.id)
            .finish()
    }
}

impl DisplayAs for MemTableScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MemTableScanExec(id={})", self.memtable.id)
    }
}

impl ExecutionPlan for MemTableScanExec {
    fn name(&self) -> &str {
        "MemTableScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "MemTableScanExec does not accept children".into(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "MemTableScanExec only supports partition 0, got {partition}"
            )));
        }

        let snapshot = self
            .memtable
            .snapshot_chunks()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if snapshot.chunks.is_empty() {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                futures::stream::empty(),
            )));
        }

        let opts = self.read_options();

        // Strictly ordered chunks are already globally sorted and duplicate-free, so we can align
        // + filter and emit them one at a time (bounded to a single chunk of transient memory).
        let ordered = chunks_strictly_ordered(&snapshot.chunks)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let state = if ordered {
            ScanState::Streaming {
                chunks: snapshot.chunks,
                next: 0,
            }
        } else {
            // Disorder: build one globally-sorted, deduped snapshot so the LSM merge sees a sorted
            // input stream. This still hands the batches out incrementally.
            let mut filtered = Vec::with_capacity(snapshot.chunks.len());
            for chunk in snapshot.chunks {
                if let Some(batch) = align_and_filter_chunk(chunk, &self.schema, &opts)? {
                    filtered.push(batch);
                }
            }
            let batches = if filtered.is_empty() {
                Vec::new()
            } else {
                prepare_scan_batches(&filtered, self.schema.clone())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            };
            ScanState::Buffered { batches, next: 0 }
        };

        Ok(Box::pin(MemTableScanStream {
            schema: self.schema.clone(),
            opts,
            state,
        }))
    }
}
