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

//! Turn [`DataEvent`] into Arrow batches (WAL / resident / Parquet — never mixed).

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use common::Result;

use super::arrow_load::StreamArrowLoader;
use crate::model::event::DataEvent;

/// Abstraction used by [`super::builder::ParquetEventBuilder`] to materialize Inserts.
pub trait WalMaterializer: Send + Sync {
    fn load_event_batches(&self, event: &DataEvent) -> Result<Vec<RecordBatch>>;
}

impl WalMaterializer for StreamArrowLoader {
    fn load_event_batches(&self, event: &DataEvent) -> Result<Vec<RecordBatch>> {
        StreamArrowLoader::load_event_batches(self, event)
    }
}

/// Arrow batches bound to a progress LSN (one logical event after materialization).
#[derive(Debug, Clone)]
pub struct ArrowStreamEvent {
    pub lsn: u64,
    pub batches: Vec<RecordBatch>,
}

impl ArrowStreamEvent {
    pub fn is_empty(&self) -> bool {
        self.batches.iter().all(|b| b.num_rows() == 0)
    }
}

/// Converts ingress [`DataEvent`]s into Arrow streams for Parquet building / sinks.
pub struct EventStreamReader {
    materializer: Option<Arc<dyn WalMaterializer>>,
}

impl EventStreamReader {
    pub fn new(materializer: Option<Arc<dyn WalMaterializer>>) -> Self {
        Self { materializer }
    }

    pub fn from_loader(loader: StreamArrowLoader) -> Self {
        Self::new(Some(Arc::new(loader)))
    }

    /// Convert a data-plane event into Arrow batches.
    ///
    /// - Watermark → `None` (control only)
    /// - Insert / FlushFile → batches via materializer (FlushFile reads Parquet, not WAL)
    pub async fn to_arrow_stream(&self, event: DataEvent) -> Result<Option<ArrowStreamEvent>> {
        if event.is_watermark() {
            return Ok(None);
        }
        let lsn = event.max_lsn();
        let Some(mat) = self.materializer.as_ref() else {
            // No loader: only resident Insert Arrow can be forwarded.
            return Ok(match event {
                DataEvent::Insert { arrow, .. } if arrow.is_resident() => Some(ArrowStreamEvent {
                    lsn,
                    batches: arrow.batches().to_vec(),
                }),
                _ => None,
            });
        };
        let batches = mat.load_event_batches(&event)?;
        if batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(None);
        }
        Ok(Some(ArrowStreamEvent { lsn, batches }))
    }
}
