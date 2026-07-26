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

//! Storage → Stream capture boundary (listener + Source bootstrap contract).

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

use super::event::{BatchEvent, LsnRange};

/// Lightweight SST identity across the storage / stream boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureFileMeta {
    pub file_path: String,
    pub min_lsn: u64,
    pub max_lsn: u64,
    pub min_ts: i64,
    pub max_ts: i64,
    pub rows: u64,
}

impl CaptureFileMeta {
    pub fn new(file_path: impl Into<String>, min_lsn: u64, max_lsn: u64) -> Self {
        Self {
            file_path: file_path.into(),
            min_lsn,
            max_lsn: max_lsn.max(min_lsn),
            min_ts: 0,
            max_ts: 0,
            rows: 0,
        }
    }

    pub fn with_stats(mut self, min_ts: i64, max_ts: i64, rows: u64) -> Self {
        self.min_ts = min_ts;
        self.max_ts = max_ts;
        self.rows = rows;
        self
    }

    pub fn has_lsn_bounds(&self) -> bool {
        self.min_lsn > 0 || self.max_lsn > 0
    }

    pub fn lsn_range(&self) -> LsnRange {
        LsnRange::new(self.min_lsn, self.max_lsn)
    }

    pub fn to_flush_event(&self, link_path: impl Into<String>) -> BatchEvent {
        BatchEvent::flush(
            self.min_lsn,
            self.max_lsn,
            link_path,
            self.min_ts,
            self.max_ts,
            self.rows,
        )
    }

    pub fn to_bulk_event(&self, link_path: impl Into<String>) -> BatchEvent {
        BatchEvent::bulk_load(self.max_lsn, link_path, self.min_ts, self.max_ts, self.rows)
    }

    pub fn to_compact_event(&self, link_path: impl Into<String>) -> BatchEvent {
        BatchEvent::compact(
            self.min_lsn,
            self.max_lsn,
            link_path,
            self.min_ts,
            self.max_ts,
            self.rows,
        )
    }
}

/// Live write-path notifications from storage (Insert / Flush / BulkLoad / Compact).
///
/// Storage never owns queues — Stream's [`CaptureSource`] implements this.
pub trait TableCaptureListener: Send + Sync + 'static {
    /// When `false`, storage skips [`Self::on_insert`] for this listener (no Arrow clone).
    /// Default: `true`. Batch-only CDC sources should return `false`.
    fn capture_wal(&self) -> bool {
        true
    }

    fn on_insert(&self, min_lsn: u64, max_lsn: u64, batch: RecordBatch);
    /// Memtable sealed for flush: `end_lsn` is the inclusive max LSN of that memtable.
    ///
    /// Source treats this as a watermark — Inserts between watermarks may merge; Mux may
    /// commit progress up to `end_lsn`. Default: no-op (batch-only / tests).
    fn on_memtable_end(&self, end_lsn: u64) {
        let _ = end_lsn;
    }
    fn on_flush(&self, meta: &CaptureFileMeta);
    fn on_bulk_load(&self, meta: &CaptureFileMeta);
    fn on_compact(&self, inputs: &[CaptureFileMeta], output: &CaptureFileMeta);
}

/// Stream Source contract for CDC capture.
///
/// Storage calls this during **first registration / bootstrap**:
/// 1. exclude writers → flush all memtables
/// 2. [`Self::on_historical_sst`] for every durable SST
/// 3. attach for live [`TableCaptureListener`] callbacks
/// 4. [`Self::on_bootstrap_done`]
///
/// Multiplexer / Sink / checkpoint stay in the Stream crate.
pub trait CaptureSource: TableCaptureListener {
    /// One historical SST after memtables have been flushed to disk.
    ///
    /// Default: same as live [`TableCaptureListener::on_flush`] (pin + enqueue).
    fn on_historical_sst(&self, meta: &CaptureFileMeta) {
        self.on_flush(meta);
    }

    /// Bootstrap finished. `frontier_lsn` is the max sealed LSN on the table (0 if empty).
    fn on_bootstrap_done(&self, frontier_lsn: u64);
}

/// Result of first-time Source registration / bootstrap (storage → Stream).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureBootstrapReport {
    pub progress_id: String,
    pub table: String,
    pub stream: String,
    /// Max LSN sealed on SST after flush-before-history (0 if table has no LSN SST yet).
    pub frontier_lsn: u64,
    /// Number of historical SST files delivered to the Source.
    pub historical_files: usize,
}

/// Arc-erase helper: `Arc<dyn CaptureSource>` → `Arc<dyn TableCaptureListener>` for live attach.
pub struct CaptureSourceHandle(pub Arc<dyn CaptureSource>);

impl TableCaptureListener for CaptureSourceHandle {
    fn capture_wal(&self) -> bool {
        self.0.capture_wal()
    }

    fn on_insert(&self, min_lsn: u64, max_lsn: u64, batch: RecordBatch) {
        self.0.on_insert(min_lsn, max_lsn, batch);
    }

    fn on_memtable_end(&self, end_lsn: u64) {
        self.0.on_memtable_end(end_lsn);
    }

    fn on_flush(&self, meta: &CaptureFileMeta) {
        self.0.on_flush(meta);
    }

    fn on_bulk_load(&self, meta: &CaptureFileMeta) {
        self.0.on_bulk_load(meta);
    }

    fn on_compact(&self, inputs: &[CaptureFileMeta], output: &CaptureFileMeta) {
        self.0.on_compact(inputs, output);
    }
}

impl CaptureSource for CaptureSourceHandle {
    fn on_historical_sst(&self, meta: &CaptureFileMeta) {
        self.0.on_historical_sst(meta);
    }

    fn on_bootstrap_done(&self, frontier_lsn: u64) {
        self.0.on_bootstrap_done(frontier_lsn);
    }
}
