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

//! CDC event types: only [`LogEvent`] (insert) and [`BatchEvent`] (file).
//!
//! Delivery across a single poll API uses [`CdcEvent`] = Insert | File — not a third payload shape.

use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

/// Inclusive LSN span: `[base_lsn, max_lsn]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LsnRange {
    pub base_lsn: u64,
    pub max_lsn: u64,
}

impl LsnRange {
    pub fn single(lsn: u64) -> Self {
        Self {
            base_lsn: lsn,
            max_lsn: lsn,
        }
    }

    pub fn new(base_lsn: u64, max_lsn: u64) -> Self {
        if base_lsn <= max_lsn {
            Self { base_lsn, max_lsn }
        } else {
            Self {
                base_lsn: max_lsn,
                max_lsn: base_lsn,
            }
        }
    }

    pub fn covers(self, lsn: u64) -> bool {
        lsn >= self.base_lsn && lsn <= self.max_lsn
    }

    pub fn ack_lsn(self) -> u64 {
        self.max_lsn
    }
}

/// Insert / log-path event (MemTable Arrow and/or LSN span for WAL load).
#[derive(Debug, Clone)]
pub struct LogEvent {
    pub lsn: LsnRange,
    /// In-memory Arrow when available. `None` → load from WAL by `lsn`.
    pub batches: Option<Vec<RecordBatch>>,
}

impl LogEvent {
    pub fn memory(base_lsn: u64, max_lsn: u64, batches: Vec<RecordBatch>) -> Self {
        Self {
            lsn: LsnRange::new(base_lsn, max_lsn),
            batches: Some(batches),
        }
    }

    pub fn memory_single(lsn: u64, batch: RecordBatch) -> Self {
        Self::memory(lsn, lsn, vec![batch])
    }

    pub fn from_lsn_range(base_lsn: u64, max_lsn: u64) -> Self {
        Self {
            lsn: LsnRange::new(base_lsn, max_lsn),
            batches: None,
        }
    }

    pub fn from_lsn(lsn: u64) -> Self {
        Self::from_lsn_range(lsn, lsn)
    }

    pub fn has_batches(&self) -> bool {
        self.batches.as_ref().is_some_and(|b| !b.is_empty())
    }

    pub fn needs_wal_load(&self) -> bool {
        !self.has_batches()
    }

    pub fn set_batches(&mut self, batches: Vec<RecordBatch>) {
        self.batches = Some(batches);
    }
}

/// How a [`BatchEvent`] was produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BatchOrigin {
    #[default]
    Flush,
    BulkLoad,
    Compact,
}

/// File-path event: Flush / BulkLoad / Compact Parquet under a stable `link_path`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchEvent {
    pub lsn: LsnRange,
    /// Stable hard-link / pin path (never a live SST path compaction may unlink).
    pub link_path: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub rows: u64,
    #[serde(default)]
    pub origin: BatchOrigin,
}

impl BatchEvent {
    pub fn flush(
        base_lsn: u64,
        max_lsn: u64,
        link_path: impl Into<String>,
        min_ts: i64,
        max_ts: i64,
        rows: u64,
    ) -> Self {
        Self {
            lsn: LsnRange::new(base_lsn, max_lsn),
            link_path: link_path.into(),
            min_ts,
            max_ts,
            rows,
            origin: BatchOrigin::Flush,
        }
    }

    pub fn bulk_load(
        lsn: u64,
        link_path: impl Into<String>,
        min_ts: i64,
        max_ts: i64,
        rows: u64,
    ) -> Self {
        Self {
            lsn: LsnRange::single(lsn),
            link_path: link_path.into(),
            min_ts,
            max_ts,
            rows,
            origin: BatchOrigin::BulkLoad,
        }
    }

    pub fn compact(
        base_lsn: u64,
        max_lsn: u64,
        link_path: impl Into<String>,
        min_ts: i64,
        max_ts: i64,
        rows: u64,
    ) -> Self {
        Self {
            lsn: LsnRange::new(base_lsn, max_lsn),
            link_path: link_path.into(),
            min_ts,
            max_ts,
            rows,
            origin: BatchOrigin::Compact,
        }
    }

    pub fn is_bulk_load(&self) -> bool {
        self.origin == BatchOrigin::BulkLoad
    }

    pub fn is_compact(&self) -> bool {
        self.origin == BatchOrigin::Compact
    }
}

/// Single poll/delivery unit: either insert ([`LogEvent`]) or file ([`BatchEvent`]).
#[derive(Debug, Clone)]
pub enum CdcEvent {
    Insert(LogEvent),
    File(BatchEvent),
}

impl CdcEvent {
    pub fn lsn_range(&self) -> LsnRange {
        match self {
            Self::Insert(e) => e.lsn,
            Self::File(e) => e.lsn,
        }
    }

    pub fn ack_lsn(&self) -> u64 {
        self.lsn_range().ack_lsn()
    }

    pub fn lsn(&self) -> u64 {
        self.ack_lsn()
    }

    pub fn base_lsn(&self) -> u64 {
        self.lsn_range().base_lsn
    }

    pub fn from_log(event: LogEvent) -> Self {
        Self::Insert(event)
    }

    pub fn from_batch(event: BatchEvent) -> Self {
        Self::File(event)
    }
}
