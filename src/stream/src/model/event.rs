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

//! Ingress event carriers for the stream data plane.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use common::LsnRange;

use crate::data::memory::SharedArrowCharge;

/// In-memory Arrow for an Insert, or explicit WAL-deferred (memory degraded).
///
/// Resident charges travel with the event until Drop (Sink done / Source Flush degrade).
#[derive(Debug, Clone)]
pub enum InsertArrow {
    /// Batches charged against the stream Arrow block (via [`SharedArrowCharge`]).
    Resident {
        batches: Vec<RecordBatch>,
        /// Empty when no stream block is attached (tests / uncapped helpers).
        charges: Vec<SharedArrowCharge>,
    },
    /// Arrow released; reload from WAL by LSN when the sink needs rows.
    Deferred,
}

impl InsertArrow {
    #[inline]
    pub fn resident(batches: Vec<RecordBatch>) -> Self {
        Self::Resident {
            batches,
            charges: Vec::new(),
        }
    }

    #[inline]
    pub fn resident_charged(batches: Vec<RecordBatch>, charge: SharedArrowCharge) -> Self {
        Self::Resident {
            batches,
            charges: vec![charge],
        }
    }

    #[inline]
    pub fn is_resident(&self) -> bool {
        matches!(self, Self::Resident { .. })
    }

    /// True when Arrow must be loaded from WAL before sink write.
    #[inline]
    pub fn needs_load(&self) -> bool {
        matches!(self, Self::Deferred)
    }

    #[inline]
    pub fn batches(&self) -> &[RecordBatch] {
        match self {
            Self::Resident { batches, .. } => batches.as_slice(),
            Self::Deferred => &[],
        }
    }

    #[inline]
    pub fn into_batches(self) -> Vec<RecordBatch> {
        match self {
            Self::Resident { batches, .. } => batches,
            Self::Deferred => Vec::new(),
        }
    }

    #[inline]
    pub fn row_count(&self) -> u64 {
        self.batches().iter().map(|b| b.num_rows() as u64).sum()
    }

    #[inline]
    pub fn memory_size(&self) -> usize {
        self.batches()
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum()
    }

    /// Merge two same-kind arrows (caller ensures LSN contiguity).
    pub fn merge_same_kind(self, other: Self) -> Option<Self> {
        match (self, other) {
            (Self::Deferred, Self::Deferred) => Some(Self::Deferred),
            (
                Self::Resident {
                    mut batches,
                    mut charges,
                },
                Self::Resident {
                    batches: right_batches,
                    charges: right_charges,
                },
            ) => {
                batches.extend(right_batches);
                charges.extend(right_charges);
                Some(Self::Resident { batches, charges })
            }
            _ => None,
        }
    }
}

/// Event entering the Mux / Source queues.
#[derive(Debug, Clone)]
pub enum IngressEvent {
    Insert {
        lsn: LsnRange,
        arrow: InsertArrow,
    },
    /// Memtable seal watermark — not forwarded to Sink as a write.
    Watermark {
        end_lsn: u64,
    },
    FlushFile {
        lsn: LsnRange,
        file_path: Arc<str>,
        rows: u64,
    },
}

/// Stable hot-path alias.
pub type DataEvent = IngressEvent;

impl IngressEvent {
    #[inline]
    pub fn insert(lsn: LsnRange, batches: Vec<RecordBatch>) -> Self {
        Self::Insert {
            lsn,
            arrow: InsertArrow::resident(batches),
        }
    }

    #[inline]
    pub fn insert_deferred(lsn: LsnRange) -> Self {
        Self::Insert {
            lsn,
            arrow: InsertArrow::Deferred,
        }
    }

    pub fn max_lsn(&self) -> u64 {
        match self {
            Self::Insert { lsn, .. } | Self::FlushFile { lsn, .. } => lsn.max_lsn,
            Self::Watermark { end_lsn } => *end_lsn,
        }
    }

    pub fn lsn(&self) -> LsnRange {
        match self {
            Self::Insert { lsn, .. } | Self::FlushFile { lsn, .. } => *lsn,
            Self::Watermark { end_lsn } => LsnRange::single(*end_lsn),
        }
    }

    pub fn is_watermark(&self) -> bool {
        matches!(self, Self::Watermark { .. })
    }

    /// Insert whose Arrow was degraded and must be loaded from WAL.
    pub fn insert_needs_load(&self) -> bool {
        matches!(
            self,
            Self::Insert {
                arrow: InsertArrow::Deferred,
                ..
            }
        )
    }
}
