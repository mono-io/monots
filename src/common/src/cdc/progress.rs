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

//! Shared CDC progress / commit / bulk-file-add contracts.
//!
//! Event payloads live in [`crate::cdc::event`].

use serde::{Deserialize, Serialize};

/// Sentinel: no capture progress pins retention → the engine may GC WAL freely.
pub const RETENTION_UNPINNED: u64 = u64::MAX;

/// Durable capture progress for one consumer identity (stream × table).
///
/// Pins WAL retention: data with LSN `<= acked_lsn` may be reclaimed for this consumer.
#[derive(Debug, Clone, Copy)]
pub struct CaptureProgress {
    /// Highest LSN the consumer has durably acknowledged (safe to GC at or below this).
    pub acked_lsn: u64,
}

/// Commit durability for capture-progress persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CommitDurability {
    /// fsync the commit record before returning (crash-safe, higher latency).
    Sync,
    /// Buffer the commit record; flush on periodic tick, explicit flush, or drop.
    #[default]
    Async,
}

impl CommitDurability {
    pub fn is_sync(self) -> bool {
        matches!(self, Self::Sync)
    }
}

/// One bulk-loaded SST published as a logical event on the global LSN timeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileAddEvent {
    /// Global LSN assigned to this bulk-load commit.
    pub lsn: u64,
    /// Original SST path in the table (may be compacted away later).
    pub original_path: String,
    /// Stable path for readers (may equal `original_path`, or a pinned hard link from BulkLoad WAL).
    pub link_path: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub rows: u64,
}
