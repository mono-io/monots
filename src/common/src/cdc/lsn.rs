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

//! Engine-global Log Sequence Number (LSN) allocator.
//!
//! Every logical write (one WAL batch append) is stamped with a globally monotonic, unique LSN.
//! Consumers track progress by LSN instead of the physical `(memtable_id, sequence)` pair, which
//! decouples replication from the storage engine's physical layout (flush, compaction, file ids).
//!
//! # Durability (no on-disk allocator file)
//!
//! The next LSN after restart is derived only from durable data:
//! - max LSN in memtable / bulk-load WAL frames, and/or
//! - max LSN sealed on SST (Parquet) metadata.
//!
//! Invariants maintained elsewhere:
//! - After flush, SST carries LSN bounds so WAL may be GC'd; otherwise at least one data-bearing
//!   WAL is retained as the LSN watermark.
//! - No WAL and no SST ⇒ no writes have ever been made durable ⇒ allocation starts at 1.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::Result;

pub struct LsnAllocator {
    /// Next LSN to hand out (starts at 1; advanced via [`Self::ensure_above`] on restart).
    next: AtomicU64,
}

impl LsnAllocator {
    /// Fresh in-memory allocator. Call [`Self::ensure_above`] after scanning WAL/SST watermarks.
    pub fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// Allocate one LSN (monotonic in this process).
    pub fn allocate(&self) -> Result<u64> {
        Ok(self.next.fetch_add(1, Ordering::SeqCst))
    }

    /// Advance so the next [`Self::allocate`] returns a value strictly greater than `max_seen`.
    ///
    /// Used on restart after scanning durable WAL / SST LSN watermarks. Idempotent when already
    /// ahead of `max_seen`. `max_seen == 0` means no durable data (no-op).
    pub fn ensure_above(&self, max_seen: u64) -> Result<()> {
        if max_seen == 0 {
            return Ok(());
        }
        let want_next = max_seen.saturating_add(1);
        loop {
            let cur = self.next.load(Ordering::Acquire);
            if cur >= want_next {
                return Ok(());
            }
            if self
                .next
                .compare_exchange(cur, want_next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    /// Highest LSN handed out so far (0 when nothing was allocated yet).
    pub fn current(&self) -> u64 {
        self.next.load(Ordering::Acquire).saturating_sub(1)
    }

    /// Next LSN that would be handed out.
    pub fn peek_next(&self) -> u64 {
        self.next.load(Ordering::Acquire)
    }
}

impl Default for LsnAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_monotonically() {
        let alloc = LsnAllocator::new();
        let a = alloc.allocate().unwrap();
        let b = alloc.allocate().unwrap();
        let c = alloc.allocate().unwrap();
        assert!(a < b && b < c);
        assert_eq!(alloc.current(), c);
    }

    #[test]
    fn fresh_allocator_starts_at_one_without_durable_data() {
        let alloc = LsnAllocator::new();
        assert_eq!(alloc.peek_next(), 1);
        assert_eq!(alloc.allocate().unwrap(), 1);
    }

    #[test]
    fn ensure_above_recovers_from_wal_or_sst_watermark() {
        let alloc = LsnAllocator::new();
        // Simulate restart: durable WAL/SST max LSN was 500.
        alloc.ensure_above(500).unwrap();
        assert_eq!(alloc.peek_next(), 501);
        assert_eq!(alloc.allocate().unwrap(), 501);
        alloc.ensure_above(400).unwrap(); // already ahead — no-op
        assert_eq!(alloc.peek_next(), 502);
    }
}
