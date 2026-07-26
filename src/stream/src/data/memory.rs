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

//! Stream CDC Arrow memory budget.
//!
//! ```text
//! StreamArrowMemoryPool  (process-wide)
//!   └── StreamArrowBlock (per stream; Drop returns capacity to the pool)
//!         └── ArrowCharge / SharedArrowCharge (per Insert event; Drop refunds the block)
//! ```
//!
//! Lifecycle: charge is acquired when a Resident Insert is created, travels with the
//! [`crate::model::event::DataEvent`] through Source → Dispatcher → Sink, and is
//! refunded when the event is dropped (Sink finished / Flush degrade / etc.).
//!
//! When acquire fails, Source keeps [`crate::model::event::InsertArrow::Deferred`].

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use tracing::{debug, warn};

/// Default process-wide CDC Arrow pool (256 MiB).
pub const DEFAULT_STREAM_ARROW_POOL_BYTES: usize = 256 * 1024 * 1024;
/// Default per-stream Block size requested from the pool (32 MiB).
pub const DEFAULT_STREAM_ARROW_BLOCK_BYTES: usize = 32 * 1024 * 1024;

/// Approximate Arrow payload size for budgeting.
pub fn record_batches_memory_size(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::get_array_memory_size).sum()
}

/// Process-wide pool of Arrow bytes reserved for Stream Source buffers.
pub struct StreamArrowMemoryPool {
    limit_bytes: usize,
    used_bytes: AtomicUsize,
}

impl StreamArrowMemoryPool {
    pub fn new(limit_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            limit_bytes: limit_bytes.max(1),
            used_bytes: AtomicUsize::new(0),
        })
    }

    pub fn limit_bytes(&self) -> usize {
        self.limit_bytes
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes.load(Ordering::Relaxed)
    }

    pub fn available_bytes(&self) -> usize {
        self.limit_bytes
            .saturating_sub(self.used_bytes.load(Ordering::Relaxed))
    }

    /// Allocate a per-stream Block. If `request` does not fit, grant remaining capacity
    /// (possibly `0` → Source always degrades Inserts to WAL-only).
    pub fn alloc_block(self: &Arc<Self>, request: usize) -> Arc<StreamArrowBlock> {
        let want = request.max(0);
        let granted = self.reserve_capacity(want);
        if granted < want {
            warn!(
                requested = want,
                granted,
                pool_limit = self.limit_bytes,
                pool_used = self.used_bytes(),
                "stream arrow block undersized; Insert may degrade to WAL load"
            );
        }
        debug!(
            capacity = granted,
            pool_used = self.used_bytes(),
            "stream arrow block allocated"
        );
        Arc::new(StreamArrowBlock {
            pool: Arc::clone(self),
            capacity: granted,
            charged: AtomicUsize::new(0),
        })
    }

    fn reserve_capacity(&self, want: usize) -> usize {
        if want == 0 {
            return 0;
        }
        loop {
            let used = self.used_bytes.load(Ordering::Relaxed);
            let avail = self.limit_bytes.saturating_sub(used);
            if avail == 0 {
                return 0;
            }
            let grant = want.min(avail);
            if self
                .used_bytes
                .compare_exchange_weak(used, used + grant, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return grant;
            }
        }
    }

    fn release_capacity(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let old = self
            .used_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(bytes))
            })
            .unwrap_or_else(|v| v);
        if old < bytes {
            warn!(
                old_bytes = old,
                release_bytes = bytes,
                "stream arrow pool capacity underflow; clamped to 0"
            );
        }
    }
}

/// Per-stream Arrow budget. Dropping the last [`Arc`] returns `capacity` to the pool.
pub struct StreamArrowBlock {
    pool: Arc<StreamArrowMemoryPool>,
    capacity: usize,
    charged: AtomicUsize,
}

impl StreamArrowBlock {
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn charged_bytes(&self) -> usize {
        self.charged.load(Ordering::Relaxed)
    }

    pub fn available_bytes(&self) -> usize {
        self.capacity
            .saturating_sub(self.charged.load(Ordering::Relaxed))
    }

    /// Try to charge `bytes` against this block. `0` always succeeds.
    pub fn try_charge(&self, bytes: usize) -> bool {
        if bytes == 0 {
            return true;
        }
        if self.capacity == 0 {
            return false;
        }
        loop {
            let cur = self.charged.load(Ordering::Relaxed);
            if cur.saturating_add(bytes) > self.capacity {
                return false;
            }
            if self
                .charged
                .compare_exchange_weak(cur, cur + bytes, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    pub fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let old = self
            .charged
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(bytes))
            })
            .unwrap_or_else(|v| v);
        if old < bytes {
            warn!(
                old_bytes = old,
                release_bytes = bytes,
                "stream arrow block charge underflow; clamped to 0"
            );
        }
    }

    /// Acquire an RAII charge for `bytes`, or `None` if the block is full.
    pub fn try_acquire(self: &Arc<Self>, bytes: usize) -> Option<SharedArrowCharge> {
        if self.try_charge(bytes) {
            Some(SharedArrowCharge::new(Arc::clone(self), bytes))
        } else {
            None
        }
    }
}

impl Drop for StreamArrowBlock {
    fn drop(&mut self) {
        let leftover = self.charged.swap(0, Ordering::AcqRel);
        if leftover > 0 {
            warn!(
                leftover,
                capacity = self.capacity,
                "stream arrow block dropped with charged Arrow still held"
            );
        }
        self.pool.release_capacity(self.capacity);
        debug!(
            returned = self.capacity,
            pool_used = self.pool.used_bytes(),
            "stream arrow block returned to pool"
        );
    }
}

/// Exclusive charge against a [`StreamArrowBlock`]. Drop refunds `bytes` to the block.
struct ArrowCharge {
    block: Arc<StreamArrowBlock>,
    bytes: usize,
}

impl Drop for ArrowCharge {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.block.release(self.bytes);
            self.bytes = 0;
        }
    }
}

/// Shareable charge ticket for a Resident Insert. Clones share one refund (last drop wins).
///
/// Travels with the event until Sink consumption / Source degrade drops the event.
#[derive(Clone)]
pub struct SharedArrowCharge(Arc<ArrowCharge>);

impl SharedArrowCharge {
    fn new(block: Arc<StreamArrowBlock>, bytes: usize) -> Self {
        Self(Arc::new(ArrowCharge { block, bytes }))
    }

    pub fn bytes(&self) -> usize {
        self.0.bytes
    }
}

impl std::fmt::Debug for SharedArrowCharge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedArrowCharge")
            .field("bytes", &self.0.bytes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_drop_returns_capacity() {
        let pool = StreamArrowMemoryPool::new(1000);
        {
            let b = pool.alloc_block(400);
            assert_eq!(b.capacity(), 400);
            assert_eq!(pool.used_bytes(), 400);
            let c = b.try_acquire(100).unwrap();
            assert_eq!(b.charged_bytes(), 100);
            drop(c);
            assert_eq!(b.charged_bytes(), 0);
        }
        assert_eq!(pool.used_bytes(), 0);
    }

    #[test]
    fn shared_charge_refunds_once() {
        let pool = StreamArrowMemoryPool::new(1000);
        let b = pool.alloc_block(400);
        let c = b.try_acquire(50).unwrap();
        let c2 = c.clone();
        assert_eq!(b.charged_bytes(), 50);
        drop(c);
        assert_eq!(b.charged_bytes(), 50);
        drop(c2);
        assert_eq!(b.charged_bytes(), 0);
    }

    #[test]
    fn undersized_when_pool_tight() {
        let pool = StreamArrowMemoryPool::new(100);
        let a = pool.alloc_block(80);
        let b = pool.alloc_block(80);
        assert_eq!(a.capacity(), 80);
        assert_eq!(b.capacity(), 20);
        assert!(b.try_acquire(21).is_none());
        assert!(b.try_acquire(20).is_some());
    }

    #[test]
    fn zero_capacity_always_rejects() {
        let pool = StreamArrowMemoryPool::new(10);
        let _a = pool.alloc_block(10);
        let b = pool.alloc_block(10);
        assert_eq!(b.capacity(), 0);
        assert!(b.try_acquire(1).is_none());
    }
}
