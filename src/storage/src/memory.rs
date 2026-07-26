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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use tracing::{debug, error, info, warn};

thread_local! {
    static IN_RECLAIM: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Error message returned when the global memtable budget is exhausted.
pub const GLOBAL_MEMORY_LIMIT_EXCEEDED: &str = "global memory limit exceeded";

/// Default fraction of [`MemoryController::limit_bytes`] that triggers proactive largest-memtable flush.
pub const DEFAULT_GLOBAL_MEMORY_SOFT_THRESHOLD_RATIO: f64 = 0.5;

/// Max reclaim attempts between soft and hard cap before failing the write.
const MAX_HARD_RECLAIM_ATTEMPTS: usize = 3;

type ReclaimHandler = Box<dyn Fn(Option<&str>) -> bool + Send + Sync>;

/// Global memory budget shared by all memtables in one engine.
pub struct MemoryController {
    limit_bytes: usize,
    soft_threshold_bytes: usize,
    used_bytes: AtomicUsize,
    reclaim_handler: OnceLock<ReclaimHandler>,
}

impl MemoryController {
    pub fn new(limit_bytes: usize) -> Self {
        Self::with_soft_threshold(limit_bytes, DEFAULT_GLOBAL_MEMORY_SOFT_THRESHOLD_RATIO)
    }

    pub fn with_soft_threshold(limit_bytes: usize, soft_threshold_ratio: f64) -> Self {
        let ratio = soft_threshold_ratio.clamp(0.0, 1.0);
        let soft_threshold_bytes = ((limit_bytes as f64) * ratio) as usize;
        Self {
            limit_bytes,
            soft_threshold_bytes,
            used_bytes: AtomicUsize::new(0),
            reclaim_handler: OnceLock::new(),
        }
    }

    /// Register reclaim callback once (engine startup). Later calls are ignored.
    pub fn set_reclaim_handler(
        &self,
        handler: impl Fn(Option<&str>) -> bool + Send + Sync + 'static,
    ) {
        if self.reclaim_handler.set(Box::new(handler)).is_err() {
            warn!("Reclaim handler was already set; ignoring subsequent initialization");
        }
    }

    /// Charge bytes unconditionally, even past the hard cap.
    pub fn reserve_unchecked(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn try_reserve(&self, bytes: usize) -> bool {
        if bytes == 0 {
            return true;
        }
        self.used_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if current.saturating_add(bytes) > self.limit_bytes {
                    None
                } else {
                    Some(current + bytes)
                }
            })
            .is_ok()
    }

    /// Soft-threshold proactive flush; hard-cap reclaim loop before rejecting the write.
    pub fn try_reserve_with_reclaim(&self, bytes: usize, writing_table: Option<&str>) -> bool {
        if self.at_or_over_limit() {
            return false;
        }

        if self.soft_threshold_bytes > 0 && self.at_or_over_soft_threshold() {
            self.invoke_reclaim(writing_table);
        }

        if self.try_reserve(bytes) {
            return true;
        }

        let in_reclaim = IN_RECLAIM.with(|flag| flag.get());
        if self.at_or_over_limit() || in_reclaim {
            return false;
        }

        for attempt in 1..=MAX_HARD_RECLAIM_ATTEMPTS {
            debug!(
                attempt,
                "Hard memory limit reached, attempting synchronous reclaim"
            );
            let used_before = self.used_bytes();
            if !self.invoke_reclaim(writing_table) {
                warn!("Reclaim handler could not free any memory");
                break;
            }
            if self.try_reserve(bytes) {
                return true;
            }
            if self.used_bytes() >= used_before {
                break;
            }
        }

        error!(
            limit = self.limit_bytes,
            used = self.used_bytes(),
            "Failed to reserve memory after max hard reclaim attempts"
        );
        false
    }

    fn invoke_reclaim(&self, writing_table: Option<&str>) -> bool {
        let Some(handler) = self.reclaim_handler.get() else {
            return false;
        };
        let Some(_guard) = ReclaimGuard::enter() else {
            return false;
        };

        let start_used = self.used_bytes();
        let reclaimed = handler(writing_table);
        if reclaimed {
            info!(
                freed = start_used.saturating_sub(self.used_bytes()),
                "Successfully reclaimed global memtable memory"
            );
        }
        reclaimed
    }

    pub fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        // saturating_sub avoids wrapping to usize::MAX on over-release bugs.
        let old = self
            .used_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes))
            })
            .unwrap_or_else(|v| v);
        if old < bytes {
            error!(
                old_bytes = old,
                release_bytes = bytes,
                "Memory accounting underflow; clamped used_bytes to 0"
            );
        }
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes.load(Ordering::Relaxed)
    }

    pub fn limit_bytes(&self) -> usize {
        self.limit_bytes
    }

    pub fn soft_threshold_bytes(&self) -> usize {
        self.soft_threshold_bytes
    }

    pub fn at_or_over_soft_threshold(&self) -> bool {
        self.soft_threshold_bytes > 0 && self.used_bytes() >= self.soft_threshold_bytes
    }

    pub fn maybe_reclaim_at_soft_threshold(&self, writing_table: Option<&str>) {
        if self.at_or_over_soft_threshold() {
            self.invoke_reclaim(writing_table);
        }
    }

    pub fn maybe_reclaim_under_pressure(&self, writing_table: Option<&str>) {
        if self.at_or_over_soft_threshold() || self.at_or_over_limit() {
            self.invoke_reclaim(writing_table);
        }
    }

    pub fn at_or_over_limit(&self) -> bool {
        self.used_bytes() >= self.limit_bytes
    }

    pub fn ensure_write_allowed(&self) -> common::Result<()> {
        if self.at_or_over_limit() {
            return Err(self.memory_limit_error());
        }
        Ok(())
    }

    pub fn memory_limit_error(&self) -> common::TsdbError {
        common::TsdbError::memory_limit_exceeded(self.used_bytes(), self.limit_bytes())
    }
}

/// Clears `IN_RECLAIM` on drop, including after panic inside the reclaim handler.
struct ReclaimGuard;

impl ReclaimGuard {
    fn enter() -> Option<Self> {
        IN_RECLAIM.with(|flag| {
            if flag.get() {
                None
            } else {
                flag.set(true);
                Some(Self)
            }
        })
    }
}

impl Drop for ReclaimGuard {
    fn drop(&mut self) {
        IN_RECLAIM.with(|flag| flag.set(false));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn reclaim_handler_frees_budget() {
        let memory = Arc::new(MemoryController::new(100));
        let mem_for_handler = memory.clone();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_handler = attempts.clone();
        memory.set_reclaim_handler(move |_prefer| {
            attempts_for_handler.fetch_add(1, Ordering::Relaxed);
            mem_for_handler.release(60);
            true
        });

        assert!(memory.try_reserve(80));
        assert!(!memory.try_reserve(30));
        assert!(memory.try_reserve_with_reclaim(30, None));
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
        assert_eq!(memory.used_bytes(), 50);
    }

    #[test]
    fn soft_threshold_triggers_proactive_reclaim() {
        let memory = Arc::new(MemoryController::with_soft_threshold(100, 0.5));
        let proactive = Arc::new(AtomicUsize::new(0));
        let proactive_for_handler = proactive.clone();
        memory.set_reclaim_handler(move |_prefer| {
            proactive_for_handler.fetch_add(1, Ordering::Relaxed);
            false
        });

        assert_eq!(memory.soft_threshold_bytes(), 50);
        assert!(memory.try_reserve(50));
        assert!(memory.at_or_over_soft_threshold());
        assert!(memory.try_reserve_with_reclaim(10, None));
        assert_eq!(proactive.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn hard_limit_blocks_writes_immediately() {
        let memory = Arc::new(MemoryController::new(100));
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_handler = attempts.clone();
        memory.set_reclaim_handler(move |_prefer| {
            attempts_for_handler.fetch_add(1, Ordering::Relaxed);
            false
        });

        assert!(memory.try_reserve(100));
        assert!(memory.at_or_over_limit());
        assert!(!memory.try_reserve_with_reclaim(1, None));
        assert_eq!(attempts.load(Ordering::Relaxed), 0);
        assert_eq!(memory.used_bytes(), 100);
        let err = memory.ensure_write_allowed().unwrap_err();
        assert!(err.is_memory_limit_exceeded());
    }

    #[test]
    fn zero_soft_threshold_disables_proactive_reclaim() {
        let memory = Arc::new(MemoryController::with_soft_threshold(100, 0.0));
        let proactive = Arc::new(AtomicUsize::new(0));
        let proactive_for_handler = proactive.clone();
        memory.set_reclaim_handler(move |_prefer| {
            proactive_for_handler.fetch_add(1, Ordering::Relaxed);
            false
        });

        assert!(memory.try_reserve(90));
        assert!(memory.try_reserve_with_reclaim(5, None));
        assert_eq!(proactive.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn nested_reclaim_does_not_deadlock() {
        let memory = Arc::new(MemoryController::with_soft_threshold(100, 0.5));
        let memory_for_handler = memory.clone();
        memory.set_reclaim_handler(move |_prefer| {
            memory_for_handler.try_reserve_with_reclaim(1, None);
            memory_for_handler.release(40);
            true
        });

        assert!(memory.try_reserve(80));
        assert!(memory.try_reserve_with_reclaim(30, None));
        assert_eq!(memory.used_bytes(), 71);
    }

    #[test]
    fn release_underflow_clamps_to_zero() {
        let memory = MemoryController::new(100);
        memory.try_reserve(10);
        memory.release(50);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn reclaim_guard_resets_after_panic() {
        let memory = Arc::new(MemoryController::new(100));
        let mem_for_handler = memory.clone();
        let panicked = Arc::new(AtomicUsize::new(0));
        let panicked_for_handler = panicked.clone();
        memory.set_reclaim_handler(move |_prefer| {
            let n = panicked_for_handler.fetch_add(1, Ordering::Relaxed);
            if n == 0 {
                panic!("boom");
            }
            mem_for_handler.release(40);
            true
        });

        assert!(memory.try_reserve(80));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = memory.try_reserve_with_reclaim(30, None);
        }));
        assert!(result.is_err());
        assert_eq!(panicked.load(Ordering::Relaxed), 1);

        assert!(memory.try_reserve_with_reclaim(30, None));
        assert_eq!(panicked.load(Ordering::Relaxed), 2);
        assert_eq!(memory.used_bytes(), 70);
    }
}
