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

//! Capture progress registry: durable LSN cursors that pin WAL / shadow retention.
//!
//! Each entry is a named [`CaptureProgress`] (`acked_lsn`) for one capture consumer
//! (typically `{stream}::log::{table}`). Contract:
//! **the storage engine must not GC any WAL / shadow data whose LSN is still above some
//! registered progress cursor.**
//!
//! Progress is persisted through a pluggable [`CommitStore`], so consumers resume from the
//! last committed LSN after restart.

use std::collections::HashMap;
use std::sync::Arc;

use common::{CaptureProgress, CommitDurability, Result, RETENTION_UNPINNED};
use dashmap::DashMap;

use super::wal::CommitStore;

/// Process-wide registry of capture progress cursors, backed by a durable [`CommitStore`].
pub struct CaptureProgressRegistry {
    progresses: DashMap<String, u64>,
    commit: Arc<dyn CommitStore>,
    default_durability: CommitDurability,
}

impl CaptureProgressRegistry {
    /// Open the registry, restoring previously committed capture progress from the commit store.
    pub fn open(
        commit: Arc<dyn CommitStore>,
        default_durability: CommitDurability,
    ) -> Result<Self> {
        let progresses = DashMap::new();
        for (progress_id, lsn) in commit.load()? {
            progresses.insert(progress_id, lsn);
        }
        Ok(Self {
            progresses,
            commit,
            default_durability,
        })
    }

    /// Register a capture progress id (idempotent). A fresh cursor starts at `start_lsn`
    /// (its `acked_lsn`), meaning everything strictly above `start_lsn` is retained until advance.
    pub fn register(&self, progress_id: &str, start_lsn: u64) -> Result<()> {
        if !self.progresses.contains_key(progress_id) {
            self.progresses.insert(progress_id.to_string(), start_lsn);
            self.commit
                .commit(progress_id, start_lsn, self.default_durability)?;
        }
        Ok(())
    }

    pub fn get(&self, progress_id: &str) -> Option<CaptureProgress> {
        self.progresses
            .get(progress_id)
            .map(|v| CaptureProgress { acked_lsn: *v })
    }

    /// Durably advance committed LSN. Monotonic: a lower LSN is ignored.
    pub fn advance(&self, progress_id: &str, lsn: u64) -> Result<()> {
        self.advance_with(progress_id, lsn, self.default_durability)
    }

    /// Advance with an explicit durability (Sync = fsync before returning, Async = buffered).
    pub fn advance_with(
        &self,
        progress_id: &str,
        lsn: u64,
        durability: CommitDurability,
    ) -> Result<()> {
        let mut entry = self.progresses.entry(progress_id.to_string()).or_insert(0);
        if lsn > *entry {
            *entry = lsn;
            drop(entry);
            self.commit.commit(progress_id, lsn, durability)?;
        }
        Ok(())
    }

    /// Drop a capture progress id and forget its cursor (no longer pins retention).
    pub fn remove(&self, progress_id: &str) -> Result<()> {
        self.progresses.remove(progress_id);
        self.commit.remove(progress_id)
    }

    /// Lowest committed LSN across all live capture progresses. Data with `max_lsn <=` this
    /// is safe to GC. Returns [`RETENTION_UNPINNED`] when none exist.
    pub fn min_retained_lsn(&self) -> u64 {
        let mut min = RETENTION_UNPINNED;
        for entry in self.progresses.iter() {
            min = min.min(*entry.value());
        }
        min
    }

    pub fn snapshot(&self) -> HashMap<String, u64> {
        self.progresses
            .iter()
            .map(|e| (e.key().clone(), *e.value()))
            .collect()
    }

    pub fn flush(&self) -> Result<()> {
        self.commit.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::super::wal::WalCommitLog;
    use super::*;

    fn registry(dir: &std::path::Path) -> CaptureProgressRegistry {
        let commit = Arc::new(WalCommitLog::open(dir.join("commit.wal")).unwrap());
        CaptureProgressRegistry::open(commit, CommitDurability::Sync).unwrap()
    }

    #[test]
    fn min_retained_reflects_slowest_progress() {
        let dir = tempfile::tempdir().unwrap();
        let reg = registry(dir.path());
        assert_eq!(reg.min_retained_lsn(), RETENTION_UNPINNED);

        reg.register("fast", 0).unwrap();
        reg.register("slow", 0).unwrap();
        reg.advance("fast", 100).unwrap();
        reg.advance("slow", 30).unwrap();
        assert_eq!(
            reg.min_retained_lsn(),
            30,
            "slowest capture progress pins retention"
        );

        reg.advance("slow", 80).unwrap();
        assert_eq!(reg.min_retained_lsn(), 80);
    }

    #[test]
    fn advance_is_monotonic_and_durable() {
        let dir = tempfile::tempdir().unwrap();
        let reg = registry(dir.path());
        reg.register("c", 0).unwrap();
        reg.advance("c", 50).unwrap();
        reg.advance("c", 40).unwrap();
        assert_eq!(reg.get("c").unwrap().acked_lsn, 50);

        let reg2 = registry(dir.path());
        assert_eq!(reg2.get("c").unwrap().acked_lsn, 50);
    }

    #[test]
    fn removing_progress_releases_retention() {
        let dir = tempfile::tempdir().unwrap();
        let reg = registry(dir.path());
        reg.register("a", 0).unwrap();
        reg.advance("a", 10).unwrap();
        assert_eq!(reg.min_retained_lsn(), 10);
        reg.remove("a").unwrap();
        assert_eq!(reg.min_retained_lsn(), RETENTION_UNPINNED);
    }
}
