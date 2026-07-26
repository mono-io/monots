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

//! Global LSN allocation + WAL retention gate (progress pin injected by Stream).
//!
//! Capture (`crate::capture`) only notifies listeners. Progress cursors live in the Stream crate;
//! Stream installs a [`RetentionPin`] so storage can GC WAL safely.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use common::{FileAddEvent, LsnAllocator, Result, RETENTION_UNPINNED};

use crate::compaction::sst::SstMeta;
use crate::wal::bulk_load::ArcBulkLoadWal;
use crate::wal::{find_wal_file_for_lsn, next_wal_file_after};

/// Stream-provided pin: lowest acked LSN across consumers (or [`RETENTION_UNPINNED`]).
pub trait RetentionPin: Send + Sync {
    fn min_retained_lsn(&self) -> u64;
}

/// Always-unpinned gate (tests / no stream registered).
#[derive(Debug, Default)]
pub struct UnpinnedRetention;

impl RetentionPin for UnpinnedRetention {
    fn min_retained_lsn(&self) -> u64 {
        RETENTION_UNPINNED
    }
}

/// Decoupling seam between the storage engine and retention / LSN allocation.
pub trait WalRetention: Send + Sync {
    fn on_write(&self) -> Result<u64>;
    fn can_gc_lsn(&self, max_lsn: u64) -> bool;

    fn on_bulk_file(&self, meta: &SstMeta) -> Result<u64>;
    fn file_events_since(&self, lsn: u64) -> Vec<FileAddEvent>;
    fn gc_pinned_files(&self, live_sst_paths: &std::collections::HashSet<String>) -> Result<()>;

    fn find_memtable_for_lsn(&self, lsn: u64) -> Option<u64>;
    fn next_memtable_after(&self, memtable_id: u64) -> Option<u64>;

    fn allocate_lsn(&self) -> Result<u64> {
        Err(common::TsdbError::Storage(
            "LSN allocation requires replication".into(),
        ))
    }
}

pub struct TableReplication {
    table_data_dir: PathBuf,
    lsn: Arc<LsnAllocator>,
    retention: Arc<ArcSwapOption<Arc<dyn RetentionPin>>>,
    bulk_wal: ArcBulkLoadWal,
}

impl TableReplication {
    fn min_retained(&self) -> u64 {
        self.retention
            .load()
            .as_ref()
            .map(|p| p.min_retained_lsn())
            .unwrap_or(RETENTION_UNPINNED)
    }
}

impl WalRetention for TableReplication {
    fn on_write(&self) -> Result<u64> {
        self.lsn.allocate()
    }

    fn can_gc_lsn(&self, max_lsn: u64) -> bool {
        let retained = self.min_retained();
        if retained == RETENTION_UNPINNED {
            return true;
        }
        max_lsn <= retained
    }

    fn on_bulk_file(&self, meta: &SstMeta) -> Result<u64> {
        let lsn = if meta.has_lsn_bounds() {
            meta.max_lsn
        } else {
            self.lsn.allocate()?
        };
        self.bulk_wal.record(lsn, meta)?;
        Ok(lsn)
    }

    fn file_events_since(&self, lsn: u64) -> Vec<FileAddEvent> {
        self.bulk_wal.since(lsn)
    }

    fn gc_pinned_files(&self, live_sst_paths: &std::collections::HashSet<String>) -> Result<()> {
        let retained = self.min_retained();
        if retained == RETENTION_UNPINNED {
            return self.bulk_wal.gc_upto(u64::MAX, live_sst_paths);
        }
        self.bulk_wal.gc_upto(retained, live_sst_paths)
    }

    fn find_memtable_for_lsn(&self, lsn: u64) -> Option<u64> {
        find_wal_file_for_lsn(&self.table_data_dir, lsn)
            .ok()
            .flatten()
    }

    fn next_memtable_after(&self, memtable_id: u64) -> Option<u64> {
        next_wal_file_after(&self.table_data_dir, memtable_id)
            .ok()
            .flatten()
    }

    fn allocate_lsn(&self) -> Result<u64> {
        self.lsn.allocate()
    }
}

/// Engine-wide LSN allocator + optional Stream retention pin.
pub struct ReplicationManager {
    lsn: Arc<LsnAllocator>,
    retention: Arc<ArcSwapOption<Arc<dyn RetentionPin>>>,
}

impl ReplicationManager {
    pub fn open(_base_dir: &Path) -> Result<Self> {
        Ok(Self {
            lsn: Arc::new(LsnAllocator::new()),
            retention: Arc::new(ArcSwapOption::empty()),
        })
    }

    pub fn lsn(&self) -> &Arc<LsnAllocator> {
        &self.lsn
    }

    pub fn allocate_lsn(&self) -> Result<u64> {
        self.lsn.allocate()
    }

    /// Stream installs its [`CaptureProgressRegistry`](monots_stream) as the GC pin.
    pub fn set_retention_pin(&self, pin: Arc<dyn RetentionPin>) {
        self.retention.store(Some(Arc::new(pin)));
    }

    pub fn clear_retention_pin(&self) {
        self.retention.store(None);
    }

    pub fn min_retained_lsn(&self) -> u64 {
        self.retention
            .load()
            .as_ref()
            .map(|p| p.min_retained_lsn())
            .unwrap_or(RETENTION_UNPINNED)
    }

    pub fn table_replication(
        &self,
        _table_name: &str,
        table_data_dir: &Path,
        bulk_wal: ArcBulkLoadWal,
    ) -> Result<Arc<dyn WalRetention>> {
        Ok(Arc::new(TableReplication {
            table_data_dir: table_data_dir.to_path_buf(),
            lsn: self.lsn.clone(),
            retention: Arc::clone(&self.retention),
            bulk_wal,
        }))
    }
}
