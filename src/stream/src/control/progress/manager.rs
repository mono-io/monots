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

//! Progress Manager: bridges Sink commits with LSM-Tree WAL/SST GC.

use std::path::Path;
use std::sync::{Arc, OnceLock};

use common::{CommitDurability, Result, TsdbError, RETENTION_UNPINNED};
use monots_storage::{LsmEngine, RetentionPin};
use tracing::{debug, info, warn};

use super::registry::CaptureProgressRegistry;
use super::wal::{CommitStore, WalCommitLog};

const DIR_STREAM: &str = "stream";
const DIR_REPLICATION_LEGACY: &str = "replication";
const FILE_COMMIT_WAL: &str = "commit.wal";

/// Payload from SinkWorker after a successful 2PC commit.
#[derive(Debug, Clone)]
pub struct SinkCommitted {
    pub stream: String,
    pub table: String,
    pub lsn: u64,
    pub files: u64,
}

/// Tracks capture progress and triggers storage GC after durable Sink commits.
pub struct ProgressManager {
    progress: Arc<CaptureProgressRegistry>,
    /// Bound once at boot; lock-free reads on the commit hot path.
    engine: OnceLock<Arc<LsmEngine>>,
    durability: CommitDurability,
}

impl ProgressManager {
    pub fn open(base_dir: &Path, durability: CommitDurability) -> Result<Arc<Self>> {
        let root = base_dir.join(DIR_STREAM);
        Self::migrate_legacy_dir(base_dir, &root)?;
        std::fs::create_dir_all(&root).map_err(|e| {
            TsdbError::Storage(format!("Failed to create stream progress dir: {e}"))
        })?;

        let commit_store: Arc<dyn CommitStore> =
            Arc::new(WalCommitLog::open(root.join(FILE_COMMIT_WAL))?);
        let progress = Arc::new(CaptureProgressRegistry::open(commit_store, durability)?);

        Ok(Arc::new(Self {
            progress,
            engine: OnceLock::new(),
            durability,
        }))
    }

    /// Bind LSM engine once; subsequent calls fail.
    pub fn bind_engine(&self, engine: Arc<LsmEngine>) -> Result<()> {
        self.engine.set(engine).map_err(|_| {
            TsdbError::Storage("LSM Engine already bound to ProgressManager".into())
        })?;
        info!("LSM Engine successfully bound to ProgressManager");
        Ok(())
    }

    pub fn progress(&self) -> &Arc<CaptureProgressRegistry> {
        &self.progress
    }

    pub fn min_retained_lsn(&self) -> u64 {
        self.progress.min_retained_lsn()
    }

    pub fn flush(&self) -> Result<()> {
        self.progress.flush()
    }

    pub fn on_sink_committed(&self, stream: &str, table: &str, lsn: u64) -> Result<()> {
        let progress_id = super::capture_progress_id(stream, table);
        self.progress
            .advance_with(&progress_id, lsn, self.durability)?;

        if let Some(engine) = self.engine.get() {
            if let Some(t) = engine.get_table(table) {
                if let Err(e) = t.gc_retained_wal() {
                    warn!(
                        stream,
                        table,
                        error = %e,
                        "Failed to GC retained WAL after sink commit"
                    );
                }
                if let Err(e) = t.gc_pinned_files() {
                    warn!(
                        stream,
                        table,
                        error = %e,
                        "Failed to GC pinned SST files after sink commit"
                    );
                }
            } else {
                debug!(
                    stream,
                    table, "Table not found in engine during GC trigger; possibly dropped"
                );
            }
        }

        Ok(())
    }

    pub fn as_retention_pin(&self) -> Arc<dyn RetentionPin> {
        Arc::new(RegistryRetentionPin {
            registry: Arc::clone(&self.progress),
        })
    }

    fn migrate_legacy_dir(base_dir: &Path, stream_root: &Path) -> Result<()> {
        let legacy = base_dir.join(DIR_REPLICATION_LEGACY);
        if !legacy.exists() || stream_root.exists() {
            return Ok(());
        }
        std::fs::rename(&legacy, stream_root).map_err(|e| {
            TsdbError::Storage(format!(
                "Failed to migrate legacy replication directory {} to {}: {}",
                legacy.display(),
                stream_root.display(),
                e
            ))
        })?;
        info!(
            from = %legacy.display(),
            to = %stream_root.display(),
            "Successfully migrated stream progress directory from legacy layout"
        );
        Ok(())
    }
}

/// Bridges [`CaptureProgressRegistry`] to storage [`RetentionPin`] without orphan impls.
struct RegistryRetentionPin {
    registry: Arc<CaptureProgressRegistry>,
}

impl RetentionPin for RegistryRetentionPin {
    fn min_retained_lsn(&self) -> u64 {
        let v = self.registry.min_retained_lsn();
        if v == RETENTION_UNPINNED {
            RETENTION_UNPINNED
        } else {
            v
        }
    }
}
