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

pub mod file;

use std::path::PathBuf;

pub use file::{AppConfig, ResolvedServerConfig};
use std::sync::Arc;

const DEFAULT_SYNC_MAX_PENDING: usize = 1024;

/// Engine-level configuration for edge deployment.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub memtable_max_bytes: usize,
    /// In-memory micro-batch row capacity for [`BatchBuffer`] / MemTable chunks.
    ///
    /// Controls how many rows are coalesced before sealing a frozen `RecordBatch`.
    /// Default [`monots_storage::DEFAULT_MEMTABLE_BATCH_MAX_ROWS`] (1024). Set via
    /// `storage.memtable_batch_max_rows` in YAML (512 is fine for lower latency).
    pub memtable_batch_max_rows: usize,
    /// Byte threshold for finishing in-memory RecordBatch chunks.
    pub memtable_batch_max_bytes: usize,
    pub compaction_threshold_bytes: u64,
    pub compaction_interval_secs: u64,
    /// File-selection strategy for background compaction (always picks a contiguous run).
    pub compaction_strategy: monots_storage::CompactionStrategy,
    /// Max number of contiguous files one compaction merge collapses into a single SST.
    pub compaction_max_merge_files: usize,
    /// Global cap on merge jobs running concurrently across all tables (IO throttle).
    pub compaction_max_concurrent_jobs: usize,
    pub global_memory_limit_bytes: usize,
    /// Fraction of `global_memory_limit_bytes` (0.0–1.0) that triggers proactive largest-memtable flush.
    pub global_memory_soft_threshold_ratio: f64,
    /// DataFusion query execution memory pool (separate from memtable budget).
    pub query_memory_limit_bytes: usize,
    /// In-memory cap for metadata caches (schemas + manifests).
    pub metadata_memory_limit_bytes: usize,
    pub sync_queue_size: usize,
    pub sync_max_pending: usize,
    /// Fallback poll interval when WAL push notify is idle (ms).
    pub sync_realtime_poll_ms: u64,
    /// Sealed WAL frame cache for CDC catch-up.
    pub sync_wal_load_cache_max_bytes: usize,
    /// WAL write durability: async (default) or sync per batch.
    pub wal_durability: monots_storage::WalDurabilityMode,
    /// Per-table WAL worker micro-batch flush threshold (bytes).
    pub wal_micro_batch_max_bytes: usize,
    /// Max on-disk size of one WAL segment before rotating to a new file (default 100 MiB).
    pub wal_segment_max_bytes: u64,
    /// Max size of one WAL block / frame (header + body, default 5 MiB).
    pub wal_block_max_bytes: usize,
    /// Global WAL backlog cap shared across all tables (bytes).
    pub wal_global_backlog_max_bytes: usize,
    /// Per-table WAL backlog cap within the global pool (bytes).
    pub wal_table_backlog_max_bytes: usize,
    /// Rows per output window when flush must re-sort / dedupe (default 8192).
    pub flush_window_rows: usize,
    /// Parquet row-group size for SST writes (default 8192).
    pub sst_max_row_group_size: usize,
    /// Enter read-only when free/total disk space drops to this ratio or below.
    pub disk_min_free_ratio: f64,
}

impl EngineConfig {
    pub fn wal_writer_options_for_table(
        &self,
        table_name: &str,
        backlog: Arc<monots_storage::WalBacklogBudget>,
        table_backlog: Arc<monots_storage::TableBacklog>,
        wal_hub: Arc<monots_storage::WalAppendHub>,
    ) -> monots_storage::WalWriterOptions {
        monots_storage::WalWriterOptions {
            durability: self.wal_durability,
            micro_batch_max_bytes: self.wal_micro_batch_max_bytes,
            segment_max_bytes: self.wal_segment_max_bytes,
            block_max_bytes: self.wal_block_max_bytes,
            backlog,
            table_backlog,
            table_name: None,
            notify: None,
        }
        .with_notify(table_name, wal_hub)
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            memtable_max_bytes: 64 * 1024 * 1024,
            memtable_batch_max_rows: monots_storage::DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
            memtable_batch_max_bytes: monots_storage::DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
            compaction_threshold_bytes: 128 * 1024 * 1024,
            compaction_interval_secs: 60,
            compaction_strategy: monots_storage::CompactionStrategy::SizeTiered,
            compaction_max_merge_files: monots_storage::DEFAULT_COMPACTION_MAX_MERGE_FILES,
            compaction_max_concurrent_jobs: monots_storage::DEFAULT_COMPACTION_MAX_CONCURRENT_JOBS,
            global_memory_limit_bytes: 512 * 1024 * 1024,
            global_memory_soft_threshold_ratio:
                monots_storage::DEFAULT_GLOBAL_MEMORY_SOFT_THRESHOLD_RATIO,
            query_memory_limit_bytes: 128 * 1024 * 1024,
            metadata_memory_limit_bytes: 16 * 1024 * 1024,
            sync_queue_size: 1000,
            sync_max_pending: DEFAULT_SYNC_MAX_PENDING,
            sync_realtime_poll_ms: 50,
            sync_wal_load_cache_max_bytes: monots_storage::DEFAULT_WAL_LOAD_CACHE_MAX_BYTES,
            wal_durability: monots_storage::WalDurabilityMode::Async,
            wal_micro_batch_max_bytes: monots_storage::DEFAULT_WAL_MICRO_BATCH_MAX_BYTES,
            wal_segment_max_bytes: monots_storage::DEFAULT_WAL_SEGMENT_MAX_BYTES,
            wal_block_max_bytes: monots_storage::DEFAULT_WAL_BLOCK_MAX_BYTES,
            wal_global_backlog_max_bytes: monots_storage::DEFAULT_WAL_GLOBAL_BACKLOG_MAX_BYTES,
            wal_table_backlog_max_bytes: monots_storage::DEFAULT_WAL_TABLE_BACKLOG_MAX_BYTES,
            flush_window_rows: monots_storage::FLUSH_WINDOW_ROWS,
            sst_max_row_group_size: 8_192,
            disk_min_free_ratio: monots_storage::DEFAULT_DISK_MIN_FREE_RATIO,
        }
    }
}
