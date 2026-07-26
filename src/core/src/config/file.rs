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

use common::{LogConfig, Result, TsdbError};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

use super::EngineConfig;

/// Top-level YAML configuration (`conf/config.yaml`).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub service: ServiceConfig,
    pub logging: LogConfig,
    pub storage: StorageYamlConfig,
    pub sync: SyncYamlConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServiceConfig {
    pub host: String,
    pub port: u16,
    pub data_dir: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StorageYamlConfig {
    pub memtable_max_bytes: usize,
    /// MemTable / BatchBuffer micro-batch row capacity (default 1024).
    ///
    /// Smaller → lower seal latency / smaller Arrow chunks; larger → fewer sealed batches.
    /// Typical range: 512–4096.
    pub memtable_batch_max_rows: usize,
    pub memtable_batch_max_bytes: usize,
    pub global_memory_limit_bytes: usize,
    /// Proactive flush when global memtable usage reaches this fraction of the hard cap (0.0–1.0).
    pub global_memory_soft_threshold_ratio: f64,
    pub metadata_memory_limit_bytes: usize,
    pub compaction_threshold_bytes: u64,
    pub compaction_interval_secs: u64,
    /// Compaction file-selection strategy: `size_tiered` (default), `file_count`, or `overlap`.
    pub compaction_strategy: String,
    /// Max number of contiguous files one compaction merge collapses into a single SST.
    pub compaction_max_merge_files: usize,
    /// Global cap on merge jobs running concurrently across all tables (IO throttle).
    pub compaction_max_concurrent_jobs: usize,
    /// Per-table WAL micro-batch flush threshold (bytes).
    pub wal_micro_batch_max_bytes: usize,
    /// Max on-disk size of one WAL segment before rotating (bytes, default 100 MiB).
    pub wal_segment_max_bytes: u64,
    /// Max size of one WAL block / frame (header + body, default 5 MiB).
    pub wal_block_max_bytes: usize,
    /// Global WAL backlog cap shared across all tables (bytes).
    pub wal_global_backlog_max_bytes: usize,
    /// Per-table WAL backlog cap within the global pool (bytes).
    pub wal_table_backlog_max_bytes: usize,
    /// Rows per flush output window when sorted-path dedupe is required.
    pub flush_window_rows: usize,
    /// Parquet max row-group size for SST writes.
    pub sst_max_row_group_size: usize,
    /// Enter read-only when free/total disk ≤ this ratio (0.0–1.0; default 0.10).
    pub disk_min_free_ratio: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SyncYamlConfig {
    pub queue_size: usize,
    pub max_pending_acks: usize,
    /// Fallback WAL tail poll interval when push notify is idle (milliseconds).
    pub realtime_poll_ms: u64,
    /// Sealed WAL decode cache for CDC catch-up (bytes).
    pub wal_load_cache_max_bytes: usize,
    pub lake_endpoint: String,
    pub lake_bucket: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            service: ServiceConfig::default(),
            logging: LogConfig::default(),
            storage: StorageYamlConfig::default(),
            sync: SyncYamlConfig::default(),
        }
    }
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".into(),
            port: 50051,
            data_dir: "data".into(),
            username: "admin".into(),
            password: "admin".into(),
        }
    }
}

impl Default for StorageYamlConfig {
    fn default() -> Self {
        let engine = EngineConfig::default();
        Self {
            memtable_max_bytes: engine.memtable_max_bytes,
            memtable_batch_max_rows: engine.memtable_batch_max_rows,
            memtable_batch_max_bytes: engine.memtable_batch_max_bytes,
            global_memory_limit_bytes: engine.global_memory_limit_bytes,
            global_memory_soft_threshold_ratio: engine.global_memory_soft_threshold_ratio,
            metadata_memory_limit_bytes: engine.metadata_memory_limit_bytes,
            compaction_threshold_bytes: engine.compaction_threshold_bytes,
            compaction_interval_secs: engine.compaction_interval_secs,
            compaction_strategy: "size_tiered".to_string(),
            compaction_max_merge_files: engine.compaction_max_merge_files,
            compaction_max_concurrent_jobs: engine.compaction_max_concurrent_jobs,
            wal_micro_batch_max_bytes: engine.wal_micro_batch_max_bytes,
            wal_segment_max_bytes: engine.wal_segment_max_bytes,
            wal_block_max_bytes: engine.wal_block_max_bytes,
            wal_global_backlog_max_bytes: engine.wal_global_backlog_max_bytes,
            wal_table_backlog_max_bytes: engine.wal_table_backlog_max_bytes,
            flush_window_rows: engine.flush_window_rows,
            sst_max_row_group_size: engine.sst_max_row_group_size,
            disk_min_free_ratio: engine.disk_min_free_ratio,
        }
    }
}

impl Default for SyncYamlConfig {
    fn default() -> Self {
        let engine = EngineConfig::default();
        Self {
            queue_size: engine.sync_queue_size,
            max_pending_acks: engine.sync_max_pending,
            realtime_poll_ms: engine.sync_realtime_poll_ms,
            wal_load_cache_max_bytes: engine.sync_wal_load_cache_max_bytes,
            lake_endpoint: String::new(),
            lake_bucket: "monots".into(),
        }
    }
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .map_err(|e| TsdbError::Config(format!("read {}: {e}", path.display())))?;
        serde_yaml::from_str(&text)
            .map_err(|e| TsdbError::Config(format!("parse {}: {e}", path.display())))
    }

    /// Resolve config file: `--config` / `MONOTS_CONF` / `$MONOTS_HOME/conf/config.yaml` / `./conf/config.yaml`.
    pub fn resolve_path(cli_path: Option<PathBuf>) -> Option<PathBuf> {
        if let Some(p) = cli_path {
            return Some(p);
        }
        if let Ok(env) = std::env::var("MONOTS_CONF") {
            return Some(PathBuf::from(env));
        }
        if let Ok(home) = std::env::var("MONOTS_HOME") {
            let p = PathBuf::from(home).join("conf").join("config.yaml");
            if p.is_file() {
                return Some(p);
            }
        }
        for candidate in ["conf/config.yaml", "./conf/config.yaml"] {
            let p = PathBuf::from(candidate);
            if p.is_file() {
                return Some(p);
            }
        }
        None
    }

    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.service.host, self.service.port)
    }

    /// Resolve `data_dir` relative to `base` when not absolute.
    pub fn resolve_data_dir(&self, base: &Path) -> PathBuf {
        let p = PathBuf::from(&self.service.data_dir);
        if p.is_absolute() {
            p
        } else {
            base.join(p)
        }
    }

    pub fn to_engine_config(&self, data_dir: PathBuf) -> EngineConfig {
        EngineConfig {
            data_dir,
            memtable_max_bytes: self.storage.memtable_max_bytes,
            memtable_batch_max_rows: self.storage.memtable_batch_max_rows,
            memtable_batch_max_bytes: self.storage.memtable_batch_max_bytes,
            compaction_threshold_bytes: self.storage.compaction_threshold_bytes,
            compaction_interval_secs: self.storage.compaction_interval_secs,
            compaction_strategy: monots_storage::CompactionStrategy::from_str_lenient(
                &self.storage.compaction_strategy,
            ),
            compaction_max_merge_files: self.storage.compaction_max_merge_files.max(2),
            compaction_max_concurrent_jobs: self.storage.compaction_max_concurrent_jobs.max(1),
            global_memory_limit_bytes: self.storage.global_memory_limit_bytes,
            global_memory_soft_threshold_ratio: self.storage.global_memory_soft_threshold_ratio,
            query_memory_limit_bytes: EngineConfig::default().query_memory_limit_bytes,
            metadata_memory_limit_bytes: self.storage.metadata_memory_limit_bytes,
            sync_queue_size: self.sync.queue_size,
            sync_max_pending: self.sync.max_pending_acks,
            sync_realtime_poll_ms: self.sync.realtime_poll_ms,
            sync_wal_load_cache_max_bytes: self.sync.wal_load_cache_max_bytes,
            wal_durability: monots_storage::WalDurabilityMode::Async,
            wal_micro_batch_max_bytes: self.storage.wal_micro_batch_max_bytes,
            wal_segment_max_bytes: self.storage.wal_segment_max_bytes,
            wal_block_max_bytes: self.storage.wal_block_max_bytes,
            wal_global_backlog_max_bytes: self.storage.wal_global_backlog_max_bytes,
            wal_table_backlog_max_bytes: self.storage.wal_table_backlog_max_bytes,
            flush_window_rows: self.storage.flush_window_rows.max(1),
            sst_max_row_group_size: self.storage.sst_max_row_group_size.max(1),
            disk_min_free_ratio: self.storage.disk_min_free_ratio.clamp(0.0, 1.0),
        }
    }

    pub fn resolve_log_dir(&self, base: &Path) -> PathBuf {
        self.logging.resolve_directory(base)
    }

    pub fn apply_lake_env(&self) {
        if !self.sync.lake_endpoint.is_empty() && std::env::var("MONOTS_LAKE_ENDPOINT").is_err() {
            std::env::set_var("MONOTS_LAKE_ENDPOINT", &self.sync.lake_endpoint);
        }
        if !self.sync.lake_bucket.is_empty() && std::env::var("MONOTS_LAKE_BUCKET").is_err() {
            std::env::set_var("MONOTS_LAKE_BUCKET", &self.sync.lake_bucket);
        }
    }
}

/// Effective server settings after merging YAML + CLI overrides.
#[derive(Debug, Clone)]
pub struct ResolvedServerConfig {
    pub listen: String,
    pub data_dir: PathBuf,
    pub username: String,
    pub password: String,
    pub engine: EngineConfig,
}

impl ResolvedServerConfig {
    pub fn from_yaml_and_overrides(
        app: &AppConfig,
        config_path: &Path,
        listen: Option<String>,
        data_dir: Option<PathBuf>,
        username: Option<String>,
        password: Option<String>,
        memtable_max_bytes: Option<usize>,
        global_memory_limit_bytes: Option<usize>,
        global_memory_soft_threshold_ratio: Option<f64>,
        sync_max_pending: Option<usize>,
    ) -> Self {
        let base = config_path
            .parent()
            .and_then(|p| p.parent())
            .unwrap_or_else(|| Path::new("."));
        let mut data_dir = data_dir.unwrap_or_else(|| app.resolve_data_dir(base));
        if let Some(home) = std::env::var("MONOTS_HOME").ok() {
            if !app.service.data_dir.starts_with('/') && data_dir.is_relative() {
                data_dir = PathBuf::from(home).join(&app.service.data_dir);
            }
        }

        let mut engine = app.to_engine_config(data_dir.clone());
        if let Some(v) = memtable_max_bytes {
            engine.memtable_max_bytes = v;
        }
        if let Some(v) = global_memory_limit_bytes {
            engine.global_memory_limit_bytes = v;
        }
        if let Some(v) = global_memory_soft_threshold_ratio {
            engine.global_memory_soft_threshold_ratio = v.clamp(0.0, 1.0);
        }
        if let Some(v) = sync_max_pending {
            engine.sync_max_pending = v;
        }

        Self {
            listen: listen.unwrap_or_else(|| app.listen_addr()),
            data_dir,
            username: username.unwrap_or_else(|| app.service.username.clone()),
            password: password.unwrap_or_else(|| app.service.password.clone()),
            engine,
        }
    }
}
