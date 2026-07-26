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

use clap::{Parser, ValueEnum};
use monots_core::EngineConfig;
use monots_storage::WalDurabilityMode;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum BenchWalMode {
    Async,
    Sync,
}

impl From<BenchWalMode> for WalDurabilityMode {
    fn from(mode: BenchWalMode) -> Self {
        match mode {
            BenchWalMode::Async => WalDurabilityMode::Async,
            BenchWalMode::Sync => WalDurabilityMode::Sync,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ScenarioKind {
    /// All threads write to the same table (contention on WAL / MemTable).
    Shared,
    /// Each thread writes to its own table (no cross-table lock contention).
    Dedicated,
    /// Run both scenarios and print a comparison table.
    All,
}

#[derive(Debug, Clone, Parser)]
#[command(name = "monots-bench", about = "MonoTS write-path benchmarks")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Clone, Parser)]
pub enum Command {
    /// Multi-threaded write throughput benchmarks.
    Write(WriteArgs),
    /// Break down single-thread async write latency.
    Profile,
    /// Sustained write load with periodic memory sampling (default 10 minutes).
    Soak(SoakArgs),
}

#[derive(Debug, Clone, Parser)]
pub struct WriteArgs {
    /// Which scenario to run.
    #[arg(long, value_enum, default_value_t = ScenarioKind::All)]
    pub scenario: ScenarioKind,

    /// Number of writer threads.
    #[arg(long, default_value_t = 5)]
    pub threads: usize,

    /// Number of tables for the dedicated scenario (default: 100).
    #[arg(long, default_value_t = 100)]
    pub tables: usize,

    /// Record batches written per thread.
    #[arg(long, default_value_t = 100)]
    pub batches: usize,

    /// Rows in each batch.
    #[arg(long, default_value_t = 1000)]
    pub rows_per_batch: usize,

    /// Temporary data directory (created if missing).
    #[arg(long)]
    pub data_dir: Option<PathBuf>,

    /// MemTable size before rotation (large value reduces flush noise during bench).
    #[arg(long, default_value_t = 512 * 1024 * 1024)]
    pub memtable_max_bytes: usize,

    /// WAL durability: async (default) or sync per batch.
    #[arg(long, value_enum, default_value_t = BenchWalMode::Async)]
    pub wal_mode: BenchWalMode,
}

#[derive(Debug, Clone, Parser)]
pub struct SoakArgs {
    /// Run duration in seconds (default 10 minutes).
    #[arg(long, default_value_t = 600)]
    pub duration_secs: u64,

    /// Memory sample interval in seconds.
    #[arg(long, default_value_t = 30)]
    pub sample_interval_secs: u64,

    /// Number of writer threads (0 = auto match `--tables`).
    #[arg(long, default_value_t = 0)]
    pub threads: usize,

    /// Number of tables (each table gets its own LSM / WAL).
    #[arg(long, default_value_t = 1)]
    pub tables: usize,

    /// Rows in each batch.
    #[arg(long, default_value_t = 100)]
    pub rows_per_batch: usize,

    /// Temporary data directory (created if missing).
    #[arg(long)]
    pub data_dir: Option<PathBuf>,

    /// MemTable size before rotation.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    pub memtable_max_bytes: usize,

    /// Global memory limit for the engine (100% hard cap — writes are rejected at this limit).
    #[arg(long, default_value_t = 512 * 1024 * 1024)]
    pub global_memory_limit_bytes: usize,

    /// Soft threshold ratio (0.0–1.0): flush largest memtable when usage reaches this fraction.
    #[arg(long, default_value_t = 0.5)]
    pub global_memory_soft_threshold_ratio: f64,

    /// WAL durability: async (default) or sync per batch.
    #[arg(long, value_enum, default_value_t = BenchWalMode::Async)]
    pub wal_mode: BenchWalMode,

    /// OS process title shown in Activity Monitor / ps (default: monots).
    #[arg(long, default_value = "monots")]
    pub process_name: Option<String>,

    /// Microseconds to wait between retries after a memory-limit write rejection.
    #[arg(long, default_value_t = 100)]
    pub memory_retry_backoff_us: u64,
}

#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub threads: usize,
    pub tables: usize,
    pub batches_per_thread: usize,
    pub rows_per_batch: usize,
    pub data_dir: PathBuf,
    pub engine: EngineConfig,
}

impl WriteArgs {
    pub fn to_bench_config(&self, data_dir: PathBuf) -> BenchConfig {
        let tables = self.tables.max(1);
        BenchConfig {
            threads: self.threads.max(1),
            tables,
            batches_per_thread: self.batches.max(1),
            rows_per_batch: self.rows_per_batch.max(1),
            data_dir: data_dir.clone(),
            engine: EngineConfig {
                data_dir,
                memtable_max_bytes: self.memtable_max_bytes,
                compaction_threshold_bytes: u64::MAX,
                compaction_interval_secs: u64::MAX,
                global_memory_limit_bytes: 2 * 1024 * 1024 * 1024,
                metadata_memory_limit_bytes: 64 * 1024 * 1024,
                wal_durability: self.wal_mode.into(),
                ..EngineConfig::default()
            },
        }
    }
}

impl SoakArgs {
    pub fn effective_threads(&self) -> usize {
        if self.threads == 0 {
            self.tables.max(1)
        } else {
            self.threads.max(1)
        }
    }

    pub fn to_bench_config(&self, data_dir: PathBuf) -> BenchConfig {
        BenchConfig {
            threads: self.effective_threads(),
            tables: self.tables.max(1),
            batches_per_thread: 1,
            rows_per_batch: self.rows_per_batch.max(1),
            data_dir: data_dir.clone(),
            engine: EngineConfig {
                data_dir,
                memtable_max_bytes: self.memtable_max_bytes,
                compaction_threshold_bytes: u64::MAX,
                compaction_interval_secs: u64::MAX,
                global_memory_limit_bytes: self.global_memory_limit_bytes,
                global_memory_soft_threshold_ratio: self.global_memory_soft_threshold_ratio,
                metadata_memory_limit_bytes: 64 * 1024 * 1024,
                wal_durability: self.wal_mode.into(),
                ..EngineConfig::default()
            },
        }
    }
}

#[cfg(test)]
mod soak_config_tests {
    use super::{BenchWalMode, SoakArgs};

    #[test]
    fn effective_threads_matches_tables_when_zero() {
        let args = SoakArgs {
            threads: 0,
            tables: 50,
            ..default_soak_args()
        };
        assert_eq!(args.effective_threads(), 50);
    }

    fn default_soak_args() -> SoakArgs {
        SoakArgs {
            duration_secs: 60,
            sample_interval_secs: 30,
            threads: 0,
            tables: 1,
            rows_per_batch: 100,
            data_dir: None,
            memtable_max_bytes: 64 * 1024 * 1024,
            global_memory_limit_bytes: 512 * 1024 * 1024,
            global_memory_soft_threshold_ratio: 0.5,
            wal_mode: BenchWalMode::Async,
            process_name: None,
            memory_retry_backoff_us: 100,
        }
    }
}
