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

//! Well-known column names and on-disk layout constants.

/// Primary time column. Required on every table; must be named `time`.
pub const TIME_COLUMN: &str = "time";

/// Legacy alias kept for gradual migration in call sites.
pub const TIMESTAMP_COLUMN: &str = TIME_COLUMN;

/// Subdirectory under table data dir for WAL (one subdir per memtable id).
pub const WAL_SEGMENTS_DIR: &str = "wal_segments";

/// Staging directory for in-progress MemTable flushes (promoted into `data_dir` when complete).
pub const FLUSH_TMP_DIR: &str = ".flush_tmp";

/// Staging directory for in-progress compaction merges (promoted into `data_dir` when complete).
pub const COMPACT_TMP_DIR: &str = ".compact_tmp";

/// Staging directory for in-progress bulk-load SST writes (promoted / sealed into `data_dir` when complete).
pub const BULK_TMP_DIR: &str = ".bulk_tmp";

/// Per-table monotonic memtable / SST version counter.
pub const SEQUENCE_FILE: &str = "sequence.json";

/// Parquet SST file suffix.
pub const SST_FILE_SUFFIX: &str = "parquet";

/// Subdirectory under data root for catalog metadata.
pub const META_DIR: &str = "meta";

/// Well-known system columns (extend as needed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WellKnownColumn {
    Time,
}

impl WellKnownColumn {
    pub fn name(self) -> &'static str {
        match self {
            Self::Time => TIME_COLUMN,
        }
    }

    pub fn is_system_column(name: &str) -> bool {
        matches!(name, TIME_COLUMN)
    }
}
