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

use thiserror::Error;

#[derive(Error, Debug)]
pub enum TsdbError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("network error: {0}")]
    Network(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("query error: {0}")]
    Query(String),

    #[error("authentication failed: {0}")]
    Auth(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("invalid schema: {0}")]
    Schema(String),

    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("insufficient memory (used {used_bytes} / limit {limit_bytes} bytes), write blocked")]
    MemoryLimitExceeded {
        used_bytes: usize,
        limit_bytes: usize,
    },

    #[error(
        "disk free space critically low (free {free_bytes} / total {total_bytes} bytes, \
         min free ratio {min_free_ratio}); storage is read-only"
    )]
    DiskReadOnly {
        free_bytes: u64,
        total_bytes: u64,
        min_free_ratio: f64,
    },

    #[error("sync error: {0}")]
    Sync(String),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}

impl From<datafusion_common::DataFusionError> for TsdbError {
    fn from(e: datafusion_common::DataFusionError) -> Self {
        Self::Query(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, TsdbError>;

impl TsdbError {
    pub fn memory_limit_exceeded(used_bytes: usize, limit_bytes: usize) -> Self {
        Self::MemoryLimitExceeded {
            used_bytes,
            limit_bytes,
        }
    }

    pub fn is_memory_limit_exceeded(&self) -> bool {
        matches!(self, Self::MemoryLimitExceeded { .. })
    }

    pub fn disk_read_only(free_bytes: u64, total_bytes: u64, min_free_ratio: f64) -> Self {
        Self::DiskReadOnly {
            free_bytes,
            total_bytes,
            min_free_ratio,
        }
    }

    pub fn is_disk_read_only(&self) -> bool {
        matches!(self, Self::DiskReadOnly { .. })
    }
}
