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

//! Shared stream metadata types used by `monots-stream`, server, and tooling.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{Result, TsdbError};

/// How a stream captures change data from source tables.
///
/// Capture mode describes **what** to read from the engine, not which sink connector you use:
///
/// | Mode | Source | Data shape |
/// |------|--------|------------|
/// | [`Batch`](Self::Batch) | Flush-produced SST files only | Parquet on disk |
/// | [`Log`](Self::Log) | WAL / logical LSN stream only | Arrow IPC batches |
/// | [`Hybrid`](Self::Hybrid) | Batch then log (as needed) | Parquet, then WAL |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamCaptureMode {
    /// Only capture flush-produced Parquet SST files (`cdc.mode = batch`).
    Batch,
    /// Only capture WAL / logical LSN events.
    ///
    /// Retained for protobuf / checkpoint wire compatibility. New DDL cannot select
    /// this mode — use [`Self::Hybrid`] when WAL tail is required.
    Log,
    /// Batch Parquet backfill, then log tail when both are needed (`cdc.mode = hybrid`).
    Hybrid,
}

impl StreamCaptureMode {
    /// Parse a user-facing `cdc.mode` value.
    ///
    /// Only [`Self::Batch`] and [`Self::Hybrid`] are accepted. Pure `log` is not a
    /// configurable capture mode (use `hybrid` when WAL tail is required).
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "batch" | "historical_only" | "historical" => Ok(Self::Batch),
            "hybrid" | "full" | "historical_then_realtime" => Ok(Self::Hybrid),
            "log" | "realtime_only" | "realtime" => Err(TsdbError::Query(
                "cdc.mode = 'log' is not supported; use 'batch' or 'hybrid'".into(),
            )),
            other => Err(TsdbError::Query(format!(
                "unsupported cdc.mode: {other} (supported: batch | hybrid)"
            ))),
        }
    }

    pub fn as_cdc_mode_str(self) -> &'static str {
        match self {
            Self::Batch => "batch",
            Self::Log => "log",
            Self::Hybrid => "hybrid",
        }
    }

    /// Captures flush-produced Parquet SST files (batch path — always on-disk Parquet).
    pub fn includes_batch(self) -> bool {
        matches!(self, Self::Batch | Self::Hybrid)
    }

    /// Captures WAL / logical LSN events (log path).
    pub fn includes_log(self) -> bool {
        matches!(self, Self::Log | Self::Hybrid)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorType {
    Delta,
    Kafka,
    /// Export Parquet files to a local directory (`sink.filesystem.path` / `sink.delta.path`).
    Filesystem,
    /// Apache Iceberg table via Catalog (`sink.iceberg.*`).
    Iceberg,
    /// Apache Pulsar topic (`sink.pulsar.*`).
    Pulsar,
    /// MQTT broker topic (`sink.mqtt.*`).
    Mqtt,
}

impl ConnectorType {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "delta" => Ok(Self::Delta),
            "kafka" => Ok(Self::Kafka),
            "filesystem" | "fs" | "file" => Ok(Self::Filesystem),
            "iceberg" => Ok(Self::Iceberg),
            "pulsar" => Ok(Self::Pulsar),
            "mqtt" => Ok(Self::Mqtt),
            other => Err(TsdbError::Query(format!(
                "unsupported sink.type: {other} (supported: delta | kafka | filesystem | iceberg | pulsar | mqtt)"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Delta => "delta",
            Self::Kafka => "kafka",
            Self::Filesystem => "filesystem",
            Self::Iceberg => "iceberg",
            Self::Pulsar => "pulsar",
            Self::Mqtt => "mqtt",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StreamPhase {
    #[default]
    Inactive,
    #[serde(alias = "preparing_realtime")]
    PreparingLog,
    #[serde(alias = "syncing_historical")]
    SyncingBatch,
    #[serde(alias = "syncing_realtime")]
    SyncingLog,
    Active,
    Completed,
    /// Fatal sink error / admin pause — durable queue kept; resume after fix.
    Suspended,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableStreamStatus {
    pub table_name: String,
    #[serde(alias = "historical_files_total")]
    pub batch_files_total: u64,
    #[serde(alias = "historical_files_acked")]
    pub batch_files_acked: u64,
    pub last_acked_file: Option<String>,
    #[serde(alias = "wal_sequence")]
    pub acked_lsn: u64,
    #[serde(alias = "historical_complete")]
    pub batch_complete: bool,
    #[serde(alias = "realtime_complete")]
    pub log_complete: bool,
}

/// Backward-compatible alias.
pub type TableCaptureStatus = TableStreamStatus;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamStatus {
    pub phase: StreamPhase,
    pub log_channel_opened_ms: i64,
    pub current_step: String,
    pub tables: Vec<TableStreamStatus>,
    pub batch_files_total: u64,
    pub batch_files_done: u64,
    pub acked_lsn: u64,
    pub last_error: Option<String>,
}

impl Default for StreamStatus {
    fn default() -> Self {
        Self {
            phase: StreamPhase::Inactive,
            log_channel_opened_ms: 0,
            current_step: "pending".into(),
            tables: Vec::new(),
            batch_files_total: 0,
            batch_files_done: 0,
            acked_lsn: 0,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamDef {
    pub name: String,
    pub connector_type: ConnectorType,
    /// Bound source tables. **Exactly one table** is required (one stream ↔ one table).
    pub source_tables: Vec<String>,
    pub capture_mode: StreamCaptureMode,
    pub delivery_format: String,
    pub delivery_files: bool,
    pub sink_path: Option<String>,
    pub kafka_brokers: Option<String>,
    pub kafka_topic: Option<String>,
    pub auto_end: bool,
    pub properties: HashMap<String, String>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableCheckpoint {
    pub acked_files: Vec<String>,
    #[serde(default)]
    pub acked_lsn: u64,
    #[serde(alias = "historical_complete")]
    pub batch_complete: bool,
    #[serde(alias = "realtime_complete")]
    pub log_complete: bool,
}

impl TableCheckpoint {
    pub fn has_acked_file(&self, path: &str) -> bool {
        self.acked_files.iter().any(|p| p == path)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamCheckpoint {
    pub stream_name: String,
    pub consumer_id: String,
    pub log_channel_opened_ms: i64,
    pub tables: HashMap<String, TableCheckpoint>,
}

impl StreamCheckpoint {
    pub fn new(stream_name: impl Into<String>, consumer_id: impl Into<String>) -> Self {
        Self {
            stream_name: stream_name.into(),
            consumer_id: consumer_id.into(),
            log_channel_opened_ms: 0,
            tables: HashMap::new(),
        }
    }

    pub fn table_mut(&mut self, table: &str) -> &mut TableCheckpoint {
        self.tables.entry(table.to_string()).or_default()
    }
}
