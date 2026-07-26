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

pub mod config;
pub mod engine;
pub mod sql;

pub use monots_catalog as metadata;
pub use monots_query as query;
pub use monots_storage as storage;

/// Stream sync / CDC (separate crate, re-exported for convenience).
pub use monots_stream as stream;

/// Shared replication & stream contracts (also available via `monots_stream` / `monots_storage`).
pub use common::{
    BatchEvent, BatchOrigin, CaptureFileMeta, CdcEvent, CommitDurability, ConnectorType,
    FileAddEvent, LogEvent, LsnAllocator, LsnRange, StreamCaptureMode, StreamCheckpoint, StreamDef,
    StreamPhase, StreamStatus, TableCaptureListener, TableCaptureStatus, TableCheckpoint,
};

pub use common::{LogConfig, LogFormat, LogGuard, LogLevel, LogRotation};
pub use config::{AppConfig, EngineConfig, ResolvedServerConfig};
pub use engine::TsdbEngine;
pub use storage::WalDurabilityMode;
