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

//! Control plane: DDL, orchestration, metadata, progress.

pub mod checkpoint;
pub mod config;
pub mod context;
pub mod ddl;
pub mod executor;
pub mod meta;
pub mod metrics;
pub mod orchestrator;
pub mod progress;
pub mod supervisor;

pub use config::{SinkWorkerConfig, StreamRuntimeConfig, DEFAULT_QUEUE_CAPACITY};
pub use context::StreamContext;
pub use ddl::{
    create_stream, drop_stream, format_stream_ddl, show_stream, show_stream_status, show_streams,
    start_stream_if_ready, StreamDdlContext, StreamMutatingOutcome,
};
pub use executor::{
    handle, init as init_executor, shutdown_gracefully, spawn, spawn_join, worker_threads,
    ExecutorConfig,
};
pub use meta::codec::{
    decode_stream_def, encode_stream_def, Versioned, STREAM_SCHEMA_MIN_SUPPORTED,
    STREAM_SCHEMA_VERSION,
};
pub use meta::StreamStore;
pub use orchestrator::{
    drop_stream_capture_progress, mark_inactive, StreamControlHandle, StreamEngine,
    StreamGarbageCollector, StreamOrchestrator, StreamRuntimeManager,
};
pub use progress::{
    capture_progress_id, parse_capture_progress_id, CaptureProgressRegistry, CommitStore,
    ProgressManager, SinkCommitted, WalCommitLog,
};
pub use supervisor::{
    PipelineSupervisor, StreamSupervisor, SupervisorError, STREAM_AUTO_RESTART_INTERVAL,
};

pub use crate::model::metrics::{
    ExecutionStep, MetricsRegistry, RuntimeStateRegistry, StreamMetrics, StreamRuntimeState,
};
