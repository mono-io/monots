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

//! MonoTS CDC Stream Engine
//!
//! Architecture:
//! - [`model`]:     Pure data structures (Def, State, Event).
//! - [`control`]:   Control plane (DDL persistence, Supervisor, Progress coordination).
//! - [`data`]:      Data plane (Ingress/Source, Mux, Acker).
//! - [`connector`]: Downstream sink implementations (Delta, Kafka).

pub mod connector;
pub mod control;
pub mod data;
pub mod model;

// ── Root API (stable for monots-core) ────────────────────────────────────────

pub use connector::{
    build_sink, build_sink_with_engine, validate_sink, NoopSink, ResolvedSinkConfig, SinkConnector,
    SinkError,
};
pub use control::{
    capture_progress_id, create_stream, drop_stream, drop_stream_capture_progress,
    format_stream_ddl, handle, init_executor, mark_inactive, parse_capture_progress_id,
    show_stream, show_stream_status, show_streams, shutdown_gracefully, spawn,
    start_stream_if_ready, worker_threads, CaptureProgressRegistry, CommitStore, ExecutorConfig,
    PipelineSupervisor, ProgressManager, SinkCommitted, SinkWorkerConfig, StreamContext,
    StreamControlHandle, StreamDdlContext, StreamEngine, StreamGarbageCollector,
    StreamMutatingOutcome, StreamOrchestrator, StreamRuntimeConfig, StreamRuntimeManager,
    StreamStore, StreamSupervisor, SupervisorError, WalCommitLog, DEFAULT_QUEUE_CAPACITY,
    STREAM_AUTO_RESTART_INTERVAL,
};
pub use data::{
    record_batches_memory_size, ArrowStreamEvent, AsyncSourceGroup, BatchPolicy, CheckpointBarrier,
    DispatchPolicy, EventStreamReader, ParquetEvent, ParquetEventBuilder, PipelineEvent,
    RoutedEvent, SharedArrowCharge, SinkControl, SinkWorker, SinkWorkerStatus, StreamArrowBlock,
    StreamArrowLoader, StreamArrowMemoryPool, StreamDispatcher, StreamPipeline, StreamSource,
    StreamSourceManager, StreamSourceRegistry, StreamSources, WorkerState,
    DEFAULT_STREAM_ARROW_BLOCK_BYTES, DEFAULT_STREAM_ARROW_POOL_BYTES, PENDING_COMPACT,
    PENDING_FLUSH,
};
pub use model::{
    default_capture_mode, ensure_single_source_table, parse_stream_def, should_run_phase,
    should_run_stream, stream_plan, stream_worker_id, DataEvent, IngressEvent, InsertArrow,
    MetricsRegistry, RuntimeStateRegistry, SinkConfig, SourceSpec, StreamDef, StreamMetrics,
    StreamRuntimeState,
};

pub use common::{
    BatchEvent, BatchOrigin, CaptureFileMeta, CdcEvent, CommitDurability, ConnectorType,
    FileAddEvent, LogEvent, LsnAllocator, LsnRange, StreamCaptureMode, StreamPhase, StreamStatus,
    TableCaptureListener, TableCaptureStatus, TableStreamStatus,
};

pub use control::checkpoint::{CheckpointStore, StreamCheckpoint, TableCheckpoint};
