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

//! Data plane: capture ingress, dispatcher, WAL-backed worker.

pub mod arrow;
pub mod barrier;
pub mod dispatcher;
pub mod ingress;
pub mod memory;
pub mod pipeline;
pub mod worker;

pub use arrow::{
    ArrowStreamEvent, EventStreamReader, ParquetEvent, ParquetEventBuilder, StreamArrowLoader,
    WalMaterializer,
};
pub use barrier::{async_gc_paths, CheckpointBarrier, PipelineEvent};
pub use dispatcher::{BatchPolicy, DispatchPolicy, StreamDispatcher};
pub use ingress::{
    AsyncSourceGroup, RoutedEvent, StreamSource, StreamSourceManager, StreamSourceRegistry,
    StreamSources, PENDING_COMPACT, PENDING_FLUSH,
};
pub use memory::{
    record_batches_memory_size, SharedArrowCharge, StreamArrowBlock, StreamArrowMemoryPool,
    DEFAULT_STREAM_ARROW_BLOCK_BYTES, DEFAULT_STREAM_ARROW_POOL_BYTES,
};
pub use pipeline::StreamPipeline;
pub use worker::{SinkControl, SinkWorker, SinkWorkerStatus, WorkerState};

pub use crate::model::event::{DataEvent, IngressEvent, InsertArrow};
