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

//! Pure data structures: stream definition, runtime metrics, pipeline events.

pub mod def;
pub mod event;
pub mod metrics;
pub mod source_spec;
pub mod state;

pub use common::{StreamCaptureMode, StreamPhase, StreamStatus, TableStreamStatus};
pub use def::{
    ensure_single_source_table, parse_stream_def, should_run_phase, should_run_stream, stream_plan,
    stream_worker_id, SinkConfig, StreamDef,
};
pub use event::{DataEvent, IngressEvent, InsertArrow};
pub use metrics::{
    ExecutionStep, MetricsRegistry, RuntimeStateRegistry, StreamMetrics, StreamRuntimeState,
};
pub use source_spec::{default_capture_mode, SourceSpec, SourceTaskSpec};
