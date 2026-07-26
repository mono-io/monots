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

//! Downstream connectors — unified 2PC [`SinkConnector`] plugins.

pub mod api;
pub mod builder;
pub mod config;
pub mod plugins;

pub use api::{NoopSink, SinkConnector, SinkError};
pub use builder::{build_sink, build_sink_with_engine};
pub use config::{
    connector_capture_mode, connector_default_format, validate_sink, ResolvedSinkConfig,
};
