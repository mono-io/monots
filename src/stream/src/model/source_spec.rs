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

//! Source-side stream configuration (capture mode, tables).

use common::StreamCaptureMode;

use crate::model::StreamDef;

#[derive(Debug, Clone)]
pub struct SourceSpec {
    pub stream_name: String,
    pub tables: Vec<String>,
    pub mode: StreamCaptureMode,
    pub auto_end: bool,
}

/// Runtime task view of [`SourceSpec`].
pub type SourceTaskSpec = SourceSpec;

impl SourceSpec {
    pub fn from_stream(def: &StreamDef) -> Self {
        Self {
            stream_name: def.name.clone(),
            tables: def.source_tables.clone(),
            mode: def.capture_mode,
            auto_end: def.auto_end,
        }
    }

    pub fn tails_log_continuously(&self) -> bool {
        self.mode.includes_log() && !self.auto_end
    }
}

/// Fallback capture mode for internal stubs (no sink selected yet).
///
/// Fallback when no connector default applies — prefer hybrid over pure log.
pub fn default_capture_mode() -> StreamCaptureMode {
    StreamCaptureMode::Hybrid
}
