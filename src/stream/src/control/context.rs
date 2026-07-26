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

//! Shared runtime context for stream supervisors.

use std::sync::Arc;

use monots_storage::LsmEngine;

use super::config::{SinkWorkerConfig, StreamRuntimeConfig};
use crate::control::meta::StreamStore;
use crate::control::progress::ProgressManager;
use crate::data::ingress::StreamSourceRegistry;
use crate::data::memory::{StreamArrowBlock, StreamArrowMemoryPool};
use crate::model::state::RuntimeStateRegistry;

#[derive(Clone)]
pub struct StreamContext {
    pub engine: Arc<LsmEngine>,
    pub store: Arc<StreamStore>,
    pub sources: StreamSourceRegistry,
    pub progress: Arc<ProgressManager>,
    pub runtime_states: RuntimeStateRegistry,
    pub runtime: StreamRuntimeConfig,
    pub arrow_pool: Arc<StreamArrowMemoryPool>,
}

impl StreamContext {
    pub fn queue_capacity(&self) -> usize {
        self.runtime.queue_capacity
    }

    pub fn poll_ms(&self) -> u64 {
        self.runtime.poll_ms
    }

    pub fn sink_worker_config(&self) -> &SinkWorkerConfig {
        &self.runtime.sink_worker
    }

    pub fn alloc_arrow_block(&self) -> Arc<StreamArrowBlock> {
        self.arrow_pool.alloc_block(self.runtime.arrow_block_bytes)
    }
}
