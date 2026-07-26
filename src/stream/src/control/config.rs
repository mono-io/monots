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

//! Process-wide stream runtime settings (shared by all streams).

use std::time::Duration;

use crate::data::memory::{DEFAULT_STREAM_ARROW_BLOCK_BYTES, DEFAULT_STREAM_ARROW_POOL_BYTES};

/// Default mpsc capacity between Dispatcher and SinkWorker.
pub const DEFAULT_QUEUE_CAPACITY: usize = 64;

pub const DEFAULT_SINK_TRANSIENT_BACKOFF_START: Duration = Duration::from_millis(100);
pub const DEFAULT_SINK_TRANSIENT_BACKOFF_CAP: Duration = Duration::from_secs(30);
pub const DEFAULT_SINK_ABORT_TIMEOUT: Duration = Duration::from_secs(3);
pub const DEFAULT_SINK_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_SINK_PING_TIMEOUT: Duration = Duration::from_secs(3);
pub const DEFAULT_SINK_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// SinkWorker timing: transient backoff, teardown, and idle heartbeat.
#[derive(Debug, Clone)]
pub struct SinkWorkerConfig {
    pub transient_backoff_start: Duration,
    pub transient_backoff_cap: Duration,
    pub abort_timeout: Duration,
    pub close_timeout: Duration,
    pub ping_timeout: Duration,
    pub heartbeat_interval: Duration,
}

impl Default for SinkWorkerConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl SinkWorkerConfig {
    pub fn new() -> Self {
        Self {
            transient_backoff_start: DEFAULT_SINK_TRANSIENT_BACKOFF_START,
            transient_backoff_cap: DEFAULT_SINK_TRANSIENT_BACKOFF_CAP,
            abort_timeout: DEFAULT_SINK_ABORT_TIMEOUT,
            close_timeout: DEFAULT_SINK_CLOSE_TIMEOUT,
            ping_timeout: DEFAULT_SINK_PING_TIMEOUT,
            heartbeat_interval: DEFAULT_SINK_HEARTBEAT_INTERVAL,
        }
    }

    pub fn with_transient_backoff(mut self, start: Duration, cap: Duration) -> Self {
        self.transient_backoff_start = start;
        self.transient_backoff_cap = cap.max(start);
        self
    }

    pub fn with_abort_timeout(mut self, d: Duration) -> Self {
        self.abort_timeout = d;
        self
    }

    pub fn with_close_timeout(mut self, d: Duration) -> Self {
        self.close_timeout = d;
        self
    }

    pub fn with_ping_timeout(mut self, d: Duration) -> Self {
        self.ping_timeout = d;
        self
    }

    pub fn with_heartbeat_interval(mut self, d: Duration) -> Self {
        self.heartbeat_interval = d;
        self
    }
}

#[derive(Debug, Clone)]
pub struct StreamRuntimeConfig {
    pub queue_capacity: usize,
    pub poll_ms: u64,
    pub lake_endpoint: Option<String>,
    pub arrow_pool_bytes: usize,
    pub arrow_block_bytes: usize,
    /// Process-wide SinkWorker timeouts / heartbeat.
    pub sink_worker: SinkWorkerConfig,
}

impl Default for StreamRuntimeConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamRuntimeConfig {
    pub fn new() -> Self {
        Self {
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            poll_ms: 1000,
            lake_endpoint: None,
            arrow_pool_bytes: DEFAULT_STREAM_ARROW_POOL_BYTES,
            arrow_block_bytes: DEFAULT_STREAM_ARROW_BLOCK_BYTES,
            sink_worker: SinkWorkerConfig::new(),
        }
    }

    pub fn with_queue_capacity(mut self, n: usize) -> Self {
        self.queue_capacity = n.max(1);
        self
    }

    pub fn with_poll_ms(mut self, ms: u64) -> Self {
        self.poll_ms = ms;
        self
    }

    pub fn with_lake_endpoint(mut self, endpoint: impl Into<Option<String>>) -> Self {
        self.lake_endpoint = endpoint.into();
        self
    }

    pub fn with_arrow_pool_bytes(mut self, n: usize) -> Self {
        self.arrow_pool_bytes = n.max(1);
        self
    }

    pub fn with_arrow_block_bytes(mut self, n: usize) -> Self {
        self.arrow_block_bytes = n;
        self
    }

    pub fn with_sink_worker(mut self, cfg: SinkWorkerConfig) -> Self {
        self.sink_worker = cfg;
        self
    }
}
