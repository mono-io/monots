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

//! Global Tokio executor for Stream data-plane tasks.
//!
//! Explicit `init` at DB boot; `spawn_join` awaits `JoinHandle` directly (no oneshot).

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use common::{Result, TsdbError};
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::task::JoinHandle;

struct StreamExecutor {
    runtime: Runtime,
    worker_threads: usize,
}

static EXECUTOR: OnceLock<StreamExecutor> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub worker_threads: Option<usize>,
    pub max_blocking_threads: usize,
    pub thread_stack_size: usize,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            worker_threads: None,
            max_blocking_threads: 128,
            thread_stack_size: 4 * 1024 * 1024,
        }
    }
}

fn build_executor(config: ExecutorConfig) -> StreamExecutor {
    let workers = config.worker_threads.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(2)
    });

    let thread_id = AtomicUsize::new(0);
    let runtime = Builder::new_multi_thread()
        .worker_threads(workers)
        .max_blocking_threads(config.max_blocking_threads)
        .thread_stack_size(config.thread_stack_size)
        .thread_name_fn(move || {
            let id = thread_id.fetch_add(1, Ordering::Relaxed);
            format!("stream-wkr-{id}")
        })
        .thread_keep_alive(Duration::from_secs(60))
        .enable_all()
        .build()
        .expect("Failed to build Stream Tokio executor. Check OS thread limits.");

    tracing::info!(
        workers,
        max_blocking = config.max_blocking_threads,
        "Stream Tokio worker pool successfully initialized"
    );

    StreamExecutor {
        runtime,
        worker_threads: workers,
    }
}

/// Explicit boot-time init. Subsequent calls are no-ops.
pub fn init(config: ExecutorConfig) {
    let _ = EXECUTOR.get_or_init(|| build_executor(config));
}

#[inline]
fn executor() -> &'static StreamExecutor {
    EXECUTOR.get_or_init(|| {
        tracing::warn!("Stream executor accessed before explicit init; falling back to defaults");
        build_executor(ExecutorConfig::default())
    })
}

#[inline]
pub fn handle() -> Handle {
    executor().runtime.handle().clone()
}

#[inline]
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    executor().runtime.spawn(future)
}

#[inline]
pub fn worker_threads() -> usize {
    executor().worker_threads
}

/// Spawn on the Stream pool and await the `JoinHandle` (no extra oneshot channel).
pub async fn spawn_join<T: Send + 'static>(
    fut: impl Future<Output = T> + Send + 'static,
) -> Result<T> {
    spawn(fut).await.map_err(|join_err| {
        TsdbError::Storage(format!(
            "Stream worker pool task aborted unexpectedly: {join_err}"
        ))
    })
}

/// Process-exit style teardown note: OnceLock keeps the Runtime until OS exit.
pub fn shutdown_gracefully(_timeout: Duration) {
    if EXECUTOR.get().is_some() {
        tracing::info!("Stream executor shutdown requested; runtime remains until process exit");
    }
}
