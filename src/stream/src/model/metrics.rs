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

//! Lock-free stream runtime metrics (atomics on the ACK hot path).

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use common::{StreamPhase, StreamStatus};
use dashmap::DashMap;
use parking_lot::RwLock;

/// Low-frequency execution step label for SHOW / ops (not on the ACK path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionStep {
    Pending,
    PreparingLog,
    SyncingBatch,
    TailingLog,
    Suspended,
    Failed,
    Custom(String),
}

impl ExecutionStep {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Pending => "pending",
            Self::PreparingLog => "preparing_log",
            Self::SyncingBatch => "syncing_batch",
            Self::TailingLog => "tailing_log",
            Self::Suspended => "suspended",
            Self::Failed => "failed",
            Self::Custom(s) => s.as_str(),
        }
    }

    fn from_label(label: impl Into<String>) -> Self {
        let s = label.into();
        match s.as_str() {
            "pending" => Self::Pending,
            "preparing_log" | "starting" | "auto_restarting" => Self::PreparingLog,
            "syncing_batch" => Self::SyncingBatch,
            "tailing_log" | "sinking" => Self::TailingLog,
            "suspended" | "paused" | "sink paused by user" => Self::Suspended,
            "failed" => Self::Failed,
            // recovering / resetting stay Custom for SHOW step detail
            _ => Self::Custom(s),
        }
    }
}

/// Per-stream runtime metrics. Hot counters are atomic; phase/step use short RwLocks.
pub struct StreamRuntimeState {
    phase: RwLock<StreamPhase>,
    current_step: RwLock<ExecutionStep>,
    last_error: RwLock<Option<String>>,
    acked_lsn: AtomicU64,
    batch_files_done: AtomicU64,
    batch_files_total: AtomicU64,
    log_channel_opened_ms: AtomicI64,
    user_stopped: AtomicBool,
    auto_restart_count: AtomicU32,
}

/// Alias matching the industrial metrics naming.
pub type StreamMetrics = StreamRuntimeState;

impl Default for StreamRuntimeState {
    fn default() -> Self {
        Self {
            phase: RwLock::new(StreamPhase::Inactive),
            current_step: RwLock::new(ExecutionStep::Pending),
            last_error: RwLock::new(None),
            acked_lsn: AtomicU64::new(0),
            batch_files_done: AtomicU64::new(0),
            batch_files_total: AtomicU64::new(0),
            log_channel_opened_ms: AtomicI64::new(0),
            user_stopped: AtomicBool::new(false),
            auto_restart_count: AtomicU32::new(0),
        }
    }
}

impl StreamRuntimeState {
    pub fn new(_source_table: &str) -> Self {
        Self::default()
    }

    pub fn phase(&self) -> StreamPhase {
        *self.phase.read()
    }

    pub fn current_step(&self) -> String {
        self.current_step.read().as_str().to_string()
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error.read().clone()
    }

    pub fn set_last_error(&self, err: Option<String>) {
        *self.last_error.write() = err;
    }

    pub fn set_current_step(&self, step: impl Into<String>) {
        *self.current_step.write() = ExecutionStep::from_label(step);
    }

    pub fn acked_lsn(&self) -> u64 {
        self.acked_lsn.load(Ordering::Relaxed)
    }

    pub fn batch_files_done(&self) -> u64 {
        self.batch_files_done.load(Ordering::Relaxed)
    }

    pub fn batch_files_total(&self) -> u64 {
        self.batch_files_total.load(Ordering::Relaxed)
    }

    pub fn set_batch_files_total(&self, total: u64) {
        self.batch_files_total.store(total, Ordering::Relaxed);
    }

    pub fn log_channel_opened_ms(&self) -> i64 {
        self.log_channel_opened_ms.load(Ordering::Relaxed)
    }

    pub fn set_log_channel_opened_ms(&self, ms: i64) {
        self.log_channel_opened_ms.store(ms, Ordering::Relaxed);
    }

    pub fn user_stopped(&self) -> bool {
        self.user_stopped.load(Ordering::Relaxed)
    }

    pub fn auto_restart_count(&self) -> u32 {
        self.auto_restart_count.load(Ordering::Relaxed)
    }

    pub fn set_phase(&self, phase: StreamPhase, step: impl Into<String>) {
        *self.phase.write() = phase;
        *self.current_step.write() = ExecutionStep::from_label(step);
    }

    pub fn set_error(&self, phase: StreamPhase, error: impl Into<String>) {
        *self.phase.write() = phase;
        *self.last_error.write() = Some(error.into());
        let step = match phase {
            StreamPhase::Suspended => ExecutionStep::Suspended,
            StreamPhase::Failed => ExecutionStep::Failed,
            _ => self.current_step.read().clone(),
        };
        *self.current_step.write() = step;
    }

    pub fn ack_batch_parquet_file(&self) {
        self.batch_files_done.fetch_add(1, Ordering::Relaxed);
    }

    pub fn ack_batch_commit(&self, files: u64) {
        self.batch_files_done.fetch_add(files, Ordering::Relaxed);
    }

    #[inline]
    pub fn ack_log_lsn(&self, lsn: u64) {
        self.acked_lsn.fetch_max(lsn, Ordering::Relaxed);
    }

    pub fn mark_user_stopped(&self) {
        self.user_stopped.store(true, Ordering::Relaxed);
        self.set_phase(StreamPhase::Suspended, "sink paused by user");
    }

    pub fn clear_user_stopped(&self) {
        self.user_stopped.store(false, Ordering::Relaxed);
    }

    pub fn note_auto_restart(&self, error: impl Into<String>) {
        let n = self.auto_restart_count.fetch_add(1, Ordering::Relaxed) + 1;
        self.set_error(StreamPhase::Failed, error);
        *self.current_step.write() = ExecutionStep::Custom(format!("auto_restart_wait_{n}"));
    }

    pub fn as_stream_status(&self) -> StreamStatus {
        StreamStatus {
            phase: self.phase(),
            log_channel_opened_ms: self.log_channel_opened_ms(),
            current_step: self.current_step(),
            tables: Vec::new(),
            batch_files_total: self.batch_files_total(),
            batch_files_done: self.batch_files_done(),
            acked_lsn: self.acked_lsn(),
            last_error: self.last_error(),
        }
    }
}

/// Sharded registry; after `Arc` is obtained, ACK updates need no map lock.
#[derive(Clone, Default)]
pub struct RuntimeStateRegistry {
    states: Arc<DashMap<String, Arc<StreamRuntimeState>>>,
}

/// Alias matching the industrial metrics naming.
pub type MetricsRegistry = RuntimeStateRegistry;

impl RuntimeStateRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ensure(&self, name: &str, source_table: &str) {
        self.states
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(StreamRuntimeState::new(source_table)));
    }

    pub fn get(&self, name: &str) -> Arc<StreamRuntimeState> {
        self.states
            .get(name)
            .map(|e| Arc::clone(e.value()))
            .unwrap_or_default()
    }

    pub fn phase(&self, name: &str) -> StreamPhase {
        self.states
            .get(name)
            .map(|e| e.phase())
            .unwrap_or(StreamPhase::Inactive)
    }

    /// Apply `f` to the live metrics handle (ACK-safe: no global write lock).
    pub fn update<F>(&self, name: &str, f: F)
    where
        F: FnOnce(&StreamRuntimeState),
    {
        let state = self
            .states
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(StreamRuntimeState::default()))
            .clone();
        f(&state);
    }

    pub fn remove(&self, name: &str) {
        self.states.remove(name);
    }
}
