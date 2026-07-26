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

//! Stream lifecycle orchestrator: DDL start/stop without owning the data-plane loop.

use std::sync::Arc;

use common::{Result, StreamPhase, TsdbError};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use monots_storage::LsmEngine;
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::context::StreamContext;
use super::executor;
use super::supervisor::PipelineSupervisor;
use crate::control::progress::{capture_progress_id, CaptureProgressRegistry};
use crate::data::SinkControl;
use crate::model::def::should_run_phase;

/// Encapsulates cancel + SinkControl sender for one live stream.
#[derive(Clone)]
pub struct StreamControlHandle {
    cancel: CancellationToken,
    sink_ctrl: Arc<RwLock<mpsc::UnboundedSender<SinkControl>>>,
}

impl StreamControlHandle {
    pub fn new(
        cancel: CancellationToken,
        sink_ctrl: Arc<RwLock<mpsc::UnboundedSender<SinkControl>>>,
    ) -> Self {
        Self { cancel, sink_ctrl }
    }

    pub async fn resume(&self) {
        let _ = self.sink_ctrl.read().await.send(SinkControl::Resume);
    }

    pub async fn pause(&self) {
        let _ = self.sink_ctrl.read().await.send(SinkControl::Pause);
    }

    pub async fn shutdown_async(&self) {
        let _ = self.sink_ctrl.read().await.send(SinkControl::Shutdown);
        self.cancel.cancel();
    }

    /// Sync hard-kill for DROP DDL / Drop paths (no `.await`).
    pub fn force_shutdown_sync(&self) {
        if let Ok(tx) = self.sink_ctrl.try_read() {
            let _ = tx.send(SinkControl::Shutdown);
        }
        self.cancel.cancel();
    }
}

/// Global stream pipeline registry and start/stop commander.
pub struct StreamOrchestrator {
    active_streams: DashMap<String, StreamControlHandle>,
}

impl StreamOrchestrator {
    pub fn new() -> Self {
        Self {
            active_streams: DashMap::new(),
        }
    }

    /// Resume if already live; otherwise spawn a [`PipelineSupervisor`].
    pub async fn start_stream(
        self: &Arc<Self>,
        ctx: StreamContext,
        stream_name: &str,
    ) -> Result<()> {
        ctx.runtime_states
            .update(stream_name, |st| st.clear_user_stopped());

        if let Some(handle) = self.active_streams.get(stream_name) {
            handle.resume().await;
            ctx.runtime_states.update(stream_name, |st| {
                st.set_phase(StreamPhase::Active, "sink resumed");
                st.set_last_error(None);
            });
            info!(stream = %stream_name, "Stream sink resumed");
            return Ok(());
        }

        let def = ctx
            .store
            .get(stream_name)
            .ok_or_else(|| TsdbError::TableNotFound(format!("stream {stream_name}")))?;

        if matches!(
            ctx.runtime_states.phase(stream_name),
            StreamPhase::Completed
        ) {
            return Ok(());
        }

        let source_table = def.source_tables.first().map(|s| s.as_str()).unwrap_or("");
        ctx.runtime_states.ensure(stream_name, source_table);
        ctx.runtime_states.update(stream_name, |st| {
            st.set_phase(StreamPhase::PreparingLog, "starting");
            st.set_last_error(None);
        });

        let cancel_token = CancellationToken::new();
        let (sink_ctrl_tx, _) = mpsc::unbounded_channel::<SinkControl>();
        let ext_sink_ctrl = Arc::new(RwLock::new(sink_ctrl_tx));
        let handle = StreamControlHandle::new(cancel_token.clone(), Arc::clone(&ext_sink_ctrl));

        match self.active_streams.entry(stream_name.to_string()) {
            Entry::Occupied(entry) => {
                entry.get().resume().await;
                ctx.runtime_states.update(stream_name, |st| {
                    st.set_phase(StreamPhase::Active, "sink resumed");
                    st.set_last_error(None);
                });
                return Ok(());
            }
            Entry::Vacant(entry) => {
                entry.insert(handle);
            }
        }

        let orchestrator_ref = Arc::clone(self);
        let supervisor_name = stream_name.to_string();
        let capacity = ctx.queue_capacity().max(1);

        executor::spawn(async move {
            PipelineSupervisor::new(ctx, def, cancel_token, ext_sink_ctrl, capacity)
                .run()
                .await;
            orchestrator_ref.active_streams.remove(&supervisor_name);
        });

        Ok(())
    }

    pub async fn stop_stream(&self, ctx: &StreamContext, stream_name: &str) -> Result<()> {
        ctx.runtime_states
            .update(stream_name, |st| st.mark_user_stopped());
        if let Some(handle) = self.active_streams.get(stream_name) {
            handle.pause().await;
            info!(stream = %stream_name, "Stream sink paused by user");
        }
        Ok(())
    }

    pub async fn abort_stream(&self, stream_name: &str) -> Result<()> {
        if let Some((_, handle)) = self.active_streams.remove(stream_name) {
            handle.shutdown_async().await;
            info!(stream = %stream_name, "Stream aborted and removed from orchestrator");
        }
        Ok(())
    }

    pub fn drop_runtime(&self, stream_name: &str) {
        if let Some((_, handle)) = self.active_streams.remove(stream_name) {
            handle.force_shutdown_sync();
            info!(stream = %stream_name, "Stream runtime dropped synchronously");
        } else {
            warn!(stream = %stream_name, "Drop runtime called but stream was not active");
        }
    }

    /// Alias for [`Self::drop_runtime`].
    pub fn stop(&self, stream_name: &str) {
        self.drop_runtime(stream_name);
    }

    pub fn is_running(&self, stream_name: &str) -> bool {
        self.active_streams.contains_key(stream_name)
    }

    pub async fn resume_all(self: &Arc<Self>, ctx: StreamContext) {
        for def in ctx.store.list() {
            let source_table = def.source_tables.first().map(|s| s.as_str()).unwrap_or("");
            ctx.runtime_states.ensure(&def.name, source_table);

            let st = ctx.runtime_states.get(&def.name);
            if st.user_stopped() || matches!(st.phase(), StreamPhase::Completed) {
                continue;
            }

            if should_run_phase(st.phase()) || matches!(st.phase(), StreamPhase::Failed) {
                if let Err(e) = self.start_stream(ctx.clone(), &def.name).await {
                    warn!(
                        stream = %def.name,
                        error = %e,
                        "System boot: failed to resume stream"
                    );
                }
            }
        }
    }
}

impl Default for StreamOrchestrator {
    fn default() -> Self {
        Self::new()
    }
}

pub type StreamRuntimeManager = StreamOrchestrator;
pub type StreamEngine = StreamOrchestrator;

/// DROP STREAM durable cleanup helpers.
pub struct StreamGarbageCollector;

impl StreamGarbageCollector {
    pub fn drop_capture_progress(
        engine: &LsmEngine,
        progress: &CaptureProgressRegistry,
        stream: &str,
        tables: &[String],
    ) -> Result<()> {
        for table in tables {
            let progress_id = capture_progress_id(stream, table);
            if let Some(t) = engine.get_table(table) {
                let _ = t.gc_retained_wal();
                let _ = t.gc_pinned_files();
            }
            progress.remove(&progress_id)?;
            let _ = engine.unregister_stream_table_capture(stream, table);
        }
        Ok(())
    }

    pub fn mark_inactive(ctx: &StreamContext, stream_name: &str) -> Result<()> {
        ctx.runtime_states.update(stream_name, |st| {
            st.set_phase(StreamPhase::Inactive, "pending");
            st.set_last_error(None);
        });
        Ok(())
    }
}

pub fn drop_stream_capture_progress(
    engine: &LsmEngine,
    progress: &CaptureProgressRegistry,
    stream: &str,
    tables: &[String],
) -> Result<()> {
    StreamGarbageCollector::drop_capture_progress(engine, progress, stream, tables)
}

pub fn mark_inactive(ctx: &StreamContext, stream_name: &str) -> Result<()> {
    StreamGarbageCollector::mark_inactive(ctx, stream_name)
}
