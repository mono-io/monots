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

//! Pipeline supervisor: lifecycle state machine for Dispatcher + SinkWorker.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::{StreamPhase, TsdbError};
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::connector::build_sink_with_engine;
use crate::control::context::StreamContext;
use crate::control::executor;
use crate::control::progress::{capture_progress_id, ProgressManager, SinkCommitted};
use crate::data::{BatchPolicy, SinkControl, StreamPipeline};
use crate::model::state::RuntimeStateRegistry;
use crate::model::StreamDef;

const BACKOFF_INITIAL: Duration = Duration::from_secs(5);
const BACKOFF_MAX: Duration = Duration::from_secs(60);

/// Backward-compatible alias for the backoff cap.
pub const STREAM_AUTO_RESTART_INTERVAL: Duration = BACKOFF_MAX;

#[derive(Debug)]
pub enum SupervisorError {
    BootstrapFailed(TsdbError),
    PipelineFatal(TsdbError),
}

/// Owns the retry loop and per-attempt channel wiring for one stream.
pub struct PipelineSupervisor {
    ctx: StreamContext,
    def: StreamDef,
    cancel: CancellationToken,
    /// Orchestrator handle; each attempt publishes a fresh SinkControl sender here.
    ext_sink_ctrl: Arc<RwLock<mpsc::UnboundedSender<SinkControl>>>,
    channel_capacity: usize,
}

/// Public alias kept for existing call sites / docs.
pub type StreamSupervisor = PipelineSupervisor;

impl PipelineSupervisor {
    pub fn new(
        ctx: StreamContext,
        def: StreamDef,
        cancel: CancellationToken,
        ext_sink_ctrl: Arc<RwLock<mpsc::UnboundedSender<SinkControl>>>,
        channel_capacity: usize,
    ) -> Self {
        Self {
            ctx,
            def,
            cancel,
            ext_sink_ctrl,
            channel_capacity: channel_capacity.max(1),
        }
    }

    /// Daemon loop: attempt → typed failure → exponential backoff → retry.
    pub async fn run(mut self) {
        let stream_name = self.def.name.clone();
        let mut backoff = BACKOFF_INITIAL;

        loop {
            if self.cancel.is_cancelled() || self.is_user_stopped() {
                break;
            }

            match self.execute_attempt().await {
                Ok(()) => break,
                Err(err) => {
                    self.record_failure(&err);
                    if self.cancel.is_cancelled() || self.is_user_stopped() {
                        break;
                    }

                    info!(
                        stream = %stream_name,
                        backoff_secs = backoff.as_secs(),
                        "Pipeline failed. Scheduling auto-restart."
                    );

                    tokio::select! {
                        biased;
                        _ = self.cancel.cancelled() => break,
                        _ = tokio::time::sleep(backoff) => {}
                    }

                    if self.cancel.is_cancelled() || self.is_user_stopped() {
                        break;
                    }

                    self.set_restarting_phase();
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
            }
        }

        info!(stream = %stream_name, "Pipeline supervisor safely terminated.");
    }

    async fn execute_attempt(&mut self) -> Result<(), SupervisorError> {
        let stream_name = &self.def.name;
        info!(
            stream = %stream_name,
            "Pipeline attempt starting (Dispatcher + SinkWorker)"
        );

        let sources_bundle = self.ctx.sources.get_stream(stream_name).ok_or_else(|| {
            SupervisorError::BootstrapFailed(TsdbError::Storage(
                "No CaptureSource registered for stream (Bootstrap incomplete)".into(),
            ))
        })?;
        if sources_bundle.tables.is_empty() {
            return Err(SupervisorError::BootstrapFailed(TsdbError::Storage(
                "No CaptureSource registered for stream (Bootstrap incomplete)".into(),
            )));
        }

        let table_sources: Vec<_> = sources_bundle
            .tables
            .iter()
            .map(|(t, s)| (t.clone(), Arc::clone(s)))
            .collect();

        let def_for_sink = self
            .def
            .clone()
            .with_lake_endpoint(self.ctx.runtime.lake_endpoint.clone());
        let sink = build_sink_with_engine(&def_for_sink, Some(&self.ctx.engine))
            .map_err(SupervisorError::BootstrapFailed)?;

        let (sink_ctrl_tx, sink_ctrl_rx) = mpsc::unbounded_channel::<SinkControl>();
        *self.ext_sink_ctrl.write().await = sink_ctrl_tx.clone();

        let (progress_tx, progress_rx) = mpsc::unbounded_channel::<SinkCommitted>();
        let progress_handle = executor::spawn(Self::progress_committer_task(
            progress_rx,
            Arc::clone(&self.ctx.progress),
            self.ctx.runtime_states.clone(),
        ));

        let table_name = self.def.source_tables.first().cloned().unwrap_or_default();

        let mut recovered_cursors = HashMap::new();
        for (table, _) in &table_sources {
            let progress_id = capture_progress_id(stream_name, table);
            if let Some(p) = self.ctx.progress.progress().get(&progress_id) {
                recovered_cursors.insert(table.clone(), p.acked_lsn);
            }
        }

        let status_states = self.ctx.runtime_states.clone();
        let status_name = stream_name.clone();
        let policy = BatchPolicy {
            channel_capacity: self.channel_capacity,
            ..BatchPolicy::default()
        };

        let pipeline = StreamPipeline::new(
            stream_name.clone(),
            table_name,
            table_sources,
            Arc::clone(&self.ctx.engine),
            sink,
            progress_tx,
            sink_ctrl_tx,
            sink_ctrl_rx,
            self.cancel.child_token(),
        )
        .with_policy(policy)
        .with_recovered_cursors(recovered_cursors)
        .with_sink_worker_config(self.ctx.runtime.sink_worker.clone())
        .with_worker_status_hook(move |st| {
            status_states.update(&status_name, |rs| {
                if let Some(err) = st.last_error {
                    rs.set_last_error(Some(err));
                }
                rs.set_current_step(st.state.as_str());
            });
        });

        let result = pipeline.run().await;
        let _ = progress_handle.await;

        match result {
            Ok(()) => {
                self.set_inactive_phase_if_applicable();
                Ok(())
            }
            Err(e) => {
                error!(stream = %stream_name, error = %e, "Stream pipeline fatally crashed");
                Err(SupervisorError::PipelineFatal(e))
            }
        }
    }

    async fn progress_committer_task(
        mut rx: mpsc::UnboundedReceiver<SinkCommitted>,
        progress_mgr: Arc<ProgressManager>,
        runtime_states: RuntimeStateRegistry,
    ) {
        while let Some(p) = rx.recv().await {
            if let Err(e) = progress_mgr.on_sink_committed(&p.stream, &p.table, p.lsn) {
                warn!(
                    stream = %p.stream,
                    table = %p.table,
                    lsn = p.lsn,
                    error = %e,
                    "Failed to durably persist sink progress"
                );
            }
            runtime_states.update(&p.stream, |st| {
                st.ack_log_lsn(p.lsn);
                st.ack_batch_commit(p.files);
                st.set_phase(StreamPhase::Active, "sinking");
            });
        }
    }

    fn is_user_stopped(&self) -> bool {
        self.ctx.runtime_states.get(&self.def.name).user_stopped()
    }

    fn record_failure(&self, err: &SupervisorError) {
        let msg = match err {
            SupervisorError::BootstrapFailed(e) => format!("Bootstrap Error: {e}"),
            SupervisorError::PipelineFatal(e) => format!("Pipeline Fatal: {e}"),
        };
        self.ctx.runtime_states.update(&self.def.name, |st| {
            if st.user_stopped() {
                st.set_error(StreamPhase::Suspended, msg);
            } else {
                st.set_error(StreamPhase::Failed, msg.clone());
                st.note_auto_restart(msg);
            }
        });
    }

    fn set_restarting_phase(&self) {
        self.ctx.runtime_states.update(&self.def.name, |st| {
            st.set_phase(StreamPhase::PreparingLog, "auto_restarting");
            st.set_last_error(None);
        });
    }

    fn set_inactive_phase_if_applicable(&self) {
        self.ctx.runtime_states.update(&self.def.name, |st| {
            if !matches!(
                st.phase(),
                StreamPhase::Failed | StreamPhase::Suspended | StreamPhase::Completed
            ) {
                st.set_phase(StreamPhase::Inactive, "stopped");
            }
        });
    }
}
