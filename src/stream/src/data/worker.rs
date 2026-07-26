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

//! WAL-backed Sink worker: 2PC + pending replay.
//!
//! - [`TxnReplayBuffer`]: LSN merge, Flush degrade, and sink replay.
//! - [`RetryEngine`]: exponential backoff for transient recovery.
//! - [`WorkerState`]: single path for status reporting.

use std::sync::Arc;
use std::time::Duration;

use common::{LsnRange, Result, TsdbError};
use monots_storage::LsmEngine;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::barrier::{CheckpointBarrier, PipelineEvent};
use crate::connector::{SinkConnector, SinkError};
use crate::control::config::SinkWorkerConfig;
use crate::control::progress::SinkCommitted;
use crate::model::event::DataEvent;

/// Worker control from the orchestrator.
#[derive(Debug)]
pub enum SinkControl {
    Pause,
    Resume,
    Shutdown,
}

/// Observable SinkWorker FSM states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerState {
    Starting,
    Sinking,
    Paused,
    Recovering,
}

impl WorkerState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Sinking => "sinking",
            Self::Paused => "paused",
            Self::Recovering => "recovering",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SinkWorkerStatus {
    pub state: WorkerState,
    pub last_error: Option<String>,
    pub pending_events: usize,
    pub commit_lsn: Option<u64>,
}

/// Exponential backoff for transient sink recovery.
#[derive(Debug, Clone)]
struct RetryEngine {
    delay: Duration,
    cap: Duration,
}

impl RetryEngine {
    fn from_config(cfg: &SinkWorkerConfig) -> Self {
        Self {
            delay: cfg.transient_backoff_start,
            cap: cfg.transient_backoff_cap,
        }
    }

    fn wait(&self) -> Duration {
        self.delay
    }

    fn advance(&mut self) {
        self.delay = (self.delay.saturating_mul(2)).min(self.cap);
    }
}

/// 2PC replay buffer: LSN cursors + FlushFile paths (Arrow dropped intentionally).
#[derive(Debug, Default)]
struct TxnReplayBuffer {
    inserts: Vec<LsnRange>,
    flush_files: Vec<DataEvent>,
}

impl TxnReplayBuffer {
    fn is_fully_covered(inner: LsnRange, cover: LsnRange) -> bool {
        inner.base_lsn >= cover.base_lsn && inner.max_lsn <= cover.max_lsn
    }

    /// Record an event; merge adjacent Insert LSNs; FlushFile degrades covered inserts.
    fn record(&mut self, event: &DataEvent, acked_lsn: u64) {
        match event {
            DataEvent::Insert { lsn, .. } => {
                if let Some(last) = self.inserts.last_mut() {
                    if last.max_lsn + 1 == lsn.base_lsn {
                        last.max_lsn = lsn.max_lsn;
                        return;
                    }
                }
                self.inserts.push(*lsn);
            }
            DataEvent::FlushFile { lsn, .. } => {
                self.inserts.retain(|r| !Self::is_fully_covered(*r, *lsn));
                if lsn.max_lsn > acked_lsn {
                    self.flush_files.push(event.clone());
                }
            }
            DataEvent::Watermark { .. } => {}
        }
    }

    /// Cursor-aligned FlushFile: degrade covered WAL ranges; keep file for commit-retry.
    fn record_degrade(&mut self, event: &DataEvent) {
        let DataEvent::FlushFile { lsn, .. } = event else {
            return;
        };
        self.inserts.retain(|r| !Self::is_fully_covered(*r, *lsn));
        self.flush_files.push(event.clone());
    }

    fn clear(&mut self) {
        self.inserts.clear();
        self.flush_files.clear();
    }

    fn max_flush_file_lsn(&self) -> Option<u64> {
        self.flush_files.iter().map(DataEvent::max_lsn).max()
    }

    fn len(&self) -> usize {
        self.inserts.len() + self.flush_files.len()
    }

    /// Replay pending files then deferred Insert LSNs into the sink.
    async fn replay_into(
        &self,
        connector: &mut dyn SinkConnector,
        cancel: &CancellationToken,
    ) -> std::result::Result<(), SinkError> {
        for file_event in &self.flush_files {
            Self::write_respecting_cancel(connector, file_event, cancel).await?;
        }
        for lsn_range in &self.inserts {
            let deferred = DataEvent::insert_deferred(*lsn_range);
            Self::write_respecting_cancel(connector, &deferred, cancel).await?;
        }
        Ok(())
    }

    async fn write_respecting_cancel(
        connector: &mut dyn SinkConnector,
        event: &DataEvent,
        cancel: &CancellationToken,
    ) -> std::result::Result<(), SinkError> {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Ok(()),
            res = connector.write(event) => res,
        }
    }
}

pub struct SinkWorker {
    stream: String,
    table: String,
    connector: Box<dyn SinkConnector>,
    ctrl_rx: mpsc::UnboundedReceiver<SinkControl>,
    data_rx: mpsc::Receiver<PipelineEvent>,
    progress_tx: mpsc::UnboundedSender<SinkCommitted>,
    cancel: CancellationToken,
    buffer: TxnReplayBuffer,
    /// Durable high-water mark last reported via [`SinkCommitted`].
    acked_lsn: u64,
    state: WorkerState,
    config: SinkWorkerConfig,
    on_status: Option<Box<dyn Fn(SinkWorkerStatus) + Send>>,
}

impl SinkWorker {
    pub fn new(
        stream: impl Into<String>,
        table: impl Into<String>,
        connector: Box<dyn SinkConnector>,
        ctrl_rx: mpsc::UnboundedReceiver<SinkControl>,
        data_rx: mpsc::Receiver<PipelineEvent>,
        progress_tx: mpsc::UnboundedSender<SinkCommitted>,
        cancel: CancellationToken,
        _engine: Arc<LsmEngine>,
        recovered_acked_lsn: u64,
    ) -> Self {
        Self {
            stream: stream.into(),
            table: table.into(),
            connector,
            ctrl_rx,
            data_rx,
            progress_tx,
            cancel,
            buffer: TxnReplayBuffer::default(),
            acked_lsn: recovered_acked_lsn,
            state: WorkerState::Starting,
            config: SinkWorkerConfig::default(),
            on_status: None,
        }
    }

    pub fn with_config(mut self, config: SinkWorkerConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_status_hook(mut self, hook: impl Fn(SinkWorkerStatus) + Send + 'static) -> Self {
        self.on_status = Some(Box::new(hook));
        self
    }

    fn transition_state(
        &mut self,
        new_state: WorkerState,
        last_error: Option<String>,
        commit_lsn: Option<u64>,
    ) {
        self.state = new_state.clone();
        if let Some(hook) = &self.on_status {
            hook(SinkWorkerStatus {
                state: new_state,
                last_error,
                pending_events: self.buffer.len(),
                commit_lsn,
            });
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let result = self.run_inner().await;
        self.graceful_teardown().await;
        result
    }

    async fn run_inner(&mut self) -> Result<()> {
        info!(stream = %self.stream, table = %self.table, "SinkWorker started");
        self.transition_state(WorkerState::Starting, None, None);
        if let Err(e) = self.connector.begin_txn().await {
            self.recover_from_transient_error(e).await?;
        }
        self.transition_state(WorkerState::Sinking, None, None);

        // Idle keep-alive: fires only when no data arrives for heartbeat_interval.
        let mut heartbeat = tokio::time::interval(self.config.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await; // consume immediate first tick

        loop {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => break,

                ctrl = self.ctrl_rx.recv() => match ctrl {
                    None | Some(SinkControl::Shutdown) => break,
                    Some(SinkControl::Pause) => {
                        if self.state != WorkerState::Paused {
                            self.transition_state(WorkerState::Paused, None, None);
                        }
                    }
                    Some(SinkControl::Resume) => {
                        if self.state == WorkerState::Paused {
                            self.transition_state(WorkerState::Sinking, None, None);
                            heartbeat.reset();
                        }
                    }
                },

                _ = heartbeat.tick(), if self.state == WorkerState::Sinking => {
                    let ping_res =
                        tokio::time::timeout(self.config.ping_timeout, self.connector.ping())
                            .await;
                    let is_dead = match ping_res {
                        Ok(Ok(())) => None,
                        Ok(Err(e)) => Some(e),
                        Err(_) => Some(SinkError::Transient("ping timed out".into())),
                    };
                    if let Some(e) = is_dead {
                        warn!(
                            stream = %self.stream,
                            error = %e,
                            "Idle heartbeat failed or timed out; recovering sink session"
                        );
                        self.recover_from_transient_error(e).await?;
                        heartbeat.reset();
                    }
                }

                msg = self.data_rx.recv(), if self.state == WorkerState::Sinking => {
                    let Some(event) = msg else { break; };
                    self.handle_event(event).await?;
                    // Live traffic proves the connection; skip redundant pings.
                    heartbeat.reset();
                }
            }
        }
        Ok(())
    }

    /// Timed abort + close so cancel/teardown never hangs on a blackholed peer.
    async fn graceful_teardown(&mut self) {
        info!(stream = %self.stream, "SinkWorker initiating graceful shutdown");

        if tokio::time::timeout(self.config.abort_timeout, self.connector.abort_txn())
            .await
            .is_err()
        {
            warn!(
                stream = %self.stream,
                timeout_secs = self.config.abort_timeout.as_secs(),
                "Teardown: abort_txn timed out"
            );
        }

        if tokio::time::timeout(self.config.close_timeout, self.connector.close())
            .await
            .is_err()
        {
            warn!(
                stream = %self.stream,
                timeout_secs = self.config.close_timeout.as_secs(),
                "Teardown: close timed out"
            );
        }

        info!(stream = %self.stream, "SinkWorker shutdown complete");
    }

    async fn handle_event(&mut self, event: PipelineEvent) -> Result<()> {
        match event {
            PipelineEvent::Data(data) => {
                if let Err(e) = TxnReplayBuffer::write_respecting_cancel(
                    &mut *self.connector,
                    &data,
                    &self.cancel,
                )
                .await
                {
                    self.recover_from_transient_error(e).await?;
                }
                self.buffer.record(&data, self.acked_lsn);
            }
            PipelineEvent::Stale { table, event } => {
                debug!(
                    %table,
                    lsn = event.max_lsn(),
                    "Stale: no write; GC on Barrier commit"
                );
            }
            PipelineEvent::Degrade { table, event } => {
                info!(
                    %table,
                    lsn = event.max_lsn(),
                    "Degrade: WAL→file registered; GC on Barrier commit"
                );
                self.buffer.record_degrade(&event);
            }
            PipelineEvent::Barrier(barrier) => {
                self.execute_2pc_commit(barrier).await?;
            }
        }
        Ok(())
    }

    async fn execute_2pc_commit(&mut self, barrier: CheckpointBarrier) -> Result<()> {
        // Commit may fail repeatedly: recover (backoff → abort → begin → replay) then retry.
        loop {
            let commit_res = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Ok(()),
                res = self.connector.commit_txn() => res,
            };

            match commit_res {
                Ok(()) => break,
                Err(SinkError::Fatal(msg)) => {
                    return Err(TsdbError::Storage(format!(
                        "Fatal sink error on commit: {msg}"
                    )));
                }
                Err(e) => {
                    warn!(
                        stream = %self.stream,
                        error = %e,
                        "Commit failed; recovering transaction before retry"
                    );
                    self.recover_from_transient_error(e).await?;
                    if self.cancel.is_cancelled() {
                        return Ok(());
                    }
                }
            }
        }

        let progress_lsn = self.progress_after_commit(&barrier);
        let file_count = barrier.files_to_unlink.len() as u64;
        let table = if barrier.table.is_empty() {
            self.table.clone()
        } else {
            barrier.table.clone()
        };

        // Sole GC point: after successful commit.
        barrier.async_gc_files();

        if let Some(lsn) = progress_lsn {
            self.acked_lsn = lsn;
            let _ = self.progress_tx.send(SinkCommitted {
                stream: self.stream.clone(),
                table,
                lsn,
                files: file_count,
            });
        }

        self.buffer.clear();
        self.transition_state(WorkerState::Sinking, None, progress_lsn);
        if let Err(e) = self.connector.begin_txn().await {
            self.recover_from_transient_error(e).await?;
        }
        Ok(())
    }

    /// max(watermark hint, newly recorded Parquet), advancing past acked.
    fn progress_after_commit(&self, barrier: &CheckpointBarrier) -> Option<u64> {
        let mut lsn = barrier.progress_lsn();
        if let Some(p) = self.buffer.max_flush_file_lsn() {
            lsn = Some(lsn.map_or(p, |w| w.max(p)));
        }
        lsn.filter(|&v| v > self.acked_lsn)
    }

    /// Backoff → abort → begin → buffer.replay until success or cancel/fatal.
    async fn recover_from_transient_error(&mut self, err: SinkError) -> Result<()> {
        if let SinkError::Fatal(msg) = err {
            return Err(TsdbError::Storage(format!("Fatal sink error: {msg}")));
        }

        warn!(
            stream = %self.stream,
            error = %err,
            "Sink interrupted; starting exponential backoff recovery"
        );
        self.transition_state(WorkerState::Recovering, Some(err.to_string()), None);

        let mut retry = RetryEngine::from_config(&self.config);
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                _ = tokio::time::sleep(retry.wait()) => {}
            }
            retry.advance();

            // Dead peer: abort must not hang recovery; log timeout for ops.
            if tokio::time::timeout(self.config.abort_timeout, self.connector.abort_txn())
                .await
                .is_err()
            {
                warn!(
                    stream = %self.stream,
                    "Recovery: abort_txn timed out; proceeding to force new txn"
                );
            }
            if let Err(e) = self.connector.begin_txn().await {
                if let SinkError::Fatal(m) = e {
                    return Err(TsdbError::Storage(m));
                }
                continue;
            }

            if let Err(e) = self
                .buffer
                .replay_into(&mut *self.connector, &self.cancel)
                .await
            {
                if let SinkError::Fatal(m) = e {
                    return Err(TsdbError::Storage(m));
                }
                continue;
            }

            info!(stream = %self.stream, "transaction recovered from pending replay");
            self.transition_state(WorkerState::Sinking, None, None);
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_engine_doubles_until_cap() {
        let cfg = SinkWorkerConfig::default();
        let mut retry = RetryEngine::from_config(&cfg);
        assert_eq!(retry.wait(), cfg.transient_backoff_start);
        retry.advance();
        assert_eq!(retry.wait(), Duration::from_millis(200));
        for _ in 0..20 {
            retry.advance();
        }
        assert_eq!(retry.wait(), cfg.transient_backoff_cap);
    }

    #[test]
    fn merges_consecutive_inserts() {
        let mut buf = TxnReplayBuffer::default();
        buf.record(&DataEvent::insert_deferred(LsnRange::new(1, 2)), 0);
        buf.record(&DataEvent::insert_deferred(LsnRange::new(3, 4)), 0);
        assert_eq!(buf.inserts.len(), 1);
        assert_eq!(buf.inserts[0].max_lsn, 4);
    }

    #[test]
    fn parquet_degrades_only_covered_inserts() {
        let mut buf = TxnReplayBuffer::default();
        buf.record(&DataEvent::insert_deferred(LsnRange::new(1, 2)), 0);
        buf.record(&DataEvent::insert_deferred(LsnRange::new(10, 20)), 0);
        buf.record(
            &DataEvent::FlushFile {
                lsn: LsnRange::new(1, 2),
                file_path: "/p/a.parquet".into(),
                rows: 1,
            },
            0,
        );
        assert_eq!(buf.inserts, vec![LsnRange::new(10, 20)]);
        assert_eq!(buf.flush_files.len(), 1);
    }

    #[test]
    fn already_acked_parquet_is_not_recorded() {
        let mut buf = TxnReplayBuffer::default();
        buf.record(
            &DataEvent::FlushFile {
                lsn: LsnRange::new(1, 40),
                file_path: "/p/old.parquet".into(),
                rows: 1,
            },
            40,
        );
        assert!(buf.flush_files.is_empty());
    }

    #[test]
    fn record_degrade_keeps_file_for_retry_and_drops_covered_wal() {
        let mut buf = TxnReplayBuffer::default();
        buf.record(&DataEvent::insert_deferred(LsnRange::new(1, 40)), 40);
        buf.record(&DataEvent::insert_deferred(LsnRange::new(100, 110)), 40);
        let file = DataEvent::FlushFile {
            lsn: LsnRange::new(1, 40),
            file_path: "/p/at-cursor.parquet".into(),
            rows: 1,
        };
        buf.record_degrade(&file);
        assert_eq!(buf.inserts, vec![LsnRange::new(100, 110)]);
        assert_eq!(buf.flush_files.len(), 1);
    }
}
