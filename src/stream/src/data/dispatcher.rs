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

//! Event-driven dispatcher: semantic-boundary checkpoints + cursor-aware EO filtering.
//!
//! - Barriers on Watermark (logical) or fresh FlushFile (physical commit point).
//! - `table_cursors` filter LSM replay after restart: `Stale` / `Degrade` (not sink payloads).
//! - Always `reserve()` a sink slot before `recv()` from sources.

use std::collections::HashMap;

use common::{Result, TsdbError};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::barrier::{CheckpointBarrier, PipelineEvent};
use crate::data::ingress::AsyncSourceGroup;
use crate::model::event::DataEvent;

/// Dispatcher knobs (channel size only; txn boundaries are event-driven).
#[derive(Debug, Clone)]
pub struct DispatchPolicy {
    pub channel_capacity: usize,
}

impl Default for DispatchPolicy {
    fn default() -> Self {
        Self {
            channel_capacity: 1024,
        }
    }
}

pub type BatchPolicy = DispatchPolicy;

pub struct StreamDispatcher {
    default_table: String,
    source_group: AsyncSourceGroup,
    downstream_tx: mpsc::Sender<PipelineEvent>,
    cancel: CancellationToken,
    /// Per-table last committed LSN (seeded from durable progress on restart).
    table_cursors: HashMap<String, u64>,
}

impl StreamDispatcher {
    pub fn new(
        default_table: impl Into<String>,
        source_group: AsyncSourceGroup,
        downstream_tx: mpsc::Sender<PipelineEvent>,
        cancel: CancellationToken,
        _policy: DispatchPolicy,
        recovered_cursors: HashMap<String, u64>,
    ) -> Self {
        Self {
            default_table: default_table.into(),
            source_group,
            downstream_tx,
            cancel,
            table_cursors: recovered_cursors,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        info!(
            table = %self.default_table,
            cursors = self.table_cursors.len(),
            "StreamDispatcher started with cursor awareness"
        );

        let mut barrier = CheckpointBarrier::new(&self.default_table);

        loop {
            if self.cancel.is_cancelled() {
                self.source_group.teardown();
                return Ok(());
            }

            tokio::select! {
                biased;

                _ = self.cancel.cancelled() => {
                    self.source_group.teardown();
                    return Ok(());
                }

                // Backpressure: hold a sink slot before pulling upstream.
                permit = self.downstream_tx.reserve() => {
                    let permit = permit.map_err(|_| {
                        TsdbError::Storage("Sink worker unexpectedly dropped".into())
                    })?;

                    let routed = tokio::select! {
                        biased;
                        _ = self.cancel.cancelled() => {
                            drop(permit);
                            self.source_group.teardown();
                            return Ok(());
                        }
                        item = self.source_group.recv() => item,
                    };

                    let current_cursor = self
                        .table_cursors
                        .get(routed.table.as_ref())
                        .copied()
                        .unwrap_or(0);
                    let event_max_lsn = routed.event.max_lsn();

                    // Idempotent filter against durable / in-session cursor.
                    if let DataEvent::FlushFile { lsn, ref file_path, .. } = routed.event {
                        if lsn.max_lsn < current_cursor {
                            debug!(
                                table = %routed.table,
                                lsn = lsn.max_lsn,
                                cursor = current_cursor,
                                "Parquet stale (< cursor)"
                            );
                            barrier.record_unlink(routed.table.as_ref(), file_path.as_ref());
                            permit.send(PipelineEvent::Stale {
                                table: routed.table.to_string(),
                                event: routed.event,
                            });
                            self.flush_barrier(&mut barrier).await?;
                            continue;
                        } else if lsn.max_lsn == current_cursor {
                            debug!(
                                table = %routed.table,
                                lsn = lsn.max_lsn,
                                "Parquet at cursor — Degrade (WAL→file, retry payload)"
                            );
                            barrier.record_unlink(routed.table.as_ref(), file_path.as_ref());
                            permit.send(PipelineEvent::Degrade {
                                table: routed.table.to_string(),
                                event: routed.event,
                            });
                            self.flush_barrier(&mut barrier).await?;
                            continue;
                        }
                    } else if let DataEvent::Insert { lsn, .. } = routed.event {
                        if lsn.max_lsn <= current_cursor {
                            drop(permit);
                            continue;
                        }
                    }

                    // Fresh data (max_lsn > cursor): advance local cursor monotonically.
                    self.table_cursors.insert(
                        routed.table.to_string(),
                        current_cursor.max(event_max_lsn),
                    );

                    barrier.record_event(routed.table.as_ref(), &routed.event);

                    let is_watermark = matches!(routed.event, DataEvent::Watermark { .. });
                    let is_parquet = matches!(routed.event, DataEvent::FlushFile { .. });

                    if is_watermark {
                        // Logical barrier: seal without sink write.
                        drop(permit);
                        self.flush_barrier(&mut barrier).await?;
                        continue;
                    }

                    permit.send(PipelineEvent::Data(routed.event));

                    if is_parquet {
                        // Fresh Parquet: force Sink commit + post-commit GC.
                        self.flush_barrier(&mut barrier).await?;
                    }
                }
            }
        }
    }

    async fn flush_barrier(&self, barrier: &mut CheckpointBarrier) -> Result<()> {
        if barrier.is_empty() {
            return Ok(());
        }
        let flushed = std::mem::replace(barrier, CheckpointBarrier::new(&self.default_table));
        if self
            .downstream_tx
            .send(PipelineEvent::Barrier(flushed))
            .await
            .is_err()
        {
            return Err(TsdbError::Storage(
                "Sink worker dropped during barrier emission".into(),
            ));
        }
        Ok(())
    }
}
