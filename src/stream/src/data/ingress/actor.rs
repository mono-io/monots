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

//! Ingress Actor: storage callback receiver and CaptureBuffer mutator.

use std::sync::Arc;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;
use tracing::{debug, info};

use super::source::SharedContext;
use crate::data::barrier::async_gc_paths;
use crate::model::event::DataEvent;

/// Commands from capture hooks into the Actor.
pub enum IngressCommand {
    Insert(DataEvent),
    Watermark(DataEvent),
    FlushFile(DataEvent),
    CompactFile(DataEvent),
    /// Fence for [`super::source::StreamSource::wait_idle`].
    DrainBarrier(oneshot::Sender<()>),
}

pub struct IngressActor {
    ctx: Arc<SharedContext>,
    inbox: UnboundedReceiver<IngressCommand>,
}

impl IngressActor {
    pub fn new(ctx: Arc<SharedContext>, inbox: UnboundedReceiver<IngressCommand>) -> Self {
        Self { ctx, inbox }
    }

    pub async fn run(mut self) {
        info!(stream = %self.ctx.stream_id, table = %self.ctx.table, "IngressActor started");

        while let Some(cmd) = self.inbox.recv().await {
            self.process(cmd);

            // Batch-drain pending cmds; one notify after the burst.
            while let Ok(next) = self.inbox.try_recv() {
                self.process(next);
            }
            self.ctx.signal_dispatch();
        }

        info!(stream = %self.ctx.stream_id, table = %self.ctx.table, "IngressActor stopped");
    }

    #[inline]
    fn process(&self, cmd: IngressCommand) {
        match cmd {
            IngressCommand::Insert(event) => {
                self.ctx.buffer.write().push_insert(event);
            }
            IngressCommand::Watermark(event) => {
                self.ctx.buffer.write().push_watermark(event);
            }
            IngressCommand::FlushFile(event) => {
                let degraded = self.ctx.apply_flush(event);
                if degraded.dropped_inserts > 0 || degraded.dropped_watermarks > 0 {
                    debug!(
                        inserts = degraded.dropped_inserts,
                        watermarks = degraded.dropped_watermarks,
                        "Degraded memory events to FlushFile"
                    );
                }
            }
            IngressCommand::CompactFile(event) => {
                let gc_paths = self.ctx.apply_compact(event).gc_paths;
                if !gc_paths.is_empty() {
                    debug!(
                        count = gc_paths.len(),
                        "Async GC orphaned SSTs after compaction"
                    );
                    async_gc_paths(gc_paths);
                }
            }
            IngressCommand::DrainBarrier(reply_tx) => {
                let _ = reply_tx.send(());
            }
        }
    }
}
