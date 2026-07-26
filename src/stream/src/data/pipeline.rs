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

//! Assembles StreamDispatcher + SinkWorker.

use std::collections::HashMap;
use std::sync::Arc;

use common::{Result, TsdbError};
use monots_storage::LsmEngine;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::barrier::PipelineEvent;
use super::dispatcher::{DispatchPolicy, StreamDispatcher};
use super::worker::{SinkControl, SinkWorker, SinkWorkerStatus};
use crate::connector::SinkConnector;
use crate::control::config::SinkWorkerConfig;
use crate::control::progress::SinkCommitted;
use crate::data::ingress::{AsyncSourceGroup, StreamSource};

pub type BatchPolicy = DispatchPolicy;

pub struct StreamPipeline {
    stream: String,
    table: String,
    sources: Vec<(String, Arc<StreamSource>)>,
    engine: Arc<LsmEngine>,
    sink: Box<dyn SinkConnector>,
    progress_tx: mpsc::UnboundedSender<SinkCommitted>,
    sink_ctrl_rx: mpsc::UnboundedReceiver<SinkControl>,
    sink_ctrl_shutdown: mpsc::UnboundedSender<SinkControl>,
    cancel: CancellationToken,
    policy: DispatchPolicy,
    recovered_cursors: HashMap<String, u64>,
    sink_worker: SinkWorkerConfig,
    worker_status_hook: Option<Box<dyn Fn(SinkWorkerStatus) + Send>>,
}

impl StreamPipeline {
    pub fn new(
        stream: impl Into<String>,
        table: impl Into<String>,
        sources: Vec<(String, Arc<StreamSource>)>,
        engine: Arc<LsmEngine>,
        sink: Box<dyn SinkConnector>,
        progress_tx: mpsc::UnboundedSender<SinkCommitted>,
        sink_ctrl_tx: mpsc::UnboundedSender<SinkControl>,
        sink_ctrl_rx: mpsc::UnboundedReceiver<SinkControl>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            stream: stream.into(),
            table: table.into(),
            sources,
            engine,
            sink,
            progress_tx,
            sink_ctrl_rx,
            sink_ctrl_shutdown: sink_ctrl_tx,
            cancel,
            policy: DispatchPolicy::default(),
            recovered_cursors: HashMap::new(),
            sink_worker: SinkWorkerConfig::default(),
            worker_status_hook: None,
        }
    }

    pub fn with_policy(mut self, policy: DispatchPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Durable per-table acked LSN cursors restored on restart (Exactly-Once filter seed).
    pub fn with_recovered_cursors(mut self, cursors: HashMap<String, u64>) -> Self {
        self.recovered_cursors = cursors;
        self
    }

    pub fn with_sink_worker_config(mut self, config: SinkWorkerConfig) -> Self {
        self.sink_worker = config;
        self
    }

    pub fn with_worker_status_hook(
        mut self,
        hook: impl Fn(SinkWorkerStatus) + Send + 'static,
    ) -> Self {
        self.worker_status_hook = Some(Box::new(hook));
        self
    }

    pub async fn run(self) -> Result<()> {
        Self::execute(
            self.stream,
            self.table,
            self.sources,
            self.sink,
            self.progress_tx,
            self.sink_ctrl_rx,
            self.sink_ctrl_shutdown,
            self.cancel,
            self.policy.channel_capacity.max(1),
            self.engine,
            self.policy,
            self.recovered_cursors,
            self.sink_worker,
            self.worker_status_hook,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn execute(
        stream: String,
        table: String,
        sources: Vec<(String, Arc<StreamSource>)>,
        connector: Box<dyn SinkConnector>,
        progress_tx: mpsc::UnboundedSender<SinkCommitted>,
        sink_ctrl_rx: mpsc::UnboundedReceiver<SinkControl>,
        sink_ctrl_shutdown: mpsc::UnboundedSender<SinkControl>,
        cancel: CancellationToken,
        channel_capacity: usize,
        engine: Arc<LsmEngine>,
        policy: DispatchPolicy,
        recovered_cursors: HashMap<String, u64>,
        sink_worker: SinkWorkerConfig,
        worker_status_hook: Option<Box<dyn Fn(SinkWorkerStatus) + Send>>,
    ) -> Result<()> {
        let (data_tx, data_rx) = mpsc::channel::<PipelineEvent>(channel_capacity);

        let recovered_acked = recovered_cursors.get(&table).copied().unwrap_or(0);
        let source_group = AsyncSourceGroup::new(sources);
        let dispatcher = StreamDispatcher::new(
            table.clone(),
            source_group,
            data_tx,
            cancel.clone(),
            policy,
            recovered_cursors,
        );

        let mut worker = SinkWorker::new(
            stream.clone(),
            table,
            connector,
            sink_ctrl_rx,
            data_rx,
            progress_tx,
            cancel.clone(),
            engine,
            recovered_acked,
        )
        .with_config(sink_worker);
        if let Some(hook) = worker_status_hook {
            worker = worker.with_status_hook(move |st| hook(st));
        }

        let disp_fut = tokio::spawn(dispatcher.run());
        let work_fut = tokio::spawn(worker.run());
        tokio::pin!(disp_fut);
        tokio::pin!(work_fut);

        let (disp_res, work_res) = tokio::select! {
            res = &mut disp_fut => {
                cancel.cancel();
                let _ = sink_ctrl_shutdown.send(SinkControl::Shutdown);
                (
                    res.unwrap_or_else(|e| Err(TsdbError::Storage(format!("dispatcher join: {e}")))),
                    work_fut.await.unwrap_or_else(|e| Err(TsdbError::Storage(format!("worker join: {e}")))),
                )
            }
            res = &mut work_fut => {
                cancel.cancel();
                let _ = sink_ctrl_shutdown.send(SinkControl::Shutdown);
                (
                    disp_fut.await.unwrap_or_else(|e| Err(TsdbError::Storage(format!("dispatcher join: {e}")))),
                    res.unwrap_or_else(|e| Err(TsdbError::Storage(format!("worker join: {e}")))),
                )
            }
        };

        disp_res.and(work_res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{NoopSink, SinkConnector, SinkError};
    use crate::data::barrier::{CheckpointBarrier, PipelineEvent};
    use crate::model::event::DataEvent;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use common::LsnRange;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    struct CountingSink {
        writes: Arc<AtomicUsize>,
        commits: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl SinkConnector for CountingSink {
        async fn begin_txn(&mut self) -> std::result::Result<(), SinkError> {
            Ok(())
        }
        async fn write(&mut self, _event: &DataEvent) -> std::result::Result<(), SinkError> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn commit_txn(&mut self) -> std::result::Result<(), SinkError> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn abort_txn(&mut self) -> std::result::Result<(), SinkError> {
            Ok(())
        }
    }

    fn rb() -> arrow::record_batch::RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64])),
                Arc::new(Int64Array::from(vec![1_i64])),
            ],
        )
        .unwrap()
    }

    fn dummy_engine() -> Arc<LsmEngine> {
        let tmp = tempfile::tempdir().unwrap();
        Arc::new(LsmEngine::new(tmp.path()).unwrap())
    }

    #[tokio::test]
    async fn barrier_gc_unlinks_once() {
        let tmp = tempfile::tempdir().unwrap();
        let link = tmp.path().join("1-2-2-0-0.parquet");
        std::fs::write(&link, b"x").unwrap();

        let mut sink = CountingSink {
            writes: Arc::new(AtomicUsize::new(0)),
            commits: Arc::new(AtomicUsize::new(0)),
        };
        let (tx, mut rx) = mpsc::channel(8);
        let data = DataEvent::FlushFile {
            lsn: LsnRange::new(2, 2),
            file_path: link.to_string_lossy().into_owned().into(),
            rows: 1,
        };
        tx.send(PipelineEvent::Data(data.clone())).await.unwrap();
        let mut barrier = CheckpointBarrier::new("t0");
        barrier.record_event("t0", &data);
        tx.send(PipelineEvent::Barrier(barrier)).await.unwrap();
        drop(tx);

        sink.begin_txn().await.unwrap();
        while let Some(cmd) = rx.recv().await {
            match cmd {
                PipelineEvent::Data(event) => sink.write(&event).await.unwrap(),
                PipelineEvent::Barrier(barrier) => {
                    sink.commit_txn().await.unwrap();
                    barrier.async_gc_files();
                    tokio::time::sleep(Duration::from_millis(80)).await;
                }
                PipelineEvent::Stale { .. } | PipelineEvent::Degrade { .. } => {}
            }
        }
        assert!(!link.exists());
    }

    #[tokio::test]
    async fn pipeline_count_barrier_commits_and_acks() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("cdc_streams").join("s1").join("t0");
        let flush = dir.join("pending").join("flush");
        std::fs::create_dir_all(&flush).unwrap();
        let link = flush.join("10-1-1-0-0.parquet");
        std::fs::write(&link, b"x").unwrap();

        let source = Arc::new(StreamSource::open("s1", "t0", &dir).unwrap());
        source.push_flush_bulk(DataEvent::FlushFile {
            lsn: LsnRange::new(1, 1),
            file_path: link.to_string_lossy().into_owned().into(),
            rows: 1,
        });
        source.push_insert(DataEvent::insert(LsnRange::new(2, 2), vec![rb()]));
        source.wait_idle();

        let writes = Arc::new(AtomicUsize::new(0));
        let commits = Arc::new(AtomicUsize::new(0));
        let sink = CountingSink {
            writes: Arc::clone(&writes),
            commits: Arc::clone(&commits),
        };
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let (sink_ctrl_tx, sink_ctrl_rx) = mpsc::unbounded_channel();
        let pipeline = StreamPipeline::new(
            "s1",
            "t0",
            vec![("t0".into(), Arc::clone(&source))],
            dummy_engine(),
            Box::new(sink),
            progress_tx,
            sink_ctrl_tx,
            sink_ctrl_rx,
            cancel.clone(),
        )
        .with_policy(DispatchPolicy {
            channel_capacity: 64,
        });

        let handle = tokio::spawn(async move { pipeline.run().await });
        let mut got_progress = false;
        for _ in 0..100 {
            if commits.load(Ordering::SeqCst) >= 1 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                if !link.exists() {
                    if let Ok(p) = progress_rx.try_recv() {
                        assert_eq!(p.lsn, 1);
                        got_progress = true;
                    }
                    if got_progress {
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cancel.cancel();
        let _ = handle.await.unwrap();

        assert!(commits.load(Ordering::SeqCst) >= 1);
        assert!(writes.load(Ordering::SeqCst) >= 2);
        assert!(!link.exists());
        assert!(got_progress);
        let _ = NoopSink::default();
    }

    #[tokio::test]
    async fn pause_requeues_via_channel_backpressure() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("cdc").join("t0");
        let flush = dir.join("pending").join("flush");
        std::fs::create_dir_all(&flush).unwrap();
        let link = flush.join("10-5-5-0-0.parquet");
        std::fs::write(&link, b"x").unwrap();

        let source = Arc::new(StreamSource::open("s1", "t0", &dir).unwrap());
        source.push_flush_bulk(DataEvent::FlushFile {
            lsn: LsnRange::new(5, 5),
            file_path: link.to_string_lossy().into_owned().into(),
            rows: 1,
        });
        source.wait_idle();

        struct HangSink;
        #[async_trait::async_trait]
        impl SinkConnector for HangSink {
            async fn begin_txn(&mut self) -> std::result::Result<(), SinkError> {
                Ok(())
            }
            async fn write(&mut self, _: &DataEvent) -> std::result::Result<(), SinkError> {
                std::future::pending::<()>().await;
                Ok(())
            }
            async fn commit_txn(&mut self) -> std::result::Result<(), SinkError> {
                Ok(())
            }
            async fn abort_txn(&mut self) -> std::result::Result<(), SinkError> {
                Ok(())
            }
        }

        let (progress_tx, _progress_rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let (sink_ctrl_tx, sink_ctrl_rx) = mpsc::unbounded_channel();
        let pipeline = StreamPipeline::new(
            "s1",
            "t0",
            vec![("t0".into(), Arc::clone(&source))],
            dummy_engine(),
            Box::new(HangSink),
            progress_tx,
            sink_ctrl_tx,
            sink_ctrl_rx,
            cancel.clone(),
        )
        .with_policy(DispatchPolicy {
            channel_capacity: 1,
        });

        let handle = tokio::spawn(async move { pipeline.run().await });
        tokio::time::sleep(Duration::from_millis(80)).await;
        cancel.cancel();
        let _ = handle.await.unwrap();

        assert!(link.exists());
    }

    #[tokio::test]
    async fn transient_then_success() {
        struct Flaky {
            attempts: Mutex<usize>,
        }
        #[async_trait::async_trait]
        impl SinkConnector for Flaky {
            async fn begin_txn(&mut self) -> std::result::Result<(), SinkError> {
                Ok(())
            }
            async fn write(&mut self, _: &DataEvent) -> std::result::Result<(), SinkError> {
                let mut n = self.attempts.lock().unwrap();
                *n += 1;
                if *n < 3 {
                    Err(SinkError::Transient("blip".into()))
                } else {
                    Ok(())
                }
            }
            async fn commit_txn(&mut self) -> std::result::Result<(), SinkError> {
                Ok(())
            }
            async fn abort_txn(&mut self) -> std::result::Result<(), SinkError> {
                Ok(())
            }
        }

        let mut sink = Flaky {
            attempts: Mutex::new(0),
        };
        let mut backoff = Duration::from_millis(1);
        let event = DataEvent::insert(LsnRange::single(1), Vec::new());
        loop {
            match sink.write(&event).await {
                Ok(()) => break,
                Err(SinkError::Transient(_)) => {
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
                Err(e) => panic!("{e}"),
            }
        }
        assert_eq!(*sink.attempts.lock().unwrap(), 3);
    }
}
