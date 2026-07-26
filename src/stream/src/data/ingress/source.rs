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

//! Stream capture facade.
//!
//! Lifecycle (must not change):
//! `Storage callback → hard_link (sync) → tx.send(IngressCommand)`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use common::{
    BatchOrigin, CaptureFileMeta, CaptureSource, LsnRange, Result, TableCaptureListener, TsdbError,
};
use monots_storage::parse_sst_filename;
use parking_lot::RwLock;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::{oneshot, Notify};

use super::actor::{IngressActor, IngressCommand};
use super::buffer::CaptureBuffer;
use crate::data::memory::StreamArrowBlock;
use crate::model::event::{DataEvent, InsertArrow};

pub const PENDING_FLUSH: &str = "flush";
pub const PENDING_COMPACT: &str = "compact";

/// Typed pending/ layout under `{stream_dir}/pending/{flush|compact}/`.
#[derive(Debug, Clone)]
pub struct CaptureLayout {
    pub stream_dir: PathBuf,
    pub pending_dir: PathBuf,
    pub flush_dir: PathBuf,
    pub compact_dir: PathBuf,
}

impl CaptureLayout {
    pub fn init(stream_dir: PathBuf) -> Result<Self> {
        let pending_dir = stream_dir.join("pending");
        let flush_dir = pending_dir.join(PENDING_FLUSH);
        let compact_dir = pending_dir.join(PENDING_COMPACT);
        for dir in [&flush_dir, &compact_dir] {
            std::fs::create_dir_all(dir).map_err(|e| {
                TsdbError::Storage(format!("create stream pending dir {}: {e}", dir.display()))
            })?;
        }
        Ok(Self {
            stream_dir,
            pending_dir,
            flush_dir,
            compact_dir,
        })
    }

    pub fn subdir_for(&self, origin: BatchOrigin) -> &Path {
        match origin {
            BatchOrigin::Compact => &self.compact_dir,
            BatchOrigin::Flush | BatchOrigin::BulkLoad => &self.flush_dir,
        }
    }
}

/// Shared state for facade + Actor. Mutable internals are private; Actor uses helpers below.
pub struct SharedContext {
    pub stream_id: String,
    pub table: String,
    pub layout: CaptureLayout,
    pub buffer: RwLock<CaptureBuffer>,
    dispatch_notify: RwLock<Option<Arc<Notify>>>,
    capture_wal: AtomicBool,
    max_dispatched_lsn: AtomicU64,
    frontier_lsn: AtomicU64,
    bootstrapped: AtomicBool,
    arrow_block: RwLock<Option<Arc<StreamArrowBlock>>>,
}

impl SharedContext {
    pub fn signal_dispatch(&self) {
        if let Some(n) = self.dispatch_notify.read().as_ref() {
            n.notify_one();
        }
    }

    /// Apply Flush into the buffer (covered Resident Inserts refund Arrow via Drop).
    pub fn apply_flush(&self, event: DataEvent) -> super::buffer::FlushDegradeResult {
        self.buffer.write().push_flush(event)
    }

    pub fn apply_compact(&self, event: DataEvent) -> super::buffer::CompactGcResult {
        self.buffer.write().push_compact(event)
    }
}

pub struct StreamSource {
    ctx: Arc<SharedContext>,
    ingress_tx: UnboundedSender<IngressCommand>,
}

impl StreamSource {
    pub fn open(
        stream_id: impl Into<String>,
        table: impl Into<String>,
        stream_dir: impl Into<PathBuf>,
    ) -> Result<Self> {
        Self::open_with_capture_wal(stream_id, table, stream_dir, true)
    }

    pub fn open_with_capture_wal(
        stream_id: impl Into<String>,
        table: impl Into<String>,
        stream_dir: impl Into<PathBuf>,
        capture_wal: bool,
    ) -> Result<Self> {
        let layout = CaptureLayout::init(stream_dir.into())?;
        let ctx = Arc::new(SharedContext {
            stream_id: stream_id.into(),
            table: table.into(),
            layout,
            buffer: RwLock::new(CaptureBuffer::new()),
            dispatch_notify: RwLock::new(None),
            capture_wal: AtomicBool::new(capture_wal),
            max_dispatched_lsn: AtomicU64::new(0),
            frontier_lsn: AtomicU64::new(0),
            bootstrapped: AtomicBool::new(false),
            arrow_block: RwLock::new(None),
        });

        let (ingress_tx, ingress_rx) = unbounded_channel();
        crate::control::executor::spawn(IngressActor::new(Arc::clone(&ctx), ingress_rx).run());
        Ok(Self { ctx, ingress_tx })
    }

    pub fn attach_arrow_block(&self, block: Arc<StreamArrowBlock>) {
        *self.ctx.arrow_block.write() = Some(block);
    }

    pub fn set_capture_wal(&self, enabled: bool) {
        self.ctx.capture_wal.store(enabled, Ordering::Release);
    }

    pub fn capture_wal_enabled(&self) -> bool {
        self.ctx.capture_wal.load(Ordering::Acquire)
    }

    pub fn attach_notify(&self, notify: Arc<Notify>) {
        let pending = self.queue_len() > 0;
        *self.ctx.dispatch_notify.write() = Some(Arc::clone(&notify));
        if pending {
            notify.notify_one();
        }
    }

    pub fn detach_notify(&self) {
        *self.ctx.dispatch_notify.write() = None;
    }

    pub fn wait_idle(&self) {
        let (tx, rx) = oneshot::channel();
        if self
            .ingress_tx
            .send(IngressCommand::DrainBarrier(tx))
            .is_err()
        {
            return;
        }
        // Park a dedicated OS thread so sync callers / #[tokio::test] never block_on on a worker.
        let _ = std::thread::spawn(move || {
            crate::control::executor::handle().block_on(async move {
                let _ = tokio::time::timeout(Duration::from_secs(30), rx).await;
            });
        })
        .join();
    }

    pub fn stream_id(&self) -> &str {
        &self.ctx.stream_id
    }

    pub fn table(&self) -> &str {
        &self.ctx.table
    }

    pub fn stream_dir(&self) -> &Path {
        &self.ctx.layout.stream_dir
    }

    pub fn pending_dir(&self) -> &Path {
        &self.ctx.layout.pending_dir
    }

    pub fn flush_dir(&self) -> &Path {
        &self.ctx.layout.flush_dir
    }

    pub fn compact_dir(&self) -> &Path {
        &self.ctx.layout.compact_dir
    }

    pub fn cursor_lsn(&self) -> Result<u64> {
        Ok(self.frontier_lsn())
    }

    pub fn frontier_lsn(&self) -> u64 {
        self.ctx.frontier_lsn.load(Ordering::Acquire)
    }

    pub fn max_dispatched_lsn(&self) -> u64 {
        self.ctx.max_dispatched_lsn.load(Ordering::Acquire)
    }

    pub fn is_bootstrapped(&self) -> bool {
        self.ctx.bootstrapped.load(Ordering::Acquire)
    }

    pub fn mark_bootstrap_done(&self, frontier_lsn: u64) {
        self.ctx.frontier_lsn.store(frontier_lsn, Ordering::Release);
        self.ctx.bootstrapped.store(true, Ordering::Release);
    }

    pub fn queue_len(&self) -> usize {
        self.ctx.buffer.read().len()
    }

    pub fn insert_len(&self) -> usize {
        self.ctx.buffer.read().len_inserts()
    }

    pub fn flush_bulk_len(&self) -> usize {
        self.ctx.buffer.read().len_flushes()
    }

    pub fn compact_len(&self) -> usize {
        self.ctx.buffer.read().len_compacts()
    }

    pub fn pending_subdir_for(&self, origin: BatchOrigin) -> &Path {
        self.ctx.layout.subdir_for(origin)
    }

    /// Sync hard-link into pending (must stay on the engine callback thread).
    pub fn link_into_pending(&self, original_path: &str, origin: BatchOrigin) -> Result<PathBuf> {
        monots_storage::hard_link_into_pending(self.ctx.layout.subdir_for(origin), original_path)
    }

    /// Acquire Arrow charge into the event; on failure keep [`InsertArrow::Deferred`].
    /// Charge travels with the event until Drop (Sink done / Flush degrade).
    fn audit_and_degrade_insert(&self, lsn: LsnRange, batches: Vec<RecordBatch>) -> DataEvent {
        let bytes = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        let arrow = if batches.is_empty() {
            InsertArrow::Deferred
        } else if let Some(block) = self.ctx.arrow_block.read().as_ref() {
            match block.try_acquire(bytes) {
                Some(charge) => InsertArrow::resident_charged(batches, charge),
                None => {
                    tracing::debug!(
                        stream = %self.ctx.stream_id,
                        table = %self.ctx.table,
                        bytes,
                        "Insert Arrow degraded to Deferred (stream arrow block full)"
                    );
                    InsertArrow::Deferred
                }
            }
        } else {
            InsertArrow::resident(batches)
        };
        DataEvent::Insert { lsn, arrow }
    }

    pub fn push_insert(&self, event: DataEvent) {
        if !self.capture_wal_enabled() {
            return;
        }
        if let DataEvent::Insert { lsn, arrow } = event {
            let batches = arrow.into_batches();
            let audited = self.audit_and_degrade_insert(lsn, batches);
            let _ = self.ingress_tx.send(IngressCommand::Insert(audited));
        }
    }

    pub fn push_flush_bulk(&self, event: DataEvent) {
        let _ = self.ingress_tx.send(IngressCommand::FlushFile(event));
    }

    pub fn push_compact(&self, event: DataEvent) {
        let _ = self.ingress_tx.send(IngressCommand::CompactFile(event));
    }

    pub fn requeue_front(&self, event: DataEvent) {
        let audited = match event {
            DataEvent::Insert {
                lsn,
                arrow: InsertArrow::Resident { batches, .. },
            } => self.audit_and_degrade_insert(lsn, batches),
            DataEvent::Insert {
                lsn,
                arrow: InsertArrow::Deferred,
            } => DataEvent::insert_deferred(lsn),
            other => other,
        };
        self.ctx
            .buffer
            .write()
            .requeue_front(audited, &self.ctx.layout.compact_dir);
        self.ctx.signal_dispatch();
    }

    pub fn peek_head_lsn(&self) -> Option<u64> {
        self.ctx.buffer.read().peek_head_lsn()
    }

    pub fn peek_heads(&self) -> (Option<u64>, Option<u64>) {
        self.ctx.buffer.read().peek_heads()
    }

    pub fn peek_next(&self) -> Option<DataEvent> {
        self.ctx.buffer.read().peek_next().cloned()
    }

    /// Pop oldest head. Deferred Inserts are forwarded as-is; sink loads Arrow.
    /// Resident ArrowCharge is refunded when the returned event is dropped (e.g. after Sink write).
    pub fn pop_next(&self) -> Option<DataEvent> {
        let event = self.ctx.buffer.write().pop_next()?;
        self.ctx
            .max_dispatched_lsn
            .fetch_max(event.max_lsn(), Ordering::AcqRel);
        Some(event)
    }

    pub fn pop_flush_bulk(&self) -> Option<DataEvent> {
        let event = self.ctx.buffer.write().pop_flush()?;
        self.ctx
            .max_dispatched_lsn
            .fetch_max(event.max_lsn(), Ordering::AcqRel);
        Some(event)
    }

    pub fn pop_compact(&self) -> Option<DataEvent> {
        let event = self.ctx.buffer.write().pop_compact()?;
        self.ctx
            .max_dispatched_lsn
            .fetch_max(event.max_lsn(), Ordering::AcqRel);
        Some(event)
    }

    pub fn recover_pending_queue(&self) -> Result<usize> {
        self.migrate_legacy_pending_files()?;

        let mut flush_events = Vec::new();
        let mut compact_events = Vec::new();
        let flush_n = self.collect_dir_events(&self.ctx.layout.flush_dir, &mut flush_events)?;
        let compact_n =
            self.collect_dir_events(&self.ctx.layout.compact_dir, &mut compact_events)?;

        flush_events.sort_by_key(|e| e.lsn().base_lsn);
        compact_events.sort_by_key(|e| e.lsn().base_lsn);
        self.ctx
            .buffer
            .write()
            .replace_file_queues(flush_events, compact_events);

        if flush_n + compact_n > 0 {
            self.ctx.signal_dispatch();
        }
        Ok(flush_n + compact_n)
    }

    fn collect_dir_events(&self, dir: &Path, out: &mut Vec<DataEvent>) -> Result<usize> {
        if !dir.exists() {
            return Ok(0);
        }
        let rd = std::fs::read_dir(dir).map_err(|e| {
            TsdbError::Storage(format!("stream read pending {}: {e}", dir.display()))
        })?;
        let before = out.len();
        for entry in rd.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Ok(id) = parse_sst_filename(name) else {
                tracing::warn!(
                    file = %name,
                    dir = %dir.display(),
                    "stream pending: skip unparseable SST name"
                );
                continue;
            };
            out.push(DataEvent::FlushFile {
                lsn: LsnRange::new(id.min_lsn, id.max_lsn),
                file_path: path.to_string_lossy().into_owned().into(),
                rows: 0,
            });
        }
        Ok(out.len() - before)
    }

    fn migrate_legacy_pending_files(&self) -> Result<()> {
        let rd = match std::fs::read_dir(&self.ctx.layout.pending_dir) {
            Ok(rd) => rd,
            Err(_) => return Ok(()),
        };
        for entry in rd.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Ok(id) = parse_sst_filename(name) else {
                continue;
            };
            let origin = if id.inner_compaction_count > 0 || id.cross_compaction_count > 0 {
                BatchOrigin::Compact
            } else {
                BatchOrigin::Flush
            };
            let dest = self.ctx.layout.subdir_for(origin).join(name);
            if dest.exists() {
                let _ = std::fs::remove_file(&path);
                continue;
            }
            if std::fs::rename(&path, &dest).is_err() {
                std::fs::copy(&path, &dest).map_err(|e| {
                    TsdbError::Storage(format!(
                        "stream migrate legacy pending {} → {}: {e}",
                        path.display(),
                        dest.display()
                    ))
                })?;
                let _ = std::fs::remove_file(&path);
            }
        }
        Ok(())
    }

    fn enqueue_file_meta(&self, meta: &CaptureFileMeta, origin: BatchOrigin) {
        if !meta.has_lsn_bounds() {
            return;
        }
        let link = match self.link_into_pending(&meta.file_path, origin) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(
                    stream = %self.ctx.stream_id,
                    table = %self.ctx.table,
                    path = %meta.file_path,
                    ?origin,
                    error = %e,
                    "stream hard-link into pending failed"
                );
                return;
            }
        };
        let event = DataEvent::FlushFile {
            lsn: meta.lsn_range(),
            file_path: link.to_string_lossy().into_owned().into(),
            rows: meta.rows,
        };
        let msg = match origin {
            BatchOrigin::Compact => IngressCommand::CompactFile(event),
            BatchOrigin::Flush | BatchOrigin::BulkLoad => IngressCommand::FlushFile(event),
        };
        let _ = self.ingress_tx.send(msg);
    }
}

impl TableCaptureListener for StreamSource {
    fn capture_wal(&self) -> bool {
        self.capture_wal_enabled()
    }

    fn on_insert(&self, min_lsn: u64, max_lsn: u64, batch: RecordBatch) {
        if !self.capture_wal_enabled() {
            return;
        }
        let event = self.audit_and_degrade_insert(LsnRange::new(min_lsn, max_lsn), vec![batch]);
        let _ = self.ingress_tx.send(IngressCommand::Insert(event));
    }

    fn on_memtable_end(&self, end_lsn: u64) {
        if !self.capture_wal_enabled() || end_lsn == 0 {
            return;
        }
        let _ = self
            .ingress_tx
            .send(IngressCommand::Watermark(DataEvent::Watermark { end_lsn }));
    }

    fn on_flush(&self, meta: &CaptureFileMeta) {
        self.enqueue_file_meta(meta, BatchOrigin::Flush);
    }

    fn on_bulk_load(&self, meta: &CaptureFileMeta) {
        self.enqueue_file_meta(meta, BatchOrigin::BulkLoad);
    }

    fn on_compact(&self, _inputs: &[CaptureFileMeta], output: &CaptureFileMeta) {
        self.enqueue_file_meta(output, BatchOrigin::Compact);
    }
}

impl CaptureSource for StreamSource {
    fn on_historical_sst(&self, meta: &CaptureFileMeta) {
        self.on_flush(meta);
    }

    fn on_bootstrap_done(&self, frontier_lsn: u64) {
        self.wait_idle();
        self.mark_bootstrap_done(frontier_lsn);
        tracing::info!(
            stream = %self.stream_id(),
            table = %self.table(),
            frontier_lsn,
            "StreamSource bootstrap done (pending/ + live)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64])),
                Arc::new(Int64Array::from(vec![1_i64])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn flush_and_compact_go_to_distinct_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let flush_sst = data.join("100-2-2-0-0.parquet");
        let compact_sst = data.join("200-2-2-1-0.parquet");
        std::fs::write(&flush_sst, b"flush").unwrap();
        std::fs::write(&compact_sst, b"compact").unwrap();

        let src = StreamSource::open("s1", "t0", tmp.path().join("s1")).unwrap();
        src.on_flush(&CaptureFileMeta::new(flush_sst.to_string_lossy(), 2, 2));
        src.wait_idle();
        src.on_compact(
            &[],
            &CaptureFileMeta::new(compact_sst.to_string_lossy(), 2, 2),
        );
        src.wait_idle();
        std::thread::sleep(std::time::Duration::from_millis(80));

        assert!(src.compact_dir().join("200-2-2-1-0.parquet").exists());
        assert!(!src.flush_dir().join("100-2-2-0-0.parquet").exists());
        assert!(!src.pending_dir().join("100-2-2-0-0.parquet").exists());
        assert_eq!(src.flush_bulk_len(), 0);
        assert_eq!(src.compact_len(), 1);
        let compact = src.pop_compact().unwrap();
        match compact {
            DataEvent::FlushFile { file_path, .. } => {
                assert!(Path::new(file_path.as_ref()).starts_with(src.compact_dir()));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn hard_link_and_prefer_older_file() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let sst = data.join("100-2-2-0-0.parquet");
        std::fs::write(&sst, b"parquet").unwrap();

        let src = StreamSource::open("s1", "t0", tmp.path().join("s1")).unwrap();
        src.on_insert(10, 10, batch());
        src.on_flush(&CaptureFileMeta::new(sst.to_string_lossy(), 2, 2));
        src.wait_idle();

        match src.pop_next().unwrap() {
            DataEvent::FlushFile { lsn, file_path, .. } => {
                assert!(Path::new(file_path.as_ref()).starts_with(src.flush_dir()));
                assert_eq!(lsn.ack_lsn(), 2);
            }
            other => panic!("{other:?}"),
        }
        std::fs::remove_file(&sst).unwrap();
        assert!(src.flush_dir().join("100-2-2-0-0.parquet").exists());
    }

    #[tokio::test]
    async fn late_flush_stays_in_buffer_after_insert_dispatched() {
        let tmp = tempfile::tempdir().unwrap();
        let src = StreamSource::open("s1", "t0", tmp.path().join("s1")).unwrap();
        let flush = src.flush_dir().join("late.parquet");
        std::fs::write(&flush, b"late").unwrap();

        src.push_insert(DataEvent::insert(LsnRange::new(1, 10), vec![batch()]));
        src.wait_idle();
        let popped = src.pop_next().unwrap();
        assert!(matches!(popped, DataEvent::Insert { .. }));
        assert_eq!(src.max_dispatched_lsn(), 10);

        src.push_flush_bulk(DataEvent::FlushFile {
            lsn: LsnRange::new(1, 10),
            file_path: flush.to_string_lossy().into_owned().into(),
            rows: 10,
        });
        src.wait_idle();

        assert_eq!(
            src.flush_bulk_len(),
            1,
            "late Flush must remain for 2PC retry"
        );
        assert_eq!(src.insert_len(), 0);
        assert!(flush.exists());
        match src.pop_next().unwrap() {
            DataEvent::FlushFile { lsn, file_path, .. } => {
                assert_eq!(lsn.max_lsn, 10);
                assert_eq!(Path::new(file_path.as_ref()), flush.as_path());
            }
            other => panic!("expected FlushFile for replay, got {other:?}"),
        }
    }

    #[test]
    fn flush_degrades_covered_inserts() {
        let tmp = tempfile::tempdir().unwrap();
        let src = StreamSource::open("s1", "t0", tmp.path().join("s1")).unwrap();
        src.push_insert(DataEvent::insert(LsnRange::new(1, 3), vec![batch()]));
        src.push_insert(DataEvent::insert(LsnRange::new(10, 11), vec![batch()]));
        src.push_flush_bulk(DataEvent::FlushFile {
            lsn: LsnRange::new(1, 5),
            file_path: src
                .flush_dir()
                .join("f.parquet")
                .to_string_lossy()
                .into_owned()
                .into(),
            rows: 3,
        });
        src.wait_idle();

        assert_eq!(src.flush_bulk_len(), 1);
        assert_eq!(
            src.insert_len(),
            1,
            "only Insert outside flush range remains"
        );
        assert_eq!(src.pop_flush_bulk().unwrap().lsn().base_lsn, 1);
        assert_eq!(src.pop_next().unwrap().lsn().base_lsn, 10);
    }

    #[tokio::test]
    async fn compact_degrades_covered_flush_and_unlinks() {
        let tmp = tempfile::tempdir().unwrap();
        let src = StreamSource::open("s1", "t0", tmp.path().join("s1")).unwrap();
        let flush_a = src.flush_dir().join("a.parquet");
        let flush_b = src.flush_dir().join("b.parquet");
        std::fs::write(&flush_a, b"a").unwrap();
        std::fs::write(&flush_b, b"b").unwrap();

        src.push_flush_bulk(DataEvent::FlushFile {
            lsn: LsnRange::new(1, 2),
            file_path: flush_a.to_string_lossy().into_owned().into(),
            rows: 1,
        });
        src.push_flush_bulk(DataEvent::FlushFile {
            lsn: LsnRange::new(3, 4),
            file_path: flush_b.to_string_lossy().into_owned().into(),
            rows: 1,
        });
        src.push_insert(DataEvent::insert(LsnRange::new(2, 2), vec![batch()]));
        src.push_compact(DataEvent::FlushFile {
            lsn: LsnRange::new(1, 4),
            file_path: src
                .compact_dir()
                .join("c.parquet")
                .to_string_lossy()
                .into_owned()
                .into(),
            rows: 2,
        });
        src.wait_idle();
        std::thread::sleep(std::time::Duration::from_millis(80));

        assert_eq!(src.compact_len(), 1);
        assert_eq!(src.flush_bulk_len(), 0);
        assert_eq!(src.insert_len(), 1);
        assert!(!flush_a.exists());
        assert!(!flush_b.exists());
    }

    #[test]
    fn same_lsn_compact_degrades_flush_keeps_insert() {
        let tmp = tempfile::tempdir().unwrap();
        let src = StreamSource::open("s1", "t0", tmp.path().join("s1")).unwrap();
        src.push_flush_bulk(DataEvent::FlushFile {
            lsn: LsnRange::single(5),
            file_path: src
                .flush_dir()
                .join("f.parquet")
                .to_string_lossy()
                .into_owned()
                .into(),
            rows: 1,
        });
        src.wait_idle();
        src.push_insert(DataEvent::insert(LsnRange::single(5), vec![batch()]));
        src.wait_idle();
        src.push_compact(DataEvent::FlushFile {
            lsn: LsnRange::single(5),
            file_path: src
                .compact_dir()
                .join("c.parquet")
                .to_string_lossy()
                .into_owned()
                .into(),
            rows: 1,
        });
        src.wait_idle();

        assert_eq!(src.compact_len(), 1);
        assert_eq!(src.flush_bulk_len(), 0);
        assert_eq!(src.insert_len(), 1);
        match src.pop_next().unwrap() {
            DataEvent::FlushFile { file_path, .. } => {
                assert!(file_path.contains(PENDING_COMPACT));
            }
            other => panic!("expected compact first, got {other:?}"),
        }
        assert!(matches!(src.pop_next().unwrap(), DataEvent::Insert { .. }));
    }

    #[test]
    fn batch_only_skips_wal_inserts() {
        let tmp = tempfile::tempdir().unwrap();
        let src =
            StreamSource::open_with_capture_wal("s1", "t0", tmp.path().join("s1"), false).unwrap();
        assert!(!src.capture_wal_enabled());
        assert!(!TableCaptureListener::capture_wal(&src));

        src.on_insert(1, 1, batch());
        src.push_insert(DataEvent::insert(LsnRange::single(2), vec![batch()]));
        src.wait_idle();
        assert_eq!(src.insert_len(), 0);

        src.set_capture_wal(true);
        src.on_insert(3, 3, batch());
        src.wait_idle();
        assert_eq!(src.insert_len(), 1);
    }

    #[test]
    fn recover_pending_rebuilds_both_queues() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("s1");
        let flush = dir.join("pending").join(PENDING_FLUSH);
        let compact = dir.join("pending").join(PENDING_COMPACT);
        std::fs::create_dir_all(&flush).unwrap();
        std::fs::create_dir_all(&compact).unwrap();
        std::fs::write(flush.join("50-5-7-0-0.parquet"), b"x").unwrap();
        std::fs::write(flush.join("40-1-3-0-0.parquet"), b"y").unwrap();
        std::fs::write(compact.join("60-1-9-1-0.parquet"), b"z").unwrap();

        let src = StreamSource::open("s1", "t0", &dir).unwrap();
        assert_eq!(src.recover_pending_queue().unwrap(), 3);
        assert_eq!(src.pop_flush_bulk().unwrap().lsn().base_lsn, 1);
        assert_eq!(src.pop_flush_bulk().unwrap().lsn().base_lsn, 5);
        assert_eq!(src.pop_compact().unwrap().lsn().max_lsn, 9);
    }

    #[test]
    fn migrate_legacy_flat_pending() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("s1");
        let pending = dir.join("pending");
        std::fs::create_dir_all(&pending).unwrap();
        std::fs::write(pending.join("10-1-2-0-0.parquet"), b"f").unwrap();
        std::fs::write(pending.join("20-3-4-1-0.parquet"), b"c").unwrap();

        let src = StreamSource::open("s1", "t0", &dir).unwrap();
        assert_eq!(src.recover_pending_queue().unwrap(), 2);
        assert!(src.flush_dir().join("10-1-2-0-0.parquet").exists());
        assert!(src.compact_dir().join("20-3-4-1-0.parquet").exists());
        assert!(!pending.join("10-1-2-0-0.parquet").exists());
    }

    #[test]
    fn arrow_block_degrades_insert_when_full_and_releases_on_pop() {
        use crate::data::memory::StreamArrowMemoryPool;

        let tmp = tempfile::tempdir().unwrap();
        let src = StreamSource::open("s1", "t0", tmp.path().join("s1")).unwrap();
        let pool = StreamArrowMemoryPool::new(1024 * 1024);
        let block = pool.alloc_block(1);
        src.attach_arrow_block(Arc::clone(&block));

        let b = batch();
        let need = b.get_array_memory_size();
        assert!(need > 1, "test assumes batch > 1 byte");

        src.on_insert(1, 1, b);
        src.wait_idle();
        assert_eq!(src.insert_len(), 1);
        assert_eq!(block.charged_bytes(), 0, "degraded Insert must not charge");

        let degraded = src.pop_next().unwrap();
        assert!(
            degraded.insert_needs_load(),
            "degraded Insert must be Deferred and forwarded to sink"
        );
        assert_eq!(src.insert_len(), 0);

        let block2 = pool.alloc_block(need * 4);
        let src2 = StreamSource::open("s2", "t0", tmp.path().join("s2")).unwrap();
        src2.attach_arrow_block(Arc::clone(&block2));
        src2.on_insert(2, 2, batch());
        src2.wait_idle();
        assert_eq!(block2.charged_bytes(), need);
        let ev = src2.pop_next().unwrap();
        match ev {
            DataEvent::Insert { arrow, .. } => {
                assert!(arrow.is_resident());
                assert_eq!(arrow.batches().len(), 1);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(block2.charged_bytes(), 0);
    }

    #[test]
    fn contiguous_same_state_inserts_merge() {
        let mut buf = CaptureBuffer::new();
        buf.push_insert(DataEvent::insert(LsnRange::new(1, 2), vec![batch()]));
        buf.push_insert(DataEvent::insert(LsnRange::new(3, 4), vec![batch()]));
        assert_eq!(buf.len_inserts(), 1);
        match buf.pop_next().unwrap() {
            DataEvent::Insert { lsn, arrow } => {
                assert_eq!(lsn.base_lsn, 1);
                assert_eq!(lsn.max_lsn, 4);
                assert_eq!(arrow.batches().len(), 2);
            }
            other => panic!("{other:?}"),
        }

        let mut buf2 = CaptureBuffer::new();
        buf2.push_insert(DataEvent::insert(LsnRange::new(1, 2), vec![batch()]));
        buf2.push_watermark(DataEvent::Watermark { end_lsn: 2 });
        buf2.push_insert(DataEvent::insert(LsnRange::new(3, 4), vec![batch()]));
        assert_eq!(buf2.len_inserts(), 3, "watermark blocks merge");

        let mut buf3 = CaptureBuffer::new();
        buf3.push_insert(DataEvent::insert(LsnRange::new(1, 2), vec![batch()]));
        buf3.push_insert(DataEvent::insert(LsnRange::new(5, 6), vec![batch()]));
        assert_eq!(buf3.len_inserts(), 2);

        let mut w = CaptureBuffer::new();
        w.push_insert(DataEvent::insert_deferred(LsnRange::new(10, 10)));
        w.push_insert(DataEvent::insert_deferred(LsnRange::new(11, 12)));
        assert_eq!(w.len_inserts(), 1);
        assert_eq!(w.pop_next().unwrap().lsn().max_lsn, 12);

        let mut m = CaptureBuffer::new();
        m.push_insert(DataEvent::insert(LsnRange::new(1, 1), vec![batch()]));
        m.push_insert(DataEvent::insert_deferred(LsnRange::new(2, 2)));
        assert_eq!(m.len_inserts(), 2, "mixed Arrow + WAL-only must not merge");
    }
}
