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

//! Shared WAL I/O pool: actor per worker + Nagle micro-batch + backlog backpressure.
//!
//! Layout: `wal_segments/{file_id:020}.wal`. Memtable freeze only updates the logical id;
//! recover trim is by batch LSN vs SST. Faulted workers reject new appends (no silent loss).

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common::{Result, TsdbError, WAL_SEGMENTS_DIR};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

use crate::wal::backlog::{TableBacklog, WalBacklogBudget};
use crate::wal::format::{
    list_numbered_wal_paths, list_wal_file_ids, numbered_wal_path, read_segment_batches,
    FramedSegmentWriter, DEFAULT_WAL_BLOCK_MAX_BYTES,
};
use crate::wal::notify::WalAppendHub;

/// Default max on-disk size of one WAL segment before rotating (100 MiB).
pub use crate::wal::format::DEFAULT_WAL_SEGMENT_MAX_BYTES;

/// WAL durability on the write path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalDurabilityMode {
    /// Enqueue and return immediately; the pool persists in the background (default).
    #[default]
    Async,
    /// Block until the batch is appended to the active WAL segment.
    Sync,
}

impl WalDurabilityMode {
    pub fn is_sync(self) -> bool {
        matches!(self, Self::Sync)
    }
}

/// Bounded queue between write path and each WAL worker (message count cap, shared by its writers).
pub const WAL_CHANNEL_CAPACITY: usize = 1024;
/// Per-worker command channel capacity (shared across all writers on that worker).
pub const WAL_WORKER_CHANNEL_CAPACITY: usize = 8192;
/// Default micro-batch flush threshold per table WAL writer (bytes).
pub const DEFAULT_WAL_MICRO_BATCH_MAX_BYTES: usize = 2 * 1024 * 1024;
/// Default global WAL backlog cap shared by all tables (bytes).
pub const DEFAULT_WAL_GLOBAL_BACKLOG_MAX_BYTES: usize = 64 * 1024 * 1024;
/// Default per-table WAL backlog cap within the global pool (bytes).
pub const DEFAULT_WAL_TABLE_BACKLOG_MAX_BYTES: usize = 2 * 1024 * 1024;
/// WAL worker flushes buffered batches after this wait (microseconds).
pub const WAL_MICRO_BATCH_MAX_WAIT_US: u64 = 500;
/// Upper bound on shared WAL worker threads.
pub const WAL_WORKER_THREADS_MAX: usize = 8;

#[inline(always)]
fn wal_batch_bytes(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size().max(1)
}

/// Shared I/O health bit between [`WalWriter`] and its worker (circuit breaker).
#[derive(Default)]
struct WalIoHealth {
    faulted: AtomicBool,
}

impl WalIoHealth {
    fn trip(&self) {
        self.faulted.store(true, Ordering::Release);
    }

    fn is_faulted(&self) -> bool {
        self.faulted.load(Ordering::Acquire)
    }

    fn check(&self) -> Result<()> {
        if self.is_faulted() {
            Err(TsdbError::Storage(
                "WAL worker faulted after I/O error".into(),
            ))
        } else {
            Ok(())
        }
    }
}

fn default_worker_count() -> usize {
    thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(1, WAL_WORKER_THREADS_MAX)
}

/// Per-table WAL writer tuning.
#[derive(Clone)]
pub struct WalWriterOptions {
    pub durability: WalDurabilityMode,
    /// Flush worker buffer when pending payload reaches this size.
    pub micro_batch_max_bytes: usize,
    /// Rotate to a new on-disk WAL file once the active segment reaches this size.
    pub segment_max_bytes: u64,
    /// Max size of one WAL block (`block_len` = header + body).
    pub block_max_bytes: usize,
    /// Shared global WAL backlog budget.
    pub backlog: Arc<WalBacklogBudget>,
    /// Per-table WAL backlog slice (semaphore-backed, no busy waiting).
    pub table_backlog: Arc<TableBacklog>,
    /// Table name for WAL append notifications (stream realtime tail).
    pub table_name: Option<Arc<str>>,
    pub notify: Option<Arc<WalAppendHub>>,
}

impl WalWriterOptions {
    pub fn with_durability(durability: WalDurabilityMode) -> Self {
        Self::for_test_table(durability)
    }

    pub fn for_test_table(durability: WalDurabilityMode) -> Self {
        let backlog = Arc::new(WalBacklogBudget::new(
            DEFAULT_WAL_GLOBAL_BACKLOG_MAX_BYTES,
            DEFAULT_WAL_TABLE_BACKLOG_MAX_BYTES,
        ));
        let table_backlog = backlog.new_table_backlog();
        Self {
            durability,
            micro_batch_max_bytes: DEFAULT_WAL_MICRO_BATCH_MAX_BYTES,
            segment_max_bytes: DEFAULT_WAL_SEGMENT_MAX_BYTES,
            block_max_bytes: DEFAULT_WAL_BLOCK_MAX_BYTES,
            backlog,
            table_backlog,
            table_name: None,
            notify: None,
        }
    }

    pub fn with_segment_max_bytes(mut self, bytes: u64) -> Self {
        self.segment_max_bytes = bytes.max(1);
        self
    }

    pub fn with_block_max_bytes(mut self, bytes: usize) -> Self {
        self.block_max_bytes = bytes.max(1);
        self
    }

    pub fn with_notify(mut self, table_name: impl AsRef<str>, notify: Arc<WalAppendHub>) -> Self {
        self.table_name = Some(Arc::from(table_name.as_ref()));
        self.notify = Some(notify);
        self
    }
}

/// Logical view of one memtable's batch frames across the flat numbered WAL chain.
pub struct MemTableWal {
    memtable_id: u64,
    wal_root: PathBuf,
}

impl MemTableWal {
    pub fn open(base_dir: &Path, memtable_id: u64) -> Result<Self> {
        let wal_root = base_dir.join(WAL_SEGMENTS_DIR);
        fs::create_dir_all(&wal_root)?;
        Ok(Self {
            memtable_id,
            wal_root,
        })
    }

    pub fn memtable_id(&self) -> u64 {
        self.memtable_id
    }

    pub fn dir(&self) -> &Path {
        &self.wal_root
    }

    pub fn wal_root(&self) -> &Path {
        &self.wal_root
    }

    /// Replay all batch frames in the flat WAL chain (ownership is by LSN, not memtable id).
    pub fn replay(&self) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        for path in list_numbered_wal_paths(&self.wal_root)? {
            match read_segment_batches(&path, self.memtable_id, true) {
                Ok(mut part) => batches.append(&mut part),
                Err(e) => {
                    tracing::warn!(
                        "skip unreadable WAL for memtable {} at {}: {e}",
                        self.memtable_id,
                        path.display()
                    );
                }
            }
        }
        Ok(batches)
    }

    /// No-op: GC is by sealed file + SST LSN coverage.
    pub fn destroy(&self) -> Result<()> {
        Ok(())
    }

    /// Compat stub: recover no longer tracks per-memtable WAL ownership.
    pub fn list_unflushed_memtable_ids(_base_dir: &Path) -> Result<Vec<u64>> {
        Ok(vec![])
    }

    /// Sorted numbered WAL file ids under the table.
    pub fn list_wal_file_ids(base_dir: &Path) -> Result<Vec<u64>> {
        list_wal_file_ids(&base_dir.join(WAL_SEGMENTS_DIR))
    }

    pub fn destroy_wal_file(base_dir: &Path, file_id: u64) -> Result<()> {
        let path = numbered_wal_path(&base_dir.join(WAL_SEGMENTS_DIR), file_id);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }
}

struct WalSegmentWriter {
    wal_root: PathBuf,
    file_id: u64,
    memtable_id: u64,
    active: Option<FramedSegmentWriter>,
    sync_writes: bool,
    segment_max_bytes: u64,
    block_max_bytes: usize,
}

impl WalSegmentWriter {
    fn memtable_id(&self) -> u64 {
        self.memtable_id
    }

    fn open(
        base_dir: &Path,
        memtable_id: u64,
        sync_writes: bool,
        segment_max_bytes: u64,
        block_max_bytes: usize,
    ) -> Result<Self> {
        let wal_root = base_dir.join(WAL_SEGMENTS_DIR);
        fs::create_dir_all(&wal_root)?;
        let block_max_bytes = block_max_bytes.clamp(1, DEFAULT_WAL_BLOCK_MAX_BYTES).max(1);
        let ids = list_wal_file_ids(&wal_root)?;
        let (file_id, active) = if let Some(&last) = ids.last() {
            let path = numbered_wal_path(&wal_root, last);
            match FramedSegmentWriter::resume_any(path) {
                Ok(mut w) => {
                    // In-memory logical id only — flush / recover are LSN-driven.
                    w.set_memtable_id(memtable_id);
                    w.set_block_max_bytes(block_max_bytes);
                    (last, Some(w))
                }
                Err(_) => {
                    // Sealed (has footer) — next append opens a new file id.
                    (last, None)
                }
            }
        } else {
            (0, None)
        };
        Ok(Self {
            wal_root,
            file_id,
            memtable_id,
            active,
            sync_writes,
            segment_max_bytes: segment_max_bytes.max(1),
            block_max_bytes,
        })
    }

    fn append(&mut self, batch: &RecordBatch, lsn: u64) -> Result<Option<u64>> {
        if self.active.is_none() {
            self.start_segment(batch.schema())?;
        }
        if self.active_on_disk_bytes()? >= self.segment_max_bytes {
            self.hard_rotate_segment(batch.schema())?;
        }
        let seg = self.active.as_mut().expect("segment just opened");
        let sequence = seg.append_batch(batch, lsn, self.sync_writes)?;
        Ok(Some(sequence))
    }

    fn active_on_disk_bytes(&mut self) -> Result<u64> {
        match self.active.as_mut() {
            Some(seg) => seg.on_disk_bytes(),
            None => Ok(0),
        }
    }

    fn start_segment(&mut self, schema: SchemaRef) -> Result<()> {
        if self.active.is_some() {
            return Ok(());
        }
        let next_id = self.file_id.saturating_add(1).max(1);
        let path = numbered_wal_path(&self.wal_root, next_id);
        self.file_id = next_id;
        self.active = Some(FramedSegmentWriter::create_with_block_max(
            path,
            self.memtable_id,
            schema,
            self.block_max_bytes,
        )?);
        Ok(())
    }

    fn finish_active_segment(&mut self) -> Result<()> {
        if let Some(seg) = self.active.take() {
            seg.finish()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if let Some(seg) = self.active.as_mut() {
            seg.sync_data()?;
        }
        Ok(())
    }

    /// Close active file and open the next numbered WAL (size-based rotate only).
    fn hard_rotate_segment(&mut self, schema: SchemaRef) -> Result<()> {
        self.finish_active_segment()?;
        let next_id = self.file_id.saturating_add(1);
        let path = numbered_wal_path(&self.wal_root, next_id);
        self.file_id = next_id;
        self.active = Some(FramedSegmentWriter::create_with_block_max(
            path,
            self.memtable_id,
            schema,
            self.block_max_bytes,
        )?);
        tracing::info!(
            file_id = self.file_id,
            memtable_id = self.memtable_id,
            max_bytes = self.segment_max_bytes,
            "WAL segment rotated by size"
        );
        Ok(())
    }

    /// Logical memtable id update only: same file continues (no WAL marker).
    fn rotate_memtable(&mut self, new_memtable_id: u64, _schema: Option<SchemaRef>) -> Result<()> {
        if new_memtable_id == self.memtable_id {
            return Ok(());
        }
        if let Some(seg) = self.active.as_mut() {
            seg.set_memtable_id(new_memtable_id);
        }
        self.memtable_id = new_memtable_id;
        Ok(())
    }
}

struct WalMicroBatch {
    /// Buffered `(batch, global_lsn)` pairs; each becomes one frame carrying its own LSN.
    pending: Vec<(Arc<RecordBatch>, u64)>,
    pending_bytes: usize,
    max_bytes: usize,
}

impl WalMicroBatch {
    fn new(max_bytes: usize) -> Self {
        Self {
            pending: Vec::with_capacity(32),
            pending_bytes: 0,
            max_bytes: max_bytes.max(1),
        }
    }

    fn push(&mut self, batch: Arc<RecordBatch>, lsn: u64) {
        self.pending_bytes += wal_batch_bytes(batch.as_ref());
        self.pending.push((batch, lsn));
    }

    fn should_flush(&self) -> bool {
        self.pending_bytes >= self.max_bytes
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    fn drain(&mut self) -> impl Iterator<Item = (Arc<RecordBatch>, u64)> + '_ {
        self.pending_bytes = 0;
        self.pending.drain(..)
    }
}

/// Per-writer segment + micro-batch state, owned by the worker thread it is assigned to.
struct WalWorker {
    table_name: Option<Arc<str>>,
    notify: Option<Arc<WalAppendHub>>,
    segment: WalSegmentWriter,
    micro_batch: WalMicroBatch,
    micro_batch_max_bytes: usize,
    table_backlog: Arc<TableBacklog>,
    backlog: Arc<WalBacklogBudget>,
    is_dirty: bool,
    /// Circuit breaker: after disk I/O failure, reject further appends.
    health: Arc<WalIoHealth>,
}

impl WalWorker {
    fn new(params: WalWriterParams) -> Result<Self> {
        let segment = WalSegmentWriter::open(
            &params.base_dir,
            params.memtable_id,
            params.sync_writes,
            params.segment_max_bytes,
            params.block_max_bytes,
        )?;
        Ok(Self {
            table_name: params.table_name,
            notify: params.notify,
            segment,
            micro_batch: WalMicroBatch::new(params.micro_batch_max_bytes),
            micro_batch_max_bytes: params.micro_batch_max_bytes,
            table_backlog: params.table_backlog,
            backlog: params.backlog,
            is_dirty: false,
            health: params.health,
        })
    }

    fn trip_fault(&mut self, e: &TsdbError) {
        tracing::error!(
            table = ?self.table_name,
            error = %e,
            "WAL I/O failed; circuit breaker open"
        );
        self.health.trip();
        self.is_dirty = false;
    }

    /// Append `batch`, flushing when forced or past the byte threshold.
    fn append(&mut self, batch: Arc<RecordBatch>, lsn: u64, force: bool) -> Result<()> {
        let bytes = wal_batch_bytes(batch.as_ref());
        self.backlog.release(&self.table_backlog, bytes);
        self.health.check()?;

        self.micro_batch.push(batch, lsn);
        self.is_dirty = true;
        if force || self.micro_batch.should_flush() {
            self.flush_micro_batch()
        } else {
            Ok(())
        }
    }

    fn flush_micro_batch(&mut self) -> Result<()> {
        self.health.check()?;
        if !self.is_dirty || self.micro_batch.is_empty() {
            self.is_dirty = false;
            return Ok(());
        }

        let result = (|| -> Result<()> {
            for (batch, lsn) in self.micro_batch.drain() {
                if let Some(sequence) = self.segment.append(batch.as_ref(), lsn)? {
                    if let (Some(table), Some(hub)) = (&self.table_name, &self.notify) {
                        hub.notify(table, self.segment.memtable_id(), sequence);
                    }
                }
            }
            self.segment.flush()?;
            Ok(())
        })();

        match result {
            Ok(()) => {
                self.is_dirty = false;
                Ok(())
            }
            Err(e) => {
                self.trip_fault(&e);
                Err(e)
            }
        }
    }

    fn flush_all(&mut self) -> Result<()> {
        self.flush_micro_batch()?;
        match self.segment.flush() {
            Ok(()) => Ok(()),
            Err(e) => {
                self.trip_fault(&e);
                Err(e)
            }
        }
    }

    fn rotate(&mut self, new_memtable_id: u64) -> Result<()> {
        self.flush_micro_batch()?;
        self.segment.rotate_memtable(new_memtable_id, None)?;
        self.micro_batch = WalMicroBatch::new(self.micro_batch_max_bytes);
        self.is_dirty = false;
        Ok(())
    }

    /// Drain pending batches, write fsynced MemTableEnd, then switch memtable id.
    fn memtable_end(
        &mut self,
        end_lsn: u64,
        closed_memtable_id: u64,
        new_memtable_id: u64,
    ) -> Result<()> {
        self.flush_micro_batch()?;
        if let Some(seg) = self.segment.active.as_mut() {
            seg.append_memtable_end(end_lsn, closed_memtable_id, new_memtable_id)?;
        } else {
            return Err(TsdbError::Storage(
                "wal memtable_end: no active segment".into(),
            ));
        }
        self.segment.rotate_memtable(new_memtable_id, None)?;
        self.micro_batch = WalMicroBatch::new(self.micro_batch_max_bytes);
        self.is_dirty = false;
        Ok(())
    }
}

/// Construction parameters carried in a [`WalCommand::Register`].
struct WalWriterParams {
    base_dir: PathBuf,
    memtable_id: u64,
    micro_batch_max_bytes: usize,
    segment_max_bytes: u64,
    block_max_bytes: usize,
    table_backlog: Arc<TableBacklog>,
    backlog: Arc<WalBacklogBudget>,
    sync_writes: bool,
    table_name: Option<Arc<str>>,
    notify: Option<Arc<WalAppendHub>>,
    health: Arc<WalIoHealth>,
}

enum WalCommand {
    Register {
        writer_id: u64,
        params: Box<WalWriterParams>,
        ack: oneshot::Sender<Result<()>>,
    },
    Append {
        writer_id: u64,
        batch: Arc<RecordBatch>,
        lsn: u64,
        ack: Option<oneshot::Sender<Result<()>>>,
    },
    Rotate {
        writer_id: u64,
        new_memtable_id: u64,
        ack: oneshot::Sender<Result<()>>,
    },
    /// Flush micro-batch, append durable [`RecordType::MemTableEnd`] (always fsync), then
    /// switch logical memtable id.
    MemTableEnd {
        writer_id: u64,
        end_lsn: u64,
        closed_memtable_id: u64,
        new_memtable_id: u64,
        ack: oneshot::Sender<Result<()>>,
    },
    Flush {
        writer_id: u64,
        ack: oneshot::Sender<Result<()>>,
    },
    Deregister {
        writer_id: u64,
        ack: Option<oneshot::Sender<Result<()>>>,
    },
}

/// Fixed pool of WAL worker threads shared by every [`WalWriter`] in the process.
///
/// Prefer attaching the pool to [`crate::engine::LsmEngine`] long-term; kept process-global
/// for API compatibility with existing open paths.
struct WalThreadPool {
    senders: Vec<mpsc::Sender<WalCommand>>,
    next_writer_id: AtomicU64,
}

static POOL: OnceLock<WalThreadPool> = OnceLock::new();

impl WalThreadPool {
    fn global() -> &'static WalThreadPool {
        POOL.get_or_init(|| WalThreadPool::new(default_worker_count()))
    }

    fn new(worker_count: usize) -> Self {
        let worker_count = worker_count.max(1).clamp(1, WAL_WORKER_THREADS_MAX);
        let mut senders = Vec::with_capacity(worker_count);
        for i in 0..worker_count {
            let (tx, rx) = mpsc::channel(WAL_WORKER_CHANNEL_CAPACITY);
            thread::Builder::new()
                .name(format!("wal-worker-{i}"))
                .spawn(move || run_worker(rx))
                .expect("spawn wal worker thread");
            senders.push(tx);
        }
        Self {
            senders,
            next_writer_id: AtomicU64::new(1),
        }
    }

    /// Assign a fresh writer id to a worker (round-robin by id).
    fn assign(&self) -> (u64, mpsc::Sender<WalCommand>) {
        let id = self.next_writer_id.fetch_add(1, Ordering::Relaxed);
        let idx = (id as usize) % self.senders.len();
        (id, self.senders[idx].clone())
    }
}

/// Worker event loop: owns per-writer state and flushes on demand or on a periodic tick.
fn run_worker(rx: mpsc::Receiver<WalCommand>) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!("wal worker runtime init failed: {e}");
            return;
        }
    };
    rt.block_on(worker_loop(rx));
}

async fn worker_loop(mut rx: mpsc::Receiver<WalCommand>) {
    let period = Duration::from_micros(WAL_MICRO_BATCH_MAX_WAIT_US);
    let mut flush_ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut workers: HashMap<u64, WalWorker> = HashMap::new();
    // Only dirty writer ids — tick flush is O(dirty), not O(all writers).
    let mut dirty_queue: VecDeque<u64> = VecDeque::with_capacity(128);

    loop {
        tokio::select! {
            cmd_opt = rx.recv() => {
                let Some(cmd) = cmd_opt else {
                    break;
                };
                handle_command(cmd, &mut workers, &mut dirty_queue);
            }
            _ = flush_ticker.tick() => {
                flush_dirty_queue(&mut workers, &mut dirty_queue);
            }
        }
    }

    for worker in workers.values_mut() {
        if let Err(e) = worker.flush_all() {
            tracing::error!("wal worker final flush failed: {e}");
        }
    }
}

fn handle_command(
    cmd: WalCommand,
    workers: &mut HashMap<u64, WalWorker>,
    dirty_queue: &mut VecDeque<u64>,
) {
    match cmd {
        WalCommand::Register {
            writer_id,
            params,
            ack,
        } => {
            let result = WalWorker::new(*params).map(|worker| {
                workers.insert(writer_id, worker);
            });
            let _ = ack.send(result);
        }
        WalCommand::Append {
            writer_id,
            batch,
            lsn,
            ack,
        } => {
            let Some(worker) = workers.get_mut(&writer_id) else {
                if let Some(ack) = ack {
                    let _ = ack.send(Err(TsdbError::Storage("wal writer not registered".into())));
                }
                return;
            };
            let force = ack.is_some();
            let was_dirty = worker.is_dirty;
            let result = worker.append(batch, lsn, force);
            if !was_dirty && worker.is_dirty {
                dirty_queue.push_back(writer_id);
            }
            if let Some(ack) = ack {
                let _ = ack.send(result);
            } else if let Err(e) = result {
                tracing::error!("async WAL append failed, worker faulted: {e}");
            }
        }
        WalCommand::Rotate {
            writer_id,
            new_memtable_id,
            ack,
        } => {
            let result = match workers.get_mut(&writer_id) {
                Some(worker) => worker.rotate(new_memtable_id),
                None => Err(TsdbError::Storage("wal writer not registered".into())),
            };
            let _ = ack.send(result);
        }
        WalCommand::MemTableEnd {
            writer_id,
            end_lsn,
            closed_memtable_id,
            new_memtable_id,
            ack,
        } => {
            let result = match workers.get_mut(&writer_id) {
                Some(worker) => worker.memtable_end(end_lsn, closed_memtable_id, new_memtable_id),
                None => Err(TsdbError::Storage("wal writer not registered".into())),
            };
            let _ = ack.send(result);
        }
        WalCommand::Flush { writer_id, ack } => {
            let result = match workers.get_mut(&writer_id) {
                Some(worker) => worker.flush_all(),
                None => Ok(()),
            };
            let _ = ack.send(result);
        }
        WalCommand::Deregister { writer_id, ack } => {
            let result = match workers.remove(&writer_id) {
                Some(mut worker) => worker.flush_all(),
                None => Ok(()),
            };
            if let Some(ack) = ack {
                let _ = ack.send(result);
            }
        }
    }
}

fn flush_dirty_queue(workers: &mut HashMap<u64, WalWorker>, dirty_queue: &mut VecDeque<u64>) {
    let n = dirty_queue.len();
    for _ in 0..n {
        let Some(writer_id) = dirty_queue.pop_front() else {
            break;
        };
        let Some(worker) = workers.get_mut(&writer_id) else {
            continue;
        };
        if !worker.is_dirty {
            continue;
        }
        if let Err(e) = worker.flush_micro_batch() {
            tracing::error!("wal timed micro-batch flush failed: {e}");
        }
        // Still dirty only if new data arrived mid-flush; faulted workers clear dirty.
        if worker.is_dirty {
            dirty_queue.push_back(writer_id);
        }
    }
}

/// Lightweight WAL handle: routes commands to its assigned worker in the shared pool.
pub struct WalWriter {
    writer_id: u64,
    tx: mpsc::Sender<WalCommand>,
    active_memtable_id: AtomicU64,
    options: WalWriterOptions,
    health: Arc<WalIoHealth>,
}

impl WalWriter {
    pub fn open(base_dir: &Path, memtable_id: u64) -> Result<Self> {
        Self::open_with_options(
            base_dir,
            memtable_id,
            WalWriterOptions::with_durability(WalDurabilityMode::default()),
        )
    }

    pub fn open_with_mode(
        base_dir: &Path,
        memtable_id: u64,
        durability: WalDurabilityMode,
    ) -> Result<Self> {
        Self::open_with_options(
            base_dir,
            memtable_id,
            WalWriterOptions::with_durability(durability),
        )
    }

    pub fn open_with_options(
        base_dir: &Path,
        memtable_id: u64,
        options: WalWriterOptions,
    ) -> Result<Self> {
        let (writer_id, tx) = WalThreadPool::global().assign();
        let health = Arc::new(WalIoHealth::default());
        let params = WalWriterParams {
            base_dir: base_dir.to_path_buf(),
            memtable_id,
            micro_batch_max_bytes: options.micro_batch_max_bytes.max(1),
            segment_max_bytes: options.segment_max_bytes.max(1),
            block_max_bytes: options
                .block_max_bytes
                .clamp(1, DEFAULT_WAL_BLOCK_MAX_BYTES),
            table_backlog: options.table_backlog.clone(),
            backlog: options.backlog.clone(),
            sync_writes: options.durability.is_sync(),
            table_name: options.table_name.clone(),
            notify: options.notify.clone(),
            health: health.clone(),
        };

        let tx_for_register = tx.clone();
        run_wal_blocking(move || {
            let (ack_tx, ack_rx) = oneshot::channel();
            tx_for_register
                .blocking_send(WalCommand::Register {
                    writer_id,
                    params: Box::new(params),
                    ack: ack_tx,
                })
                .map_err(|_| TsdbError::Storage("wal pool stopped".into()))?;
            ack_rx
                .blocking_recv()
                .map_err(|_| TsdbError::Storage("wal register ack missing".into()))?
        })?;

        Ok(Self {
            writer_id,
            tx,
            active_memtable_id: AtomicU64::new(memtable_id),
            options,
            health,
        })
    }

    pub fn durability_mode(&self) -> WalDurabilityMode {
        self.options.durability
    }

    pub fn options(&self) -> &WalWriterOptions {
        &self.options
    }

    pub fn backlog_used(&self) -> usize {
        self.options.table_backlog.used_bytes()
    }

    pub fn active_memtable_id(&self) -> u64 {
        self.active_memtable_id.load(Ordering::Acquire)
    }

    /// Append `batch` stamped with global `lsn` (0 when replication is disabled).
    pub async fn append_batch(&self, batch: Arc<RecordBatch>, lsn: u64) -> Result<()> {
        if self.options.durability.is_sync() {
            self.write_sync(batch, lsn).await
        } else {
            self.write_async(batch, lsn).await
        }
    }

    async fn enqueue_append(
        &self,
        batch: Arc<RecordBatch>,
        lsn: u64,
        ack: Option<oneshot::Sender<Result<()>>>,
    ) -> Result<()> {
        self.health.check()?;
        let bytes = wal_batch_bytes(batch.as_ref());
        self.options
            .backlog
            .reserve(&self.options.table_backlog, bytes)
            .await;
        if self
            .tx
            .send(WalCommand::Append {
                writer_id: self.writer_id,
                batch,
                lsn,
                ack,
            })
            .await
            .is_err()
        {
            self.options
                .backlog
                .release(&self.options.table_backlog, bytes);
            return Err(TsdbError::Storage("wal pool stopped".into()));
        }
        Ok(())
    }

    pub async fn write_sync(&self, batch: Arc<RecordBatch>, lsn: u64) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.enqueue_append(batch, lsn, Some(ack_tx)).await?;
        ack_rx
            .await
            .map_err(|_| TsdbError::Storage("wal write ack missing".into()))?
    }

    pub async fn write_async(&self, batch: Arc<RecordBatch>, lsn: u64) -> Result<()> {
        self.enqueue_append(batch, lsn, None).await
    }

    pub async fn flush_async(&self) -> Result<()> {
        self.health.check()?;
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(WalCommand::Flush {
                writer_id: self.writer_id,
                ack: ack_tx,
            })
            .await
            .map_err(|_| TsdbError::Storage("wal pool stopped".into()))?;
        ack_rx
            .await
            .map_err(|_| TsdbError::Storage("wal flush ack missing".into()))?
    }

    pub fn flush(&self) -> Result<()> {
        self.health.check()?;
        let tx = self.tx.clone();
        let writer_id = self.writer_id;
        run_wal_blocking(move || {
            let (ack_tx, ack_rx) = oneshot::channel();
            tx.blocking_send(WalCommand::Flush {
                writer_id,
                ack: ack_tx,
            })
            .map_err(|_| TsdbError::Storage("wal pool stopped".into()))?;
            ack_rx
                .blocking_recv()
                .map_err(|_| TsdbError::Storage("wal flush ack missing".into()))?
        })
    }

    pub async fn rotate_async(&self, new_memtable_id: u64) -> Result<()> {
        self.health.check()?;
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(WalCommand::Rotate {
                writer_id: self.writer_id,
                new_memtable_id,
                ack: ack_tx,
            })
            .await
            .map_err(|_| TsdbError::Storage("wal pool stopped".into()))?;
        ack_rx
            .await
            .map_err(|_| TsdbError::Storage("wal rotate ack missing".into()))??;
        self.active_memtable_id
            .store(new_memtable_id, Ordering::Release);
        Ok(())
    }

    pub fn rotate(&self, new_memtable_id: u64) -> Result<()> {
        self.health.check()?;
        let tx = self.tx.clone();
        let writer_id = self.writer_id;
        run_wal_blocking(move || {
            let (ack_tx, ack_rx) = oneshot::channel();
            tx.blocking_send(WalCommand::Rotate {
                writer_id,
                new_memtable_id,
                ack: ack_tx,
            })
            .map_err(|_| TsdbError::Storage("wal pool stopped".into()))?;
            ack_rx
                .blocking_recv()
                .map_err(|_| TsdbError::Storage("wal rotate ack missing".into()))?
        })?;
        self.active_memtable_id
            .store(new_memtable_id, Ordering::Release);
        Ok(())
    }

    /// Seal the closed memtable in WAL (`MemTableEnd` + fsync) and switch to `new_memtable_id`.
    pub fn memtable_end(
        &self,
        end_lsn: u64,
        closed_memtable_id: u64,
        new_memtable_id: u64,
    ) -> Result<()> {
        self.health.check()?;
        let tx = self.tx.clone();
        let writer_id = self.writer_id;
        run_wal_blocking(move || {
            let (ack_tx, ack_rx) = oneshot::channel();
            tx.blocking_send(WalCommand::MemTableEnd {
                writer_id,
                end_lsn,
                closed_memtable_id,
                new_memtable_id,
                ack: ack_tx,
            })
            .map_err(|_| TsdbError::Storage("wal pool stopped".into()))?;
            ack_rx
                .blocking_recv()
                .map_err(|_| TsdbError::Storage("wal memtable_end ack missing".into()))?
        })?;
        self.active_memtable_id
            .store(new_memtable_id, Ordering::Release);
        Ok(())
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        // Never block Drop: async send on Tokio, else try_send (fail-fast).
        let tx = self.tx.clone();
        let writer_id = self.writer_id;
        let cmd = WalCommand::Deregister {
            writer_id,
            ack: None,
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = tx.send(cmd).await;
            });
        } else {
            let _ = tx.try_send(cmd);
        }
    }
}

/// Run a short WAL RPC that uses `blocking_send` / `blocking_recv`.
///
/// - Multi-thread Tokio: [`tokio::task::block_in_place`] (no extra OS threads / no starvation).
/// - Current-thread Tokio / tests: offload onto a scoped OS thread (cannot `block_in_place`).
/// - Outside Tokio: run inline.
fn run_wal_blocking<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send,
    T: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(f),
            tokio::runtime::RuntimeFlavor::CurrentThread => {
                std::thread::scope(|scope| scope.spawn(f).join().expect("wal blocking op"))
            }
            _ => std::thread::scope(|scope| scope.spawn(f).join().expect("wal blocking op")),
        },
        Err(_) => f(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::format::numbered_wal_path;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample_batch(v: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()
    }

    fn sample_batch_rows(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
    }

    #[tokio::test]
    async fn rotate_keeps_appending_same_file_without_bind() {
        let dir = tempfile::tempdir().unwrap();
        let wal = WalWriter::open(dir.path(), 1).unwrap();
        wal.write_sync(Arc::new(sample_batch(1)), 0).await.unwrap();
        wal.rotate(2).unwrap();
        wal.write_sync(Arc::new(sample_batch(2)), 0).await.unwrap();
        wal.flush().unwrap();

        // Flat chain: both logical mids see the full file content (LSN trim is at table recover).
        assert_eq!(
            MemTableWal::open(dir.path(), 1)
                .unwrap()
                .replay()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            MemTableWal::open(dir.path(), 2)
                .unwrap()
                .replay()
                .unwrap()
                .len(),
            2
        );
        let files = MemTableWal::list_wal_file_ids(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn segment_append_writes_multiple_batches_to_one_file() {
        let dir = tempfile::tempdir().unwrap();
        let wal = WalWriter::open(dir.path(), 9).unwrap();
        for i in 0..50 {
            wal.write_sync(Arc::new(sample_batch(i)), 0).await.unwrap();
        }
        wal.flush().unwrap();

        let mem_wal = MemTableWal::open(dir.path(), 9).unwrap();
        let batches = mem_wal.replay().unwrap();
        assert_eq!(batches.len(), 50);
        let files = MemTableWal::list_wal_file_ids(dir.path()).unwrap();
        assert_eq!(files, vec![1]);
        assert!(numbered_wal_path(mem_wal.wal_root(), 1).is_file());
    }

    #[tokio::test]
    async fn new_segments_use_numbered_flat_wal_files() {
        let dir = tempfile::tempdir().unwrap();
        let wal = WalWriter::open(dir.path(), 12).unwrap();
        wal.write_sync(Arc::new(sample_batch(1)), 0).await.unwrap();
        wal.flush().unwrap();
        let mem_wal = MemTableWal::open(dir.path(), 12).unwrap();
        let path = numbered_wal_path(mem_wal.wal_root(), 1);
        assert!(path.is_file());
        assert!(path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .ends_with(".wal"));
    }

    #[tokio::test]
    async fn rotate_finishes_segment_before_switching_memtable() {
        let dir = tempfile::tempdir().unwrap();
        let wal = WalWriter::open(dir.path(), 10).unwrap();
        wal.write_sync(Arc::new(sample_batch(1)), 0).await.unwrap();
        wal.rotate(11).unwrap();
        wal.write_sync(Arc::new(sample_batch(2)), 0).await.unwrap();
        wal.flush().unwrap();

        let old = MemTableWal::open(dir.path(), 10).unwrap().replay().unwrap();
        let new = MemTableWal::open(dir.path(), 11).unwrap().replay().unwrap();
        assert_eq!(old.len(), 2);
        assert_eq!(new.len(), 2);
    }

    #[tokio::test]
    async fn replay_reads_multi_row_batches() {
        let dir = tempfile::tempdir().unwrap();
        let wal = WalWriter::open(dir.path(), 3).unwrap();
        wal.write_sync(Arc::new(sample_batch_rows(&[1, 2, 3])), 0)
            .await
            .unwrap();
        wal.write_sync(Arc::new(sample_batch_rows(&[4, 5])), 0)
            .await
            .unwrap();
        wal.flush().unwrap();

        let batches = MemTableWal::open(dir.path(), 3).unwrap().replay().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[1].num_rows(), 2);
    }

    #[tokio::test]
    async fn flush_drains_pending_channel_appends() {
        let dir = tempfile::tempdir().unwrap();
        let wal = WalWriter::open_with_mode(dir.path(), 6, WalDurabilityMode::Async).unwrap();
        for i in 0..50 {
            wal.write_async(Arc::new(sample_batch(i)), 0).await.unwrap();
        }
        wal.flush().unwrap();

        let batches = MemTableWal::open(dir.path(), 6).unwrap().replay().unwrap();
        assert_eq!(batches.len(), 50);
    }

    #[tokio::test]
    async fn async_mode_queues_without_blocking() {
        let dir = tempfile::tempdir().unwrap();
        let wal = WalWriter::open_with_mode(dir.path(), 5, WalDurabilityMode::Async).unwrap();
        for i in 0..100 {
            wal.write_async(Arc::new(sample_batch(i)), 0).await.unwrap();
        }
        wal.flush().unwrap();
        assert_eq!(
            MemTableWal::open(dir.path(), 5)
                .unwrap()
                .replay()
                .unwrap()
                .len(),
            100
        );
    }

    #[tokio::test]
    async fn micro_batch_flushes_on_byte_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let options = WalWriterOptions {
            micro_batch_max_bytes: 64 * 1024,
            ..WalWriterOptions::with_durability(WalDurabilityMode::Async)
        };
        let wal = WalWriter::open_with_options(dir.path(), 8, options).unwrap();
        let mut total_bytes = 0usize;
        let mut count = 0usize;
        while total_bytes < 64 * 1024 {
            let batch = Arc::new(sample_batch_rows(&vec![count as i64; 256]));
            total_bytes += wal_batch_bytes(batch.as_ref());
            wal.write_async(batch, 0).await.unwrap();
            count += 1;
        }
        wal.flush().unwrap();
        let batches = MemTableWal::open(dir.path(), 8).unwrap().replay().unwrap();
        assert_eq!(batches.len(), count);
    }

    #[tokio::test]
    async fn micro_batch_timed_flush_fires_under_steady_ingest() {
        // Ghost-timeout regression: with timeout(recv), busy ingest never flushes until
        // byte threshold. Interval+select must flush within a few millseconds even if
        // messages keep arriving below the byte cap.
        let dir = tempfile::tempdir().unwrap();
        let options = WalWriterOptions {
            micro_batch_max_bytes: 16 * 1024 * 1024, // huge: force timer path
            ..WalWriterOptions::with_durability(WalDurabilityMode::Async)
        };
        let wal = WalWriter::open_with_options(dir.path(), 42, options).unwrap();
        for i in 0..8 {
            wal.write_async(Arc::new(sample_batch(i)), 0).await.unwrap();
            tokio::time::sleep(Duration::from_micros(100)).await;
        }

        // Poll until timer-driven flush drains all 8 (avoid CI scheduling flake).
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        let mut batches = Vec::new();
        while tokio::time::Instant::now() < deadline {
            batches = MemTableWal::open(dir.path(), 42).unwrap().replay().unwrap();
            if batches.len() >= 8 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            batches.len(),
            8,
            "timed micro-batch flush must land under steady low-rate ingest"
        );
    }

    #[tokio::test]
    async fn global_backpressure_limits_shared_budget() {
        let dir = tempfile::tempdir().unwrap();
        let backlog = Arc::new(WalBacklogBudget::new(32 * 1024, 32 * 1024));
        let options_a = WalWriterOptions {
            micro_batch_max_bytes: 1024 * 1024,
            backlog: backlog.clone(),
            durability: WalDurabilityMode::Async,
            table_backlog: backlog.new_table_backlog(),
            table_name: None,
            notify: None,
            segment_max_bytes: DEFAULT_WAL_SEGMENT_MAX_BYTES,
            block_max_bytes: DEFAULT_WAL_BLOCK_MAX_BYTES,
        };
        let options_b = WalWriterOptions {
            table_backlog: backlog.new_table_backlog(),
            ..options_a.clone()
        };
        let wal_a = WalWriter::open_with_options(&dir.path().join("a"), 9, options_a).unwrap();
        let wal_b = WalWriter::open_with_options(&dir.path().join("b"), 10, options_b).unwrap();
        for i in 0..100 {
            wal_a
                .write_async(Arc::new(sample_batch_rows(&vec![i; 512])), 0)
                .await
                .unwrap();
            wal_b
                .write_async(Arc::new(sample_batch_rows(&vec![i; 512])), 0)
                .await
                .unwrap();
        }
        assert!(backlog.global_used() <= backlog.global_limit());
        wal_a.flush().unwrap();
        wal_b.flush().unwrap();
    }

    #[tokio::test]
    async fn size_based_rotate_and_soft_memtable_rebind() {
        let dir = tempfile::tempdir().unwrap();
        let options = WalWriterOptions::with_durability(WalDurabilityMode::Sync)
            .with_segment_max_bytes(2 * 1024);
        let wal = WalWriter::open_with_options(dir.path(), 1, options).unwrap();

        for i in 0..40 {
            wal.write_sync(Arc::new(sample_batch_rows(&vec![i; 64])), 0)
                .await
                .unwrap();
        }
        wal.rotate(2).unwrap();
        wal.write_sync(Arc::new(sample_batch(99)), 0).await.unwrap();
        wal.flush().unwrap();

        let b1 = MemTableWal::open(dir.path(), 1).unwrap().replay().unwrap();
        let b2 = MemTableWal::open(dir.path(), 2).unwrap().replay().unwrap();
        assert!(!b1.is_empty(), "WAL still has frames after size rotate");
        assert_eq!(b1.len(), b2.len(), "replay reads the flat WAL chain");
        assert!(b2.len() > 1, "post-rotate batch is in the same chain");
    }
}
