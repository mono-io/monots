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

use crate::capture::table_capturer::{sst_to_capture_meta, TableCapturer};
use crate::compaction::dedup::{plan_flush, DedupeConfig, FlushPlan};
use crate::compaction::sst::{
    cleanup_sst_staging, flush_tmp_dir, promote_sst_from_flush_tmp,
    write_sst_streaming_try_with_config, FileIndex, SstMeta, SstWriteConfig,
};
use crate::compaction::sst_id::SstIdentity;
use crate::disk_space::DiskSpaceController;
use crate::memory::MemoryController;
use crate::memtable::MemTable;
use crate::replication::WalRetention;
use crate::sequence::TableSequence;
use crate::version::TableVersion;
use crate::wal::bulk_load::ArcBulkLoadWal;
use crate::wal::{BulkLoadWal, WalWriter, WalWriterOptions};
use arc_swap::ArcSwapOption;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common::{Result, TsdbError, WAL_SEGMENTS_DIR};
use parking_lot::{Mutex, RwLock};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};
use tracing::{error, info, instrument};

/// Tunables for flush windowing and Parquet SST writes (replaces hard-coded constants).
#[derive(Debug, Clone)]
pub struct SstFlushOptions {
    pub dedupe: DedupeConfig,
    pub write: SstWriteConfig,
}

impl Default for SstFlushOptions {
    fn default() -> Self {
        Self {
            dedupe: DedupeConfig::default(),
            write: SstWriteConfig::default(),
        }
    }
}

impl SstFlushOptions {
    pub fn from_sizes(flush_window_rows: usize, max_row_group_size: usize) -> Self {
        Self {
            dedupe: DedupeConfig {
                flush_window_rows: flush_window_rows.max(1),
            },
            write: SstWriteConfig {
                max_row_group_size: max_row_group_size.max(1),
                ..SstWriteConfig::default()
            },
        }
    }
}

/// Max flush attempts while draining the immutable queue in one scheduling turn.
/// Prevents CPU spin if `flush_one_immutable` returns `None` without making progress.
const MAX_FLUSH_DRAIN_ATTEMPTS: usize = 1000;

/// Cap concurrent `spawn_blocking` reclaim tasks so a table-storm cannot exhaust the blocking pool.
fn reclaim_semaphore() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| {
        let n = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .clamp(2, 8);
        Semaphore::new(n)
    })
}

/// Sized wrappers so ArcSwap can hold trait objects (RefCnt requires Sized).
struct FlushHook(Arc<dyn Fn(SstMeta) + Send + Sync>);
struct RetentionHandle(Arc<dyn WalRetention>);
struct CapturerHandle(Arc<dyn TableCapturer>);

impl std::ops::Deref for RetentionHandle {
    type Target = dyn WalRetention;
    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl std::ops::Deref for CapturerHandle {
    type Target = dyn TableCapturer;
    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

/// Controls when sealed memtable WAL is replayed into SST during [`LsmTable::open_with_options`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalRecoveryMode {
    /// Replay WAL before returning (lazy load, tests, CREATE TABLE).
    #[default]
    Immediate,
    /// Leave WAL replay to [`LsmTable::recover_disk`] (engine boot after Stream attach).
    Deferred,
}

/// Optional open-time behavior for [`LsmTable::open_with_options`].
#[derive(Debug, Clone, Copy, Default)]
pub struct TableOpenOptions {
    pub wal_recovery: WalRecoveryMode,
}

impl TableOpenOptions {
    pub fn deferred() -> Self {
        Self {
            wal_recovery: WalRecoveryMode::Deferred,
        }
    }
}

/// Per-table LSM tree.
pub struct LsmTable {
    pub name: Arc<str>,
    data_dir: PathBuf,
    schema: RwLock<SchemaRef>,
    memtable_max_bytes: usize,
    memtable_batch_max_rows: usize,
    memtable_batch_max_bytes: usize,
    memory: Arc<MemoryController>,
    sequence: Mutex<TableSequence>,

    version: RwLock<TableVersion>,

    /// Live WAL writer; absent until [`Self::recover_disk`] on deferred opens.
    wal: ArcSwapOption<WalWriter>,
    /// Set only while disk recovery is still pending (engine boot).
    pending_open: Mutex<Option<PendingOpen>>,
    /// Bulk-load WAL (`wal_segments/bulk_load/`): LSN-ordered hard links + entries.log.
    bulk_wal: ArcBulkLoadWal,
    /// Max LSN observed in durable WAL at open (before recovery may destroy segments).
    recovered_max_lsn: AtomicU64,
    flush_notify: Arc<Notify>,
    flush_lock: Mutex<()>,
    /// Held for the duration of `put_batch` so cross-table reclaim cannot flush mid-write.
    write_lock: tokio::sync::Mutex<()>,
    on_flush: ArcSwapOption<FlushHook>,
    replication: ArcSwapOption<RetentionHandle>,
    capturer: ArcSwapOption<CapturerHandle>,
    disk: ArcSwapOption<DiskSpaceController>,
    /// Flush / Parquet write tunables (row-group size, flush window).
    flush_opts: RwLock<SstFlushOptions>,
}

struct PendingOpen {
    wal_options: WalWriterOptions,
    max_bulk_id: u64,
}

impl LsmTable {
    fn require_wal(&self) -> Result<Arc<WalWriter>> {
        self.wal.load_full().ok_or_else(|| {
            TsdbError::Storage(format!(
                "table {} WAL is not active yet (call recover_disk first)",
                self.name
            ))
        })
    }

    fn ensure_write_ready(&self) -> Result<()> {
        if self.pending_open.lock().is_some() {
            return Err(TsdbError::Storage(format!(
                "table {} is still Starting (disk recovery not finished)",
                self.name
            )));
        }
        self.require_wal().map(|_| ())
    }

    /// Open a table; WAL replay timing is controlled by [`TableOpenOptions`].
    pub fn open_with_options(
        name: impl AsRef<str>,
        data_dir: impl AsRef<Path>,
        schema: SchemaRef,
        memtable_max_bytes: usize,
        memtable_batch_max_rows: usize,
        memtable_batch_max_bytes: usize,
        memory: Arc<MemoryController>,
        persisted_ssts: Vec<SstMeta>,
        wal_options: WalWriterOptions,
        options: TableOpenOptions,
    ) -> Result<Arc<Self>> {
        let table = Self::init_memory(
            name,
            data_dir,
            schema,
            memtable_max_bytes,
            memtable_batch_max_rows,
            memtable_batch_max_bytes,
            memory,
            persisted_ssts,
            wal_options,
        )?;
        if options.wal_recovery == WalRecoveryMode::Immediate {
            table.recover_disk()?;
        }
        Ok(table)
    }

    /// Open with immediate disk WAL recovery (lazy load, tests, on-demand table load).
    pub fn open(
        name: impl AsRef<str>,
        data_dir: impl AsRef<Path>,
        schema: SchemaRef,
        memtable_max_bytes: usize,
        memtable_batch_max_rows: usize,
        memtable_batch_max_bytes: usize,
        memory: Arc<MemoryController>,
        persisted_ssts: Vec<SstMeta>,
        wal_options: WalWriterOptions,
    ) -> Result<Arc<Self>> {
        Self::open_with_options(
            name,
            data_dir,
            schema,
            memtable_max_bytes,
            memtable_batch_max_rows,
            memtable_batch_max_bytes,
            memory,
            persisted_ssts,
            wal_options,
            TableOpenOptions::default(),
        )
    }

    /// Mount table memory + catalog SST index; does **not** replay memtable WAL or open live WAL.
    ///
    /// Engine boot uses this during [`crate::lifecycle::EngineLifecycle::Starting`], then
    /// attaches Stream capture, then calls [`Self::recover_disk`].
    fn init_memory(
        name: impl AsRef<str>,
        data_dir: impl AsRef<Path>,
        schema: SchemaRef,
        memtable_max_bytes: usize,
        memtable_batch_max_rows: usize,
        memtable_batch_max_bytes: usize,
        memory: Arc<MemoryController>,
        persisted_ssts: Vec<SstMeta>,
        wal_options: WalWriterOptions,
    ) -> Result<Arc<Self>> {
        let name: Arc<str> = Arc::from(name.as_ref());
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;
        cleanup_sst_staging(&data_dir)?;

        let sequence = TableSequence::load(&data_dir)?;
        let file_index = Arc::new(FileIndex::from_persisted(persisted_ssts)?);
        let bulk_wal = BulkLoadWal::open(&data_dir)?;
        let mut recovered_max_lsn = crate::wal::max_lsn_in_table_wals(&data_dir)?;
        recovered_max_lsn =
            recovered_max_lsn.max(crate::wal::max_lsn_in_sst_metas(&file_index.snapshot()));
        let max_bulk_id = bulk_wal.recover_into_index(&file_index)?;

        // Empty placeholder mutable until recover_disk allocates the real active id + WAL.
        let mutable = Self::new_memtable(
            0,
            schema.clone(),
            memtable_max_bytes,
            memtable_batch_max_rows,
            memtable_batch_max_bytes,
            memory.clone(),
        );

        Ok(Arc::new(Self {
            name,
            data_dir,
            schema: RwLock::new(schema.clone()),
            memtable_max_bytes,
            memtable_batch_max_rows,
            memtable_batch_max_bytes,
            memory,
            sequence: Mutex::new(sequence),
            version: RwLock::new(TableVersion::new(mutable, file_index)),
            wal: ArcSwapOption::from(None),
            pending_open: Mutex::new(Some(PendingOpen {
                wal_options,
                max_bulk_id,
            })),
            bulk_wal,
            recovered_max_lsn: AtomicU64::new(recovered_max_lsn),
            flush_notify: Arc::new(Notify::new()),
            flush_lock: Mutex::new(()),
            write_lock: tokio::sync::Mutex::new(()),
            on_flush: ArcSwapOption::from(None::<Arc<FlushHook>>),
            replication: ArcSwapOption::from(None::<Arc<RetentionHandle>>),
            capturer: ArcSwapOption::from(None::<Arc<CapturerHandle>>),
            disk: ArcSwapOption::from(None::<Arc<DiskSpaceController>>),
            flush_opts: RwLock::new(SstFlushOptions::default()),
        }))
    }

    /// Replay sealed memtable WAL → SST and install the live memtable + WAL writer.
    ///
    /// Idempotent. On engine boot, call after Stream capture is attached so recovery flushes
    /// go through [`Self::link_and_capture_flush`].
    pub fn recover_disk(self: &Arc<Self>) -> Result<()> {
        let pending = match self.pending_open.lock().take() {
            Some(pending) => pending,
            None if self.wal.load_full().is_some() => return Ok(()),
            None => {
                return Err(TsdbError::Storage(format!(
                    "table {} has no pending disk recovery",
                    self.name
                )));
            }
        };

        let max_recovered_id = if crate::wal::has_recoverable_memtable_wal(&self.data_dir)? {
            self.recover_wal_to_sst()?
        } else {
            tracing::debug!(
                table = %self.name,
                path = %self.data_dir.display(),
                "no recoverable memtable WAL; skip WAL content recovery"
            );
            0
        }
        .max(pending.max_bulk_id);

        let active_id = {
            let mut sequence = self.sequence.lock();
            let active_id = sequence.next_id()?;
            if max_recovered_id > 0 {
                sequence.ensure_at_least(max_recovered_id.saturating_add(1))?;
            }
            active_id
        };

        {
            let mut version = self.version.write();
            version.mutable = Self::new_memtable(
                active_id,
                self.schema.read().clone(),
                self.memtable_max_bytes,
                self.memtable_batch_max_rows,
                self.memtable_batch_max_bytes,
                self.memory.clone(),
            );
        }

        let wal = WalWriter::open_with_options(&self.data_dir, active_id, pending.wal_options)?;
        self.wal.store(Some(Arc::new(wal)));

        tracing::info!(
            table = %self.name,
            active_memtable_id = active_id,
            max_recovered_id,
            "disk recovery complete; table ready for writes"
        );
        Ok(())
    }

    /// Whether this table still needs [`Self::recover_disk`].
    pub fn needs_disk_recovery(&self) -> bool {
        self.pending_open.lock().is_some()
    }

    fn new_memtable(
        id: u64,
        schema: SchemaRef,
        memtable_max_bytes: usize,
        memtable_batch_max_rows: usize,
        memtable_batch_max_bytes: usize,
        memory: Arc<MemoryController>,
    ) -> Arc<MemTable> {
        MemTable::new(
            id,
            schema,
            memtable_max_bytes,
            memory,
            memtable_batch_max_rows,
            memtable_batch_max_bytes,
        )
    }

    /// Replay unflushed WAL batches into SST (LSN trim against disk frontier).
    ///
    /// Online flush is memory-size driven; crash recovery only rebuilds batch frames whose LSN
    /// is not yet covered by SST metadata (`lsn > sst_max_lsn`). Soft size limits keep peak
    /// recover memory near `memtable_max_bytes`.
    fn recover_wal_to_sst(&self) -> Result<u64> {
        use crate::wal::walk_unflushed_partitions;
        use std::cell::Cell;

        let data_dir = &self.data_dir;
        let schema = self.schema.read().clone();
        let file_index = self.file_index();
        let flush_opts = self.flush_opts.read().clone();
        let sst_max_lsn = crate::wal::max_lsn_in_sst_metas(&file_index.snapshot());
        let sst_has_files = !file_index.snapshot().is_empty();
        let next_id = Cell::new(1u64);
        let max_recovered_id = Cell::new(0u64);

        walk_unflushed_partitions(
            data_dir,
            sst_max_lsn,
            sst_has_files,
            self.memtable_max_bytes,
            |part| {
                let memtable_id = next_id.get();
                next_id.set(memtable_id.saturating_add(1));
                max_recovered_id.set(max_recovered_id.get().max(memtable_id));

                let mem = MemTable::from_batches(
                    memtable_id,
                    schema.clone(),
                    self.memtable_max_bytes,
                    self.memory.clone(),
                    self.memtable_batch_max_rows,
                    self.memtable_batch_max_bytes,
                    part.batches,
                )?;

                let (base_lsn, max_lsn) = if part.max_lsn > 0 {
                    let base = if part.base_lsn == 0 {
                        part.max_lsn
                    } else {
                        part.base_lsn
                    };
                    (base, part.max_lsn)
                } else {
                    (0, 0)
                };

                match Self::write_memtable_parquet(
                    mem.clone(),
                    &data_dir.to_path_buf(),
                    schema.clone(),
                    &flush_opts,
                    base_lsn,
                    max_lsn,
                ) {
                    Ok(mut meta) => {
                        self.link_and_capture_flush(&mut meta)?;
                        file_index.insert(meta.clone());
                        mem.release_memory();
                        tracing::info!(
                            table = %self.name,
                            "recovered WAL partition {memtable_id} ({} rows, lsn={}) to {}",
                            meta.row_count,
                            meta.max_lsn,
                            meta.file_path
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            table = %self.name,
                            "failed to flush recovered WAL partition {memtable_id} in {}: {e}",
                            data_dir.display()
                        );
                    }
                }
                Ok(())
            },
        )?;

        Self::gc_sealed_wal_files_at(data_dir, &file_index)?;
        Ok(max_recovered_id.get())
    }

    /// Write memtable chunk list to a new SST on disk (does not update the live version).
    ///
    /// Bytes land under `{data_dir}/.flush_tmp/` first, then are atomically renamed into
    /// `data_dir/` before capture / FileIndex see the path — so incomplete Parquet never
    /// becomes visible storage.
    ///
    /// Uses a truly streaming write: on the common monotonic-ingest path the chunks are appended
    /// to the Parquet writer one at a time (zero-copy, no full-memtable `concat`); on the disorder
    /// path rows are reordered via a timestamp-sorted coordinate index and materialized one output
    /// window at a time. Either way peak flush memory stays bounded rather than doubling the
    /// memtable. The bounded transient working set is intentionally *not* charged to the global
    /// `MemoryController` for now.
    fn write_memtable_parquet(
        memtable: Arc<MemTable>,
        data_dir: &PathBuf,
        schema: SchemaRef,
        flush_opts: &SstFlushOptions,
        min_lsn: u64,
        max_lsn: u64,
    ) -> Result<SstMeta> {
        let snapshot = memtable.snapshot_chunks()?;
        if snapshot.chunks.is_empty() {
            return Err(TsdbError::Storage("nothing to flush".into()));
        }

        tracing::info!(
            memtable_id = memtable.id,
            min_lsn,
            max_lsn,
            chunk_count = snapshot.layout.full_chunks + usize::from(snapshot.layout.tail_rows > 0),
            full_chunks = snapshot.layout.full_chunks,
            tail_rows = snapshot.layout.tail_rows,
            total_rows = snapshot.layout.total_rows,
            ram_cost = snapshot.layout.ram_cost,
            "flushing memtable batches to SST"
        );

        let memtable_id = memtable.id;
        let identity = SstIdentity::fresh_flush(min_lsn, max_lsn);
        // Write under `.flush_tmp/` first so readers never observe a partial SST; then promote.
        let tmp_dir = flush_tmp_dir(data_dir);
        std::fs::create_dir_all(&tmp_dir)?;
        let staged = match plan_flush(&snapshot.chunks, schema.clone())? {
            FlushPlan::Streaming(chunks) => write_sst_streaming_try_with_config(
                &identity,
                &tmp_dir,
                schema,
                chunks.into_iter().map(Ok),
                &flush_opts.write,
            ),
            FlushPlan::Sorted(sorted) => write_sst_streaming_try_with_config(
                &identity,
                &tmp_dir,
                schema,
                sorted.window_batches(flush_opts.dedupe.flush_window_rows.max(1)),
                &flush_opts.write,
            ),
        };
        let meta = match staged {
            Ok(meta) => promote_sst_from_flush_tmp(meta, data_dir)?,
            Err(e) => {
                // Best-effort: drop this attempt's staging file if the writer left one behind.
                let staging = tmp_dir.join(identity.filename());
                let _ = std::fs::remove_file(staging);
                return Err(e);
            }
        };
        tracing::debug!(
            memtable_id,
            min_lsn,
            max_lsn,
            "SST written for memtable flush"
        );
        Ok(meta)
    }

    /// Override flush window / Parquet writer tunables (engine / YAML).
    pub fn set_sst_flush_options(&self, opts: SstFlushOptions) {
        *self.flush_opts.write() = opts;
    }

    pub fn sst_flush_options(&self) -> SstFlushOptions {
        self.flush_opts.read().clone()
    }

    /// Atomically install a flushed immutable memtable as SST and drop the memtable.
    fn apply_immutable_flush(&self, memtable_id: u64, meta: SstMeta) -> bool {
        let mut version = self.version.write();
        if version
            .immutables
            .first()
            .is_some_and(|m| m.id == memtable_id)
        {
            let mem = version.immutables.remove(0);
            version.sstables.insert(meta);
            drop(version);
            mem.release_memory();
            return true;
        }
        false
    }

    /// Freeze active → immutable and install a fresh active memtable (LSM classic rotation).
    ///
    /// Writes a durable [`RecordType::MemTableEnd`] WAL frame (fsynced) at the sealed
    /// memtable's max LSN, notifies capture with a watermark, then wakes the flush worker.
    fn freeze_active_memtable(&self) -> Result<bool> {
        let new_id = self.sequence.lock().next_id()?;

        let sealed = {
            let mut version = self.version.write();
            version.mutable.seal()?;
            if version.mutable.size_bytes() == 0 {
                None
            } else {
                let frozen_id = version.mutable.id;
                let (_base, end_lsn) = version.mutable.lsn_span();
                let new_mutable = Self::new_memtable(
                    new_id,
                    self.schema.read().clone(),
                    self.memtable_max_bytes,
                    self.memtable_batch_max_rows,
                    self.memtable_batch_max_bytes,
                    self.memory.clone(),
                );
                let old = std::mem::replace(&mut version.mutable, new_mutable);
                version.immutables.push(old);
                tracing::info!(
                    table = %self.name,
                    frozen_memtable_id = frozen_id,
                    new_memtable_id = new_id,
                    end_lsn,
                    immutable_queue_len = version.immutables.len(),
                    "active memtable frozen"
                );
                Some((frozen_id, end_lsn))
            }
        };

        let Some((frozen_id, end_lsn)) = sealed else {
            return Ok(false);
        };

        // MemTableEnd + fsync, then logical memtable id switch (replaces plain rotate).
        self.require_wal()?
            .memtable_end(end_lsn, frozen_id, new_id)?;
        if let Some(cap) = self.capturer.load().as_ref() {
            cap.on_memtable_end(end_lsn);
        }
        self.flush_notify.notify_one();
        Ok(true)
    }

    /// Attach the replication substrate: enables global-LSN stamping on writes and progress-gated WAL
    /// retention (WAL for a flushed memtable is kept until every capture progress has committed past it).
    pub fn set_replication(&self, retention: Arc<dyn WalRetention>) {
        self.replication
            .store(Some(Arc::new(RetentionHandle(retention))));
    }

    /// Attach per-table capture listener (Insert / Flush[+BulkLoad] / Compact).
    ///
    /// Only install after CREATE STREAM — see [`crate::LsmEngine::register_capture_source`].
    pub fn set_capturer(&self, capturer: Arc<dyn TableCapturer>) {
        self.capturer
            .store(Some(Arc::new(CapturerHandle(capturer))));
    }

    pub fn clear_capturer(&self) {
        self.capturer.store(None);
    }

    pub fn capturer(&self) -> Option<Arc<dyn TableCapturer>> {
        self.capturer.load_full().map(|h| Arc::clone(&h.0))
    }

    pub fn set_on_flush(&self, cb: Arc<dyn Fn(SstMeta) + Send + Sync>) {
        self.on_flush.store(Some(Arc::new(FlushHook(cb))));
    }

    /// Shared disk watermark (engine-wide). Blocks user writes when free space is critical.
    pub fn set_disk_space(&self, disk: Arc<DiskSpaceController>) {
        self.disk.store(Some(disk));
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.read().clone()
    }

    /// Flush-on-DDL: after [`Self::flush_all`], rotate the active memtable to `new_schema`.
    /// WAL rotate first; memory swap only after I/O succeeds.
    #[instrument(skip_all, fields(table = %self.name))]
    pub fn apply_schema_evolution(&self, new_schema: SchemaRef) -> Result<()> {
        if let Some(disk) = self.disk.load_full() {
            disk.ensure_writable()?;
        }

        let new_id = self.sequence.lock().next_id()?;
        let new_mutable = Self::new_memtable(
            new_id,
            new_schema.clone(),
            self.memtable_max_bytes,
            self.memtable_batch_max_rows,
            self.memtable_batch_max_bytes,
            self.memory.clone(),
        );

        self.require_wal()?.rotate(new_id).map_err(|e| {
            error!(error = %e, "WAL rotate failed during schema evolution; memory state preserved");
            e
        })?;

        *self.schema.write() = new_schema.clone();
        {
            let mut version = self.version.write();
            version.mutable.release_memory();
            version.immutables.clear();
            version.mutable = new_mutable;
        }

        info!(
            new_memtable_id = new_id,
            columns = new_schema.fields().len(),
            "schema evolution applied: active memtable rotated"
        );
        Ok(())
    }

    /// Blocks concurrent writes (used during DDL schema evolution).
    pub async fn block_writes(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.write_lock.lock().await
    }

    pub fn file_index(&self) -> Arc<FileIndex> {
        self.version.read().sstables.clone()
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn bulk_wal(&self) -> ArcBulkLoadWal {
        Arc::clone(&self.bulk_wal)
    }

    /// Max LSN observed in durable WAL / SST metadata when this table was opened.
    pub fn recovered_max_lsn(&self) -> u64 {
        self.recovered_max_lsn.load(Ordering::Acquire)
    }

    /// Whether BulkLoad WAL still pins this SST path (compaction must defer unlink).
    pub fn bulk_load_pins(&self, path: &str) -> bool {
        self.bulk_wal.pins_path(path)
    }

    pub fn active_wal_memtable_id(&self) -> u64 {
        self.wal
            .load_full()
            .map(|w| w.active_memtable_id())
            .unwrap_or(0)
    }

    /// Ids of sealed (immutable) memtables still resident in memory + closed WAL,
    /// oldest first. These are the "degrade to WAL segment" capture units.
    pub fn sealed_memtable_ids(&self) -> Vec<u64> {
        self.version
            .read()
            .immutables
            .iter()
            .map(|m| m.id)
            .collect()
    }

    /// Total bytes charged for active + queued immutable memtables.
    pub fn pending_memtable_bytes(&self) -> usize {
        let version = self.version.read();
        let mut total = version.mutable.charged_bytes();
        for imm in version.immutables.iter() {
            total += imm.charged_bytes();
        }
        total
    }

    /// Footprint of live memtable data (may differ slightly from charged during rotation).
    pub fn pending_memtable_footprint(&self) -> usize {
        let version = self.version.read();
        let mut total = version.mutable.footprint_bytes();
        for imm in version.immutables.iter() {
            total += imm.footprint_bytes();
        }
        total
    }

    /// Flush one in-memory memtable to SST to release global memory budget.
    /// Prefers the oldest immutable buffer; otherwise freezes and flushes active.
    pub fn reclaim_memtable_memory(&self) -> Result<bool> {
        if !self.version.read().immutables.is_empty() {
            return Ok(self.flush_one_immutable()?.is_some());
        }
        Ok(self.flush_active_memtable()?.is_some())
    }

    /// Like [`Self::reclaim_memtable_memory`], but skips active flush when a write is in progress.
    pub fn try_reclaim_memtable_memory(&self) -> Result<bool> {
        if !self.version.read().immutables.is_empty() {
            return Ok(self.flush_one_immutable()?.is_some());
        }
        let rotated = {
            let Ok(_write_guard) = self.write_lock.try_lock() else {
                return Ok(false);
            };
            self.freeze_active_memtable()?
        };
        if !rotated {
            return Ok(false);
        }
        self.flush_notify.notify_one();
        self.flush_one_immutable().map(|m| m.is_some())
    }

    /// Bytes in the active memtable only.
    pub fn active_memtable_bytes(&self) -> usize {
        self.version.read().mutable.size_bytes()
    }

    /// Bytes in the oldest immutable memtable, if any.
    pub fn oldest_immutable_bytes(&self) -> usize {
        self.version
            .read()
            .immutables
            .first()
            .map(|m| m.size_bytes())
            .unwrap_or(0)
    }

    pub fn global_memory_used_bytes(&self) -> usize {
        self.memory.used_bytes()
    }

    pub fn allocate_file_id(&self) -> Result<u64> {
        self.sequence.lock().next_id()
    }

    /// Write path: enqueue WAL first, then memtable shares the same `Arc` batch.
    #[instrument(skip(self, batch), fields(table = %self.name), level = "debug")]
    pub async fn put_batch(&self, batch: RecordBatch) -> Result<()> {
        self.ensure_write_ready()?;
        if let Some(disk) = self.disk.load_full() {
            disk.ensure_writable()?;
        }

        if self.memory.at_or_over_limit() {
            self.reclaim_off_thread().await?;
        }

        {
            let _write_guard = self.write_lock.lock().await;
            let batch = Arc::new(batch);
            let wal = self.require_wal()?;
            debug_assert_eq!(self.version.read().mutable.id, wal.active_memtable_id());

            self.memory.ensure_write_allowed()?;
            if let Some(disk) = self.disk.load_full() {
                disk.ensure_writable()?;
            }

            let rep = self.replication.load();
            let Some(rep) = rep.as_ref() else {
                return Err(TsdbError::Storage(
                    "LSN required: attach replication before writing (engine.register_table does this)"
                        .into(),
                ));
            };
            let lsn = rep.on_write()?;
            debug_assert!(lsn > 0, "allocated LSN must be non-zero");

            wal.append_batch(batch.clone(), lsn).await?;

            let mem = self.version.read().mutable.clone();
            mem.record_lsn(lsn);
            let should_rotate = mem.insert_for_table(batch.clone(), Some(&self.name))?;

            if let Some(cap) = self.capturer.load().as_ref() {
                cap.on_insert(lsn, batch.as_ref().clone());
            }

            if should_rotate {
                self.switch_memtable().await?;
            }
        }

        self.flush_notify.notify_one();

        if self.memory.at_or_over_soft_threshold() {
            self.reclaim_off_thread().await?;
        }
        Ok(())
    }

    /// Run one reclaim on the blocking pool (bounded concurrency).
    async fn reclaim_off_thread(&self) -> Result<()> {
        let hard = self.memory.at_or_over_limit();
        let permit = if hard {
            reclaim_semaphore()
                .acquire()
                .await
                .map_err(|e| TsdbError::Storage(e.to_string()))?
        } else {
            match reclaim_semaphore().try_acquire() {
                Ok(p) => p,
                Err(_) => return Ok(()),
            }
        };
        let memory = self.memory.clone();
        let table = self.name.clone();
        let result = tokio::task::spawn_blocking(move || {
            memory.maybe_reclaim_under_pressure(Some(&table));
        })
        .await
        .map_err(|e| TsdbError::Storage(e.to_string()));
        drop(permit);
        result
    }

    async fn switch_memtable(&self) -> Result<()> {
        let new_id = self.sequence.lock().next_id()?;

        let sealed = {
            let mut version = self.version.write();
            version.mutable.seal()?;
            if version.mutable.size_bytes() == 0 {
                None
            } else {
                let frozen_id = version.mutable.id;
                let (_base, end_lsn) = version.mutable.lsn_span();
                let new_mutable = Self::new_memtable(
                    new_id,
                    self.schema.read().clone(),
                    self.memtable_max_bytes,
                    self.memtable_batch_max_rows,
                    self.memtable_batch_max_bytes,
                    self.memory.clone(),
                );
                let old = std::mem::replace(&mut version.mutable, new_mutable);
                version.immutables.push(old);
                tracing::info!(
                    table = %self.name,
                    frozen_memtable_id = frozen_id,
                    new_memtable_id = new_id,
                    end_lsn,
                    immutable_queue_len = version.immutables.len(),
                    "memtable rotated"
                );
                Some((frozen_id, end_lsn))
            }
        };

        let Some((frozen_id, end_lsn)) = sealed else {
            return Ok(());
        };

        // Blocking MemTableEnd+fsync on the WAL pool (must be durable before continue).
        let wal = self.require_wal()?.clone();
        tokio::task::spawn_blocking(move || wal.memtable_end(end_lsn, frozen_id, new_id))
            .await
            .map_err(|e| TsdbError::Storage(format!("wal memtable_end join: {e}")))??;
        if let Some(cap) = self.capturer.load().as_ref() {
            cap.on_memtable_end(end_lsn);
        }
        Ok(())
    }

    fn destroy_wal_for_memtable(&self, _memtable_id: u64) -> Result<()> {
        self.gc_sealed_wal_files()
    }

    /// Drop sealed numbered WAL files once SST (+ capture) cover their max LSN.
    fn gc_sealed_wal_files(&self) -> Result<()> {
        use crate::wal::format::{list_wal_file_ids, numbered_wal_path};
        use crate::wal::{can_drop_wal_file, lsn_range_in_segment};

        let wal_root = self.wal_root();
        let ids = list_wal_file_ids(&wal_root)?;
        let Some(&active) = ids.last() else {
            return Ok(());
        };
        let index = self.file_index();
        let rep = self.replication.load_full();
        for file_id in ids {
            if file_id == active {
                continue;
            }
            let path = numbered_wal_path(&wal_root, file_id);
            let (_, hi) = lsn_range_in_segment(&path)?;
            if let Some(rep) = rep.as_ref() {
                if hi > 0 && !rep.can_gc_lsn(hi) {
                    tracing::debug!(
                        table = %self.name,
                        file_id,
                        max_lsn = hi,
                        "WAL retained for lagging capture progress"
                    );
                    continue;
                }
            }
            if !can_drop_wal_file(&self.data_dir, file_id, index.as_ref())? {
                continue;
            }
            if path.exists() {
                std::fs::remove_file(&path)?;
                tracing::info!(
                    table = %self.name,
                    file_id,
                    max_lsn = hi,
                    "sealed WAL file reclaimed"
                );
            }
        }
        Ok(())
    }

    fn gc_sealed_wal_files_at(data_dir: &Path, file_index: &FileIndex) -> Result<()> {
        use crate::wal::format::{list_wal_file_ids, numbered_wal_path};
        use crate::wal::{can_drop_wal_file, lsn_range_in_segment};

        let wal_root = data_dir.join(WAL_SEGMENTS_DIR);
        let ids = list_wal_file_ids(&wal_root)?;
        let Some(&active) = ids.last() else {
            return Ok(());
        };
        for file_id in ids {
            if file_id == active {
                continue;
            }
            let path = numbered_wal_path(&wal_root, file_id);
            let (_, hi) = lsn_range_in_segment(&path)?;
            if !can_drop_wal_file(data_dir, file_id, file_index)? {
                continue;
            }
            if path.exists() {
                std::fs::remove_file(&path)?;
                tracing::info!(
                    file_id,
                    max_lsn = hi,
                    "sealed WAL file reclaimed after recover"
                );
            }
        }
        Ok(())
    }

    /// Reclaim sealed WAL files once SST + every capture progress have passed their LSN range.
    pub fn gc_retained_wal(&self) -> Result<()> {
        self.gc_sealed_wal_files()
    }

    fn notify_flush(&self, meta: &SstMeta) {
        if let Some(hook) = self.on_flush.load().as_ref() {
            (hook.0)(meta.clone());
        }
    }

    /// Point-in-time view for reads: mutable/immutable memtables + frozen SST manifest.
    pub fn get_snapshots(&self) -> (Arc<MemTable>, Vec<Arc<MemTable>>, Vec<SstMeta>) {
        self.version.read().snapshot()
    }

    fn flush_one_immutable_locked(&self) -> Result<Option<SstMeta>> {
        let mem = {
            let version = self.version.read();
            if version.immutables.is_empty() {
                return Ok(None);
            }
            version.immutables[0].clone()
        };

        mem.seal()?;
        if mem.size_bytes() == 0 {
            let mut version = self.version.write();
            if version.immutables.first().is_some_and(|m| m.id == mem.id) {
                version.immutables.remove(0);
            }
            let _ = self.destroy_wal_for_memtable(mem.id);
            return Ok(None);
        }

        let memtable_id = mem.id;
        // Stamp SST from the memtable's inclusive LSN span (durable truth after flush is SST).
        let (base_lsn, max_lsn) = mem.lsn_span();
        if base_lsn > 0 || max_lsn > 0 {
            debug_assert!(
                base_lsn <= max_lsn,
                "flush LSN span inverted: [{base_lsn}, {max_lsn}]"
            );
            // Disk-level CDC order: this flush must not land behind an already-indexed SST span
            // that ends after our base (FIFO immutables + global LSN allocate keep this true).
            let frontier = crate::wal::max_lsn_in_sst_metas(&self.file_index().snapshot());
            if frontier > 0 && base_lsn > 0 && base_lsn <= frontier {
                debug_assert!(
                    self.file_index().covers_lsn(base_lsn),
                    "flush base_lsn {base_lsn} overlaps SST frontier {frontier} without coverage"
                );
            }
        }
        let mut meta = Self::write_memtable_parquet(
            mem,
            &self.data_dir,
            self.schema.read().clone(),
            &self.flush_opts.read(),
            base_lsn,
            max_lsn,
        )?;

        // Capturer enqueue **before** FileIndex update so every stream sees the event first.
        self.link_and_capture_flush(&mut meta)?;

        if !self.apply_immutable_flush(memtable_id, meta.clone()) {
            return Ok(None);
        }

        self.destroy_wal_for_memtable(memtable_id)?;
        self.notify_flush(&meta);
        tracing::info!(
            table = %self.name,
            memtable_id,
            base_lsn = meta.base_lsn,
            max_lsn = meta.max_lsn,
            rows = meta.row_count,
            bytes = meta.file_size,
            min_ts = meta.min_ts,
            max_ts = meta.max_ts,
            path = %meta.file_path,
            "immutable memtable flushed to SST"
        );
        Ok(Some(meta))
    }

    pub fn flush_one_immutable(&self) -> Result<Option<SstMeta>> {
        let _flush_guard = self.flush_lock.lock();
        self.flush_one_immutable_locked()
    }

    /// Freeze the active buffer then flush it from the immutable queue.
    pub fn flush_active_memtable(&self) -> Result<Option<SstMeta>> {
        let _flush_guard = self.flush_lock.lock();
        if !self.freeze_active_memtable()? {
            return Ok(None);
        }
        self.flush_one_immutable_locked()
    }

    pub fn flush_all(&self) -> Result<Vec<SstMeta>> {
        let _flush_guard = self.flush_lock.lock();
        let mut flushed = Vec::new();
        let _ = self.freeze_active_memtable()?;
        // Bound the drain + require progress on Ok(None) (empty front memtable shrinks the queue).
        let mut attempts = 0usize;
        loop {
            let before = self.version.read().immutables.len();
            if before == 0 {
                break;
            }
            attempts += 1;
            if attempts > MAX_FLUSH_DRAIN_ATTEMPTS {
                return Err(TsdbError::Storage(format!(
                    "flush_all exceeded {MAX_FLUSH_DRAIN_ATTEMPTS} attempts with {} immutables left",
                    before
                )));
            }
            match self.flush_one_immutable_locked()? {
                Some(meta) => flushed.push(meta),
                None => {
                    let after = self.version.read().immutables.len();
                    if after == 0 {
                        break;
                    }
                    if after >= before {
                        return Err(TsdbError::Storage(
                            "flush_one_immutable returned None without draining immutable queue"
                                .into(),
                        ));
                    }
                }
            }
        }
        Ok(flushed)
    }

    /// Drain the immutable queue under `flush_lock` (via [`Self::flush_one_immutable`]).
    ///
    /// Fail-fast: circuit-breaker on attempt count; `Ok(None)` must shrink the queue
    /// (empty-memtable drain) or it is treated as state corruption — never spin.
    fn drain_immutables_fail_fast(&self) -> Result<()> {
        let mut attempts = 0usize;
        loop {
            let before = self.version.read().immutables.len();
            if before == 0 {
                return Ok(());
            }
            attempts += 1;
            if attempts > MAX_FLUSH_DRAIN_ATTEMPTS {
                return Err(TsdbError::Storage(format!(
                    "immutable flush drain exceeded {MAX_FLUSH_DRAIN_ATTEMPTS} attempts \
                     ({before} still queued); refusing to spin"
                )));
            }
            match self.flush_one_immutable()? {
                Some(_) => {}
                None => {
                    let after = self.version.read().immutables.len();
                    if after == 0 {
                        return Ok(());
                    }
                    if after >= before {
                        return Err(TsdbError::Storage(
                            "flush_one_immutable returned None but immutable queue did not shrink; \
                             state stuck, refusing to spin"
                                .into(),
                        ));
                    }
                    // Front was an empty memtable and was dropped — continue draining.
                }
            }
        }
    }

    pub fn spawn_background_flush(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let notify = Arc::clone(&self.flush_notify);
        let table_name = self.name.clone();
        tokio::spawn(async move {
            let mut error_backoff = Duration::from_millis(100);
            loop {
                // Wait without holding `LsmTable` so DROP TABLE can release the last Arc.
                notify.notified().await;
                let Some(worker) = weak.upgrade() else {
                    tracing::info!(
                        table = %table_name,
                        "LsmTable dropped, shutting down background flush task"
                    );
                    break;
                };

                let result =
                    tokio::task::spawn_blocking(move || worker.drain_immutables_fail_fast()).await;

                match result {
                    Ok(Ok(())) => {
                        error_backoff = Duration::from_millis(100);
                    }
                    Ok(Err(e)) => {
                        tracing::error!(
                            table = %table_name,
                            error = %e,
                            backoff_ms = error_backoff.as_millis() as u64,
                            "background flush failed, backing off"
                        );
                        tokio::time::sleep(error_backoff).await;
                        error_backoff = (error_backoff * 2).min(Duration::from_secs(10));
                    }
                    Err(join_err) => {
                        tracing::error!(
                            table = %table_name,
                            error = %join_err,
                            "background flush task panicked"
                        );
                        break;
                    }
                }
            }
        });
    }

    pub fn release_memory(&self) {
        let version = self.version.read();
        version.mutable.release_memory();
        for mem in version.immutables.iter() {
            mem.release_memory();
        }
    }

    /// Two-phase bulk load: write staging SST without write lock, then briefly lock to seal LSN.
    #[instrument(skip(self, source), fields(table = %self.name))]
    pub async fn bulk_load_parquet_async(self: &Arc<Self>, source: &Path) -> Result<SstMeta> {
        if let Some(disk) = self.disk.load_full() {
            disk.ensure_writable()?;
        }

        let source = source.to_path_buf();
        let data_dir = self.data_dir.clone();
        let schema = self.schema.read().clone();
        let mut meta = tokio::task::spawn_blocking(move || {
            let staging = SstIdentity::staging();
            crate::bulk_load::write_bulk_parquet(&source, &data_dir, &staging, schema)
        })
        .await
        .map_err(|e| TsdbError::Storage(format!("bulk load task panicked: {e}")))??;

        let staging_path = PathBuf::from(meta.file_path.clone());
        let mut sealed_path: Option<PathBuf> = None;
        let commit = async {
            let _write_guard = self.write_lock.lock().await;
            self.flush_memtables_before_bulk_load_async().await?;
            let (min_lsn, max_lsn) = self.allocate_bulk_lsn()?;
            let identity = SstIdentity::fresh_flush(min_lsn, max_lsn);
            meta = crate::bulk_load::seal_bulk_sst_identity(meta, &identity, &self.data_dir)?;
            sealed_path = Some(PathBuf::from(meta.file_path.clone()));
            self.link_and_capture_bulk_load(&mut meta)?;
            self.file_index().insert(meta.clone());
            self.publish_bulk_load(&mut meta)?;
            self.notify_flush(&meta);
            Ok::<SstMeta, TsdbError>(meta)
        }
        .await;

        match commit {
            Ok(meta) => {
                tracing::info!(
                    table = %self.name,
                    rows = meta.row_count,
                    bytes = meta.file_size,
                    base_lsn = meta.base_lsn,
                    max_lsn = meta.max_lsn,
                    "bulk load completed via two-phase seal"
                );
                Ok(meta)
            }
            Err(e) => {
                if let Some(path) = sealed_path {
                    let _ = std::fs::remove_file(path);
                } else {
                    let _ = std::fs::remove_file(&staging_path);
                }
                Err(e)
            }
        }
    }

    /// Multi-file two-phase bulk load (staging writes unlocked, LSN seal under one write lock).
    #[instrument(skip(self, paths), fields(table = %self.name))]
    pub async fn bulk_load_parquet_paths_async(
        self: &Arc<Self>,
        paths: &[PathBuf],
    ) -> Result<crate::bulk_load::BulkLoadResult> {
        if let Some(disk) = self.disk.load_full() {
            disk.ensure_writable()?;
        }

        let mut files = Vec::new();
        for path in paths {
            files.extend(crate::bulk_load::collect_parquet_inputs(path)?);
        }
        files.sort();
        files.dedup();

        let data_dir = self.data_dir.clone();
        let schema = self.schema.read().clone();
        let sources = files.clone();
        let staged = tokio::task::spawn_blocking(move || {
            let mut metas = Vec::with_capacity(sources.len());
            for source in sources {
                let staging = SstIdentity::staging();
                metas.push(crate::bulk_load::write_bulk_parquet(
                    &source,
                    &data_dir,
                    &staging,
                    schema.clone(),
                )?);
            }
            Ok::<Vec<SstMeta>, TsdbError>(metas)
        })
        .await
        .map_err(|e| TsdbError::Storage(format!("bulk load task panicked: {e}")))??;

        let staging_paths: Vec<PathBuf> = staged
            .iter()
            .map(|m| PathBuf::from(m.file_path.clone()))
            .collect();

        let commit = async {
            let _write_guard = self.write_lock.lock().await;
            self.flush_memtables_before_bulk_load_async().await?;

            let mut metas = Vec::with_capacity(staged.len());
            let mut rows_loaded = 0u64;
            for mut meta in staged {
                let (min_lsn, max_lsn) = self.allocate_bulk_lsn()?;
                let identity = SstIdentity::fresh_flush(min_lsn, max_lsn);
                meta = crate::bulk_load::seal_bulk_sst_identity(meta, &identity, &self.data_dir)?;
                self.link_and_capture_bulk_load(&mut meta)?;
                self.file_index().insert(meta.clone());
                self.publish_bulk_load(&mut meta)?;
                rows_loaded += meta.row_count as u64;
                metas.push(meta);
            }
            let _ = self.gc_pinned_files();
            for meta in &metas {
                self.notify_flush(meta);
            }
            Ok(crate::bulk_load::BulkLoadResult {
                files_loaded: metas.len() as u32,
                rows_loaded,
                metas,
            })
        }
        .await;

        match commit {
            Ok(result) => Ok(result),
            Err(e) => {
                for path in staging_paths {
                    let _ = std::fs::remove_file(path);
                }
                Err(e)
            }
        }
    }

    /// Seal active (+ queued immutable) memtables before allocating a BulkLoad LSN.
    ///
    /// Caller must hold `write_lock` so no concurrent DML can insert between the flush frontier
    /// and the BulkLoad LSN. This guarantees:
    /// `… prior DML LSNs (sealed on SST) < BulkLoad LSN < subsequent DML LSNs …`
    async fn flush_memtables_before_bulk_load_async(self: &Arc<Self>) -> Result<()> {
        {
            let _flush_guard = self.flush_lock.lock();
            let frozen = self.freeze_active_memtable()?;
            if frozen {
                tracing::info!(
                    table = %self.name,
                    "flushed active memtable before bulk load to seal DML LSN frontier"
                );
            }
        }
        let worker = Arc::clone(self);
        tokio::task::spawn_blocking(move || worker.drain_immutables_fail_fast())
            .await
            .map_err(|e| TsdbError::Storage(format!("flush task join: {e}")))?
    }

    /// Notify registered table capturer of Flush (enqueue SST path).
    ///
    /// Call **before** updating FileIndex / disk SST list. No-op when no stream registered.
    fn link_and_capture_flush(&self, meta: &mut SstMeta) -> Result<()> {
        if let Some(cap) = self.capturer.load_full() {
            cap.on_flush(meta);
        }
        Ok(())
    }

    /// Notify registered table capturer of BulkLoad (separate from Flush).
    fn link_and_capture_bulk_load(&self, meta: &mut SstMeta) -> Result<()> {
        if let Some(cap) = self.capturer.load_full() {
            cap.on_bulk_load(meta);
        }
        Ok(())
    }

    /// First-time CDC bootstrap under writer exclusion:
    /// 1. hold `write_lock` (no DML)
    /// 2. [`Self::flush_all`] so every MemTable is sealed as SST
    /// 3. deliver every durable SST to `source.on_historical_sst`
    ///
    /// Caller must **not** have attached this Source as a live capturer yet (avoids
    /// double-delivery of the just-flushed SSTs), then attach for live events and call
    /// [`common::CaptureSource::on_bootstrap_done`].
    pub async fn bootstrap_capture_history(
        self: &Arc<Self>,
        source: &dyn common::CaptureSource,
    ) -> Result<(
        u64,   /* frontier_lsn */
        usize, /* historical_files */
    )> {
        let _write_guard = self.write_lock.lock().await;
        // Seal active + immutable memtables while no concurrent puts can race.
        // Run flush off the async worker; `write_lock` stays held across the join.
        let worker = Arc::clone(self);
        tokio::task::spawn_blocking(move || worker.flush_all())
            .await
            .map_err(|e| TsdbError::Storage(format!("bootstrap flush join: {e}")))??;

        let _flush_guard = self.flush_lock.lock();
        let metas = {
            let version = self.version.read();
            version.sstables.snapshot()
        };
        let mut frontier_lsn = 0u64;
        let mut historical_files = 0usize;
        for meta in metas {
            if !meta.has_lsn_bounds() {
                continue;
            }
            frontier_lsn = frontier_lsn.max(meta.max_lsn);
            source.on_historical_sst(&sst_to_capture_meta(&meta));
            historical_files += 1;
        }
        Ok((frontier_lsn, historical_files))
    }

    /// Allocate the BulkLoad LSN that will be sealed into the SST filename.
    ///
    /// Must run after [`Self::flush_memtables_before_bulk_load`] under `write_lock` so this LSN
    /// is strictly after every prior DML LSN already sealed onto SST / flushed memtables.
    fn allocate_bulk_lsn(&self) -> Result<(u64, u64)> {
        let Some(rep) = self.replication.load_full() else {
            return Err(TsdbError::Storage(
                "LSN required: attach replication before bulk load (engine.register_table does this)"
                    .into(),
            ));
        };
        let sst_frontier = crate::wal::max_lsn_in_sst_metas(&self.file_index().snapshot());
        let lsn = rep.allocate_lsn()?;
        if lsn <= sst_frontier {
            return Err(TsdbError::Storage(format!(
                "bulk-load LSN {lsn} must be strictly after flushed SST frontier {sst_frontier}"
            )));
        }
        Ok((lsn, lsn))
    }

    /// Record BulkLoad into the BulkLoad WAL. LSN is already sealed on `meta` / filename.
    /// Caller must hold `write_lock` and have already flushed memtables so BulkLoad LSN follows
    /// the sealed DML frontier.
    fn publish_bulk_load(&self, meta: &mut SstMeta) -> Result<()> {
        let Some(rep) = self.replication.load_full() else {
            return Err(TsdbError::Storage(
                "LSN required: attach replication before bulk load (engine.register_table does this)"
                    .into(),
            ));
        };
        let lsn = rep.on_bulk_file(meta)?;
        debug_assert_eq!(lsn, meta.max_lsn, "bulk-load LSN must match sealed meta");
        Ok(())
    }

    /// Bulk-load add-file events strictly after `lsn` (the CDC tailing cursor). Empty when
    /// replication is off.
    pub fn file_events_since(&self, lsn: u64) -> Vec<common::FileAddEvent> {
        self.replication
            .load_full()
            .map(|rep| rep.file_events_since(lsn))
            .unwrap_or_default()
    }

    /// Notify registered table capturer of compaction (enqueue Compact event).
    pub fn notify_compaction(
        &self,
        inputs: &[crate::compaction::sst::SstMeta],
        output: &crate::compaction::sst::SstMeta,
    ) {
        if let Some(cap) = self.capturer.load_full() {
            cap.on_compact(inputs, output);
        }
    }

    /// Reclaim pinned bulk-load files every capture progress has committed past.
    /// Also deletes orphaned bulk SST originals once their BulkLoad WAL entry is gone.
    pub fn gc_pinned_files(&self) -> Result<()> {
        let live: std::collections::HashSet<String> = self
            .file_index()
            .snapshot()
            .into_iter()
            .map(|m| m.file_path)
            .collect();
        if let Some(rep) = self.replication.load_full() {
            rep.gc_pinned_files(&live)?;
        } else {
            // No slots: drop all BulkLoad WAL entries; keep SSTs still referenced by FileIndex.
            self.bulk_wal.gc_upto(u64::MAX, &live)?;
        }
        Ok(())
    }

    pub fn wal_root(&self) -> PathBuf {
        self.data_dir.join(WAL_SEGMENTS_DIR)
    }

    /// The memtable whose WAL segment holds `lsn` — used to open a tailing cursor at resume.
    pub fn find_wal_memtable_for_lsn(&self, lsn: u64) -> Option<u64> {
        self.replication
            .load_full()
            .and_then(|rep| rep.find_memtable_for_lsn(lsn))
    }

    /// The next memtable id after `memtable_id`, to advance the tailing cursor across segments.
    pub fn next_wal_memtable_after(&self, memtable_id: u64) -> Option<u64> {
        self.replication
            .load_full()
            .and_then(|rep| rep.next_memtable_after(memtable_id))
    }

    pub fn flush_wal(&self) -> Result<()> {
        self.require_wal()?.flush()
    }

    pub async fn flush_wal_async(&self) -> Result<()> {
        self.require_wal()?.flush_async().await
    }
}

impl Drop for LsmTable {
    fn drop(&mut self) {
        // Wake the background flush waiter so its weak upgrade can exit cleanly.
        self.flush_notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::{DEFAULT_MEMTABLE_BATCH_MAX_BYTES, DEFAULT_MEMTABLE_BATCH_MAX_ROWS};
    use crate::replication::ReplicationManager;
    use crate::wal::format::WalFrameCursor;
    use crate::wal::{MemTableWal, WalWriter, WalWriterOptions};
    use arrow::array::{AsArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use std::sync::Arc;

    fn sample_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn sample_batch(ts: i64, v: i64) -> RecordBatch {
        RecordBatch::try_new(
            sample_schema(),
            vec![
                Arc::new(Int64Array::from(vec![ts])),
                Arc::new(Int64Array::from(vec![v])),
            ],
        )
        .unwrap()
    }

    fn open_table(dir: &Path, durability: crate::wal::WalDurabilityMode) -> Arc<LsmTable> {
        let memory = Arc::new(MemoryController::new(64 * 1024 * 1024));
        let table = LsmTable::open(
            "metrics",
            dir,
            sample_schema(),
            1024 * 1024,
            DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
            DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
            memory,
            vec![],
            WalWriterOptions::with_durability(durability),
        )
        .unwrap();
        let repl = ReplicationManager::open(dir).unwrap();
        let max_lsn = table
            .recovered_max_lsn()
            .max(crate::wal::max_lsn_in_table_wals(table.data_dir()).unwrap());
        if max_lsn > 0 {
            repl.lsn().ensure_above(max_lsn).unwrap();
        }
        let retention = repl
            .table_replication(&table.name, table.data_dir(), table.bulk_wal())
            .unwrap();
        table.set_replication(retention);
        table
    }

    #[tokio::test]
    async fn open_recovers_wal_into_parquet_and_clears_wal() {
        let dir = tempfile::tempdir().unwrap();
        let schema = sample_schema();
        let memory = Arc::new(MemoryController::new(64 * 1024 * 1024));

        {
            let wal = WalWriter::open(dir.path(), 3).unwrap();
            wal.write_sync(Arc::new(sample_batch(100, 1)), 0)
                .await
                .unwrap();
            wal.write_sync(Arc::new(sample_batch(101, 2)), 0)
                .await
                .unwrap();
            wal.flush().unwrap();
            // Recovery only flushes sealed memtables (MemTableEnd frame).
            wal.memtable_end(2, 3, 4).unwrap();
        }

        let table = LsmTable::open(
            "metrics",
            dir.path(),
            schema,
            1024 * 1024,
            DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
            DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
            memory,
            vec![],
            WalWriterOptions::with_durability(crate::wal::WalDurabilityMode::Sync),
        )
        .unwrap();

        assert_eq!(table.file_index().snapshot().len(), 1);
        assert_eq!(table.version.read().mutable.size_bytes(), 0);
        // Flat WAL may still exist as the active file after recover+SST; content is durable in SST.
        assert!(
            crate::wal::sst_has_lsn_watermark(table.file_index().as_ref())
                || table.file_index().snapshot()[0].row_count > 0
        );
    }

    #[tokio::test]
    async fn flush_active_rotates_active_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let table = open_table(dir.path(), crate::wal::WalDurabilityMode::Async);

        table.put_batch(sample_batch(100, 1)).await.unwrap();
        let active_before = table.version.read().mutable.id;
        let meta = table
            .flush_active_memtable()
            .unwrap()
            .expect("expected SST flush");
        assert_eq!(meta.row_count, 1);
        assert!(meta.max_lsn > 0, "flushed SST must carry sealed LSN");
        assert!(
            Path::new(&meta.file_path).parent() == Some(dir.path()),
            "flushed SST must live in table data_dir, not .flush_tmp: {}",
            meta.file_path
        );
        let tmp = flush_tmp_dir(dir.path());
        if tmp.is_dir() {
            let leftovers: Vec<_> = std::fs::read_dir(&tmp)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("parquet"))
                .collect();
            assert!(
                leftovers.is_empty(),
                "flush_tmp must be empty after successful flush: {leftovers:?}"
            );
        }

        let active_after = table.version.read().mutable.id;
        assert_ne!(active_before, active_after);

        table.put_batch(sample_batch(101, 2)).await.unwrap();
        assert!(table.version.read().mutable.size_bytes() > 0);
        assert!(table.version.read().immutables.is_empty());
    }

    #[tokio::test]
    async fn wal_frame_order_matches_memtable_put_order() {
        let dir = tempfile::tempdir().unwrap();
        let table = open_table(dir.path(), crate::wal::WalDurabilityMode::Sync);

        let puts = [(10_i64, 1_i64), (20, 2), (30, 3)];
        let mut lsns = Vec::new();
        for &(ts, v) in &puts {
            table.put_batch(sample_batch(ts, v)).await.unwrap();
        }
        table.flush_wal().unwrap();

        // MemTable visible order == put order.
        let mem = table.version.read().mutable.clone();
        let batches = mem.get_batches_snapshot();
        let mut mem_ts = Vec::new();
        for b in &batches {
            let col = b.column(0).as_primitive::<Int64Type>();
            for i in 0..b.num_rows() {
                mem_ts.push(col.value(i));
            }
        }
        assert_eq!(mem_ts, vec![10, 20, 30]);

        // WAL on-disk frame LSN order == put order (strictly increasing).
        let files = MemTableWal::list_wal_file_ids(table.data_dir()).unwrap();
        let path = crate::wal::format::numbered_wal_path(&table.wal_root(), files[0]);
        let mut cursor = WalFrameCursor::open(&path, files[0]).unwrap().expect("wal");
        while let Some(ev) = cursor.next_batch().unwrap() {
            lsns.push(ev.lsn);
            assert!(ev.lsn > 0);
        }
        assert_eq!(lsns.len(), 3);
        assert!(lsns[0] < lsns[1] && lsns[1] < lsns[2]);
    }

    #[tokio::test]
    async fn open_without_wal_skips_recovery_and_uses_sst_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let identity = crate::compaction::sst_id::SstIdentity::fresh_flush(40, 50);
        let path = dir.path().join(identity.filename());
        std::fs::write(&path, b"parquet-placeholder").unwrap();
        let meta = crate::compaction::sst::SstMeta::from_identity(
            identity,
            path.to_string_lossy().into_owned(),
            10,
            20,
            1,
            1,
        );

        let table = LsmTable::open(
            "metrics",
            dir.path(),
            sample_schema(),
            1024 * 1024,
            DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
            DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
            Arc::new(MemoryController::new(64 * 1024 * 1024)),
            vec![meta],
            WalWriterOptions::with_durability(crate::wal::WalDurabilityMode::Sync),
        )
        .unwrap();

        assert!(
            !crate::wal::has_recoverable_memtable_wal(table.data_dir()).unwrap(),
            "no memtable WAL to recover"
        );
        assert_eq!(table.recovered_max_lsn(), 50);
        assert!(crate::wal::sst_has_lsn_watermark(
            table.file_index().as_ref()
        ));
    }

    #[tokio::test]
    async fn flush_with_lsn_allows_wal_gc() {
        let dir = tempfile::tempdir().unwrap();
        let table = open_table(dir.path(), crate::wal::WalDurabilityMode::Sync);
        table.put_batch(sample_batch(100, 1)).await.unwrap();
        let mid = table.version.read().mutable.id;
        let meta = table
            .flush_active_memtable()
            .unwrap()
            .expect("flush produces SST");
        assert!(meta.has_lsn_bounds());
        assert!(
            crate::wal::can_drop_wal_for_lsn_watermark(
                table.data_dir(),
                mid,
                table.file_index().as_ref()
            )
            .unwrap(),
            "SST LSN watermark allows dropping flushed WAL"
        );
    }

    #[tokio::test]
    async fn disk_read_only_rejects_writes() {
        let dir = tempfile::tempdir().unwrap();
        let table = open_table(dir.path(), crate::wal::WalDurabilityMode::Async);
        let disk = Arc::new(crate::disk_space::DiskSpaceController::with_min_free_ratio(
            dir.path(),
            0.999,
        ));
        table.set_disk_space(disk);

        let err = table.put_batch(sample_batch(1, 1)).await.unwrap_err();
        assert!(
            err.is_disk_read_only(),
            "expected disk read-only, got {err}"
        );
    }
}
