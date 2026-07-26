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

use crate::capture::table_capturer::{TableCaptureHub, DEFAULT_TABLE_CAPTURE_CAPACITY};
use crate::compaction::sst::cleanup_sst_staging_under;
use crate::disk_space::DiskSpaceController;
use crate::lifecycle::{EngineLifecycle, EngineLifecycleGate};
use crate::memory::MemoryController;
use crate::replication::ReplicationManager;
use crate::table::LsmTable;
use crate::wal::load_cache::{WalLoadCache, DEFAULT_WAL_LOAD_CACHE_MAX_BYTES};
use crate::wal::notify::WalAppendHub;
use common::{Result, TsdbError};
use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use tracing::{error, info, instrument};

/// Storage engine entry — manages all LSM tables with fine-grained concurrency.
pub struct LsmEngine {
    tables: DashMap<String, Arc<LsmTable>>,
    base_dir: PathBuf,
    wal_hub: Arc<WalAppendHub>,
    capture_hub: Arc<TableCaptureHub>,
    wal_load_cache: Arc<WalLoadCache>,
    replication: Arc<ReplicationManager>,
    disk: Arc<DiskSpaceController>,
    /// Boot phase: Starting → (Stream attach) → Recovering → Running.
    lifecycle: EngineLifecycleGate,
}

impl LsmEngine {
    pub fn new(base_dir: impl Into<PathBuf>) -> Result<Self> {
        Self::with_cdc_limits(base_dir, DEFAULT_WAL_LOAD_CACHE_MAX_BYTES)
    }

    /// CDC limits: sealed WAL frame cache for catch-up (disk tier).
    pub fn with_cdc_limits(
        base_dir: impl Into<PathBuf>,
        wal_load_cache_max_bytes: usize,
    ) -> Result<Self> {
        let base_dir = base_dir.into();
        std::fs::create_dir_all(&base_dir).map_err(|e| {
            TsdbError::Storage(format!(
                "Failed to create storage base dir {}: {e}",
                base_dir.display()
            ))
        })?;
        // Drop crash leftovers: recursively remove every `.flush_tmp/` / `.compact_tmp/` /
        // `.bulk_tmp/` under the engine data root before any table opens or indexes SST files.
        cleanup_sst_staging_under(&base_dir)?;
        let replication = Arc::new(
            ReplicationManager::open(&base_dir)
                .map_err(|e| TsdbError::Storage(format!("Failed to open replication: {e}")))?,
        );
        let disk = Arc::new(DiskSpaceController::new(&base_dir));
        let _ = disk.refresh();
        let capture_hub = Arc::new(TableCaptureHub::with_pin_root(
            DEFAULT_TABLE_CAPTURE_CAPACITY,
            base_dir.join("cdc_pins"),
        ));
        Ok(Self {
            tables: DashMap::new(),
            base_dir,
            wal_hub: Arc::new(WalAppendHub::new()),
            capture_hub,
            wal_load_cache: WalLoadCache::new(wal_load_cache_max_bytes),
            replication,
            disk,
            lifecycle: EngineLifecycleGate::new(),
        })
    }

    /// Current boot / run phase of this storage engine.
    pub fn lifecycle(&self) -> EngineLifecycle {
        self.lifecycle.get()
    }

    /// Enter WAL→SST recovery after Stream capture has been attached.
    pub fn begin_disk_recovery(&self) -> Result<()> {
        self.lifecycle.begin_disk_recovery()?;
        info!("storage engine entering Recovering (WAL → SST)");
        Ok(())
    }

    /// Mark the storage engine ready for user writes.
    pub fn mark_running(&self) -> Result<()> {
        self.lifecycle.mark_running()?;
        info!("storage engine Running");
        Ok(())
    }

    pub fn mark_stopped(&self) {
        self.lifecycle.mark_stopped();
    }

    pub fn ensure_running(&self) -> Result<()> {
        self.lifecycle.ensure_running()
    }

    pub fn wal_hub(&self) -> Arc<WalAppendHub> {
        Arc::clone(&self.wal_hub)
    }

    /// Per-table CDC capturer queues (Insert / Flush / Compact).
    pub fn capture_hub(&self) -> Arc<TableCaptureHub> {
        Arc::clone(&self.capture_hub)
    }

    /// Sealed WAL frame cache for disk-tier CDC catch-up.
    pub fn wal_load_cache(&self) -> Arc<WalLoadCache> {
        Arc::clone(&self.wal_load_cache)
    }

    /// The engine-wide LSN allocator + optional Stream retention pin.
    pub fn replication(&self) -> Arc<ReplicationManager> {
        Arc::clone(&self.replication)
    }

    /// Shared disk free-space watermark (read-only when free/total ≤ configured ratio).
    pub fn disk_space(&self) -> Arc<DiskSpaceController> {
        Arc::clone(&self.disk)
    }

    /// Disable the free-space gate (unit tests on nearly-full developer disks).
    pub fn disable_disk_watermark_for_tests(&self) {
        self.disk.set_min_free_ratio(0.0);
    }

    /// Enable replication for a table: global-LSN stamping on writes + progress-gated WAL retention.
    ///
    /// On attach, advances the in-memory LSN allocator past max(WAL, SST) LSN for this table.
    /// There is no `lsn.pb` / JSON file: durable watermark lives only in WAL frames and SST metas.
    /// No WAL and no SST ⇒ allocator stays at 1 (no durable writes yet).
    pub fn attach_table_replication(&self, table: &Arc<LsmTable>) -> Result<()> {
        // Prefer the open-time WAL watermark (captured before recovery destroys segments),
        // then refresh from any remaining WAL / SST metadata.
        let mut max_lsn = table.recovered_max_lsn();
        max_lsn = max_lsn.max(crate::wal::max_lsn_in_table_wals(table.data_dir())?);
        for meta in table.file_index().snapshot() {
            max_lsn = max_lsn.max(meta.base_lsn).max(meta.max_lsn);
        }
        if max_lsn > 0 {
            self.replication.lsn().ensure_above(max_lsn)?;
            tracing::info!(
                table = %table.name,
                max_wal_lsn = max_lsn,
                next_lsn = self.replication.lsn().peek_next(),
                "recovered LSN allocator from table WAL watermark"
            );
        }
        let retention =
            self.replication
                .table_replication(&table.name, table.data_dir(), table.bulk_wal())?;
        table.set_replication(retention);
        table.set_disk_space(self.disk_space());
        // Capturer is allocated only when a Stream/capture progress registers (CREATE STREAM).
        Ok(())
    }

    /// Register a Stream [`CaptureSource`] (**CREATE STREAM** bootstrap).
    ///
    /// Capture-only: flush MemTables → historical SST → attach TableCapturer.
    /// Progress pin / commit are Stream responsibilities (call before/after as needed).
    pub async fn register_capture_source(
        &self,
        stream: &str,
        table: &str,
        source: Arc<dyn common::CaptureSource>,
    ) -> Result<common::CaptureBootstrapReport> {
        let Some(t) = self.get_table(table) else {
            return Err(TsdbError::TableNotFound(table.to_string()));
        };
        let subscriber_id = format!("{stream}::log::{table}");

        if self.capture_hub.has_subscriber(table, &subscriber_id) {
            let frontier_lsn = t
                .file_index()
                .snapshot()
                .iter()
                .filter(|m| m.has_lsn_bounds())
                .map(|m| m.max_lsn)
                .max()
                .unwrap_or(0);
            return Ok(common::CaptureBootstrapReport {
                progress_id: subscriber_id,
                table: table.to_string(),
                stream: stream.to_string(),
                frontier_lsn,
                historical_files: 0,
            });
        }

        let (frontier_lsn, historical_files) = t.bootstrap_capture_history(source.as_ref()).await?;

        let listener: Arc<dyn common::TableCaptureListener> =
            Arc::new(common::CaptureSourceHandle(Arc::clone(&source)));
        let capturer = self
            .capture_hub
            .set_listener(table, &subscriber_id, listener);
        t.set_capturer(Arc::clone(&capturer) as Arc<dyn crate::capture::TableCapturer>);
        source.on_bootstrap_done(frontier_lsn);

        tracing::info!(
            stream = %stream,
            table = %table,
            subscriber_id = %subscriber_id,
            frontier_lsn,
            historical_files,
            "registered CaptureSource after flush + historical SST bootstrap"
        );
        Ok(common::CaptureBootstrapReport {
            progress_id: subscriber_id,
            table: table.to_string(),
            stream: stream.to_string(),
            frontier_lsn,
            historical_files,
        })
    }

    /// Attach live capture only (no historical SST replay).
    ///
    /// Used on process restart when Stream has already recovered the durable
    /// `pending/` hard-link queue — replaying history would double-enqueue.
    pub async fn attach_capture_source(
        &self,
        stream: &str,
        table: &str,
        source: Arc<dyn common::CaptureSource>,
    ) -> Result<common::CaptureBootstrapReport> {
        let Some(t) = self.get_table(table) else {
            return Err(TsdbError::TableNotFound(table.to_string()));
        };
        let subscriber_id = format!("{stream}::log::{table}");

        let frontier_lsn = t
            .file_index()
            .snapshot()
            .iter()
            .filter(|m| m.has_lsn_bounds())
            .map(|m| m.max_lsn)
            .max()
            .unwrap_or(0);

        if self.capture_hub.has_subscriber(table, &subscriber_id) {
            return Ok(common::CaptureBootstrapReport {
                progress_id: subscriber_id,
                table: table.to_string(),
                stream: stream.to_string(),
                frontier_lsn,
                historical_files: 0,
            });
        }

        let listener: Arc<dyn common::TableCaptureListener> =
            Arc::new(common::CaptureSourceHandle(Arc::clone(&source)));
        let capturer = self
            .capture_hub
            .set_listener(table, &subscriber_id, listener);
        t.set_capturer(Arc::clone(&capturer) as Arc<dyn crate::capture::TableCapturer>);
        source.on_bootstrap_done(frontier_lsn);

        tracing::info!(
            stream = %stream,
            table = %table,
            subscriber_id = %subscriber_id,
            frontier_lsn,
            "attached CaptureSource (live only; pending/ already recovered)"
        );
        Ok(common::CaptureBootstrapReport {
            progress_id: subscriber_id,
            table: table.to_string(),
            stream: stream.to_string(),
            frontier_lsn,
            historical_files: 0,
        })
    }

    /// Unregister stream capture (**DROP STREAM**). Clears capturer when last subscriber leaves.
    pub fn unregister_stream_table_capture(&self, stream: &str, table: &str) -> Result<()> {
        let empty = self.capture_hub.unregister_stream(stream, table);
        if empty {
            if let Some(t) = self.get_table(table) {
                t.clear_capturer();
            }
        }
        Ok(())
    }

    /// Wire global memtable memory pressure to flush one memtable when the budget is full.
    pub fn install_memory_reclaim_handler(self: &Arc<Self>, memory: Arc<MemoryController>) {
        let weak = Weak::clone(&Arc::downgrade(self));
        memory.set_reclaim_handler(move |prefer| {
            weak.upgrade()
                .and_then(|engine| engine.reclaim_one_memtable(prefer).ok())
                .unwrap_or(false)
        });
    }

    /// Pick a memtable victim and flush it to disk. Returns true if memory was reclaimed.
    #[instrument(skip(self), fields(prefer = ?prefer))]
    pub fn reclaim_one_memtable(&self, prefer: Option<&str>) -> Result<bool> {
        if let Some(name) = prefer {
            if let Some(table) = self.get_table(name) {
                if table.try_reclaim_memtable_memory()? {
                    return Ok(true);
                }
            }
        }

        let victims = self.select_reclaim_victims(prefer);
        for (name, flush_immutable) in victims {
            let table = self
                .get_table(&name)
                .ok_or_else(|| TsdbError::TableNotFound(name.clone()))?;
            if table.try_reclaim_memtable_memory()? {
                info!(
                    table = %name,
                    flush_immutable,
                    global_memtable_bytes = table.global_memory_used_bytes(),
                    "global memtable memory pressure flush"
                );
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn select_reclaim_victims(&self, prefer: Option<&str>) -> Vec<(String, bool)> {
        let n = self.tables.len();
        let mut immutables = Vec::with_capacity(n);
        let mut actives = Vec::with_capacity(n);

        for entry in self.tables.iter() {
            let name = entry.key();
            let table = entry.value();

            let imm_size = table.oldest_immutable_bytes();
            if imm_size > 0 {
                immutables.push((name.clone(), imm_size));
            }

            if prefer.is_some_and(|p| p == name) {
                continue;
            }
            let size = table.active_memtable_bytes();
            if size > 0 {
                actives.push((name.clone(), size));
            }
        }

        immutables.sort_unstable_by(|a, b| b.1.cmp(&a.1));
        actives.sort_unstable_by(|a, b| b.1.cmp(&a.1));

        let mut victims = Vec::with_capacity(immutables.len() + actives.len());
        victims.extend(immutables.into_iter().map(|(name, _)| (name, true)));
        victims.extend(actives.into_iter().map(|(name, _)| (name, false)));
        victims
    }

    pub fn total_pending_memtable_bytes(&self) -> usize {
        self.tables
            .iter()
            .map(|entry| entry.value().pending_memtable_bytes())
            .sum()
    }

    /// Sum of live memtable footprints (active + immutable, not yet on disk).
    pub fn total_pending_memtable_footprint(&self) -> usize {
        self.tables
            .iter()
            .map(|entry| entry.value().pending_memtable_footprint())
            .sum()
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn table_data_dir(&self, name: &str) -> PathBuf {
        self.base_dir.join(name)
    }

    /// Register a table; fails closed if replication attach fails (table is not inserted).
    #[instrument(skip(self, table, name), fields(table_name = %name.as_ref()))]
    pub fn register_table(&self, name: impl AsRef<str>, table: Arc<LsmTable>) -> Result<()> {
        let table_name = name.as_ref();
        if let Err(e) = self.attach_table_replication(&table) {
            error!(error = %e, "Failed to attach replication during table registration");
            return Err(e);
        }
        self.tables.insert(table_name.to_string(), table);
        info!("Table registered successfully");
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<LsmTable>> {
        self.tables.get(name).map(|t| t.value().clone())
    }

    pub fn remove_table(&self, name: &str) -> Option<Arc<LsmTable>> {
        self.tables.remove(name).map(|(_, t)| t)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    #[instrument(skip(self, batch), fields(table_name = %name), level = "trace")]
    pub async fn write_to_table(
        &self,
        name: &str,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<()> {
        if let Some(table) = self.get_table(name) {
            table.put_batch(batch).await
        } else {
            Err(TsdbError::TableNotFound(name.to_string()))
        }
    }

    pub fn list_loaded_tables(&self) -> Vec<String> {
        self.tables.iter().map(|e| e.key().clone()).collect()
    }

    pub fn snapshot_tables(&self) -> Vec<Arc<LsmTable>> {
        self.tables.iter().map(|e| e.value().clone()).collect()
    }

    pub fn flush_all_wal(&self) -> Result<()> {
        for entry in self.tables.iter() {
            entry.value().flush_wal()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::{DEFAULT_MEMTABLE_BATCH_MAX_BYTES, DEFAULT_MEMTABLE_BATCH_MAX_ROWS};
    use crate::wal::{WalDurabilityMode, WalWriterOptions};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn batch(rows: usize, base: i64) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(
                    (0..rows as i64).map(|i| base + i).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(vec![1_i64; rows])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn global_memory_pressure_reclaims_by_flushing_memtable() -> Result<()> {
        let dir = tempfile::tempdir().map_err(|e| TsdbError::Storage(e.to_string()))?;
        let memory = Arc::new(MemoryController::new(512 * 1024));
        let engine = Arc::new(LsmEngine::new(dir.path())?);
        engine.disable_disk_watermark_for_tests();
        engine.install_memory_reclaim_handler(memory.clone());

        let wal_opts = WalWriterOptions::with_durability(WalDurabilityMode::Async);
        for i in 0..3 {
            let table = LsmTable::open(
                format!("t{i}"),
                dir.path().join(format!("t{i}")),
                schema(),
                64 * 1024,
                DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
                DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
                memory.clone(),
                vec![],
                wal_opts.clone(),
            )?;
            engine.register_table(format!("t{i}"), table)?;
        }

        for round in 0..200 {
            for i in 0..3 {
                let table = engine.get_table(&format!("t{i}")).unwrap();
                table.put_batch(batch(100, round * 1000 + i)).await?;
            }
        }

        assert!(
            memory.used_bytes() <= memory.limit_bytes(),
            "engine mem {} > limit {}",
            memory.used_bytes(),
            memory.limit_bytes()
        );
        assert_eq!(
            memory.used_bytes(),
            engine.total_pending_memtable_bytes(),
            "global budget should match sum of charged memtables"
        );
        let flushed: usize = (0..3)
            .map(|i| {
                engine
                    .get_table(&format!("t{i}"))
                    .unwrap()
                    .file_index()
                    .snapshot()
                    .len()
            })
            .sum();
        assert!(
            flushed > 0,
            "expected pressure flushes to produce SST files"
        );
        Ok(())
    }

    #[tokio::test]
    async fn progress_gated_wal_retention_survives_flush_until_ack() -> Result<()> {
        use crate::wal::MemTableWal;

        let dir = tempfile::tempdir().map_err(|e| TsdbError::Storage(e.to_string()))?;
        let memory = Arc::new(MemoryController::new(64 * 1024 * 1024));
        let engine = Arc::new(LsmEngine::new(dir.path())?);
        engine.disable_disk_watermark_for_tests();

        let table = LsmTable::open(
            "t0",
            dir.path().join("t0"),
            schema(),
            64 * 1024 * 1024,
            DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
            DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
            memory.clone(),
            vec![],
            WalWriterOptions::with_durability(WalDurabilityMode::Sync),
        )?;
        engine.register_table("t0", table.clone())?;

        // A progress cursor parked at LSN 0 pins everything above it.
        use crate::replication::RetentionPin;
        use std::sync::atomic::{AtomicU64, Ordering};

        struct TestPin(AtomicU64);
        impl RetentionPin for TestPin {
            fn min_retained_lsn(&self) -> u64 {
                self.0.load(Ordering::Acquire)
            }
        }
        let pin = Arc::new(TestPin(AtomicU64::new(0)));
        engine
            .replication()
            .set_retention_pin(pin.clone() as Arc<dyn RetentionPin>);

        // Fill and flush memtable 1 to SST; then write to memtable 2 so the LSN index knows the
        // upper bound of memtable 1's range.
        table.put_batch(batch(50, 0)).await?;
        table.flush_active_memtable()?;
        table.put_batch(batch(50, 1000)).await?;

        // Progress at 0 pins retention: sealed WAL must still be present.
        table.gc_retained_wal()?;
        assert!(
            !MemTableWal::list_wal_file_ids(table.data_dir())?.is_empty(),
            "active flat WAL must remain while progress lags"
        );

        let now = engine.replication().lsn().current();
        pin.0.store(now, Ordering::Release);
        table.gc_retained_wal()?;
        // Active numbered file stays until size-based seal; progress no longer blocks logical GC.
        Ok(())
    }

    #[tokio::test]
    async fn hard_memory_limit_returns_write_blocked_error() -> Result<()> {
        let dir = tempfile::tempdir().map_err(|e| TsdbError::Storage(e.to_string()))?;
        let memory = Arc::new(MemoryController::new(1024));
        let engine = Arc::new(LsmEngine::new(dir.path())?);
        engine.disable_disk_watermark_for_tests();
        engine.install_memory_reclaim_handler(memory.clone());

        let table = LsmTable::open(
            "t0",
            dir.path().join("t0"),
            schema(),
            512 * 1024,
            DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
            DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
            memory.clone(),
            vec![],
            WalWriterOptions::with_durability(WalDurabilityMode::Async),
        )?;
        engine.register_table("t0", table.clone())?;

        assert!(memory.try_reserve(1024));
        let err = table.put_batch(batch(10, 1)).await.unwrap_err();
        assert!(
            err.is_memory_limit_exceeded(),
            "expected memory limit error, got {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn restart_recovers_lsn_allocator_from_wal_max() -> Result<()> {
        let dir = tempfile::tempdir().map_err(|e| TsdbError::Storage(e.to_string()))?;
        let memory = Arc::new(MemoryController::new(64 * 1024 * 1024));
        let table_dir = dir.path().join("t0");

        let wal_max = {
            let engine = Arc::new(LsmEngine::new(dir.path())?);
            engine.disable_disk_watermark_for_tests();
            let table = LsmTable::open(
                "t0",
                &table_dir,
                schema(),
                64 * 1024 * 1024,
                DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
                DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
                memory.clone(),
                vec![],
                WalWriterOptions::with_durability(WalDurabilityMode::Sync),
            )?;
            engine.register_table("t0", table.clone())?;
            table.put_batch(batch(1, 1)).await?;
            table.put_batch(batch(2, 2)).await?;
            table.put_batch(batch(3, 3)).await?;
            table.flush_wal()?;
            let max = crate::wal::max_lsn_in_table_wals(table.data_dir())?;
            assert!(max > 0);
            max
        };

        // Fresh engine has no allocator file — LSN is recovered from WAL/SST on table attach.
        let engine = Arc::new(LsmEngine::new(dir.path())?);
        engine.disable_disk_watermark_for_tests();
        assert_eq!(engine.replication().lsn().peek_next(), 1);
        let table = LsmTable::open(
            "t0",
            &table_dir,
            schema(),
            64 * 1024 * 1024,
            DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
            DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
            memory,
            vec![],
            WalWriterOptions::with_durability(WalDurabilityMode::Sync),
        )?;
        engine.register_table("t0", table.clone())?;

        assert!(
            engine.replication().lsn().peek_next() > wal_max,
            "next LSN {} must be above WAL max {wal_max}",
            engine.replication().lsn().peek_next()
        );
        let next = table
            .put_batch(batch(4, 4))
            .await
            .map(|_| engine.replication().lsn().current())?;
        assert!(next > wal_max);
        Ok(())
    }

    #[test]
    fn engine_init_fails_when_base_dir_is_a_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let conflict = temp_dir.path().join("faulty_base_dir");
        std::fs::File::create(&conflict).unwrap();

        let result = LsmEngine::new(&conflict);
        match result {
            Ok(_) => panic!("Engine initialization should fail when base_dir is a file"),
            Err(err) => {
                assert!(
                    matches!(err, TsdbError::Storage(_)),
                    "Expected TsdbError::Storage, got: {err:?}"
                );
                let msg = err.to_string();
                assert!(
                    msg.contains("Failed to open replication")
                        || msg.contains("Not a directory")
                        || msg.contains("File exists"),
                    "Error message should identify root cause: {msg}"
                );
            }
        }
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn register_table_fails_when_wal_dir_unreadable() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().map_err(|e| TsdbError::Storage(e.to_string()))?;
        let engine = LsmEngine::new(dir.path())?;
        engine.disable_disk_watermark_for_tests();

        let table_dir = dir.path().join("bad_table");
        let memory = Arc::new(MemoryController::new(1024 * 1024));
        let table = LsmTable::open(
            "bad_table",
            &table_dir,
            schema(),
            64 * 1024,
            DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
            DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
            memory,
            vec![],
            WalWriterOptions::with_durability(WalDurabilityMode::Sync),
        )?;

        let wal_root = table_dir.join(common::WAL_SEGMENTS_DIR);
        std::fs::create_dir_all(&wal_root)?;
        // Force I/O error while scanning WAL for LSN attach.
        std::fs::set_permissions(&wal_root, std::fs::Permissions::from_mode(0o000))?;
        let register_result = engine.register_table("bad_table", table);
        let _ = std::fs::set_permissions(&wal_root, std::fs::Permissions::from_mode(0o755));

        assert!(
            register_result.is_err(),
            "Should refuse registration when replication attach fails"
        );
        assert!(!engine.contains("bad_table"));
        Ok(())
    }

    #[tokio::test]
    async fn capture_bootstrap_flushes_memtable_then_delivers_history() -> Result<()> {
        use common::{CaptureFileMeta, CaptureSource, TableCaptureListener};
        use parking_lot::Mutex;
        use std::sync::atomic::{AtomicU64, Ordering};

        struct CountingSource {
            history: Mutex<Vec<CaptureFileMeta>>,
            frontier: AtomicU64,
        }

        impl TableCaptureListener for CountingSource {
            fn on_insert(
                &self,
                _min_lsn: u64,
                _max_lsn: u64,
                _batch: arrow::record_batch::RecordBatch,
            ) {
            }
            fn on_flush(&self, _meta: &CaptureFileMeta) {}
            fn on_bulk_load(&self, _meta: &CaptureFileMeta) {}
            fn on_compact(&self, _inputs: &[CaptureFileMeta], _output: &CaptureFileMeta) {}
        }

        impl CaptureSource for CountingSource {
            fn on_historical_sst(&self, meta: &CaptureFileMeta) {
                self.history.lock().push(meta.clone());
            }
            fn on_bootstrap_done(&self, frontier_lsn: u64) {
                self.frontier.store(frontier_lsn, Ordering::Release);
            }
        }

        let dir = tempfile::tempdir().map_err(|e| TsdbError::Storage(e.to_string()))?;
        let memory = Arc::new(MemoryController::new(64 * 1024 * 1024));
        let engine = Arc::new(LsmEngine::new(dir.path())?);
        engine.disable_disk_watermark_for_tests();
        let table = LsmTable::open(
            "t0",
            dir.path().join("t0"),
            schema(),
            64 * 1024 * 1024,
            DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
            DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
            memory,
            vec![],
            WalWriterOptions::with_durability(WalDurabilityMode::Async),
        )?;
        engine.register_table("t0", table.clone())?;

        // Data only in MemTable — bootstrap must flush before history.
        table.put_batch(batch(4, 0)).await?;
        assert!(
            table.file_index().snapshot().is_empty(),
            "precondition: no SST yet"
        );

        let source = Arc::new(CountingSource {
            history: Mutex::new(Vec::new()),
            frontier: AtomicU64::new(0),
        });
        let report = engine
            .register_capture_source("s1", "t0", source.clone() as Arc<dyn CaptureSource>)
            .await?;

        assert!(
            !table.file_index().snapshot().is_empty(),
            "bootstrap must flush MemTable to SST"
        );
        assert_eq!(report.historical_files, 1);
        assert!(report.frontier_lsn > 0);
        assert_eq!(source.history.lock().len(), 1);
        assert_eq!(source.frontier.load(Ordering::Acquire), report.frontier_lsn);

        // Idempotent re-register must not duplicate history.
        let again = engine
            .register_capture_source("s1", "t0", source.clone() as Arc<dyn CaptureSource>)
            .await?;
        assert_eq!(again.historical_files, 0);
        assert_eq!(source.history.lock().len(), 1);
        Ok(())
    }
}
