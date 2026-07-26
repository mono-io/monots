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

//! Proto-backed metadata store (v1): in-memory cache + WAL + snapshot + degrade.
//!
//! ## Layout (`{data_dir}/meta/`)
//! - `header.pb` — store version, seq, persist mode
//! - `wal/metadata.wal` — append-only framed [`MetadataRecord`](proto::meta::MetadataRecord)
//! - `snapshots/latest.pb` — periodic full [`StoreSnapshot`](proto::meta::StoreSnapshot)
//!
//! ## Performance
//! - Hot reads: segmented in-memory maps (no disk, no global clone)
//! - DDL / flush: O(1) WAL append (~200B), not full-file rewrite
//! - Compaction: amortized when WAL ≥ 64 records or 512 KiB

mod convert;
mod crc32;
mod memory;
mod snapshot;
mod wal;

#[cfg(test)]
mod tests;

use common::{Result, TsdbError};
use convert::{sst_from_proto, sst_to_proto};
use dashmap::DashMap;
pub use memory::MetaMemoryStats;
use memory::{estimate_dashmap_bytes, MetaMemoryBudget};
use parking_lot::RwLock;
use prost::Message;
pub use snapshot::STORE_VERSION as METADATA_STORE_VERSION;
pub use snapshot::{decode_framed_payload, encode_framed_payload, encode_snapshot_frame};
use snapshot::{MetaSnapshot, STORE_VERSION};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use wal::MetaWal;

use crate::catalog::{ColumnDef, TableMeta, TableRuntimeMeta};
use crate::types::normalize_type_name;
use common::TIMESTAMP_COLUMN;
use monots_storage::sst::SstMeta;

pub const META_MAGIC: u32 = 0x4154454D; // "META" LE

/// How metadata is persisted to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistMode {
    /// WAL + snapshot on disk (normal).
    Durable,
    /// Disk unavailable; in-memory only until recovery succeeds.
    MemoryOnly,
}

impl PersistMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::MemoryOnly => "memory_only",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "memory_only" => Self::MemoryOnly,
            _ => Self::Durable,
        }
    }
}

pub struct MetaStore {
    root: PathBuf,
    schemas: Arc<DashMap<String, proto::meta::TableSchema>>,
    manifests: Arc<DashMap<String, proto::meta::TableManifest>>,
    seq: AtomicU64,
    budget: Arc<MetaMemoryBudget>,
    mode: Arc<RwLock<PersistMode>>,
    wal: Arc<RwLock<MetaWal>>,
    snapshot: MetaSnapshot,
    pending: Arc<RwLock<Vec<Vec<u8>>>>,
}

impl MetaStore {
    /// Open or create metadata store under `{data_dir}/meta/`.
    pub fn open(data_dir: &Path, memory_limit_bytes: usize) -> Result<Arc<Self>> {
        let root = data_dir.join("meta");
        std::fs::create_dir_all(&root)?;
        std::fs::create_dir_all(root.join("wal"))?;

        let budget = Arc::new(MetaMemoryBudget::new(memory_limit_bytes));
        let snapshot = MetaSnapshot::new(&root);
        let wal = Arc::new(RwLock::new(MetaWal::open(&root.join("wal"))?));

        let schemas = Arc::new(DashMap::new());
        let manifests = Arc::new(DashMap::new());
        let seq = AtomicU64::new(0);

        if let Some(snap) = snapshot.load()? {
            for (name, schema) in snap.schemas {
                schemas.insert(name, schema);
            }
            for (name, manifest) in snap.manifests {
                manifests.insert(name, manifest);
            }
            seq.store(snap.seq, Ordering::SeqCst);
        }

        for frame in wal.read().replay()? {
            let rec = proto::meta::MetadataRecord::decode(frame.as_slice())
                .map_err(|e| TsdbError::Storage(format!("wal record decode: {e}")))?;
            apply_record(&schemas, &manifests, &seq, &rec)?;
        }

        budget.reset(estimate_dashmap_bytes(&schemas, &manifests));

        let store = Arc::new(Self {
            root,
            schemas,
            manifests,
            seq,
            budget,
            mode: Arc::new(RwLock::new(PersistMode::Durable)),
            wal,
            snapshot,
            pending: Arc::new(RwLock::new(Vec::new())),
        });

        store.validate_all_schemas()?;

        if let Ok(bytes) = std::fs::read(store.root.join("header.pb")) {
            if let Ok(header) = proto::meta::MetaStoreHeader::decode(bytes.as_slice()) {
                *store.mode.write() = PersistMode::from_str(&header.persist_mode);
            }
        }

        store.write_header(*store.mode.read(), store.current_seq())?;
        Ok(store)
    }

    pub fn persist_mode(&self) -> PersistMode {
        *self.mode.read()
    }

    pub fn memory_stats(&self) -> MetaMemoryStats {
        self.budget.stats()
    }

    pub fn current_seq(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn validate_all_schemas(&self) -> Result<()> {
        for entry in self.schemas.iter() {
            validate_schema_proto(entry.value())?;
        }
        Ok(())
    }

    pub fn wal_bytes(&self) -> u64 {
        self.wal.read().bytes()
    }

    pub fn snapshot_exists(&self) -> bool {
        self.snapshot.exists()
    }

    pub fn list_tables(&self) -> Vec<String> {
        let mut names: Vec<_> = self.schemas.iter().map(|e| e.key().clone()).collect();
        names.sort();
        names
    }

    pub fn get_table_meta(&self, table: &str) -> Option<TableMeta> {
        let schema = self.schemas.get(table)?;
        let files = self
            .manifests
            .get(table)
            .map(|m| m.files.iter().map(sst_from_proto).collect())
            .unwrap_or_default();
        Some(TableMeta {
            table_name: schema.table_name.clone(),
            columns: schema
                .columns
                .iter()
                .map(|c| ColumnDef {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                })
                .collect(),
            data_dir: schema.data_dir.clone(),
            runtime: TableRuntimeMeta {
                parquet_files: files,
            },
        })
    }

    pub fn put_schema(&self, schema: proto::meta::TableSchema) -> Result<()> {
        validate_schema_proto(&schema)?;
        let table = schema.table_name.clone();
        let new_bytes = estimate_schema_bytes(&schema);
        // Account for the delta so schema evolution (e.g. ADD COLUMN, which replaces the schema
        // with a larger one) keeps the budget accurate instead of silently under-counting.
        let old_bytes = self
            .schemas
            .get(&table)
            .map(|s| estimate_schema_bytes(s.value()))
            .unwrap_or(0);
        if new_bytes > old_bytes {
            let delta = new_bytes - old_bytes;
            if !self.budget.try_reserve(delta) {
                return Err(TsdbError::Storage(format!(
                    "metadata memory budget exceeded ({} bytes)",
                    self.budget.limit_bytes()
                )));
            }
        } else if old_bytes > new_bytes {
            self.budget.release(old_bytes - new_bytes);
        }

        self.schemas.insert(table.clone(), schema.clone());
        self.manifests.entry(table).or_default();
        let seq = self.next_seq();

        self.append_record(proto::meta::MetadataRecord {
            seq,
            timestamp_ms: now_ms(),
            op: Some(proto::meta::metadata_record::Op::PutSchema(
                proto::meta::PutSchema {
                    schema: Some(schema),
                },
            )),
        })
    }

    pub fn set_manifest(&self, table: &str, files: Vec<SstMeta>) -> Result<()> {
        if !self.schemas.contains_key(table) {
            return Err(TsdbError::TableNotFound(table.to_string()));
        }
        let proto_files: Vec<_> = files.into_iter().map(sst_to_proto).collect();
        let delta = estimate_manifest_delta(table, &proto_files);
        if let Some(old) = self.manifests.get(table) {
            let old_bytes = estimate_manifest_delta(table, &old.files);
            self.budget.release(old_bytes);
        }
        if !self.budget.try_reserve(delta) {
            return Err(TsdbError::Storage(format!(
                "metadata memory budget exceeded for manifest on {table}"
            )));
        }

        self.manifests.insert(
            table.to_string(),
            proto::meta::TableManifest {
                files: proto_files.clone(),
            },
        );
        let seq = self.next_seq();

        self.append_record(proto::meta::MetadataRecord {
            seq,
            timestamp_ms: now_ms(),
            op: Some(proto::meta::metadata_record::Op::SetManifest(
                proto::meta::SetManifest {
                    table: table.to_string(),
                    files: proto_files,
                },
            )),
        })
    }

    pub fn drop_table(&self, table: &str) -> Result<()> {
        if !self.schemas.contains_key(table) {
            return Err(TsdbError::TableNotFound(table.to_string()));
        }
        let schema_bytes = self
            .schemas
            .get(table)
            .map(|s| estimate_schema_bytes(s.value()))
            .unwrap_or(0);
        let manifest_bytes = self
            .manifests
            .get(table)
            .map(|m| estimate_manifest_delta(table, &m.files))
            .unwrap_or(0);

        self.schemas.remove(table);
        self.manifests.remove(table);
        let seq = self.next_seq();

        self.budget.release(schema_bytes);
        self.budget.release(manifest_bytes);

        self.append_record(proto::meta::MetadataRecord {
            seq,
            timestamp_ms: now_ms(),
            op: Some(proto::meta::metadata_record::Op::DropTable(
                proto::meta::DropTable {
                    table: table.to_string(),
                },
            )),
        })
    }

    /// Async variant of [`Self::put_schema`]: runs the full in-memory + WAL write on the blocking
    /// pool so the caller's async worker thread is never parked on `fsync`.
    pub async fn put_schema_async(
        self: &Arc<Self>,
        schema: proto::meta::TableSchema,
    ) -> Result<()> {
        let this = Arc::clone(self);
        tokio::task::spawn_blocking(move || this.put_schema(schema))
            .await
            .map_err(|e| TsdbError::Storage(format!("meta put_schema join failed: {e}")))?
    }

    /// Async variant of [`Self::set_manifest`] (offloaded to the blocking pool).
    pub async fn set_manifest_async(
        self: &Arc<Self>,
        table: String,
        files: Vec<SstMeta>,
    ) -> Result<()> {
        let this = Arc::clone(self);
        tokio::task::spawn_blocking(move || this.set_manifest(&table, files))
            .await
            .map_err(|e| TsdbError::Storage(format!("meta set_manifest join failed: {e}")))?
    }

    /// Async variant of [`Self::drop_table`] (offloaded to the blocking pool).
    pub async fn drop_table_async(self: &Arc<Self>, table: String) -> Result<()> {
        let this = Arc::clone(self);
        tokio::task::spawn_blocking(move || this.drop_table(&table))
            .await
            .map_err(|e| TsdbError::Storage(format!("meta drop_table join failed: {e}")))?
    }

    /// Flush pending in-memory records to WAL after a degrade recovery.
    pub fn try_recover_durable(&self) -> Result<()> {
        if *self.mode.read() != PersistMode::MemoryOnly {
            return Ok(());
        }
        let pending: Vec<_> = self.pending.write().drain(..).collect();
        for frame in pending {
            let mut wal = self.wal.write();
            wal.append(&frame)?;
        }
        *self.mode.write() = PersistMode::Durable;
        self.write_header(PersistMode::Durable, self.current_seq())?;
        tracing::info!("metadata store recovered durable persistence");
        Ok(())
    }

    pub fn compact(&self) -> Result<()> {
        let snap = self.build_snapshot();
        self.snapshot.save(&snap)?;
        self.wal.write().truncate()?;
        self.wal.read().sync()?;
        self.write_header(*self.mode.read(), snap.seq)?;
        Ok(())
    }

    #[cfg(test)]
    pub fn pending_len(&self) -> usize {
        self.pending.read().len()
    }

    #[cfg(test)]
    pub fn set_persist_mode_for_test(&self, mode: PersistMode) {
        *self.mode.write() = mode;
    }

    fn write_header(&self, mode: PersistMode, seq: u64) -> Result<()> {
        Self::write_header_to(&self.root, mode, seq)
    }

    fn append_record(&self, rec: proto::meta::MetadataRecord) -> Result<()> {
        let bytes = rec.encode_to_vec();
        if *self.mode.read() == PersistMode::MemoryOnly {
            self.pending.write().push(bytes);
            return Ok(());
        }
        let append_result = {
            let mut wal = self.wal.write();
            wal.append(&bytes)
        };
        match append_result {
            Ok(()) => {
                let needs_compact = self.wal.read().needs_compaction();
                if needs_compact {
                    if let Err(e) = self.compact() {
                        tracing::warn!("metadata compaction failed: {e}");
                    }
                }
                Ok(())
            }
            Err(e) => {
                tracing::error!("metadata wal append failed, degrading to memory_only: {e}");
                *self.mode.write() = PersistMode::MemoryOnly;
                self.pending.write().push(bytes);
                self.write_header(PersistMode::MemoryOnly, self.current_seq())
                    .ok();
                Ok(())
            }
        }
    }

    fn build_snapshot(&self) -> proto::meta::StoreSnapshot {
        proto::meta::StoreSnapshot {
            store_version: STORE_VERSION,
            seq: self.current_seq(),
            schemas: self
                .schemas
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
            manifests: self
                .manifests
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
        }
    }

    fn write_header_to(root: &Path, mode: PersistMode, seq: u64) -> Result<()> {
        let header = proto::meta::MetaStoreHeader {
            magic: META_MAGIC,
            store_version: STORE_VERSION,
            snapshot_seq: seq,
            persist_mode: mode.as_str().to_string(),
        };
        let path = root.join("header.pb");
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, header.encode_to_vec())?;
        std::fs::rename(tmp, path)?;
        Ok(())
    }
}

fn apply_record(
    schemas: &DashMap<String, proto::meta::TableSchema>,
    manifests: &DashMap<String, proto::meta::TableManifest>,
    seq: &AtomicU64,
    rec: &proto::meta::MetadataRecord,
) -> Result<()> {
    let current = seq.load(Ordering::SeqCst);
    if rec.seq > current {
        seq.store(rec.seq, Ordering::SeqCst);
    }
    match &rec.op {
        Some(proto::meta::metadata_record::Op::PutSchema(op)) => {
            if let Some(schema) = &op.schema {
                let name = schema.table_name.clone();
                schemas.insert(name.clone(), schema.clone());
                manifests.entry(name).or_default();
            }
        }
        Some(proto::meta::metadata_record::Op::SetManifest(op)) => {
            manifests.insert(
                op.table.clone(),
                proto::meta::TableManifest {
                    files: op.files.clone(),
                },
            );
        }
        Some(proto::meta::metadata_record::Op::DropTable(op)) => {
            schemas.remove(&op.table);
            manifests.remove(&op.table);
        }
        None => {}
    }
    Ok(())
}

fn estimate_schema_bytes(schema: &proto::meta::TableSchema) -> usize {
    let mut n = schema.table_name.len() + schema.data_dir.len() + 48;
    for c in &schema.columns {
        n += c.name.len() + c.data_type.len() + 16;
    }
    n
}

fn estimate_manifest_delta(table: &str, files: &[proto::meta::ParquetFileMeta]) -> usize {
    let mut n = table.len() + 32;
    for f in files {
        n += f.file_path.len() + 64;
    }
    n
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn validate_schema_proto(schema: &proto::meta::TableSchema) -> Result<()> {
    if schema.table_name.trim().is_empty() {
        return Err(TsdbError::Schema("table name cannot be empty".into()));
    }
    if schema.columns.is_empty() {
        return Err(TsdbError::Schema(
            "table must have at least one column".into(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for col in &schema.columns {
        if col.name.trim().is_empty() {
            return Err(TsdbError::Schema("column name cannot be empty".into()));
        }
        if !seen.insert(col.name.clone()) {
            return Err(TsdbError::Schema(format!(
                "duplicate column name: {}",
                col.name
            )));
        }
        normalize_type_name(&col.data_type)?;
    }
    if !schema.columns.iter().any(|c| c.name == TIMESTAMP_COLUMN) {
        return Err(TsdbError::Schema(format!(
            "table must include a time column named `{TIMESTAMP_COLUMN}`"
        )));
    }
    Ok(())
}
