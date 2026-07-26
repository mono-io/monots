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

//! SST metadata, file index, and Parquet writers.
//!
//! Writers use configurable [`SstWriteConfig`] (row-group size / compression). Sync APIs remain
//! the primary path for `spawn_blocking` flush/compaction; [`write_sst_streaming_try_async`] is
//! available for fully async callers.

use crate::compaction::sst_id::{parse_sst_filename, SstIdentity};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common::{
    time_column_index, time_value_at, Result, TsdbError, BULK_TMP_DIR, COMPACT_TMP_DIR,
    FLUSH_TMP_DIR,
};
use parking_lot::RwLock;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Tunables for SST Parquet writes (replaces hard-coded writer defaults).
#[derive(Debug, Clone)]
pub struct SstWriteConfig {
    pub max_row_group_size: usize,
    pub compression: Compression,
}

impl Default for SstWriteConfig {
    fn default() -> Self {
        Self {
            max_row_group_size: 8_192,
            // Snappy: high throughput default for hot TSDB flushes (ZSTD available via config).
            compression: Compression::SNAPPY,
        }
    }
}

impl SstWriteConfig {
    pub fn writer_properties(&self) -> WriterProperties {
        WriterProperties::builder()
            .set_compression(self.compression)
            .set_max_row_group_size(self.max_row_group_size.max(1))
            .build()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SstMeta {
    pub file_path: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub row_count: usize,
    pub file_size: u64,
    pub creation_time_ms: i64,
    pub inner_compaction_count: u32,
    pub cross_compaction_count: u32,
    /// Inclusive CDC LSN lower bound — also encoded in the SST filename.
    #[serde(default)]
    pub base_lsn: u64,
    /// Inclusive CDC LSN upper bound — also encoded in the SST filename.
    #[serde(default)]
    pub max_lsn: u64,
}

impl SstMeta {
    pub fn from_identity(
        identity: SstIdentity,
        file_path: String,
        min_ts: i64,
        max_ts: i64,
        row_count: usize,
        file_size: u64,
    ) -> Self {
        Self {
            file_path,
            min_ts,
            max_ts,
            row_count,
            file_size,
            creation_time_ms: identity.creation_time_ms,
            inner_compaction_count: identity.inner_compaction_count,
            cross_compaction_count: identity.cross_compaction_count,
            base_lsn: identity.min_lsn,
            max_lsn: identity.max_lsn,
        }
    }

    pub fn with_lsn_bounds(mut self, base_lsn: u64, max_lsn: u64) -> Self {
        self.base_lsn = base_lsn;
        self.max_lsn = max_lsn;
        self
    }

    pub fn has_lsn_bounds(&self) -> bool {
        self.base_lsn > 0 || self.max_lsn > 0
    }

    pub fn identity(&self) -> SstIdentity {
        SstIdentity::from_parts(
            self.creation_time_ms,
            self.base_lsn,
            self.max_lsn,
            self.inner_compaction_count,
            self.cross_compaction_count,
        )
    }

    pub fn covers_lsn(&self, lsn: u64) -> bool {
        self.has_lsn_bounds() && lsn >= self.base_lsn && lsn <= self.max_lsn
    }

    /// Ensure on-disk filename matches persisted LSN identity metadata.
    ///
    /// Staging (`staging-*.parquet`) files are intentionally not parseable as sealed
    /// identities — they are validated only by suffix / prefix so two-phase bulk load
    /// can call `validate` before seal.
    pub fn validate(&self) -> Result<()> {
        let file_name = Path::new(&self.file_path)
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| TsdbError::Storage(format!("invalid SST path: {}", self.file_path)))?;
        if crate::compaction::sst_id::is_staging_sst_filename(file_name) {
            if self.base_lsn > self.max_lsn {
                return Err(TsdbError::Storage(format!(
                    "SST LSN range inverted for {}",
                    self.file_path
                )));
            }
            return Ok(());
        }
        let parsed = parse_sst_filename(file_name)?;
        if parsed.creation_time_ms != self.creation_time_ms
            || parsed.min_lsn != self.base_lsn
            || parsed.max_lsn != self.max_lsn
            || parsed.inner_compaction_count != self.inner_compaction_count
            || parsed.cross_compaction_count != self.cross_compaction_count
        {
            return Err(TsdbError::Storage(format!(
                "SST metadata mismatch for {}",
                self.file_path
            )));
        }
        if self.base_lsn > self.max_lsn {
            return Err(TsdbError::Storage(format!(
                "SST LSN range inverted for {}",
                self.file_path
            )));
        }
        Ok(())
    }
}

pub type SstFile = SstMeta;

pub struct FileIndex {
    pub files: RwLock<Vec<SstMeta>>,
}

impl FileIndex {
    pub fn new() -> Self {
        Self {
            files: RwLock::new(Vec::new()),
        }
    }

    pub fn from_persisted(files: Vec<SstMeta>) -> Result<Self> {
        for file in &files {
            file.validate()?;
        }
        let mut sorted = files;
        sorted.sort_by_key(|f| f.min_ts);
        Ok(Self {
            files: RwLock::new(sorted),
        })
    }

    /// True if any SST's sealed LSN span covers `lsn`.
    pub fn covers_lsn(&self, lsn: u64) -> bool {
        self.files.read().iter().any(|f| f.covers_lsn(lsn))
    }

    /// True if any SST covers the inclusive LSN range `[base, max]`.
    pub fn covers_lsn_range(&self, base: u64, max: u64) -> bool {
        if base > max {
            return true;
        }
        // Conservative: every endpoint (and mid if needed) covered by some file's span.
        // For WAL GC we only need the sealed memtable range to be durable in some SST(s).
        self.files
            .read()
            .iter()
            .any(|f| f.has_lsn_bounds() && f.base_lsn <= base && f.max_lsn >= max)
            || {
                // Allow coverage by a union of overlapping SSTs.
                let mut cursor = base;
                let files = self.files.read();
                while cursor <= max {
                    let Some(f) = files.iter().find(|f| f.covers_lsn(cursor)) else {
                        return false;
                    };
                    if f.max_lsn >= max {
                        return true;
                    }
                    cursor = f.max_lsn.saturating_add(1);
                }
                true
            }
    }

    pub fn insert(&self, meta: SstMeta) {
        meta.validate()
            .expect("inserted SST metadata must be valid");
        let mut files = self.files.write();
        let pos = files
            .binary_search_by_key(&meta.min_ts, |f| f.min_ts)
            .unwrap_or_else(|e| e);
        files.insert(pos, meta);
    }

    /// Stamp CDC LSN bounds onto an already-indexed SST.
    ///
    /// Prefer sealing LSN **before** write so the filename already embeds the span; this helper
    /// only updates in-memory metadata (does not rename the file).
    pub fn set_lsn_bounds(&self, file_path: &str, base_lsn: u64, max_lsn: u64) {
        let mut files = self.files.write();
        if let Some(f) = files.iter_mut().find(|m| m.file_path == file_path) {
            f.base_lsn = base_lsn;
            f.max_lsn = max_lsn;
        }
    }

    pub fn replace_range(&self, start: usize, count: usize, replacement: SstMeta) {
        replacement
            .validate()
            .expect("replacement SST metadata must be valid");
        let mut files = self.files.write();
        for _ in 0..count {
            if start < files.len() {
                files.remove(start);
            }
        }
        files.insert(start, replacement);
    }

    pub fn snapshot(&self) -> Vec<SstMeta> {
        self.files.read().clone()
    }
}

fn open_writer(
    file: std::fs::File,
    schema: SchemaRef,
    config: &SstWriteConfig,
) -> Result<parquet::arrow::ArrowWriter<std::fs::File>> {
    parquet::arrow::ArrowWriter::try_new(file, schema, Some(config.writer_properties()))
        .map_err(|e| TsdbError::Storage(format!("parquet writer: {e}")))
}

/// Staging dir for in-progress MemTable flushes: `{data_dir}/.flush_tmp/`.
pub fn flush_tmp_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(FLUSH_TMP_DIR)
}

/// Staging dir for in-progress compaction merges: `{data_dir}/.compact_tmp/`.
pub fn compact_tmp_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(COMPACT_TMP_DIR)
}

/// Staging dir for in-progress bulk-load writes: `{data_dir}/.bulk_tmp/`.
pub fn bulk_tmp_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(BULK_TMP_DIR)
}

/// Atomically promote a completed SST from a staging path into `data_dir/`.
///
/// Capture / FileIndex must only see the promoted path (never a partial staging file).
pub fn promote_sst_from_tmp(meta: SstMeta, data_dir: &Path) -> Result<SstMeta> {
    let tmp_path = PathBuf::from(&meta.file_path);
    let file_name = tmp_path.file_name().ok_or_else(|| {
        TsdbError::Storage(format!("invalid SST staging path: {}", meta.file_path))
    })?;
    let final_path = data_dir.join(file_name);
    if tmp_path != final_path {
        if final_path.exists() {
            return Err(TsdbError::Storage(format!(
                "SST already exists: {}",
                final_path.display()
            )));
        }
        std::fs::rename(&tmp_path, &final_path)?;
    }
    let mut promoted = meta;
    promoted.file_path = final_path.to_string_lossy().into_owned();
    promoted.validate()?;
    Ok(promoted)
}

/// Atomically promote a completed flush SST from `.flush_tmp/` into `data_dir/`.
pub fn promote_sst_from_flush_tmp(meta: SstMeta, data_dir: &Path) -> Result<SstMeta> {
    promote_sst_from_tmp(meta, data_dir)
}

/// Atomically promote a completed compaction SST from `.compact_tmp/` into `data_dir/`.
pub fn promote_sst_from_compact_tmp(meta: SstMeta, data_dir: &Path) -> Result<SstMeta> {
    promote_sst_from_tmp(meta, data_dir)
}

fn cleanup_staging_dir(data_dir: &Path, staging_name: &str, label: &str) -> Result<()> {
    let tmp = data_dir.join(staging_name);
    if !tmp.exists() {
        return Ok(());
    }
    match std::fs::remove_dir_all(&tmp) {
        Ok(()) => {
            tracing::info!(
                path = %tmp.display(),
                staging = label,
                "startup removed SST staging directory"
            );
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => {
            tracing::warn!(
                path = %tmp.display(),
                staging = label,
                error = %e,
                "failed to remove SST staging directory on startup; retrying entry-wise"
            );
            // Best-effort fallback if remove_dir_all failed mid-way.
            if tmp.is_dir() {
                for entry in std::fs::read_dir(&tmp)? {
                    let path = entry?.path();
                    let _ = if path.is_dir() {
                        std::fs::remove_dir_all(&path)
                    } else {
                        std::fs::remove_file(&path)
                    };
                }
                let _ = std::fs::remove_dir(&tmp);
            } else {
                let _ = std::fs::remove_file(&tmp);
            }
            if tmp.exists() {
                return Err(TsdbError::Storage(format!(
                    "failed to remove SST staging dir {}: {e}",
                    tmp.display()
                )));
            }
            Ok(())
        }
    }
}

/// Drop leftover incomplete flush artifacts under `.flush_tmp/` (crash / failed flush).
pub fn cleanup_flush_tmp(data_dir: &Path) -> Result<()> {
    cleanup_staging_dir(data_dir, FLUSH_TMP_DIR, "flush")
}

/// Drop leftover incomplete compaction artifacts under `.compact_tmp/` (crash / failed merge).
pub fn cleanup_compact_tmp(data_dir: &Path) -> Result<()> {
    cleanup_staging_dir(data_dir, COMPACT_TMP_DIR, "compact")
}

/// Drop leftover incomplete bulk-load artifacts under `.bulk_tmp/` (crash / failed ingest).
pub fn cleanup_bulk_tmp(data_dir: &Path) -> Result<()> {
    cleanup_staging_dir(data_dir, BULK_TMP_DIR, "bulk")
}

/// Drop leftover flush + compaction + bulk-load staging under a table data dir.
pub fn cleanup_sst_staging(data_dir: &Path) -> Result<()> {
    cleanup_flush_tmp(data_dir)?;
    cleanup_compact_tmp(data_dir)?;
    cleanup_bulk_tmp(data_dir)
}

/// Scan `{root}` and each immediate child table dir for leftover SST staging dirs (engine startup).
pub fn cleanup_flush_tmp_under(root: &Path) -> Result<()> {
    cleanup_sst_staging_under(root)
}

/// Recursively delete every `.flush_tmp/` / `.compact_tmp/` / `.bulk_tmp/` under `{root}`
/// (engine startup).
///
/// Incomplete Parquet from crash mid-flush / mid-merge / mid-bulk-load must never survive
/// into a new process.
pub fn cleanup_sst_staging_under(root: &Path) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    // Always clear staging next to the engine root itself (legacy / misnested layouts).
    cleanup_sst_staging(root)?;
    if !root.is_dir() {
        return Ok(());
    }

    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(
                    path = %dir.display(),
                    error = %e,
                    "skip unreadable dir while scanning SST staging"
                );
                continue;
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            if name == FLUSH_TMP_DIR || name == COMPACT_TMP_DIR || name == BULK_TMP_DIR {
                // Entire staging tree is disposable — delete in place, do not descend.
                let label = match name {
                    FLUSH_TMP_DIR => "flush",
                    COMPACT_TMP_DIR => "compact",
                    _ => "bulk",
                };
                let parent = path.parent().unwrap_or(root);
                cleanup_staging_dir(parent, name, label)?;
                continue;
            }
            // Do not walk into other engine namespaces / hidden dirs.
            if matches!(
                name,
                "meta" | "replication" | "stream" | "query_spill" | "wal_segments"
            ) || name.starts_with('.')
            {
                continue;
            }
            stack.push(path);
        }
    }
    Ok(())
}

/// Write a sorted, deduped batch as a new on-disk SST file.
pub fn write_sst(
    identity: &SstIdentity,
    merged: &RecordBatch,
    data_dir: &Path,
    min_ts: i64,
    max_ts: i64,
) -> Result<SstMeta> {
    write_sst_with_config(
        identity,
        merged,
        data_dir,
        min_ts,
        max_ts,
        &SstWriteConfig::default(),
    )
}

pub fn write_sst_with_config(
    identity: &SstIdentity,
    merged: &RecordBatch,
    data_dir: &Path,
    min_ts: i64,
    max_ts: i64,
    config: &SstWriteConfig,
) -> Result<SstMeta> {
    std::fs::create_dir_all(data_dir)?;
    let file_path = data_dir.join(identity.filename());
    let file = std::fs::File::create(&file_path)?;
    let mut writer = open_writer(file, merged.schema(), config)?;
    writer.write(merged)?;
    writer.close()?;
    let file_size = std::fs::metadata(&file_path)?.len();
    Ok(SstMeta::from_identity(
        *identity,
        file_path.to_string_lossy().to_string(),
        min_ts,
        max_ts,
        merged.num_rows(),
        file_size,
    ))
}

/// Write an SST from a stream of sorted batches (bounded memory compaction path).
pub fn write_sst_streaming<I>(
    identity: &SstIdentity,
    data_dir: &Path,
    min_ts: i64,
    max_ts: i64,
    row_count: usize,
    schema: SchemaRef,
    batches: I,
) -> Result<SstMeta>
where
    I: IntoIterator<Item = RecordBatch>,
{
    write_sst_streaming_with_config(
        identity,
        data_dir,
        min_ts,
        max_ts,
        row_count,
        schema,
        batches,
        &SstWriteConfig::default(),
    )
}

pub fn write_sst_streaming_with_config<I>(
    identity: &SstIdentity,
    data_dir: &Path,
    min_ts: i64,
    max_ts: i64,
    row_count: usize,
    schema: SchemaRef,
    batches: I,
    config: &SstWriteConfig,
) -> Result<SstMeta>
where
    I: IntoIterator<Item = RecordBatch>,
{
    std::fs::create_dir_all(data_dir)?;
    let file_path = data_dir.join(identity.filename());
    let file = std::fs::File::create(&file_path)?;
    let mut writer = open_writer(file, schema, config)?;
    for batch in batches {
        if batch.num_rows() > 0 {
            writer.write(&batch)?;
        }
    }
    writer.close()?;
    let file_size = std::fs::metadata(&file_path)?.len();
    Ok(SstMeta::from_identity(
        *identity,
        file_path.to_string_lossy().to_string(),
        min_ts,
        max_ts,
        row_count,
        file_size,
    ))
}

/// Write an SST from a fallible stream of **sorted** batches, computing bounds on the fly.
pub fn write_sst_streaming_try<I>(
    identity: &SstIdentity,
    data_dir: &Path,
    schema: SchemaRef,
    batches: I,
) -> Result<SstMeta>
where
    I: IntoIterator<Item = Result<RecordBatch>>,
{
    write_sst_streaming_try_with_config(
        identity,
        data_dir,
        schema,
        batches,
        &SstWriteConfig::default(),
    )
}

pub fn write_sst_streaming_try_with_config<I>(
    identity: &SstIdentity,
    data_dir: &Path,
    schema: SchemaRef,
    batches: I,
    config: &SstWriteConfig,
) -> Result<SstMeta>
where
    I: IntoIterator<Item = Result<RecordBatch>>,
{
    let ts_idx = time_column_index(&schema)?;

    std::fs::create_dir_all(data_dir)?;
    let file_path = data_dir.join(identity.filename());
    let file = std::fs::File::create(&file_path)?;
    let mut writer = open_writer(file, schema.clone(), config)?;

    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut row_count = 0usize;
    for batch in batches {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        let ts_col = batch.column(ts_idx);
        min_ts = min_ts.min(time_value_at(ts_col, 0)?);
        max_ts = max_ts.max(time_value_at(ts_col, batch.num_rows() - 1)?);
        row_count += batch.num_rows();
        writer.write(&batch)?;
    }
    writer.close()?;

    if row_count == 0 {
        let _ = std::fs::remove_file(&file_path);
        return Err(TsdbError::Storage("nothing to flush".into()));
    }

    let file_size = std::fs::metadata(&file_path)?.len();
    Ok(SstMeta::from_identity(
        *identity,
        file_path.to_string_lossy().to_string(),
        min_ts,
        max_ts,
        row_count,
        file_size,
    ))
}

/// Async streaming SST write (non-blocking for Tokio callers).
pub async fn write_sst_streaming_try_async<S>(
    identity: &SstIdentity,
    data_dir: &Path,
    schema: SchemaRef,
    config: SstWriteConfig,
    mut batches: S,
) -> Result<SstMeta>
where
    S: futures_util::Stream<Item = Result<RecordBatch>> + Unpin,
{
    use futures_util::StreamExt;
    use parquet::arrow::AsyncArrowWriter;

    tokio::fs::create_dir_all(data_dir).await?;
    let file_path = data_dir.join(identity.filename());
    let file = tokio::fs::File::create(&file_path).await?;
    let mut writer =
        AsyncArrowWriter::try_new(file, schema.clone(), Some(config.writer_properties()))
            .map_err(|e| TsdbError::Storage(format!("async parquet writer: {e}")))?;

    let ts_idx = time_column_index(&schema)?;
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut row_count = 0usize;

    while let Some(batch) = batches.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        let ts_col = batch.column(ts_idx);
        min_ts = min_ts.min(time_value_at(ts_col, 0)?);
        max_ts = max_ts.max(time_value_at(ts_col, batch.num_rows() - 1)?);
        row_count += batch.num_rows();
        writer
            .write(&batch)
            .await
            .map_err(|e| TsdbError::Storage(format!("async parquet write: {e}")))?;
    }
    writer
        .close()
        .await
        .map_err(|e| TsdbError::Storage(format!("async parquet close: {e}")))?;

    if row_count == 0 {
        let _ = tokio::fs::remove_file(&file_path).await;
        return Err(TsdbError::Storage("nothing to flush".into()));
    }

    let file_size = tokio::fs::metadata(&file_path).await?.len();
    Ok(SstMeta::from_identity(
        *identity,
        file_path.to_string_lossy().to_string(),
        min_ts,
        max_ts,
        row_count,
        file_size,
    ))
}

impl Default for FileIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_mismatched_metadata() {
        let identity = SstIdentity::from_parts(1_000, 7, 7, 0, 0);
        let meta = SstMeta::from_identity(
            identity,
            "/data/t/1000-99-99-0-0.parquet".into(),
            1,
            2,
            10,
            512,
        );
        assert!(meta.validate().is_err());
    }

    #[test]
    fn flushed_ids_expand_compaction_span() {
        let index = FileIndex::new();
        let identity = SstIdentity::from_parts(1, 10, 20, 1, 0);
        let meta = SstMeta::from_identity(
            identity,
            format!("/data/t/{}", identity.filename()),
            0,
            1,
            1,
            1,
        );
        index.insert(meta);
        assert!(index.covers_lsn(10));
        assert!(index.covers_lsn(20));
        assert!(index.covers_lsn_range(10, 20));
        assert!(!index.covers_lsn(9));
    }

    #[test]
    fn promotes_flush_tmp_into_data_dir_and_cleanup_drops_leftovers() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path();
        let tmp = flush_tmp_dir(data_dir);
        std::fs::create_dir_all(&tmp).unwrap();

        let identity = SstIdentity::fresh_flush(3, 3);
        let staging = tmp.join(identity.filename());
        std::fs::write(&staging, b"parquet-bytes").unwrap();
        let meta = SstMeta::from_identity(
            identity,
            staging.to_string_lossy().into_owned(),
            1,
            2,
            1,
            13,
        );

        let promoted = promote_sst_from_flush_tmp(meta, data_dir).unwrap();
        assert!(!staging.exists());
        assert!(Path::new(&promoted.file_path).exists());
        assert_eq!(Path::new(&promoted.file_path).parent(), Some(data_dir));

        // Simulate a crash leftover and ensure open cleanup removes it.
        let leftover = tmp.join("dead-1-1-0-0.parquet");
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(&leftover, b"orphan").unwrap();
        cleanup_flush_tmp(data_dir).unwrap();
        assert!(!leftover.exists());
        assert!(
            !tmp.exists(),
            "empty .flush_tmp should be removed on startup cleanup"
        );
    }

    #[test]
    fn promotes_compact_tmp_into_data_dir_and_cleanup_drops_leftovers() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path();
        let tmp = compact_tmp_dir(data_dir);
        std::fs::create_dir_all(&tmp).unwrap();

        let identity = SstIdentity::from_parts(2_000, 1, 2, 1, 0);
        let staging = tmp.join(identity.filename());
        std::fs::write(&staging, b"parquet-bytes").unwrap();
        let meta = SstMeta::from_identity(
            identity,
            staging.to_string_lossy().into_owned(),
            1,
            2,
            1,
            13,
        );

        let promoted = promote_sst_from_compact_tmp(meta, data_dir).unwrap();
        assert!(!staging.exists());
        assert!(Path::new(&promoted.file_path).exists());
        assert_eq!(Path::new(&promoted.file_path).parent(), Some(data_dir));

        let leftover = tmp.join("dead-merge-1-2-1-0.parquet");
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(&leftover, b"orphan").unwrap();
        cleanup_compact_tmp(data_dir).unwrap();
        assert!(!leftover.exists());
        assert!(
            !tmp.exists(),
            "empty .compact_tmp should be removed on startup cleanup"
        );
    }

    #[test]
    fn startup_cleanup_recursively_removes_nested_staging_dirs() {
        let root = tempfile::tempdir().unwrap();
        let nested = root.path().join("db").join("t0");
        let flush_tmp = flush_tmp_dir(&nested);
        let compact_tmp = compact_tmp_dir(&nested);
        let bulk_tmp = bulk_tmp_dir(&nested);
        std::fs::create_dir_all(&flush_tmp).unwrap();
        std::fs::create_dir_all(&compact_tmp).unwrap();
        std::fs::create_dir_all(&bulk_tmp).unwrap();
        std::fs::write(flush_tmp.join("a.parquet"), b"x").unwrap();
        std::fs::write(compact_tmp.join("b.parquet"), b"y").unwrap();
        std::fs::write(bulk_tmp.join("c.parquet"), b"z").unwrap();

        cleanup_sst_staging_under(root.path()).unwrap();
        assert!(!flush_tmp.exists());
        assert!(!compact_tmp.exists());
        assert!(!bulk_tmp.exists());
    }

    #[test]
    fn startup_cleanup_scans_table_dirs_under_engine_root() {
        let root = tempfile::tempdir().unwrap();
        let t0 = root.path().join("t0");
        let flush_tmp = flush_tmp_dir(&t0);
        let compact_tmp = compact_tmp_dir(&t0);
        std::fs::create_dir_all(&flush_tmp).unwrap();
        std::fs::create_dir_all(&compact_tmp).unwrap();
        let flush_leftover = flush_tmp.join("crash-9-9-0-0.parquet");
        let compact_leftover = compact_tmp.join("crash-merge-1-2-1-0.parquet");
        std::fs::write(&flush_leftover, b"orphan").unwrap();
        std::fs::write(&compact_leftover, b"orphan").unwrap();

        cleanup_flush_tmp_under(root.path()).unwrap();
        assert!(!flush_leftover.exists());
        assert!(!compact_leftover.exists());
        assert!(!flush_tmp.exists());
        assert!(!compact_tmp.exists());
    }
}
