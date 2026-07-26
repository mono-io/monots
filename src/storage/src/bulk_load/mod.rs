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

//! Bulk ingest external Parquet files into the SST (cold) layer.
//!
//! Industrial-grade path:
//! 1. **O(1) metadata inspection** via Parquet footer row-group statistics.
//! 2. **Crash-safe staging** under `{data_dir}/.bulk_tmp/` with Drop-guard cleanup.
//! 3. **Streaming SST write** (windowed when disordered) — never builds one giant RecordBatch.
//! 4. **Atomic seal** via `rename` into the LSN-bound final filename.

use crate::compaction::dedup::{plan_flush, FlushPlan, FLUSH_WINDOW_ROWS};
use crate::compaction::parquet_read::{
    parquet_file_time_bounds, read_parquet_file, ParquetReadOptions,
};
use crate::compaction::reader::BatchAligner;
use crate::compaction::sst::{
    bulk_tmp_dir, promote_sst_from_tmp, write_sst_streaming_try_with_config, FileIndex, SstMeta,
    SstWriteConfig,
};
use crate::compaction::sst_id::SstIdentity;
use crate::validate::validate_write_batch;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common::{time_column_index, time_value_at, Result, TsdbError};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default)]
pub struct ParquetInspect {
    pub row_count: usize,
    pub min_ts: i64,
    pub max_ts: i64,
}

pub struct BulkLoadResult {
    pub files_loaded: u32,
    pub rows_loaded: u64,
    pub metas: Vec<SstMeta>,
}

/// Removes a staging path on drop unless [`Self::defuse`] was called.
struct StagingGuard {
    path: PathBuf,
    active: bool,
}

impl StagingGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, active: true }
    }

    fn defuse(mut self) {
        self.active = false;
    }
}

impl Drop for StagingGuard {
    fn drop(&mut self) {
        if self.active {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// O(1) Parquet footer sniff for row count + time bounds.
///
/// Prefer row-group statistics; only fall back to a projected data scan when stats are missing.
pub fn inspect_parquet(path: &Path, catalog_schema: SchemaRef) -> Result<ParquetInspect> {
    if !path.is_file() {
        return Ok(ParquetInspect::default());
    }

    if let Some((min_ts, max_ts, row_count)) = parquet_file_time_bounds(path)? {
        return Ok(ParquetInspect {
            row_count,
            min_ts,
            max_ts,
        });
    }

    tracing::warn!(
        path = %path.display(),
        "Parquet file lacks time-column statistics; falling back to projected scan"
    );
    inspect_parquet_by_scan(path, catalog_schema)
}

fn inspect_parquet_by_scan(path: &Path, catalog_schema: SchemaRef) -> Result<ParquetInspect> {
    // Project only what BatchAligner needs; still avoid prepare_flush_batch mega-merge.
    let batches = read_parquet_file(path, catalog_schema, &ParquetReadOptions::default())?;
    if batches.is_empty() {
        return Ok(ParquetInspect {
            row_count: 0,
            min_ts: 0,
            max_ts: 0,
        });
    }

    let mut row_count = 0usize;
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let ts_idx = time_column_index(batch.schema())?;
        let ts_col = batch.column(ts_idx);
        for row in 0..batch.num_rows() {
            let ts = time_value_at(ts_col, row)?;
            min_ts = min_ts.min(ts);
            max_ts = max_ts.max(ts);
            row_count += 1;
        }
    }
    if row_count == 0 {
        return Ok(ParquetInspect {
            row_count: 0,
            min_ts: 0,
            max_ts: 0,
        });
    }
    Ok(ParquetInspect {
        row_count,
        min_ts,
        max_ts,
    })
}

/// Stream-read an external Parquet into a crash-safe staging SST under `.bulk_tmp/`.
///
/// Does **not** update [`FileIndex`]. CDC two-phase callers seal with
/// [`seal_bulk_sst_identity`] under the write lock; one-shot ingest promotes into `data_dir`.
pub fn write_bulk_parquet(
    source: &Path,
    data_dir: &Path,
    identity: &SstIdentity,
    catalog_schema: SchemaRef,
) -> Result<SstMeta> {
    if !source.is_file() {
        return Err(TsdbError::Storage(format!(
            "bulk load path is not a file: {}",
            source.display()
        )));
    }
    if source.extension().and_then(|e| e.to_str()) != Some("parquet") {
        return Err(TsdbError::Storage(format!(
            "bulk load expects .parquet files: {}",
            source.display()
        )));
    }

    let tmp_dir = bulk_tmp_dir(data_dir);
    fs::create_dir_all(&tmp_dir)?;
    let staging_path = tmp_dir.join(identity.filename());
    if staging_path.exists() {
        fs::remove_file(&staging_path)?;
    }
    let guard = StagingGuard::new(staging_path.clone());

    let chunks = read_aligned_chunks(source, catalog_schema.clone())?;
    if chunks.is_empty() {
        return Err(TsdbError::Storage(format!(
            "empty parquet file: {}",
            source.display()
        )));
    }

    let meta = match plan_flush(&chunks, catalog_schema.clone())? {
        FlushPlan::Streaming(stream_chunks) => write_sst_streaming_try_with_config(
            identity,
            &tmp_dir,
            catalog_schema,
            stream_chunks.into_iter().map(Ok),
            &SstWriteConfig::default(),
        ),
        FlushPlan::Sorted(sorted) => write_sst_streaming_try_with_config(
            identity,
            &tmp_dir,
            catalog_schema,
            sorted.window_batches(FLUSH_WINDOW_ROWS),
            &SstWriteConfig::default(),
        ),
    };

    let meta = match meta {
        Ok(meta) => meta,
        Err(e) => {
            drop(guard); // remove partial staging file
            return Err(e);
        }
    };
    meta.validate()?;
    guard.defuse();
    Ok(meta)
}

fn read_aligned_chunks(source: &Path, catalog_schema: SchemaRef) -> Result<Vec<RecordBatch>> {
    let file = File::open(source)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| TsdbError::Storage(e.to_string()))?;
    let mut reader = builder
        .build()
        .map_err(|e| TsdbError::Storage(e.to_string()))?;

    let mut chunks = Vec::new();
    while let Some(batch) = reader.next() {
        let batch = batch.map_err(|e| TsdbError::Storage(e.to_string()))?;
        if batch.num_rows() == 0 {
            continue;
        }
        let aligned = BatchAligner::align(batch, catalog_schema.clone())?;
        validate_write_batch(&aligned, catalog_schema.as_ref())?;
        chunks.push(aligned);
    }
    Ok(chunks)
}

/// Seal a staging SST with the final LSN identity via atomic rename + metadata rewrite.
///
/// Used by two-phase bulk load: heavy write completes under `.bulk_tmp/`, then a brief
/// critical section assigns LSN and renames into the durable CDC filename under `data_dir/`.
pub fn seal_bulk_sst_identity(
    meta: SstMeta,
    identity: &SstIdentity,
    data_dir: &Path,
) -> Result<SstMeta> {
    let old_path = PathBuf::from(&meta.file_path);
    let new_path = data_dir.join(identity.filename());
    if old_path != new_path {
        if new_path.exists() {
            return Err(TsdbError::Storage(format!(
                "SST already exists: {}",
                new_path.display()
            )));
        }
        fs::create_dir_all(data_dir)?;
        fs::rename(&old_path, &new_path).map_err(|e| {
            TsdbError::Storage(format!(
                "atomic seal rename {} -> {}: {e}",
                old_path.display(),
                new_path.display()
            ))
        })?;
    }

    let mut sealed = meta;
    sealed.file_path = new_path.to_string_lossy().into_owned();
    sealed.creation_time_ms = identity.creation_time_ms;
    sealed.base_lsn = identity.min_lsn;
    sealed.max_lsn = identity.max_lsn;
    sealed.inner_compaction_count = identity.inner_compaction_count;
    sealed.cross_compaction_count = identity.cross_compaction_count;
    sealed.validate()?;
    Ok(sealed)
}

/// Copy a validated Parquet into table storage and insert into [`FileIndex`].
///
/// Writes under `.bulk_tmp/` first, then atomically promotes into `data_dir/`.
/// `identity` must already embed the sealed CDC LSN span (filename key).
pub fn ingest_parquet_file(
    source: &Path,
    data_dir: &Path,
    identity: &SstIdentity,
    catalog_schema: SchemaRef,
    file_index: &FileIndex,
) -> Result<SstMeta> {
    let staged = write_bulk_parquet(source, data_dir, identity, catalog_schema)?;
    let meta = promote_sst_from_tmp(staged, data_dir)?;
    file_index.insert(meta.clone());
    Ok(meta)
}

/// Load one file or all `.parquet` files in a directory (non-recursive).
///
/// `next_identity` yields the LSN-sealed SST identity per file.
pub fn ingest_parquet_paths(
    paths: &[PathBuf],
    data_dir: &Path,
    catalog_schema: SchemaRef,
    file_index: &FileIndex,
    mut next_identity: impl FnMut() -> Result<SstIdentity>,
) -> Result<BulkLoadResult> {
    let mut files = Vec::new();
    for path in paths {
        files.extend(collect_parquet_inputs(path)?);
    }
    files.sort();
    files.dedup();

    let mut metas = Vec::new();
    let mut rows_loaded = 0u64;
    for source in files {
        let identity = next_identity()?;
        let meta = ingest_parquet_file(
            &source,
            data_dir,
            &identity,
            catalog_schema.clone(),
            file_index,
        )?;
        rows_loaded += meta.row_count as u64;
        metas.push(meta);
    }

    Ok(BulkLoadResult {
        files_loaded: metas.len() as u32,
        rows_loaded,
        metas,
    })
}

/// Collect `.parquet` inputs from a file or a non-recursive directory.
pub fn collect_parquet_inputs(path: &Path) -> Result<Vec<PathBuf>> {
    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }
    if path.is_dir() {
        let mut out = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let p = entry.path();
            if p.is_file() && p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                out.push(p);
            }
        }
        out.sort();
        return Ok(out);
    }
    Err(TsdbError::Storage(format!(
        "bulk load path not found: {}",
        path.display()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    fn catalog_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn write_sample_parquet(path: &Path) {
        let schema = catalog_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
            .build();
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn ingests_external_parquet_as_sst() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("external.parquet");
        write_sample_parquet(&source);

        let data_dir = tmp.path().join("table");
        let index = FileIndex::new();
        let meta = ingest_parquet_file(
            &source,
            &data_dir,
            &SstIdentity::fresh_flush(7, 7),
            catalog_schema(),
            &index,
        )
        .unwrap();
        assert_eq!(meta.row_count, 3);
        assert_eq!(meta.min_ts, 10);
        assert_eq!(meta.max_ts, 30);
        assert_eq!(meta.base_lsn, 7);
        assert_eq!(meta.max_lsn, 7);
        assert!(meta.file_path.ends_with("-7-7-0-0.parquet"));
        assert!(Path::new(&meta.file_path).exists());
        assert_eq!(
            Path::new(&meta.file_path).parent(),
            Some(data_dir.as_path())
        );
        assert_eq!(index.snapshot().len(), 1);
        // Staging must not leave leftovers after successful ingest.
        let bulk_tmp = bulk_tmp_dir(&data_dir);
        if bulk_tmp.exists() {
            let leftovers: Vec<_> = fs::read_dir(&bulk_tmp)
                .unwrap()
                .filter_map(|e| e.ok())
                .collect();
            assert!(leftovers.is_empty(), "{leftovers:?}");
        }
    }

    #[test]
    fn seals_staging_sst_with_final_lsn_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("external.parquet");
        write_sample_parquet(&source);
        let data_dir = tmp.path().join("table");
        let staging = SstIdentity::staging();
        let staged = write_bulk_parquet(&source, &data_dir, &staging, catalog_schema()).unwrap();
        let staging_path = PathBuf::from(&staged.file_path);
        assert!(staging_path.exists());
        assert!(
            staging_path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("staging-") && n.ends_with(".parquet")),
            "staging write must use UUID staging name: {}",
            staging_path.display()
        );
        assert!(
            staging_path
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|n| n.to_str())
                == Some(".bulk_tmp"),
            "staging write must land under .bulk_tmp: {}",
            staging_path.display()
        );

        let sealed =
            seal_bulk_sst_identity(staged, &SstIdentity::fresh_flush(42, 42), &data_dir).unwrap();
        assert!(!staging_path.exists());
        assert!(Path::new(&sealed.file_path).exists());
        assert_eq!(sealed.base_lsn, 42);
        assert_eq!(sealed.max_lsn, 42);
        assert!(sealed.file_path.ends_with("-42-42-0-0.parquet"));
        assert_eq!(
            Path::new(&sealed.file_path).parent(),
            Some(data_dir.as_path())
        );
    }

    #[test]
    fn staging_guard_cleans_partial_file_on_error() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("table");
        let source = tmp.path().join("missing.parquet");
        // Non-parquet extension triggers early error before create — use parquet name with garbage.
        fs::write(&source, b"not-a-parquet").unwrap();
        let err = write_bulk_parquet(
            &source,
            &data_dir,
            &SstIdentity::staging(),
            catalog_schema(),
        )
        .unwrap_err();
        assert!(!err.to_string().is_empty());
        let bulk_tmp = bulk_tmp_dir(&data_dir);
        if bulk_tmp.exists() {
            let leftovers: Vec<_> = fs::read_dir(&bulk_tmp)
                .unwrap()
                .filter_map(|e| e.ok())
                .collect();
            assert!(
                leftovers.is_empty(),
                "failed bulk write must not leave staging SST: {leftovers:?}"
            );
        }
    }

    #[test]
    fn loads_multiple_files_from_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let import_dir = tmp.path().join("import");
        fs::create_dir_all(&import_dir).unwrap();
        write_sample_parquet(&import_dir.join("a.parquet"));
        write_sample_parquet(&import_dir.join("b.parquet"));

        let data_dir = tmp.path().join("table");
        let index = FileIndex::new();
        let mut next_id = 1u64;
        let result =
            ingest_parquet_paths(&[import_dir], &data_dir, catalog_schema(), &index, || {
                let id = next_id;
                next_id += 1;
                Ok(SstIdentity::fresh_flush(id, id))
            })
            .unwrap();

        assert_eq!(result.files_loaded, 2);
        assert_eq!(result.rows_loaded, 6);
        assert_eq!(index.snapshot().len(), 2);
    }

    #[test]
    fn rejects_non_parquet_extension() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("data.csv");
        fs::write(&source, "timestamp,value\n1,2").unwrap();
        let index = FileIndex::new();
        let err = ingest_parquet_file(
            &source,
            &tmp.path(),
            &SstIdentity::fresh_flush(1, 1),
            catalog_schema(),
            &index,
        )
        .unwrap_err();
        assert!(err.to_string().contains("parquet"));
    }

    #[test]
    fn rejects_schema_type_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("bad.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64])),
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();
        let file = fs::File::create(&source).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let catalog = catalog_schema();
        let index = FileIndex::new();
        let err = ingest_parquet_file(
            &source,
            &tmp.path(),
            &SstIdentity::fresh_flush(1, 1),
            catalog,
            &index,
        )
        .unwrap_err();
        assert!(err.to_string().contains("type mismatch"));
    }

    #[test]
    fn rejects_empty_parquet_file() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("empty.parquet");
        let schema = catalog_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
            ],
        )
        .unwrap();
        let file = fs::File::create(&source).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let index = FileIndex::new();
        let err = ingest_parquet_file(
            &source,
            &tmp.path(),
            &SstIdentity::fresh_flush(1, 1),
            catalog_schema(),
            &index,
        )
        .unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn inspect_uses_footer_statistics_for_bounds() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("bounds.parquet");
        let schema = catalog_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100_i64, 50, 200])),
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
            .build();
        let file = fs::File::create(&source).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let inspect = inspect_parquet(&source, schema).unwrap();
        assert_eq!(inspect.row_count, 3);
        assert_eq!(inspect.min_ts, 50);
        assert_eq!(inspect.max_ts, 200);
        // Footer path must succeed (not only scan fallback).
        assert!(parquet_file_time_bounds(&source).unwrap().is_some());
    }
}
