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

//! Delta Lake connector plugin (`sink.type = delta`).
//!
//! Supports local filesystem and S3-compatible object storage (`s3://`, `s3a://`)
//! via deltalake's official `s3` feature (`deltalake::aws::register_handlers`).
//!
//! Industrial-grade guarantees:
//! - **ACID 2PC**: Stage/rename (or upload) Parquet first, then commit `_delta_log`.
//! - **State retention**: Caches `DeltaTable` to avoid re-parsing the transaction log.
//! - **Phantom file safety**: Failed log commits leave unreferenced data files for `VACUUM`.
//! - **OCC with Inner Retry**: Automatically refreshes snapshot and retries on concurrent
//!   writer conflicts (max 5 attempts with backoff).
//!
//! Credentials: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION`.
//! MinIO / custom S3 API URL belongs on the stream as DDL `sink.delta.endpoint`
//! (process-wide `MONOTS_LAKE_ENDPOINT` is only a fallback when `sink.delta.endpoint` is omitted). Single-writer S3
//! commits use `AWS_S3_ALLOW_UNSAFE_RENAME=true` (same model as FunctionStream).

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Once;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::DataType as ArrowDataType;
use deltalake::kernel::{Action, Add, DataType as DeltaDataType, Protocol, StructField};
use deltalake::operations::transaction::CommitBuilder;
use deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::{
    open_table, open_table_with_storage_options, DeltaOps, DeltaTable, DeltaTableError,
};
use object_store::path::Path as ObjectPath;
use object_store::PutPayload;
use tokio::fs;
use tracing::{debug, info, warn};

use crate::connector::{SinkConnector, SinkError};
use crate::model::event::DataEvent;

use super::parquet_dir::ParquetDirStaging;

/// Max OCC retries with snapshot refresh when concurrent writers collide on `_delta_log`.
const OCC_MAX_RETRIES: usize = 5;

static AWS_HANDLERS: Once = Once::new();

fn ensure_aws_handlers() {
    AWS_HANDLERS.call_once(|| {
        deltalake::aws::register_handlers(None);
    });
}

fn is_s3_uri(uri: &str) -> bool {
    let u = uri.to_ascii_lowercase();
    u.starts_with("s3://") || u.starts_with("s3a://")
}

fn is_unsupported_object_uri(uri: &str) -> bool {
    let u = uri.to_ascii_lowercase();
    u.starts_with("gs://")
        || u.starts_with("gcs://")
        || u.starts_with("abfs://")
        || u.starts_with("abfss://")
}

fn is_object_uri(uri: &str) -> bool {
    is_s3_uri(uri) || is_unsupported_object_uri(uri)
}

fn normalize_table_uri(raw: &str) -> String {
    let s = raw.trim();
    if let Some(rest) = s.strip_prefix("file://") {
        let path = rest.strip_prefix("localhost").unwrap_or(rest);
        path.to_string()
    } else {
        s.to_string()
    }
}

fn staging_root_for(uri: &str) -> PathBuf {
    if is_object_uri(uri) {
        let safe: String = uri
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        std::env::temp_dir().join("monots_delta_staging").join(safe)
    } else {
        PathBuf::from(uri)
    }
}

fn storage_options(endpoint: Option<&str>) -> HashMap<String, String> {
    let mut opts = HashMap::new();
    // Single writer per table/stream (FunctionStream default).
    opts.insert("AWS_S3_ALLOW_UNSAFE_RENAME".into(), "true".into());
    if let Some(ep) = endpoint.filter(|s| !s.is_empty()) {
        opts.insert("AWS_ENDPOINT_URL".into(), ep.to_string());
        opts.insert("AWS_ENDPOINT".into(), ep.to_string());
        opts.insert("AWS_ALLOW_HTTP".into(), "true".into());
    }
    opts
}

pub struct DeltaSink {
    table_uri: String,
    remote: bool,
    staging: ParquetDirStaging,
    delta_table: Option<DeltaTable>,
    storage_options: HashMap<String, String>,
    in_transaction: bool,
}

impl DeltaSink {
    /// `path_or_uri`: local path, `file://…`, or `s3://bucket/prefix`.
    pub fn new(
        path_or_uri: impl Into<String>,
        _table: Option<String>,
        endpoint: Option<String>,
    ) -> Self {
        let table_uri = normalize_table_uri(&path_or_uri.into());
        let remote = is_object_uri(&table_uri);
        let staging_path = staging_root_for(&table_uri);
        let storage_options = storage_options(endpoint.as_deref());

        Self {
            staging: ParquetDirStaging::new(staging_path, None),
            table_uri,
            remote,
            delta_table: None,
            storage_options,
            in_transaction: false,
        }
    }

    async fn open_or_create_local(
        &self,
        schema_hint: Option<Vec<StructField>>,
    ) -> Result<Option<DeltaTable>, SinkError> {
        match open_table(&self.table_uri).await {
            Ok(t) => {
                info!(
                    uri = %self.table_uri,
                    version = ?t.version(),
                    "opened existing Delta table"
                );
                Ok(Some(t))
            }
            Err(DeltaTableError::NotATable(_)) => {
                let Some(columns) = schema_hint else {
                    debug!(
                        uri = %self.table_uri,
                        "Delta table not found yet; deferring create until first commit"
                    );
                    return Ok(None);
                };
                info!(
                    uri = %self.table_uri,
                    columns = columns.len(),
                    "Delta table not found; initializing local _delta_log"
                );
                let table = DeltaOps::try_from_uri(&self.table_uri)
                    .await
                    .map_err(|e| SinkError::Fatal(format!("Init DeltaOps failed: {e}")))?
                    .create()
                    .with_save_mode(SaveMode::Ignore)
                    .with_actions([Action::Protocol(Protocol::new(1, 1))])
                    .with_columns(columns)
                    .await
                    .map_err(|e| SinkError::Fatal(format!("Create Delta table failed: {e}")))?;
                Ok(Some(table))
            }
            Err(e) => Err(SinkError::Transient(format!(
                "Failed to open Delta table at {}: {e}",
                self.table_uri
            ))),
        }
    }

    async fn open_or_create_s3(
        &self,
        schema_hint: Option<Vec<StructField>>,
    ) -> Result<Option<DeltaTable>, SinkError> {
        ensure_aws_handlers();
        let opts = self.storage_options.clone();

        match open_table_with_storage_options(self.table_uri.clone(), opts.clone()).await {
            Ok(t) => {
                info!(
                    uri = %self.table_uri,
                    version = ?t.version(),
                    "opened existing Delta table on S3"
                );
                Ok(Some(t))
            }
            Err(DeltaTableError::NotATable(_)) => {
                let Some(columns) = schema_hint else {
                    debug!(
                        uri = %self.table_uri,
                        "S3 Delta table not found yet; deferring create until first commit"
                    );
                    return Ok(None);
                };
                info!(
                    uri = %self.table_uri,
                    columns = columns.len(),
                    "S3 Delta table not found; initializing _delta_log"
                );
                let table = DeltaOps::try_from_uri_with_storage_options(&self.table_uri, opts)
                    .await
                    .map_err(|e| {
                        SinkError::Fatal(format!(
                            "Init DeltaOps on S3 failed (check AWS_* / endpoint): {e}"
                        ))
                    })?
                    .create()
                    .with_save_mode(SaveMode::Ignore)
                    .with_actions([Action::Protocol(Protocol::new(1, 1))])
                    .with_columns(columns)
                    .await
                    .map_err(|e| {
                        SinkError::Fatal(format!("Create Delta table on S3 failed: {e}"))
                    })?;
                Ok(Some(table))
            }
            Err(e) => Err(SinkError::Transient(format!(
                "Failed to open S3 Delta table at {}: {e}",
                self.table_uri
            ))),
        }
    }

    async fn open_or_create(
        &self,
        schema_hint: Option<Vec<StructField>>,
    ) -> Result<Option<DeltaTable>, SinkError> {
        if is_unsupported_object_uri(&self.table_uri) {
            return Err(SinkError::Fatal(format!(
                "unsupported Delta URI scheme in {} (supported: local path, file://, s3://, s3a://)",
                self.table_uri
            )));
        }
        if is_s3_uri(&self.table_uri) {
            self.open_or_create_s3(schema_hint).await
        } else {
            self.open_or_create_local(schema_hint).await
        }
    }

    async fn ensure_delta_link(
        &mut self,
        schema_hint: Option<Vec<StructField>>,
    ) -> Result<(), SinkError> {
        if self.delta_table.is_some() {
            return Ok(());
        }
        info!(
            uri = %self.table_uri,
            remote = self.remote,
            "DeltaSink: establishing Delta Table link"
        );
        self.delta_table = self.open_or_create(schema_hint).await?;
        Ok(())
    }

    fn relative_add_path(&self, file_path: &Path) -> Result<String, SinkError> {
        if self.remote {
            Ok(file_path
                .file_name()
                .ok_or_else(|| SinkError::Fatal("Invalid staged file name".into()))?
                .to_string_lossy()
                .to_string())
        } else {
            let root = Path::new(&self.table_uri);
            Ok(file_path
                .strip_prefix(root)
                .map_err(|_| {
                    SinkError::Fatal(format!(
                        "Staged file {} is outside Delta root {}",
                        file_path.display(),
                        root.display()
                    ))
                })?
                .to_string_lossy()
                .replace('\\', "/"))
        }
    }

    /// Object URI: PUT locally staged Parquet into the table store before Add commit.
    async fn publish_data_files(
        &self,
        table: &DeltaTable,
        committed_files: &[(PathBuf, u64)],
    ) -> Result<(), SinkError> {
        if !self.remote {
            return Ok(());
        }

        let store = table.object_store();
        for (file_path, _) in committed_files {
            let relative = self.relative_add_path(file_path)?;
            let bytes = fs::read(file_path).await.map_err(|e| {
                SinkError::Transient(format!(
                    "Failed to read staged file {} for upload: {e}",
                    file_path.display()
                ))
            })?;
            let key = ObjectPath::from(relative.as_str());
            store
                .put(&key, PutPayload::from(bytes))
                .await
                .map_err(|e| {
                    SinkError::Transient(format!(
                        "Failed to upload {} to {}: {e}",
                        file_path.display(),
                        self.table_uri
                    ))
                })?;
            debug!(
                key = %key,
                uri = %self.table_uri,
                "uploaded staged Parquet to object storage"
            );
        }
        Ok(())
    }

    async fn build_add_actions(
        &self,
        committed_files: &[(PathBuf, u64)],
    ) -> Result<Vec<Action>, SinkError> {
        let mut actions = Vec::with_capacity(committed_files.len());

        for (file_path, rows) in committed_files {
            let meta = fs::metadata(file_path).await.map_err(|e| {
                SinkError::Fatal(format!(
                    "Failed to stat committed file {}: {e}",
                    file_path.display()
                ))
            })?;

            let modification_time = meta
                .modified()
                .unwrap_or_else(|_| SystemTime::now())
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            let add = Add {
                path: self.relative_add_path(file_path)?,
                size: meta.len() as i64,
                partition_values: HashMap::new(),
                modification_time,
                data_change: true,
                stats: Some(format!(r#"{{"numRecords":{rows}}}"#)),
                ..Default::default()
            };
            actions.push(Action::Add(add));
        }

        Ok(actions)
    }

    /// Phase 2: append Add actions to `_delta_log` via OCC CommitBuilder with inner retry loop.
    async fn commit_to_delta_log(
        &mut self,
        committed_files: Vec<(PathBuf, u64)>,
    ) -> Result<(), SinkError> {
        if committed_files.is_empty() {
            return Ok(());
        }

        let schema_hint = schema_from_parquet(&committed_files[0].0)?;
        self.ensure_delta_link(Some(schema_hint)).await?;

        {
            let table = self.delta_table.as_ref().ok_or_else(|| {
                SinkError::Fatal("Delta table handle missing after ensure_delta_link".into())
            })?;
            self.publish_data_files(table, &committed_files).await?;
        }

        let actions = self.build_add_actions(&committed_files).await?;

        // Inner OCC retry loop: refresh snapshot and retry on concurrent writer conflicts
        // so staged/uploaded Parquet files are not left permanently uncommitted after a
        // single VersionAlreadyExists / CommitConflict.
        let mut attempt = 0;
        loop {
            let table = self
                .delta_table
                .as_mut()
                .ok_or_else(|| SinkError::Fatal("Delta table handle missing".into()))?;

            if attempt > 0 {
                if let Err(e) = table.update().await {
                    warn!(error = %e, "Failed to refresh table state during OCC retry");
                }
            }

            let snapshot = match table.snapshot() {
                Ok(s) => s.clone(),
                Err(e) => {
                    return Err(SinkError::Fatal(format!("Delta snapshot unavailable: {e}")));
                }
            };
            let log_store = table.log_store().clone();

            let operation = DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            };

            // Disable CommitBuilder's own retry budget so each attempt uses a freshly
            // refreshed snapshot from our outer loop (builder default is 15 retries on a
            // fixed snapshot base).
            let commit_result = CommitBuilder::default()
                .with_max_retries(1)
                .with_actions(actions.clone())
                .build(Some(&snapshot), log_store, operation)
                .await;

            match commit_result {
                Ok(finalized) => {
                    let version = finalized.version();
                    table.state = Some(finalized.snapshot());
                    if let Err(e) = table.update().await {
                        warn!(
                            error = %e,
                            "Failed to refresh cached Delta table state; next txn may reload"
                        );
                    }
                    if self.remote {
                        for (path, _) in &committed_files {
                            let _ = fs::remove_file(path).await;
                        }
                    }
                    debug!(
                        version,
                        files = committed_files.len(),
                        remote = self.remote,
                        "Delta log transaction committed"
                    );
                    return Ok(());
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if (err_str.contains("VersionAlreadyExists")
                        || err_str.contains("Concurrent")
                        || err_str.contains("CommitConflict")
                        || err_str.contains("MaxCommitAttempts"))
                        && attempt < OCC_MAX_RETRIES
                    {
                        attempt += 1;
                        warn!(
                            attempt,
                            max_retries = OCC_MAX_RETRIES,
                            "Delta OCC conflict detected; retrying commit with refreshed snapshot..."
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(50 * attempt as u64))
                            .await;
                        continue;
                    }

                    // Fatal or retries exhausted: clear handle to force reload on next attempt
                    self.delta_table = None;
                    return Err(SinkError::Transient(format!(
                        "Delta log commit failed after retries: {err_str}"
                    )));
                }
            }
        }
    }
}

fn schema_from_parquet(path: &Path) -> Result<Vec<StructField>, SinkError> {
    let file = File::open(path).map_err(|e| {
        SinkError::Fatal(format!(
            "Failed to open parquet for Delta schema {}: {e}",
            path.display()
        ))
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        SinkError::Fatal(format!(
            "Failed to read parquet schema {}: {e}",
            path.display()
        ))
    })?;
    let arrow_schema = builder.schema().clone();
    arrow_schema
        .fields()
        .iter()
        .map(|f| arrow_field_to_delta(f))
        .collect()
}

fn arrow_field_to_delta(field: &arrow::datatypes::Field) -> Result<StructField, SinkError> {
    let dt = arrow_type_to_delta(field.data_type())?;
    Ok(StructField::new(
        field.name().to_string(),
        dt,
        field.is_nullable(),
    ))
}

fn arrow_type_to_delta(dt: &ArrowDataType) -> Result<DeltaDataType, SinkError> {
    Ok(match dt {
        ArrowDataType::Boolean => DeltaDataType::BOOLEAN,
        ArrowDataType::Int8 | ArrowDataType::Int16 | ArrowDataType::Int32 => DeltaDataType::INTEGER,
        ArrowDataType::Int64 => DeltaDataType::LONG,
        ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64 => DeltaDataType::LONG,
        ArrowDataType::Float32 => DeltaDataType::FLOAT,
        ArrowDataType::Float64 => DeltaDataType::DOUBLE,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DeltaDataType::STRING,
        ArrowDataType::Binary | ArrowDataType::LargeBinary => DeltaDataType::BINARY,
        ArrowDataType::Date32 | ArrowDataType::Date64 => DeltaDataType::DATE,
        ArrowDataType::Timestamp(_, _) => DeltaDataType::TIMESTAMP,
        ArrowDataType::Decimal128(p, s) => {
            let precision = u8::try_from(*p).map_err(|_| {
                SinkError::Fatal(format!("decimal precision {p} out of range for Delta"))
            })?;
            let scale = u8::try_from((*s).unsigned_abs()).map_err(|_| {
                SinkError::Fatal(format!("decimal scale {s} out of range for Delta"))
            })?;
            DeltaDataType::decimal(precision, scale)
                .map_err(|e| SinkError::Fatal(format!("invalid decimal for Delta: {e}")))?
        }
        other => {
            return Err(SinkError::Fatal(format!(
                "unsupported Arrow type for Delta schema: {other}"
            )));
        }
    })
}

#[cfg(test)]
mod schema_map_tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn unsigned_integers_widen_to_long() {
        for dt in [
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ] {
            assert_eq!(arrow_type_to_delta(&dt).unwrap(), DeltaDataType::LONG);
        }
    }

    #[test]
    fn detects_uris() {
        assert!(is_s3_uri("s3://bucket/table"));
        assert!(is_s3_uri("s3a://bucket/table"));
        assert!(is_unsupported_object_uri("gs://bucket/table"));
        assert!(!is_object_uri("/tmp/lake"));
        assert!(!is_object_uri("file:///tmp/lake"));
    }

    #[test]
    fn normalizes_file_uri() {
        assert_eq!(normalize_table_uri("file:///tmp/lake"), "/tmp/lake");
        assert_eq!(normalize_table_uri("/tmp/lake"), "/tmp/lake");
    }
}

#[async_trait::async_trait]
impl SinkConnector for DeltaSink {
    async fn begin_txn(&mut self) -> Result<(), SinkError> {
        self.ensure_delta_link(None).await?;

        self.staging
            .begin_txn()
            .await
            .map_err(|e| SinkError::Transient(format!("Staging begin failed: {e}")))?;

        self.in_transaction = true;
        Ok(())
    }

    async fn write(&mut self, event: &DataEvent) -> Result<(), SinkError> {
        if !self.in_transaction {
            return Err(SinkError::Fatal(
                "write invoked outside of active transaction".into(),
            ));
        }

        self.staging
            .write(event, "delta")
            .await
            .map_err(|e| SinkError::Transient(format!("Delta staging write failed: {e}")))?;

        Ok(())
    }

    async fn commit_txn(&mut self) -> Result<(), SinkError> {
        if !self.in_transaction {
            return Err(SinkError::Fatal(
                "commit invoked without active transaction".into(),
            ));
        }

        let committed_files =
            self.staging.commit_txn_with_paths().await.map_err(|e| {
                SinkError::Transient(format!("Staging physical commit failed: {e}"))
            })?;

        self.commit_to_delta_log(committed_files).await?;
        self.in_transaction = false;
        Ok(())
    }

    async fn abort_txn(&mut self) -> Result<(), SinkError> {
        if self.in_transaction {
            if let Err(e) = self.staging.abort_txn().await {
                warn!(error = %e, "DeltaSink: failed to cleanup temp files during abort");
            }
            self.in_transaction = false;
            info!("DeltaSink: transaction aborted, staging cleaned");
        }
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), SinkError> {
        self.ensure_delta_link(None).await
    }

    async fn close(&mut self) -> Result<(), SinkError> {
        self.delta_table = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2])),
                Arc::new(StringArray::from(vec![Some("east"), Some("west")])),
            ],
        )
        .unwrap()
    }

    async fn write_parquet(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        let file = File::create(&path).unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, sample_batch().schema(), None).unwrap();
        writer.write(&sample_batch()).unwrap();
        writer.close().unwrap();
        path
    }

    #[tokio::test]
    async fn commit_creates_delta_log_and_reuses_handle() {
        let dir = tempdir().unwrap();
        let table_path = dir.path().join("lake");
        fs::create_dir_all(&table_path).await.unwrap();
        let src_dir = dir.path().join("src");
        fs::create_dir_all(&src_dir).await.unwrap();
        let src = write_parquet(&src_dir, "part-0.parquet").await;

        let mut sink = DeltaSink::new(table_path.to_string_lossy(), None, None);
        sink.begin_txn().await.unwrap();
        sink.write(&DataEvent::FlushFile {
            file_path: Arc::from(src.to_string_lossy().as_ref()),
            rows: 2,
            lsn: common::LsnRange::single(1),
        })
        .await
        .unwrap();
        sink.commit_txn().await.unwrap();

        assert!(table_path.join("_delta_log").is_dir());
        assert!(table_path.join("part-0.parquet").is_file());
        assert!(sink.delta_table.is_some());
        let v1 = sink.delta_table.as_ref().unwrap().version();

        let src2 = write_parquet(&src_dir, "part-1.parquet").await;
        sink.begin_txn().await.unwrap();
        sink.write(&DataEvent::FlushFile {
            file_path: Arc::from(src2.to_string_lossy().as_ref()),
            rows: 2,
            lsn: common::LsnRange::single(2),
        })
        .await
        .unwrap();
        sink.commit_txn().await.unwrap();
        let v2 = sink.delta_table.as_ref().unwrap().version();
        assert!(v2 > v1);
    }

    #[tokio::test]
    async fn abort_removes_tmp_only() {
        let dir = tempdir().unwrap();
        let table_path = dir.path().join("lake");
        fs::create_dir_all(&table_path).await.unwrap();
        let src = write_parquet(dir.path(), "part-0.parquet").await;

        let mut sink = DeltaSink::new(table_path.to_string_lossy(), None, None);
        sink.begin_txn().await.unwrap();
        sink.write(&DataEvent::FlushFile {
            file_path: Arc::from(src.to_string_lossy().as_ref()),
            rows: 2,
            lsn: common::LsnRange::single(1),
        })
        .await
        .unwrap();
        assert!(table_path.join("part-0.parquet.tmp").is_file());
        sink.abort_txn().await.unwrap();
        assert!(!table_path.join("part-0.parquet.tmp").exists());
        assert!(!table_path.join("part-0.parquet").exists());
        assert!(!table_path.join("_delta_log").exists());
    }

    #[tokio::test]
    async fn file_uri_works_like_local_path() {
        let dir = tempdir().unwrap();
        let table_path = dir.path().join("lake");
        fs::create_dir_all(&table_path).await.unwrap();
        let src = write_parquet(dir.path(), "part-0.parquet").await;

        let uri = format!("file://{}", table_path.display());
        let mut sink = DeltaSink::new(uri, None, None);
        assert!(!sink.remote);
        sink.begin_txn().await.unwrap();
        sink.write(&DataEvent::FlushFile {
            file_path: Arc::from(src.to_string_lossy().as_ref()),
            rows: 2,
            lsn: common::LsnRange::single(1),
        })
        .await
        .unwrap();
        sink.commit_txn().await.unwrap();
        assert!(table_path.join("_delta_log").is_dir());
    }

    #[tokio::test]
    async fn unsupported_gs_uri_is_fatal_on_ping() {
        let mut sink = DeltaSink::new("gs://bucket/table", None, None);
        let err = sink.ping().await.unwrap_err();
        assert!(err.is_fatal());
        assert!(err.to_string().contains("unsupported"));
    }
}
