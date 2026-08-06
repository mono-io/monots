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

//! Iceberg connector plugin (`sink.type = iceberg`).
//!
//! Catalog types (Flink-compatible DDL):
//! - `hadoop` — warehouse + version-hint (full append commits)
//! - `rest` — Iceberg REST Catalog (full append commits)
//! - `hive` / `glue` — catalog connect + create/load; commits require upstream
//!   `update_table` (not available in iceberg-catalog-hms/glue 0.4)

mod catalog;
mod hadoop;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::{Field, Schema as ArrowSchema};
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::spec::{DataFileFormat, Schema as IcebergSchema};
use iceberg::table::Table;
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::connector::api::{SinkConnector, SinkError};
use crate::connector::plugins::parquet_dir::ParquetDirStaging;
use crate::model::event::DataEvent;
use crate::model::IcebergSinkOptions;

use self::catalog::{build_catalog, table_ident, IcebergCatalogHandle};

pub struct IcebergSink {
    options: IcebergSinkOptions,
    catalog: Option<IcebergCatalogHandle>,
    table: Option<Table>,
    /// Owns the staging directory; must outlive `staging`.
    _staging_root: TempDir,
    staging: ParquetDirStaging,
    in_transaction: bool,
}

impl IcebergSink {
    pub fn new(options: IcebergSinkOptions) -> Result<Self, SinkError> {
        let staging_root = TempDir::new()
            .map_err(|e| SinkError::Fatal(format!("iceberg staging tempdir: {e}")))?;
        let staging = ParquetDirStaging::new(staging_root.path().to_path_buf(), None);
        Ok(Self {
            options,
            catalog: None,
            table: None,
            _staging_root: staging_root,
            staging,
            in_transaction: false,
        })
    }

    async fn ensure_catalog(&mut self) -> Result<&IcebergCatalogHandle, SinkError> {
        if self.catalog.is_none() {
            let handle = build_catalog(&self.options).await?;
            self.catalog = Some(handle);
        }
        Ok(self.catalog.as_ref().expect("just set"))
    }

    async fn ensure_table(&mut self, schema_hint: Option<&IcebergSchema>) -> Result<(), SinkError> {
        if self.table.is_some() {
            return Ok(());
        }
        let ident = table_ident(&self.options)?;
        let create_if_missing = self.options.create_table_if_not_exists;

        {
            let catalog = self.ensure_catalog().await?;
            if !catalog.supports_commits() {
                return Err(SinkError::Fatal(format!(
                    "iceberg catalog-type={} does not support table commits yet \
                     (iceberg-catalog-hms/glue 0.4 lack update_table); use hadoop or rest",
                    self.options.catalog_type.as_str()
                )));
            }
        }

        let catalog = self.ensure_catalog().await?;
        let cat = catalog.as_catalog();

        if cat.table_exists(&ident).await.map_err(map_iceberg)? {
            let table = cat.load_table(&ident).await.map_err(map_iceberg)?;
            self.table = Some(table);
            return Ok(());
        }

        if !create_if_missing {
            return Err(SinkError::Fatal(format!(
                "Iceberg table {} does not exist and create-table-if-not-exists=false",
                self.options.table_ident_display()
            )));
        }

        let schema = schema_hint.ok_or_else(|| {
            SinkError::Fatal(
                "cannot create Iceberg table before first Parquet schema is known".into(),
            )
        })?;

        ensure_namespace(cat, ident.namespace()).await?;

        let creation = TableCreation::builder()
            .name(ident.name().to_string())
            .schema(schema.clone())
            .build();
        let table = cat
            .create_table(ident.namespace(), creation)
            .await
            .map_err(map_iceberg)?;
        info!(
            table = %self.options.table_ident_display(),
            catalog = %self.options.catalog_name,
            "created Iceberg table"
        );
        self.table = Some(table);
        Ok(())
    }

    async fn append_parquet_files(&mut self, files: Vec<(PathBuf, u64)>) -> Result<(), SinkError> {
        if files.is_empty() {
            return Ok(());
        }

        let schema = schema_from_parquet(&files[0].0)?;
        self.ensure_table(Some(&schema)).await?;

        let catalog = self
            .catalog
            .as_ref()
            .ok_or_else(|| SinkError::Fatal("Iceberg catalog missing after ensure_table".into()))?;
        if !catalog.supports_commits() {
            return Err(SinkError::Fatal(
                "Iceberg catalog does not support commits".into(),
            ));
        }

        let (iceberg_schema, file_io, table_metadata, ident) = {
            let table = self
                .table
                .as_ref()
                .ok_or_else(|| SinkError::Fatal("Iceberg table missing".into()))?;
            (
                table.metadata().current_schema().clone(),
                table.file_io().clone(),
                table.metadata().clone(),
                table.identifier().clone(),
            )
        };

        let location_gen = DefaultLocationGenerator::new(table_metadata).map_err(map_iceberg)?;
        // Unique prefix per commit — DefaultFileNameGenerator resets to 00000 each build.
        let file_name_gen = DefaultFileNameGenerator::new(
            format!("monots-{}", Uuid::new_v4()),
            None,
            DataFileFormat::Parquet,
        );

        let mut all_data_files = Vec::new();
        for (path, _) in &files {
            let batches = read_parquet_batches(path, iceberg_schema.as_ref())?;
            if batches.is_empty() {
                continue;
            }

            let pw = ParquetWriterBuilder::new(
                WriterProperties::builder().build(),
                iceberg_schema.clone(),
                file_io.clone(),
                location_gen.clone(),
                file_name_gen.clone(),
            );
            let mut writer = DataFileWriterBuilder::new(pw, None)
                .build()
                .await
                .map_err(map_iceberg)?;

            for batch in batches {
                writer.write(batch).await.map_err(map_iceberg)?;
            }
            let data_files = writer.close().await.map_err(map_iceberg)?;
            all_data_files.extend(data_files);
        }

        if all_data_files.is_empty() {
            return Ok(());
        }

        // Reload table for latest snapshot before commit.
        let fresh = match catalog {
            IcebergCatalogHandle::Hadoop(c) => c.load_table(&ident).await,
            IcebergCatalogHandle::Rest(c) => c.load_table(&ident).await,
            IcebergCatalogHandle::Glue(c) => c.load_table(&ident).await,
        }
        .map_err(map_iceberg)?;
        let mut action = Transaction::new(&fresh)
            .fast_append(None, vec![])
            .map_err(map_iceberg)?;
        action.add_data_files(all_data_files).map_err(map_iceberg)?;
        let tx = action.apply().await.map_err(map_iceberg)?;
        let updated = match catalog {
            IcebergCatalogHandle::Hadoop(c) => tx.commit(c.as_ref()).await,
            IcebergCatalogHandle::Rest(c) => tx.commit(c.as_ref()).await,
            IcebergCatalogHandle::Glue(c) => tx.commit(c.as_ref()).await,
        }
        .map_err(map_iceberg)?;
        self.table = Some(updated);
        debug!(
            table = %self.options.table_ident_display(),
            files = files.len(),
            "Iceberg fast_append committed"
        );
        Ok(())
    }
}

async fn ensure_namespace(catalog: &dyn Catalog, ns: &NamespaceIdent) -> Result<(), SinkError> {
    if catalog.namespace_exists(ns).await.map_err(map_iceberg)? {
        return Ok(());
    }
    let _ = catalog
        .create_namespace(ns, Default::default())
        .await
        .map_err(map_iceberg)?;
    Ok(())
}

fn map_iceberg(err: iceberg::Error) -> SinkError {
    let msg = err.to_string();
    let lower = msg.to_lowercase();
    if lower.contains("timeout")
        || lower.contains("connection")
        || lower.contains("unavailable")
        || lower.contains("temporarily")
    {
        SinkError::Transient(msg)
    } else {
        SinkError::Fatal(msg)
    }
}

fn schema_from_parquet(path: &Path) -> Result<IcebergSchema, SinkError> {
    let file = std::fs::File::open(path)
        .map_err(|e| SinkError::Fatal(format!("open parquet {}: {e}", path.display())))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| SinkError::Fatal(format!("parquet reader: {e}")))?;
    let arrow_schema = builder.schema().clone();
    let with_ids = assign_field_ids(arrow_schema.as_ref());
    arrow_schema_to_schema(&with_ids).map_err(map_iceberg)
}

fn assign_field_ids(schema: &ArrowSchema) -> ArrowSchema {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let mut field = Field::new(f.name(), f.data_type().clone(), f.is_nullable());
            let mut meta = f.metadata().clone();
            meta.insert(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                (i as i32 + 1).to_string(),
            );
            field.set_metadata(meta);
            field
        })
        .collect();
    ArrowSchema::new_with_metadata(fields, schema.metadata().clone())
}

fn read_parquet_batches(
    path: &Path,
    iceberg_schema: &IcebergSchema,
) -> Result<Vec<arrow::array::RecordBatch>, SinkError> {
    let file = std::fs::File::open(path)
        .map_err(|e| SinkError::Fatal(format!("open parquet {}: {e}", path.display())))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| SinkError::Fatal(format!("parquet reader: {e}")))?;
    let reader = builder
        .build()
        .map_err(|e| SinkError::Fatal(format!("parquet build: {e}")))?;

    // Target arrow schema with field ids from Iceberg schema.
    let target = iceberg::arrow::schema_to_arrow_schema(iceberg_schema).map_err(map_iceberg)?;
    let target = Arc::new(target);

    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| SinkError::Fatal(format!("parquet batch: {e}")))?;
        let batch = align_batch_schema(batch, target.clone())?;
        out.push(batch);
    }
    Ok(out)
}

fn align_batch_schema(
    batch: arrow::array::RecordBatch,
    target: Arc<ArrowSchema>,
) -> Result<arrow::array::RecordBatch, SinkError> {
    if batch.schema().fields().len() != target.fields().len() {
        return Err(SinkError::Fatal(format!(
            "parquet column count {} != iceberg schema {}",
            batch.num_columns(),
            target.fields().len()
        )));
    }
    arrow::array::RecordBatch::try_new(target, batch.columns().to_vec())
        .map_err(|e| SinkError::Fatal(format!("align record batch schema: {e}")))
}

#[async_trait::async_trait]
impl SinkConnector for IcebergSink {
    async fn begin_txn(&mut self) -> Result<(), SinkError> {
        let _ = self.ensure_catalog().await?;
        self.staging
            .begin_txn()
            .await
            .map_err(|e| SinkError::Transient(format!("Iceberg staging begin failed: {e}")))?;
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
            .write(event, "iceberg")
            .await
            .map_err(|e| SinkError::Transient(format!("Iceberg staging write failed: {e}")))?;
        Ok(())
    }

    async fn commit_txn(&mut self) -> Result<(), SinkError> {
        if !self.in_transaction {
            return Err(SinkError::Fatal(
                "commit invoked without active transaction".into(),
            ));
        }
        let committed = self
            .staging
            .commit_txn_with_paths()
            .await
            .map_err(|e| SinkError::Transient(format!("Iceberg staging commit failed: {e}")))?;

        if let Err(e) = self.append_parquet_files(committed).await {
            self.in_transaction = false;
            return Err(e);
        }
        self.in_transaction = false;
        Ok(())
    }

    async fn abort_txn(&mut self) -> Result<(), SinkError> {
        if self.in_transaction {
            if let Err(e) = self.staging.abort_txn().await {
                warn!(error = %e, "IcebergSink: failed to cleanup temp files during abort");
            }
            self.in_transaction = false;
            info!("IcebergSink: transaction aborted");
        }
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), SinkError> {
        let _ = self.ensure_catalog().await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), SinkError> {
        self.table = None;
        self.catalog = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::IcebergCatalogType;
    use std::collections::HashMap;

    fn hadoop_opts(warehouse: &str) -> IcebergSinkOptions {
        let mut m = HashMap::new();
        m.insert("sink.iceberg.catalog-type".into(), "hadoop".into());
        m.insert("sink.iceberg.catalog-name".into(), "test_cat".into());
        m.insert("sink.iceberg.warehouse".into(), warehouse.into());
        m.insert("sink.iceberg.namespace".into(), "db".into());
        m.insert("sink.iceberg.table".into(), "t".into());
        IcebergSinkOptions::from_ddl(&m).unwrap()
    }

    #[tokio::test]
    async fn hadoop_catalog_builds() {
        let dir = tempfile::tempdir().unwrap();
        let opts = hadoop_opts(dir.path().to_str().unwrap());
        assert_eq!(opts.catalog_type, IcebergCatalogType::Hadoop);
        let mut sink = IcebergSink::new(opts).unwrap();
        sink.begin_txn().await.unwrap();
        sink.abort_txn().await.unwrap();
    }

    #[tokio::test]
    async fn hadoop_append_creates_metadata() {
        use arrow::array::{Int64Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let warehouse = tempfile::tempdir().unwrap();
        let opts = hadoop_opts(warehouse.path().to_str().unwrap());
        let mut sink = IcebergSink::new(opts).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let src_dir = tempfile::tempdir().unwrap();
        let parquet_path = src_dir.path().join("part-0.parquet");
        {
            let file = std::fs::File::create(&parquet_path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        sink.begin_txn().await.unwrap();
        sink.write(&DataEvent::FlushFile {
            lsn: common::LsnRange::single(1),
            file_path: Arc::from(parquet_path.to_string_lossy().as_ref()),
            rows: 3,
        })
        .await
        .unwrap();
        sink.commit_txn().await.unwrap();

        let hint = warehouse
            .path()
            .join("db")
            .join("t")
            .join("metadata")
            .join("version-hint.text");
        assert!(hint.exists(), "version-hint should exist after commit");
        let meta = warehouse.path().join("db").join("t").join("metadata");
        let metas: Vec<_> = std::fs::read_dir(&meta)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".metadata.json"))
            .collect();
        assert!(!metas.is_empty(), "metadata json should exist");
    }

    #[tokio::test]
    async fn hadoop_minio_write_smoke() {
        // Requires Docker MinIO on 127.0.0.1:19000 — skip if unreachable.
        if tokio::net::TcpStream::connect("127.0.0.1:19000")
            .await
            .is_err()
        {
            eprintln!("skip hadoop_minio_write_smoke: MinIO not up");
            return;
        }
        use arrow::array::{Int64Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let mut m = HashMap::new();
        m.insert("sink.iceberg.catalog-type".into(), "hadoop".into());
        m.insert("sink.iceberg.catalog-name".into(), "minio_cat".into());
        m.insert(
            "sink.iceberg.warehouse".into(),
            "s3://monots/iceberg-unit-wh".into(),
        );
        m.insert("sink.iceberg.namespace".into(), "unit_ns".into());
        m.insert("sink.iceberg.table".into(), "unit_t".into());
        m.insert(
            "sink.iceberg.endpoint".into(),
            "http://127.0.0.1:19000".into(),
        );
        m.insert("sink.iceberg.access.key".into(), "minioadmin".into());
        m.insert("sink.iceberg.secret.key".into(), "minioadmin".into());
        m.insert("sink.iceberg.region".into(), "us-east-1".into());
        m.insert("sink.iceberg.path.style.access".into(), "true".into());
        let opts = IcebergSinkOptions::from_ddl(&m).unwrap();
        let mut sink = IcebergSink::new(opts).unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2])),
                Arc::new(Int64Array::from(vec![10i64, 20])),
            ],
        )
        .unwrap();
        let src_dir = tempfile::tempdir().unwrap();
        let parquet_path = src_dir.path().join("part.parquet");
        {
            let file = std::fs::File::create(&parquet_path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        sink.begin_txn().await.unwrap();
        sink.write(&DataEvent::FlushFile {
            lsn: common::LsnRange::single(1),
            file_path: Arc::from(parquet_path.to_string_lossy().as_ref()),
            rows: 2,
        })
        .await
        .unwrap();
        sink.commit_txn().await.expect("minio iceberg commit");
    }
}
