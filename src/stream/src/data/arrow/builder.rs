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

//! Micro-batch builder: append Arrow / DataEvents → one [`ParquetEvent`].

use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use common::{Result, TsdbError};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tokio::fs::File;

use super::reader::{ArrowStreamEvent, EventStreamReader, WalMaterializer};
use crate::model::event::DataEvent;

/// Physical Parquet file produced by [`ParquetEventBuilder`].
#[derive(Debug, Clone)]
pub struct ParquetEvent {
    pub lsn: u64,
    pub file_path: Arc<str>,
    pub rows: u64,
}

impl ParquetEvent {
    pub fn from_flush_file(lsn: u64, file_path: impl Into<Arc<str>>, rows: u64) -> Self {
        Self {
            lsn,
            file_path: file_path.into(),
            rows,
        }
    }
}

/// Unified stream/batch accumulator that builds a single Parquet file event.
pub struct ParquetEventBuilder {
    file_path: PathBuf,
    writer: Option<AsyncArrowWriter<File>>,
    max_lsn: u64,
    rows_written: u64,
    reader: EventStreamReader,
}

impl ParquetEventBuilder {
    pub fn new(file_path: PathBuf, wal_provider: Option<Arc<dyn WalMaterializer>>) -> Self {
        Self {
            file_path,
            writer: None,
            max_lsn: 0,
            rows_written: 0,
            reader: EventStreamReader::new(wal_provider),
        }
    }

    /// Append a scheduler [`DataEvent`] (Insert / FlushFile). Watermarks are ignored.
    ///
    /// FlushFile is decoded from the Parquet path (not WAL); Insert Deferred uses WAL.
    pub async fn append_data_event(&mut self, event: DataEvent) -> Result<()> {
        if let Some(arrow_stream) = self.reader.to_arrow_stream(event).await? {
            self.append_arrow_stream(arrow_stream).await?;
        }
        Ok(())
    }

    /// Stream Arrow batches into the underlying Parquet writer (backpressured by `await`).
    pub async fn append_arrow_stream(&mut self, arrow_event: ArrowStreamEvent) -> Result<()> {
        self.max_lsn = self.max_lsn.max(arrow_event.lsn);
        for batch in arrow_event.batches {
            self.append_batch(batch).await?;
        }
        Ok(())
    }

    async fn append_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if self.writer.is_none() {
            if let Some(parent) = self.file_path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    TsdbError::Storage(format!("create parquet parent {}: {e}", parent.display()))
                })?;
            }
            let file = File::create(&self.file_path).await.map_err(|e| {
                TsdbError::Storage(format!(
                    "create parquet file failed {}: {e}",
                    self.file_path.display()
                ))
            })?;
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let writer = AsyncArrowWriter::try_new(file, batch.schema(), Some(props))
                .map_err(|e| TsdbError::Storage(format!("init AsyncArrowWriter failed: {e}")))?;
            self.writer = Some(writer);
        }
        let w = self.writer.as_mut().expect("writer initialized above");
        w.write(&batch)
            .await
            .map_err(|e| TsdbError::Storage(format!("write arrow to parquet failed: {e}")))?;
        self.rows_written += batch.num_rows() as u64;
        Ok(())
    }

    /// Seal the writer and build the physical event (or `None` if nothing was written).
    pub async fn build(mut self) -> Result<Option<ParquetEvent>> {
        let Some(w) = self.writer.take() else {
            return Ok(None);
        };
        w.close()
            .await
            .map_err(|e| TsdbError::Storage(format!("close parquet writer failed: {e}")))?;
        Ok(Some(ParquetEvent {
            lsn: self.max_lsn,
            file_path: Arc::from(self.file_path.to_string_lossy().into_owned()),
            rows: self.rows_written,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use common::LsnRange;
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])),
                Arc::new(Int64Array::from(vec![10_i64, 20])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn build_parquet_from_resident_inserts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.parquet");
        let mut builder = ParquetEventBuilder::new(path.clone(), None);
        builder
            .append_data_event(DataEvent::insert(LsnRange::new(1, 1), vec![batch()]))
            .await
            .unwrap();
        builder
            .append_data_event(DataEvent::insert(LsnRange::new(2, 2), vec![batch()]))
            .await
            .unwrap();
        let ev = builder.build().await.unwrap().expect("parquet event");
        assert_eq!(ev.lsn, 2);
        assert_eq!(ev.rows, 4);
        assert!(path.is_file());
    }

    #[tokio::test]
    async fn empty_builder_yields_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.parquet");
        let builder = ParquetEventBuilder::new(path, None);
        assert!(builder.build().await.unwrap().is_none());
    }
}
