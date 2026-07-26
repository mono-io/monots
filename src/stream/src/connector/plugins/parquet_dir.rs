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

//! Shared Parquet directory staging used by filesystem / delta plugins.
//!
//! Transactional write path:
//! - `write` copies into `*.parquet.tmp`
//! - `commit_txn` atomically renames to `*.parquet`
//! - `abort_txn` unlinks tracked `.tmp` files

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use tokio::fs;

use crate::connector::api::SinkError;
use crate::model::event::DataEvent;

pub(super) struct ParquetDirStaging {
    dest_dir: PathBuf,
    table: Option<String>,
    in_transaction: bool,
    /// `.tmp` paths (+ optional row counts) written in the current txn.
    pending_tmps: Vec<(PathBuf, u64)>,
}

impl ParquetDirStaging {
    pub fn new(path: PathBuf, table: Option<String>) -> Self {
        Self {
            dest_dir: path,
            table,
            in_transaction: false,
            pending_tmps: Vec::new(),
        }
    }

    fn dest_for(&self, src_path: &Path) -> PathBuf {
        let file_name = src_path.file_name().unwrap_or_default();
        match &self.table {
            Some(table) => self.dest_dir.join(table).join(file_name),
            None => self.dest_dir.join(file_name),
        }
    }

    fn tmp_for(final_path: &Path) -> PathBuf {
        let mut os: OsString = final_path.as_os_str().to_owned();
        os.push(".tmp");
        PathBuf::from(os)
    }

    fn final_from_tmp(tmp_path: &Path) -> Option<PathBuf> {
        let s = tmp_path.to_string_lossy();
        let stripped = s.strip_suffix(".tmp")?;
        Some(PathBuf::from(stripped))
    }

    pub async fn begin_txn(&mut self) -> Result<(), SinkError> {
        if self.in_transaction {
            return Err(SinkError::Fatal(
                "begin_txn while a staging transaction is already open".into(),
            ));
        }
        self.pending_tmps.clear();
        self.in_transaction = true;
        Ok(())
    }

    pub async fn write(&mut self, event: &DataEvent, protocol: &str) -> Result<(), SinkError> {
        if !self.in_transaction {
            return Err(SinkError::Fatal(
                "staging write outside of active txn".into(),
            ));
        }

        match event {
            DataEvent::FlushFile {
                file_path, rows, ..
            } => {
                let src_path = Path::new(file_path.as_ref());
                let dest_path = self.dest_for(src_path);
                let tmp_path = Self::tmp_for(&dest_path);

                if let Some(parent) = tmp_path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| SinkError::Fatal(format!("failed to create dest dir: {e}")))?;
                }

                fs::copy(src_path, &tmp_path).await.map_err(|e| {
                    SinkError::Transient(format!("async file copy to .tmp failed: {e}"))
                })?;

                tracing::debug!(
                    protocol,
                    src = %src_path.display(),
                    tmp = %tmp_path.display(),
                    rows,
                    "staged Parquet file as .tmp"
                );
                self.pending_tmps.push((tmp_path, *rows));
            }
            DataEvent::Insert { .. } => {
                tracing::debug!(protocol, "skipping Insert (log path)");
            }
            DataEvent::Watermark { .. } => {
                return Err(SinkError::Fatal(
                    "Watermark must not reach sink write".into(),
                ));
            }
        }
        Ok(())
    }

    /// Atomically rename staged `.tmp` files and return finalized paths + row counts.
    pub async fn commit_txn_with_paths(&mut self) -> Result<Vec<(PathBuf, u64)>, SinkError> {
        if !self.in_transaction {
            return Err(SinkError::Fatal("staging commit without active txn".into()));
        }

        let mut committed = Vec::with_capacity(self.pending_tmps.len());
        for (tmp_path, rows) in self.pending_tmps.drain(..) {
            let final_path = Self::final_from_tmp(&tmp_path).ok_or_else(|| {
                SinkError::Fatal(format!(
                    "invalid staging tmp path (missing .tmp suffix): {}",
                    tmp_path.display()
                ))
            })?;
            fs::rename(&tmp_path, &final_path).await.map_err(|e| {
                SinkError::Transient(format!(
                    "atomic rename {} → {} failed: {e}",
                    tmp_path.display(),
                    final_path.display()
                ))
            })?;
            tracing::debug!(
                tmp = %tmp_path.display(),
                dest = %final_path.display(),
                rows,
                "committed Parquet file"
            );
            committed.push((final_path, rows));
        }

        self.in_transaction = false;
        Ok(committed)
    }

    pub async fn commit_txn(&mut self) -> Result<(), SinkError> {
        self.commit_txn_with_paths().await.map(|_| ())
    }

    pub async fn abort_txn(&mut self) -> Result<(), SinkError> {
        if !self.in_transaction {
            return Ok(());
        }

        let mut first_err: Option<SinkError> = None;
        for (tmp_path, _) in self.pending_tmps.drain(..) {
            match fs::remove_file(&tmp_path).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    let err = SinkError::Transient(format!(
                        "failed to unlink staging tmp {}: {e}",
                        tmp_path.display()
                    ));
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
            }
        }

        self.in_transaction = false;
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn tiny_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap()
    }

    async fn write_src_parquet(dir: &Path) -> PathBuf {
        let path = dir.join("src-000.parquet");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, tiny_batch().schema(), None).unwrap();
        writer.write(&tiny_batch()).unwrap();
        writer.close().unwrap();
        path
    }

    #[tokio::test]
    async fn commit_renames_tmp_to_parquet() {
        let dir = tempdir().unwrap();
        let src = write_src_parquet(dir.path()).await;
        let dest = dir.path().join("out");
        let mut staging = ParquetDirStaging::new(dest.clone(), None);

        staging.begin_txn().await.unwrap();
        staging
            .write(
                &DataEvent::FlushFile {
                    file_path: Arc::from(src.to_string_lossy().as_ref()),
                    rows: 1,
                    lsn: common::LsnRange::single(1),
                },
                "filesystem",
            )
            .await
            .unwrap();

        let tmp = dest.join("src-000.parquet.tmp");
        assert!(tmp.is_file());
        staging.commit_txn().await.unwrap();
        assert!(!tmp.exists());
        assert!(dest.join("src-000.parquet").is_file());
    }

    #[tokio::test]
    async fn abort_unlinks_tmp() {
        let dir = tempdir().unwrap();
        let src = write_src_parquet(dir.path()).await;
        let dest = dir.path().join("out");
        let mut staging = ParquetDirStaging::new(dest.clone(), None);

        staging.begin_txn().await.unwrap();
        staging
            .write(
                &DataEvent::FlushFile {
                    file_path: Arc::from(src.to_string_lossy().as_ref()),
                    rows: 1,
                    lsn: common::LsnRange::single(1),
                },
                "filesystem",
            )
            .await
            .unwrap();

        let tmp = dest.join("src-000.parquet.tmp");
        assert!(tmp.is_file());
        staging.abort_txn().await.unwrap();
        assert!(!tmp.exists());
        assert!(!dest.join("src-000.parquet").exists());
    }
}
