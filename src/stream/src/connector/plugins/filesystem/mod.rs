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

//! Filesystem connector plugin (`sink.type = filesystem`).
//!
//! Guarantees:
//! - **Exactly-Once (2PC)**: Writes to `.tmp`, atomic rename on commit.
//! - **Pre-flight Checks**: Validates dir existence & permissions via `ping`.
//! - **Crash Recovery**: Sweeps orphaned `.tmp` files on boot.
//! - **Txn Cleanup**: Unlinks `.tmp` files on abort.

use std::path::{Path, PathBuf};

use tokio::fs;
use tracing::{debug, info, warn};

use crate::connector::{SinkConnector, SinkError};
use crate::model::event::DataEvent;

use super::parquet_dir::ParquetDirStaging;

pub struct FilesystemSink {
    path: PathBuf,
    staging: ParquetDirStaging,
    in_transaction: bool,
    bootstrapped: bool,
}

impl FilesystemSink {
    pub fn new(path: PathBuf, table: Option<String>) -> Self {
        Self {
            staging: ParquetDirStaging::new(path.clone(), table),
            path,
            in_transaction: false,
            bootstrapped: false,
        }
    }

    /// Ensure dir exists, is writable, and perform one-time boot cleanup.
    ///
    /// Lazy self-check: orphan sweep runs on the first `begin_txn` *or* `ping`,
    /// so a long-lived heartbeat can clear crash leftovers before the next txn.
    async fn ensure_directory(&mut self) -> Result<(), SinkError> {
        match fs::metadata(&self.path).await {
            Ok(meta) => {
                if !meta.is_dir() {
                    return Err(SinkError::Fatal(format!(
                        "Not a directory: {}",
                        self.path.display()
                    )));
                }
                if meta.permissions().readonly() {
                    return Err(SinkError::Fatal(format!(
                        "Read-only directory: {}",
                        self.path.display()
                    )));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(&self.path)
                    .await
                    .map_err(|err| SinkError::Fatal(format!("Failed to create dir: {err}")))?;
            }
            Err(e) => {
                return Err(SinkError::Transient(format!("Dir check failed: {e}")));
            }
        }

        // One-time cleanup for orphaned .tmp files from previous crashes.
        if !self.bootstrapped {
            self.cleanup_orphans().await;
            self.bootstrapped = true;
        }

        Ok(())
    }

    /// Sweep and remove all `.tmp` files left behind by killed processes.
    ///
    /// Walks the sink root recursively and only deletes paths whose extension
    /// is exactly `tmp` (e.g. `foo.parquet.tmp`), never live `.parquet` data.
    async fn cleanup_orphans(&self) {
        let count = remove_tmp_tree(&self.path).await;
        if count > 0 {
            info!(
                count,
                dir = %self.path.display(),
                "Boot sweep: cleaned up orphaned .tmp files from previous crashes"
            );
        }
    }
}

/// Recursively unlink `*.tmp` under `root`. Returns how many files were removed.
async fn remove_tmp_tree(root: &Path) -> u64 {
    let mut dir = match fs::read_dir(root).await {
        Ok(d) => d,
        Err(_) => return 0,
    };

    let mut count = 0u64;
    while let Ok(Some(entry)) = dir.next_entry().await {
        let path = entry.path();
        let file_type = match entry.file_type().await {
            Ok(t) => t,
            Err(_) => continue,
        };

        if file_type.is_dir() {
            count += Box::pin(remove_tmp_tree(&path)).await;
            continue;
        }

        if !file_type.is_file() {
            continue;
        }

        let is_tmp = path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("tmp"));
        if !is_tmp {
            continue;
        }

        match fs::remove_file(&path).await {
            Ok(()) => count += 1,
            Err(_) => {
                warn!(path = %path.display(), "Failed to remove orphaned .tmp file");
            }
        }
    }
    count
}

#[async_trait::async_trait]
impl SinkConnector for FilesystemSink {
    async fn begin_txn(&mut self) -> Result<(), SinkError> {
        self.ensure_directory().await?;

        self.staging
            .begin_txn()
            .await
            .map_err(|e| SinkError::Transient(format!("Staging begin failed: {e}")))?;

        self.in_transaction = true;
        Ok(())
    }

    async fn write(&mut self, event: &DataEvent) -> Result<(), SinkError> {
        if !self.in_transaction {
            return Err(SinkError::Fatal("write outside of active txn".into()));
        }

        self.staging
            .write(event, "filesystem")
            .await
            .map_err(|e| SinkError::Transient(format!("FS write failed: {e}")))?;

        Ok(())
    }

    async fn commit_txn(&mut self) -> Result<(), SinkError> {
        if !self.in_transaction {
            return Err(SinkError::Fatal("commit without active txn".into()));
        }

        // Staging handles atomic `tokio::fs::rename` from `.tmp` to `.parquet`.
        self.staging
            .commit_txn()
            .await
            .map_err(|e| SinkError::Transient(format!("FS commit failed: {e}")))?;

        self.in_transaction = false;
        debug!("Filesystem txn committed");
        Ok(())
    }

    async fn abort_txn(&mut self) -> Result<(), SinkError> {
        if self.in_transaction {
            // Staging unlinks tracked `.tmp` files for the current txn.
            // Crash recovery (kill -9) is covered by `cleanup_orphans` on next boot.
            if let Err(e) = self.staging.abort_txn().await {
                warn!(error = %e, "Failed to cleanup .tmp files during abort");
            }
            self.in_transaction = false;
            info!("Filesystem txn aborted, temporary files unlinked");
        }
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), SinkError> {
        self.ensure_directory().await
    }

    async fn close(&mut self) -> Result<(), SinkError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn ping_sweeps_orphaned_tmp_under_table_subdir() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let table_dir = root.join("metrics");
        fs::create_dir_all(&table_dir).await.unwrap();
        let orphan = table_dir.join("crash.parquet.tmp");
        fs::write(&orphan, b"orphan").await.unwrap();
        let live = table_dir.join("ok.parquet");
        fs::write(&live, b"data").await.unwrap();

        let mut sink = FilesystemSink::new(root, Some("metrics".into()));
        sink.ping().await.unwrap();

        assert!(!orphan.exists());
        assert!(live.exists());
        assert!(sink.bootstrapped);
    }

    #[tokio::test]
    async fn write_without_begin_is_fatal() {
        let dir = tempdir().unwrap();
        let mut sink = FilesystemSink::new(dir.path().to_path_buf(), None);
        let err = sink
            .write(&DataEvent::Watermark { end_lsn: 1 })
            .await
            .unwrap_err();
        assert!(err.is_fatal());
    }
}
