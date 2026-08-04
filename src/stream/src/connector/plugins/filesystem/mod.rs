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
//! Supports local paths, `file://`, `s3://`, and `s3a://` (MinIO via endpoint).
//!
//! Guarantees:
//! - **Local Exactly-Once (2PC)**: Writes to `.tmp`, atomic rename on commit.
//! - **Object store**: Stage locally, then stream-upload finalized Parquet keys.
//! - **Pre-flight Checks**: Validates dir / S3 connectivity via `ping`.
//! - **Crash Recovery**: Sweeps orphaned local `.tmp` files on boot.
//! - **Txn Cleanup**: Unlinks local `.tmp` (and remote staging) on abort.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use tokio::fs;
use tracing::{debug, info, warn};

use crate::connector::{SinkConnector, SinkError};
use crate::model::event::DataEvent;
use crate::model::DeltaSinkOptions;

use super::object_uri::{
    build_s3_store, is_object_uri, is_s3_uri, is_unsupported_object_uri, normalize_uri,
    object_key_for_staged, staging_root_for, upload_file_chunked,
};
use super::parquet_dir::ParquetDirStaging;

const MAX_UPLOAD_CONCURRENCY: usize = 16;

pub struct FilesystemSink {
    uri: String,
    remote: bool,
    staging_root: PathBuf,
    staging: ParquetDirStaging,
    endpoint: Option<String>,
    options: DeltaSinkOptions,
    static_credentials: bool,
    store: Option<Arc<dyn ObjectStore>>,
    object_root: ObjectPath,
    upload_concurrency: usize,
    in_transaction: bool,
    bootstrapped: bool,
}

impl FilesystemSink {
    /// `path_or_uri`: local path, `file://…`, or `s3://bucket/prefix`.
    pub fn new(
        path_or_uri: impl Into<String>,
        table: Option<String>,
        endpoint: Option<String>,
        options: DeltaSinkOptions,
    ) -> Self {
        let uri = normalize_uri(&path_or_uri.into());
        let remote = is_object_uri(&uri);
        let staging_root = staging_root_for(&uri, "fs");
        let static_credentials = options.access_key.is_some() || options.secret_key.is_some();
        let upload_concurrency =
            (options.connection_maximum as usize).clamp(1, MAX_UPLOAD_CONCURRENCY);

        Self {
            staging: ParquetDirStaging::new(staging_root.clone(), table),
            staging_root,
            uri,
            remote,
            endpoint,
            options,
            static_credentials,
            store: None,
            object_root: ObjectPath::default(),
            upload_concurrency,
            in_transaction: false,
            bootstrapped: false,
        }
    }

    fn refresh_credentials(&mut self) {
        if self.static_credentials {
            warn!("static DDL credentials cannot be rotated; clearing object store handle only");
        } else {
            info!("rebuilding filesystem S3 client for default AWS credential chain");
        }
        self.store = None;
    }

    fn maybe_refresh_on_error(&mut self, err: &SinkError) {
        let msg = err.to_string().to_ascii_lowercase();
        if msg.contains("403")
            || msg.contains("401")
            || msg.contains("forbidden")
            || msg.contains("accessdenied")
            || msg.contains("expired")
            || msg.contains("invalidtoken")
            || msg.contains("auth")
        {
            self.refresh_credentials();
        }
    }

    async fn ensure_store(&mut self) -> Result<(), SinkError> {
        if !self.remote {
            return Ok(());
        }
        if is_unsupported_object_uri(&self.uri) {
            return Err(SinkError::Fatal(format!(
                "unsupported filesystem URI scheme in {} (supported: local path, file://, s3://, s3a://)",
                self.uri
            )));
        }
        if !is_s3_uri(&self.uri) {
            return Err(SinkError::Fatal(format!(
                "unsupported filesystem URI: {} (supported: local path, file://, s3://, s3a://)",
                self.uri
            )));
        }
        if self.store.is_some() {
            return Ok(());
        }
        let (store, root) = build_s3_store(&self.uri, self.endpoint.as_deref(), &self.options)?;
        self.store = Some(store);
        self.object_root = root;
        Ok(())
    }

    /// Ensure local staging dir exists; for remote, also verify the S3 client builds.
    async fn ensure_ready(&mut self) -> Result<(), SinkError> {
        if self.remote {
            self.ensure_store().await?;
            fs::create_dir_all(&self.staging_root).await.map_err(|e| {
                SinkError::Fatal(format!(
                    "failed to create filesystem staging dir {}: {e}",
                    self.staging_root.display()
                ))
            })?;
        } else {
            match fs::metadata(&self.staging_root).await {
                Ok(meta) => {
                    if !meta.is_dir() {
                        return Err(SinkError::Fatal(format!(
                            "Not a directory: {}",
                            self.staging_root.display()
                        )));
                    }
                    if meta.permissions().readonly() {
                        return Err(SinkError::Fatal(format!(
                            "Read-only directory: {}",
                            self.staging_root.display()
                        )));
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    fs::create_dir_all(&self.staging_root)
                        .await
                        .map_err(|err| SinkError::Fatal(format!("Failed to create dir: {err}")))?;
                }
                Err(e) => {
                    return Err(SinkError::Transient(format!("Dir check failed: {e}")));
                }
            }
        }

        if !self.bootstrapped {
            self.cleanup_orphans().await;
            self.bootstrapped = true;
        }

        Ok(())
    }

    async fn cleanup_orphans(&self) {
        let count = remove_tmp_tree(&self.staging_root).await;
        if count > 0 {
            info!(
                count,
                dir = %self.staging_root.display(),
                "Boot sweep: cleaned up orphaned .tmp files from previous crashes"
            );
        }
    }

    async fn publish_remote(&self, committed_files: &[(PathBuf, u64)]) -> Result<(), SinkError> {
        let store = self.store.as_ref().ok_or_else(|| {
            SinkError::Fatal("filesystem object store missing after ensure_store".into())
        })?;

        let jobs: Result<Vec<_>, SinkError> = committed_files
            .iter()
            .map(|(path, _)| {
                let key = object_key_for_staged(&self.object_root, &self.staging_root, path)?;
                Ok((path.clone(), key))
            })
            .collect();
        let jobs = jobs?;

        let uri = self.uri.clone();
        let concurrency = self.upload_concurrency;
        let results: Vec<Result<(), SinkError>> = stream::iter(jobs)
            .map(|(file_path, key)| {
                let store = store.clone();
                let uri = uri.clone();
                async move {
                    upload_file_chunked(store, key.clone(), &file_path, &uri).await?;
                    debug!(key = %key, path = %file_path.display(), "filesystem Parquet upload complete");
                    let _ = fs::remove_file(&file_path).await;
                    Ok(())
                }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;

        for r in results {
            r?;
        }
        Ok(())
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
        self.ensure_ready().await?;

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

        let committed = self
            .staging
            .commit_txn_with_paths()
            .await
            .map_err(|e| SinkError::Transient(format!("FS staging commit failed: {e}")))?;

        if self.remote {
            if let Err(e) = self.publish_remote(&committed).await {
                self.maybe_refresh_on_error(&e);
                // Best-effort: leave local committed files for retry / ops inspection.
                self.in_transaction = false;
                return Err(e);
            }
        }

        self.in_transaction = false;
        debug!(remote = self.remote, "Filesystem txn committed");
        Ok(())
    }

    async fn abort_txn(&mut self) -> Result<(), SinkError> {
        if self.in_transaction {
            if let Err(e) = self.staging.abort_txn().await {
                warn!(error = %e, "Failed to cleanup .tmp files during abort");
            }
            self.in_transaction = false;
            info!("Filesystem txn aborted, temporary files unlinked");
        }
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), SinkError> {
        self.ensure_ready().await
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

        let mut sink = FilesystemSink::new(
            root.to_string_lossy(),
            Some("metrics".into()),
            None,
            DeltaSinkOptions::default(),
        );
        sink.ping().await.unwrap();

        assert!(!orphan.exists());
        assert!(live.exists());
        assert!(sink.bootstrapped);
    }

    #[tokio::test]
    async fn write_without_begin_is_fatal() {
        let dir = tempdir().unwrap();
        let mut sink = FilesystemSink::new(
            dir.path().to_string_lossy(),
            None,
            None,
            DeltaSinkOptions::default(),
        );
        let err = sink
            .write(&DataEvent::Watermark { end_lsn: 1 })
            .await
            .unwrap_err();
        assert!(err.is_fatal());
    }

    #[tokio::test]
    async fn rejects_unsupported_uri_scheme() {
        let mut sink =
            FilesystemSink::new("gs://bucket/path", None, None, DeltaSinkOptions::default());
        let err = sink.ping().await.unwrap_err();
        assert!(err.to_string().contains("unsupported"), "{err}");
    }

    #[test]
    fn normalizes_file_uri_to_local() {
        let sink = FilesystemSink::new(
            "file:///tmp/monots-fs-test",
            None,
            None,
            DeltaSinkOptions::default(),
        );
        assert!(!sink.remote);
        assert_eq!(sink.uri, "/tmp/monots-fs-test");
    }
}
