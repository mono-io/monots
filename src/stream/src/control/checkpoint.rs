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

//! Source checkpoint persistence (restart / recovery truth).
//!
//! Async I/O, in-memory `can_rewrite` cache (no read-before-write on save),
//! parent-dir fsync after rename, and path sanitization against traversal.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tracing::{debug, warn};

pub use common::{StreamCheckpoint, TableCheckpoint};

use common::{Result, TsdbError};

use crate::control::meta::codec::{
    decode_versioned_checkpoint, encode_versioned_checkpoint, Versioned, STREAM_SCHEMA_VERSION,
};

/// Checkpoint store with schema rewrite cache and crash-safe durable writes.
pub struct CheckpointStore {
    root: PathBuf,
    /// `stream::worker` → whether this binary may overwrite the on-disk file.
    rewrite_cache: DashMap<String, bool>,
}

impl CheckpointStore {
    pub async fn open(root: impl Into<PathBuf>) -> Result<Arc<Self>> {
        let root = root.into();
        fs::create_dir_all(&root).await.map_err(|e| {
            TsdbError::Storage(format!(
                "Failed to create checkpoint root {}: {e}",
                root.display()
            ))
        })?;
        Ok(Arc::new(Self {
            root,
            rewrite_cache: DashMap::new(),
        }))
    }

    /// Sync open for control-plane boot paths that are not yet async.
    pub fn open_blocking(root: impl Into<PathBuf>) -> Result<Arc<Self>> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(|e| {
            TsdbError::Storage(format!(
                "Failed to create checkpoint root {}: {e}",
                root.display()
            ))
        })?;
        Ok(Arc::new(Self {
            root,
            rewrite_cache: DashMap::new(),
        }))
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn resolve_safe_path(&self, stream: &str, worker: &str) -> Result<PathBuf> {
        if stream.is_empty()
            || worker.is_empty()
            || stream.contains(['/', '\\', '\0'])
            || worker.contains(['/', '\\', '\0'])
            || stream.contains("..")
            || worker.contains("..")
        {
            return Err(TsdbError::Storage(
                "Invalid characters in stream or worker ID".into(),
            ));
        }
        Ok(self.root.join(format!("{stream}__{worker}.pb")))
    }

    #[inline]
    fn cache_key(stream: &str, worker: &str) -> String {
        format!("{stream}::{worker}")
    }

    pub async fn load(&self, stream_name: &str, worker_id: &str) -> Result<StreamCheckpoint> {
        let path = self.resolve_safe_path(stream_name, worker_id)?;
        let key = Self::cache_key(stream_name, worker_id);

        if !fs::try_exists(&path).await.unwrap_or(false) {
            self.rewrite_cache.insert(key, true);
            return Ok(StreamCheckpoint::new(stream_name, worker_id));
        }

        let bytes = fs::read(&path).await.map_err(|e| {
            TsdbError::Storage(format!(
                "Failed to read checkpoint at {}: {e}",
                path.display()
            ))
        })?;

        let versioned = decode_versioned_checkpoint(&bytes).map_err(|e| {
            TsdbError::Storage(format!(
                "Failed to decode checkpoint at {}: {e}",
                path.display()
            ))
        })?;

        let can_rewrite = versioned.can_rewrite();
        self.rewrite_cache.insert(key, can_rewrite);

        if !can_rewrite {
            warn!(
                stream = %stream_name,
                consumer = %worker_id,
                disk_schema_version = versioned.disk_schema_version,
                local_schema_version = STREAM_SCHEMA_VERSION,
                "Loaded a checkpoint from a future version; save operations will be strictly rejected"
            );
        }

        debug!(stream = %stream_name, consumer = %worker_id, "Checkpoint successfully loaded");
        Ok(versioned.inner)
    }

    pub async fn save(&self, cp: &StreamCheckpoint) -> Result<()> {
        let path = self.resolve_safe_path(&cp.stream_name, &cp.consumer_id)?;
        let key = Self::cache_key(&cp.stream_name, &cp.consumer_id);

        let can_rewrite = if let Some(v) = self.rewrite_cache.get(&key) {
            *v
        } else {
            let allowed = self.probe_can_rewrite(&path).await?;
            self.rewrite_cache.insert(key.clone(), allowed);
            allowed
        };

        if !can_rewrite {
            return Err(TsdbError::Storage(format!(
                "Refusing to rewrite checkpoint `{}/{}`: on-disk schema_version > local {}",
                cp.stream_name, cp.consumer_id, STREAM_SCHEMA_VERSION
            )));
        }

        let cp_clone = cp.clone();
        let bytes = tokio::task::spawn_blocking(move || {
            encode_versioned_checkpoint(&Versioned::fresh(cp_clone))
        })
        .await
        .map_err(|e| TsdbError::Storage(format!("Task spawn failed: {e}")))??;

        let tmp_path = path.with_extension("pb.tmp");
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)
                .await
                .map_err(|e| {
                    TsdbError::Storage(format!(
                        "Create temp checkpoint {}: {e}",
                        tmp_path.display()
                    ))
                })?;
            file.write_all(&bytes).await.map_err(|e| {
                TsdbError::Storage(format!("Write temp checkpoint {}: {e}", tmp_path.display()))
            })?;
            file.sync_all().await.map_err(|e| {
                TsdbError::Storage(format!("Sync temp checkpoint {}: {e}", tmp_path.display()))
            })?;
        }

        fs::rename(&tmp_path, &path).await.map_err(|e| {
            TsdbError::Storage(format!(
                "Atomic rename failed {} to {}: {e}",
                tmp_path.display(),
                path.display()
            ))
        })?;

        self.sync_directory(&self.root).await?;
        self.rewrite_cache.insert(key, true);
        Ok(())
    }

    pub async fn delete(&self, stream_name: &str, worker_id: &str) -> Result<()> {
        let path = self.resolve_safe_path(stream_name, worker_id)?;
        let key = Self::cache_key(stream_name, worker_id);

        match fs::remove_file(&path).await {
            Ok(()) => {
                self.rewrite_cache.remove(&key);
                self.sync_directory(&self.root).await?;
                debug!(stream = %stream_name, "Checkpoint cleanly removed");
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(TsdbError::Storage(format!(
                    "Failed to remove checkpoint {}: {e}",
                    path.display()
                )));
            }
        }
        Ok(())
    }

    async fn probe_can_rewrite(&self, path: &Path) -> Result<bool> {
        if !fs::try_exists(path).await.unwrap_or(false) {
            return Ok(true);
        }
        let bytes = fs::read(path).await.map_err(|e| {
            TsdbError::Storage(format!(
                "Failed to read checkpoint at {}: {e}",
                path.display()
            ))
        })?;
        Ok(decode_versioned_checkpoint(&bytes)
            .map(|v| v.can_rewrite())
            .unwrap_or(true))
    }

    async fn sync_directory(&self, dir_path: &Path) -> Result<()> {
        if let Ok(dir) = File::open(dir_path).await {
            let _ = dir.sync_all().await;
        }
        Ok(())
    }

    pub fn load_blocking_arc(
        self: &Arc<Self>,
        stream_name: &str,
        worker_id: &str,
    ) -> Result<StreamCheckpoint> {
        let store = Arc::clone(self);
        let stream = stream_name.to_string();
        let worker = worker_id.to_string();
        block_on_stream(async move { store.load(&stream, &worker).await })
    }

    pub fn save_blocking_arc(self: &Arc<Self>, cp: &StreamCheckpoint) -> Result<()> {
        let store = Arc::clone(self);
        let cp = cp.clone();
        block_on_stream(async move { store.save(&cp).await })
    }

    pub fn delete_blocking_arc(self: &Arc<Self>, stream_name: &str, worker_id: &str) -> Result<()> {
        let store = Arc::clone(self);
        let stream = stream_name.to_string();
        let worker = worker_id.to_string();
        block_on_stream(async move { store.delete(&stream, &worker).await })
    }
}

fn block_on_stream<T>(
    fut: impl std::future::Future<Output = Result<T>> + Send + 'static,
) -> Result<T>
where
    T: Send + 'static,
{
    std::thread::spawn(move || crate::control::executor::handle().block_on(fut))
        .join()
        .map_err(|_| TsdbError::Storage("checkpoint worker thread panicked".into()))?
}

/// Compatibility path helper (sanitized).
pub fn resolve_checkpoint_path(root: &Path, stream_name: &str, worker_id: &str) -> Result<PathBuf> {
    CheckpointStore {
        root: root.to_path_buf(),
        rewrite_cache: DashMap::new(),
    }
    .resolve_safe_path(stream_name, worker_id)
}

pub fn checkpoint_path(root: &Path, stream_name: &str, worker_id: &str) -> Result<PathBuf> {
    resolve_checkpoint_path(root, stream_name, worker_id)
}

/// Sync wrappers for legacy callers (prefer [`CheckpointStore`] via [`StreamStore`]).
pub fn load_checkpoint(
    root: &Path,
    stream_name: &str,
    worker_id: &str,
) -> Result<StreamCheckpoint> {
    let store = CheckpointStore::open_blocking(root)?;
    store.load_blocking_arc(stream_name, worker_id)
}

pub fn save_checkpoint(root: &Path, cp: &StreamCheckpoint) -> Result<()> {
    let store = CheckpointStore::open_blocking(root)?;
    // Warm cache from disk once so rewrite checks match prior behavior.
    let _ = store.load_blocking_arc(&cp.stream_name, &cp.consumer_id);
    store.save_blocking_arc(cp)
}

pub fn delete_checkpoint(root: &Path, stream_name: &str, worker_id: &str) -> Result<()> {
    let store = CheckpointStore::open_blocking(root)?;
    store.delete_blocking_arc(stream_name, worker_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn save_load_roundtrip_without_reread_penalty() {
        let tmp = tempfile::tempdir().unwrap();
        let store = CheckpointStore::open(tmp.path().join("cp")).await.unwrap();
        let cp = StreamCheckpoint::new("s1", "w1");
        store.save(&cp).await.unwrap();
        store.save(&cp).await.unwrap();
        let loaded = store.load("s1", "w1").await.unwrap();
        assert_eq!(loaded.stream_name, "s1");
        assert_eq!(loaded.consumer_id, "w1");
    }

    #[tokio::test]
    async fn rejects_path_traversal() {
        let tmp = tempfile::tempdir().unwrap();
        let store = CheckpointStore::open(tmp.path().join("cp")).await.unwrap();
        assert!(store.load("../evil", "w").await.is_err());
        assert!(store.load("s", "a/b").await.is_err());
    }
}
