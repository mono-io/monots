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

//! Persist stream definitions under `{data_dir}/streams/*.pb`.
//!
//! Per-stream async locks isolate writers. DashMap refs are never held across `.await`.
//! Parent-dir fsync after rename/unlink makes metadata durable across power loss.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::{info, warn};

use common::{Result, TsdbError};

use crate::control::checkpoint::{CheckpointStore, StreamCheckpoint};
use crate::control::meta::codec::{
    decode_versioned_stream_def, encode_versioned_stream_def, Versioned, STREAM_SCHEMA_VERSION,
};
use crate::model::StreamDef;

pub struct StreamStore {
    root: PathBuf,
    streams: DashMap<String, Versioned<StreamDef>>,
    /// Per-stream serialize of mutate → disk → map.
    name_locks: DashMap<String, Arc<Mutex<()>>>,
    checkpoints: Arc<CheckpointStore>,
}

impl StreamStore {
    pub async fn open(data_dir: &Path) -> Result<Arc<Self>> {
        let root = data_dir.join("streams");
        fs::create_dir_all(&root)
            .await
            .map_err(|e| TsdbError::Storage(format!("Failed to create streams dir: {e}")))?;

        let checkpoints_root = data_dir.join("sync_checkpoints");
        let checkpoints = CheckpointStore::open(checkpoints_root).await?;

        let store = Arc::new(Self {
            root,
            streams: DashMap::new(),
            name_locks: DashMap::new(),
            checkpoints,
        });
        store.reload().await?;
        Ok(store)
    }

    /// Clone the per-name lock Arc; drop the DashMap entry guard before awaiting.
    fn get_name_lock(&self, name: &str) -> Arc<Mutex<()>> {
        self.name_locks.entry(name.to_string()).or_default().clone()
    }

    async fn reload(&self) -> Result<()> {
        self.streams.clear();
        if !matches!(fs::metadata(&self.root).await, Ok(m) if m.is_dir()) {
            return Ok(());
        }

        let mut entries = fs::read_dir(&self.root)
            .await
            .map_err(|e| TsdbError::Storage(format!("Failed to read streams dir: {e}")))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| TsdbError::Storage(e.to_string()))?
        {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("pb") {
                continue;
            }

            let bytes = fs::read(&path).await.map_err(|e| {
                TsdbError::Storage(format!("Failed to read stream def {}: {e}", path.display()))
            })?;

            let v = decode_versioned_stream_def(&bytes).map_err(|e| {
                TsdbError::Storage(format!(
                    "Protobuf decode failed for {}: {e}",
                    path.display()
                ))
            })?;

            if v.disk_schema_version > STREAM_SCHEMA_VERSION {
                warn!(
                    stream = %v.inner.name,
                    disk_schema_version = v.disk_schema_version,
                    local_schema_version = STREAM_SCHEMA_VERSION,
                    "Loaded stream written by a newer binary; updates will be refused until upgrade"
                );
            }
            self.streams.insert(v.inner.name.clone(), v);
        }

        info!(
            loaded_streams = self.streams.len(),
            "Stream store reloaded successfully"
        );
        Ok(())
    }

    pub fn list(&self) -> Vec<StreamDef> {
        let mut out: Vec<_> = self
            .streams
            .iter()
            .map(|e| e.value().inner.clone())
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    pub fn get(&self, name: &str) -> Option<StreamDef> {
        self.streams.get(name).map(|e| e.value().inner.clone())
    }

    pub async fn put(&self, def: StreamDef) -> Result<()> {
        crate::model::def::ensure_single_source_table(&def)?;

        let name = def.name.clone();
        let lock_arc = self.get_name_lock(&name);
        let _guard = lock_arc.lock().await;

        let v = if let Some(existing) = self.streams.get(&name) {
            if !existing.can_rewrite() {
                return Err(TsdbError::Storage(format!(
                    "refusing to overwrite stream `{name}` written with schema_version {} \
                     (this binary is {}); upgrade first",
                    existing.disk_schema_version, STREAM_SCHEMA_VERSION
                )));
            }
            Versioned {
                inner: def,
                disk_schema_version: existing.disk_schema_version,
                min_reader_version: existing.min_reader_version,
            }
        } else {
            Versioned::fresh(def)
        };

        self.persist_versioned(&v).await?;
        self.streams.insert(name, Versioned::fresh(v.inner));
        Ok(())
    }

    pub async fn update(&self, name: &str, mutator: impl FnOnce(&mut StreamDef)) -> Result<()> {
        let lock_arc = self.get_name_lock(name);
        let _guard = lock_arc.lock().await;

        let mut v = self
            .streams
            .get(name)
            .map(|e| e.value().clone())
            .ok_or_else(|| TsdbError::TableNotFound(format!("stream {name}")))?;

        if !v.can_rewrite() {
            return Err(TsdbError::Storage(format!(
                "refusing to update stream `{name}`: on-disk schema_version {} > {}",
                v.disk_schema_version, STREAM_SCHEMA_VERSION
            )));
        }

        mutator(&mut v.inner);
        crate::model::def::ensure_single_source_table(&v.inner)?;

        self.persist_versioned(&v).await?;
        self.streams
            .insert(name.to_string(), Versioned::fresh(v.inner));
        Ok(())
    }

    async fn persist_versioned(&self, v: &Versioned<StreamDef>) -> Result<()> {
        let path = self.stream_path(&v.inner.name)?;
        let tmp_path = path.with_extension("pb.tmp");

        let v_clone = v.clone();
        let bytes = tokio::task::spawn_blocking(move || encode_versioned_stream_def(&v_clone))
            .await
            .map_err(|e| TsdbError::Storage(format!("Protobuf encode task failed: {e}")))??;

        {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)
                .await
                .map_err(|e| {
                    TsdbError::Storage(format!("Stream create {}: {e}", tmp_path.display()))
                })?;
            f.write_all(&bytes).await.map_err(|e| {
                TsdbError::Storage(format!("Stream write {}: {e}", tmp_path.display()))
            })?;
            f.sync_all().await.map_err(|e| {
                TsdbError::Storage(format!("Stream sync {}: {e}", tmp_path.display()))
            })?;
        }

        fs::rename(&tmp_path, &path).await.map_err(|e| {
            TsdbError::Storage(format!(
                "Atomic rename {} → {}: {e}",
                tmp_path.display(),
                path.display()
            ))
        })?;
        self.sync_directory(&self.root).await?;
        Ok(())
    }

    pub async fn remove(&self, name: &str, delete_checkpoint: bool) -> Result<()> {
        let lock_arc = self.get_name_lock(name);
        let _guard = lock_arc.lock().await;

        let removed = self.streams.remove(name);
        let _ = removed;
        let path = self.stream_path(name)?;

        match fs::remove_file(&path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(TsdbError::Storage(format!(
                    "Remove stream file {}: {e}",
                    path.display()
                )));
            }
        }
        self.sync_directory(&self.root).await?;

        if delete_checkpoint {
            // Checkpoint identity is always `stream::<name>` (see stream_worker_id).
            let worker = format!("stream::{name}");
            self.checkpoints.delete(name, &worker).await?;
        }

        drop(_guard);
        drop(lock_arc);
        // Drop map entry only when no other task still holds the Arc (avoids orphan locks).
        self.name_locks
            .remove_if(name, |_, arc| Arc::strong_count(arc) == 1);
        Ok(())
    }

    pub fn checkpoints_root(&self) -> &Path {
        self.checkpoints.root()
    }

    pub fn checkpoint_store(&self) -> Arc<CheckpointStore> {
        Arc::clone(&self.checkpoints)
    }

    pub async fn load_checkpoint(
        &self,
        stream_name: &str,
        consumer_id: &str,
    ) -> Result<StreamCheckpoint> {
        self.checkpoints.load(stream_name, consumer_id).await
    }

    pub async fn save_checkpoint(&self, cp: &StreamCheckpoint) -> Result<()> {
        self.checkpoints.save(cp).await
    }

    pub async fn delete_checkpoint(&self, stream_name: &str, consumer_id: &str) -> Result<()> {
        self.checkpoints.delete(stream_name, consumer_id).await
    }

    fn stream_path(&self, name: &str) -> Result<PathBuf> {
        if name.is_empty() || name.contains(['/', '\\', '\0']) || name.contains("..") {
            return Err(TsdbError::Storage(format!(
                "Invalid stream name for file path: {name}"
            )));
        }
        Ok(self.root.join(format!("{name}.pb")))
    }

    async fn sync_directory(&self, dir_path: &Path) -> Result<()> {
        if let Ok(dir) = fs::File::open(dir_path).await {
            let _ = dir.sync_all().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::meta::codec::{
        encode_versioned_stream_def, Versioned, STREAM_SCHEMA_MIN_SUPPORTED, STREAM_SCHEMA_VERSION,
    };
    use common::{ConnectorType, StreamCaptureMode};
    use prost::Message;

    fn sample(name: &str) -> StreamDef {
        StreamDef {
            name: name.into(),
            source_tables: vec!["t0".into()],
            capture_mode: StreamCaptureMode::Hybrid,
            sink_config: crate::model::SinkConfig::Delta {
                path: "/tmp/x".into(),
                endpoint: None,
            },
            created_at_ms: 1,
            auto_end: false,
        }
    }

    #[tokio::test]
    async fn put_reload_roundtrip_protobuf() {
        let tmp = tempfile::tempdir().unwrap();
        let store = StreamStore::open(tmp.path()).await.unwrap();
        store.put(sample("s1")).await.unwrap();
        assert!(tmp.path().join("streams").join("s1.pb").is_file());

        let store2 = StreamStore::open(tmp.path()).await.unwrap();
        let def = store2.get("s1").unwrap();
        assert_eq!(def.name, "s1");
        assert_eq!(def.source_tables, vec!["t0".to_string()]);
        assert_eq!(def.connector_type(), ConnectorType::Delta);
    }

    #[tokio::test]
    async fn rejects_path_traversal_name() {
        let tmp = tempfile::tempdir().unwrap();
        let store = StreamStore::open(tmp.path()).await.unwrap();
        let mut def = sample("ok");
        def.name = "../evil".into();
        assert!(store.put(def).await.is_err());
    }

    #[tokio::test]
    async fn refuses_update_when_disk_is_newer_schema() {
        let tmp = tempfile::tempdir().unwrap();
        let store = StreamStore::open(tmp.path()).await.unwrap();
        store.put(sample("s1")).await.unwrap();

        {
            let mut e = store.streams.get_mut("s1").unwrap();
            e.disk_schema_version = STREAM_SCHEMA_VERSION + 5;
            e.min_reader_version = STREAM_SCHEMA_MIN_SUPPORTED;
        }
        match store
            .update("s1", |d| {
                d.sink_config = crate::model::SinkConfig::Delta {
                    path: "/tmp/x".into(),
                    endpoint: None,
                };
            })
            .await
        {
            Ok(()) => panic!("expected update refusal"),
            Err(e) => assert!(e.to_string().contains("refusing to update"), "{e}"),
        }
    }

    #[tokio::test]
    async fn reload_rejects_min_reader_too_new() {
        let tmp = tempfile::tempdir().unwrap();
        let streams = tmp.path().join("streams");
        std::fs::create_dir_all(&streams).unwrap();

        let bytes = encode_versioned_stream_def(&Versioned::fresh(sample("future"))).unwrap();
        let mut file = proto::stream::StreamDefFile::decode(&bytes[..]).unwrap();
        file.schema_version = 99;
        file.min_reader_version = STREAM_SCHEMA_VERSION + 3;
        std::fs::write(streams.join("future.pb"), file.encode_to_vec()).unwrap();

        match StreamStore::open(tmp.path()).await {
            Ok(_) => panic!("expected open to fail for future min_reader_version"),
            Err(e) => assert!(e.to_string().contains("min_reader_version"), "{e}"),
        }
    }

    #[tokio::test]
    async fn concurrent_put_same_name_serializes() {
        let tmp = tempfile::tempdir().unwrap();
        let store = StreamStore::open(tmp.path()).await.unwrap();
        store.put(sample("s1")).await.unwrap();

        let s1 = Arc::clone(&store);
        let s2 = Arc::clone(&store);
        let t1 = tokio::spawn(async move {
            for i in 0..40 {
                let mut d = sample("s1");
                d.sink_config = crate::model::SinkConfig::Delta {
                    path: format!("/tmp/a{i}"),
                    endpoint: None,
                };
                s1.put(d).await.unwrap();
            }
        });
        let t2 = tokio::spawn(async move {
            for i in 0..40 {
                let mut d = sample("s1");
                d.sink_config = crate::model::SinkConfig::Delta {
                    path: format!("/tmp/b{i}"),
                    endpoint: None,
                };
                s2.put(d).await.unwrap();
            }
        });
        t1.await.unwrap();
        t2.await.unwrap();

        let def = store.get("s1").unwrap();
        let path = def.sink_path().unwrap_or("");
        assert!(
            path.contains("/tmp/a") || path.contains("/tmp/b"),
            "got {path}"
        );
        let bytes = std::fs::read(tmp.path().join("streams").join("s1.pb")).unwrap();
        let on_disk = decode_versioned_stream_def(&bytes).unwrap().inner;
        assert_eq!(on_disk.sink_path(), def.sink_path());
    }
}
