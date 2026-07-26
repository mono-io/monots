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

//! WAL materialize result cache for CDC catch-up.
//!
//! Key: **table + exact LSN range** → `Arc<Vec<RecordBatch>>`.
//! Eviction uses the `lru` crate with a shared byte budget (default 32 MiB).

use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use common::{LsnRange, Result};
use lru::LruCache;
use parking_lot::Mutex;

use crate::wal::format::{
    list_numbered_wal_paths, numbered_wal_path, WalFrameCursor, WalFrameEvent,
};

/// Default WAL materialize cache for Stream CDC (32 MiB).
pub const DEFAULT_WAL_LOAD_CACHE_MAX_BYTES: usize = 32 * 1024 * 1024;

/// Cache key: table name + exact LSN range requested by materialize / sink.
#[derive(Debug, Clone, Eq)]
pub struct WalLoadKey {
    pub table: Arc<str>,
    pub base_lsn: u64,
    pub max_lsn: u64,
}

impl WalLoadKey {
    pub fn new(table: impl Into<Arc<str>>, range: LsnRange) -> Self {
        Self {
            table: table.into(),
            base_lsn: range.base_lsn,
            max_lsn: range.max_lsn,
        }
    }
}

impl PartialEq for WalLoadKey {
    fn eq(&self, other: &Self) -> bool {
        self.base_lsn == other.base_lsn
            && self.max_lsn == other.max_lsn
            && self.table.as_ref() == other.table.as_ref()
    }
}

impl Hash for WalLoadKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table.as_ref().hash(state);
        self.base_lsn.hash(state);
        self.max_lsn.hash(state);
    }
}

struct CachedBatches {
    batches: Arc<Vec<RecordBatch>>,
    bytes: usize,
}

struct WalLoadCacheInner {
    used_bytes: usize,
    entries: LruCache<WalLoadKey, CachedBatches>,
}

/// Per-engine WAL materialize cache (shared by all streams).
pub struct WalLoadCache {
    limit_bytes: usize,
    inner: Mutex<WalLoadCacheInner>,
}

impl WalLoadCache {
    pub fn new(limit_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            limit_bytes: limit_bytes.max(1),
            inner: Mutex::new(WalLoadCacheInner {
                used_bytes: 0,
                // Unbounded by entry count; eviction is driven by `limit_bytes`.
                entries: LruCache::unbounded(),
            }),
        })
    }

    pub fn limit_bytes(&self) -> usize {
        self.limit_bytes
    }

    pub fn used_bytes(&self) -> usize {
        self.inner.lock().used_bytes
    }

    pub fn len(&self) -> usize {
        self.inner.lock().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Exact `(table, LSN range)` hit — O(1) after hash, promotes in LRU.
    pub fn get_batches(&self, key: &WalLoadKey) -> Option<Arc<Vec<RecordBatch>>> {
        self.inner
            .lock()
            .entries
            .get(key)
            .map(|e| Arc::clone(&e.batches))
    }

    /// Insert materialized batches; evicts LRU entries until under the byte budget.
    pub fn put_batches(&self, key: WalLoadKey, batches: Vec<RecordBatch>) -> Arc<Vec<RecordBatch>> {
        let bytes: usize = batches
            .iter()
            .map(|b| b.get_array_memory_size().saturating_add(64))
            .sum();
        let batches = Arc::new(batches);
        if bytes == 0 {
            return batches;
        }
        let mut guard = self.inner.lock();
        if let Some(old) = guard.entries.put(
            key,
            CachedBatches {
                batches: Arc::clone(&batches),
                bytes,
            },
        ) {
            guard.used_bytes = guard.used_bytes.saturating_sub(old.bytes);
        }
        guard.used_bytes = guard.used_bytes.saturating_add(bytes);
        while guard.used_bytes > self.limit_bytes {
            match guard.entries.pop_lru() {
                Some((_, old)) => guard.used_bytes = guard.used_bytes.saturating_sub(old.bytes),
                None => break,
            }
        }
        batches
    }
}

/// Ordered numbered WAL paths under a table `wal_segments/` root.
pub fn list_wal_segment_paths(wal_root: &Path) -> Result<Vec<PathBuf>> {
    list_numbered_wal_paths(wal_root)
}

/// Streaming reader for one numbered WAL file.
pub struct WalLoadCursor {
    file_id: u64,
    cursor: WalFrameCursor,
    finished: bool,
}

impl WalLoadCursor {
    pub fn open(wal_root: &Path, file_id: u64) -> Result<Option<Self>> {
        let path = numbered_wal_path(wal_root, file_id);
        if !path.exists() {
            return Ok(None);
        }
        let Some(cursor) = WalFrameCursor::open(&path, file_id)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            file_id,
            cursor,
            finished: false,
        }))
    }

    pub fn file_id(&self) -> u64 {
        self.file_id
    }

    pub fn finished(&self) -> bool {
        self.finished
    }

    pub fn next_batch(&mut self, min_lsn: u64) -> Result<Option<WalFrameEvent>> {
        if self.finished {
            return Ok(None);
        }
        loop {
            match self.cursor.next_batch()? {
                Some(ev) => {
                    if ev.lsn >= min_lsn {
                        return Ok(Some(ev));
                    }
                }
                None => {
                    self.finished = self.cursor.finished();
                    return Ok(None);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::format::{list_wal_file_ids, numbered_wal_path, FramedSegmentWriter};
    use arrow::array::{AsArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use common::LsnRange;
    use std::fs;
    use std::sync::Arc;

    fn schema() -> arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn batch(ts: i64) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![ts])),
                Arc::new(Int64Array::from(vec![1_i64])),
            ],
        )
        .unwrap()
    }

    fn batch_bytes() -> usize {
        batch(1).get_array_memory_size().saturating_add(64)
    }

    #[test]
    fn list_orders_numbered_files() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(numbered_wal_path(dir.path(), 2), b"x").unwrap();
        fs::write(numbered_wal_path(dir.path(), 1), b"x").unwrap();
        let paths = list_wal_segment_paths(dir.path()).unwrap();
        let names: Vec<_> = paths
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        assert_eq!(
            names,
            vec![
                "00000000000000000001.wal".to_string(),
                "00000000000000000002.wal".to_string(),
            ]
        );
    }

    #[test]
    fn load_cursor_reads_numbered_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_id = 7u64;
        {
            let path = numbered_wal_path(dir.path(), file_id);
            let mut w = FramedSegmentWriter::create(path, file_id, schema()).unwrap();
            w.append_batch(&batch(1), 10, true).unwrap();
            w.append_batch(&batch(2), 20, true).unwrap();
        }

        let mut cur = WalLoadCursor::open(dir.path(), file_id)
            .unwrap()
            .expect("chain");
        let a = cur.next_batch(0).unwrap().expect("first");
        assert_eq!(a.lsn, 10);
        let b = cur.next_batch(0).unwrap().expect("second");
        assert_eq!(b.lsn, 20);
        assert!(cur.next_batch(0).unwrap().is_none());
    }

    #[test]
    fn load_cursor_skips_below_min_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let file_id = 1u64;
        {
            let path = numbered_wal_path(dir.path(), file_id);
            let mut w = FramedSegmentWriter::create(path, file_id, schema()).unwrap();
            w.append_batch(&batch(1), 10, true).unwrap();
            w.append_batch(&batch(2), 20, true).unwrap();
            w.append_batch(&batch(3), 30, true).unwrap();
        }
        let mut cur = WalLoadCursor::open(dir.path(), file_id)
            .unwrap()
            .expect("open");
        let first = cur.next_batch(20).unwrap().expect("from 20");
        assert_eq!(first.lsn, 20);
    }

    #[test]
    fn miss_then_hit_returns_same_rows() {
        let cache = WalLoadCache::new(16 << 20);
        let key = WalLoadKey::new("metrics", LsnRange::new(10, 20));
        assert!(cache.get_batches(&key).is_none());

        let put = cache.put_batches(key.clone(), vec![batch(10), batch(20)]);
        assert_eq!(put.len(), 2);
        assert_eq!(cache.len(), 1);

        let hit = cache.get_batches(&key).expect("hit");
        assert_eq!(hit.len(), 2);
        assert_eq!(hit[0].column(0).as_primitive::<Int64Type>().value(0), 10);
        assert_eq!(hit[1].column(0).as_primitive::<Int64Type>().value(0), 20);
    }

    #[test]
    fn different_tables_and_ranges_do_not_collide() {
        let cache = WalLoadCache::new(16 << 20);
        let a = WalLoadKey::new("a", LsnRange::new(1, 1));
        let b = WalLoadKey::new("b", LsnRange::new(1, 1));
        let a2 = WalLoadKey::new("a", LsnRange::new(1, 2));
        cache.put_batches(a.clone(), vec![batch(1)]);
        cache.put_batches(b.clone(), vec![batch(2)]);
        cache.put_batches(a2.clone(), vec![batch(3)]);
        assert_eq!(cache.len(), 3);
        assert_eq!(
            cache.get_batches(&a).unwrap()[0]
                .column(0)
                .as_primitive::<Int64Type>()
                .value(0),
            1
        );
        assert_eq!(
            cache.get_batches(&b).unwrap()[0]
                .column(0)
                .as_primitive::<Int64Type>()
                .value(0),
            2
        );
        assert_eq!(
            cache.get_batches(&a2).unwrap()[0]
                .column(0)
                .as_primitive::<Int64Type>()
                .value(0),
            3
        );
    }

    #[test]
    fn lru_evicts_least_recently_used_under_byte_budget() {
        let budget = batch_bytes() + batch_bytes() / 2;
        let cache = WalLoadCache::new(budget);
        let k1 = WalLoadKey::new("t", LsnRange::new(1, 1));
        let k2 = WalLoadKey::new("t", LsnRange::new(2, 2));
        let k3 = WalLoadKey::new("t", LsnRange::new(3, 3));

        cache.put_batches(k1.clone(), vec![batch(1)]);
        cache.put_batches(k2.clone(), vec![batch(2)]);
        // Budget fits ~1 entry; k1 should be gone, k2 present.
        assert!(cache.get_batches(&k1).is_none());
        assert!(cache.get_batches(&k2).is_some());
        assert!(cache.used_bytes() <= cache.limit_bytes());

        // Touch k2 then insert k3 — k2 is MRU so k2 may survive if one slot; k3 replaces.
        let _ = cache.get_batches(&k2);
        cache.put_batches(k3.clone(), vec![batch(3)]);
        assert!(cache.get_batches(&k3).is_some());
        assert!(cache.used_bytes() <= cache.limit_bytes());
        assert!(cache.len() <= 1);
    }

    #[test]
    fn replace_same_key_does_not_inflate_used_bytes() {
        let cache = WalLoadCache::new(16 << 20);
        let key = WalLoadKey::new("t", LsnRange::new(1, 1));
        cache.put_batches(key.clone(), vec![batch(1)]);
        let used_after_first = cache.used_bytes();
        cache.put_batches(key.clone(), vec![batch(9)]);
        let used_after_replace = cache.used_bytes();
        assert_eq!(cache.len(), 1);
        assert_eq!(used_after_first, used_after_replace);
        assert_eq!(
            cache.get_batches(&key).unwrap()[0]
                .column(0)
                .as_primitive::<Int64Type>()
                .value(0),
            9
        );
    }

    #[test]
    fn empty_batches_are_not_cached() {
        let cache = WalLoadCache::new(16 << 20);
        let key = WalLoadKey::new("t", LsnRange::new(1, 1));
        let _ = cache.put_batches(key.clone(), vec![]);
        assert!(cache.is_empty());
        assert_eq!(cache.used_bytes(), 0);
        assert!(cache.get_batches(&key).is_none());
    }

    #[test]
    fn list_wal_file_ids_skips_bulk_load_dir() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("bulk_load")).unwrap();
        fs::write(numbered_wal_path(dir.path(), 3), b"x").unwrap();
        assert_eq!(list_wal_file_ids(dir.path()).unwrap(), vec![3]);
    }
}
