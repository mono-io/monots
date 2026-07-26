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

//! Per-table capturer: storage **only notifies** [`TableCaptureListener`]s.
//!
//! ## Hard-link contract (critical)
//! Engines may compact/unlink SST files at any time after the callback returns.
//! Therefore every production listener **must** hard-link into its own sandbox
//! **inside** the synchronous `on_flush` / `on_bulk_load` / `on_compact`
//! callback before enqueueing. Never put the engine's `file_path` on an async
//! queue and link later — that is a dangling-path race.
//!
//! `pending_dir` **must** share the same filesystem mount as the engine data dir
//! so `hard_link` succeeds. Cross-device failure is fatal (no silent `copy`).
//!
//! ## Listener set = copy-on-write
//! Subscribers are published through [`arc_swap::ArcSwap`]. The write path only
//! loads an immutable snapshot; CREATE/DROP STREAM clones + swaps (rare).
//!
//! Production listener: [`monots_stream::StreamSource`].

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;
use arrow::record_batch::RecordBatch;
use common::{CaptureFileMeta, Result, TableCaptureListener, TsdbError};
use dashmap::DashMap;
use parking_lot::Mutex;

use crate::compaction::sst::SstMeta;

/// Default capacity hint kept for API stability (listeners own their own queues).
pub const DEFAULT_TABLE_CAPTURE_CAPACITY: usize = 4096;

/// Convert storage SST metadata into the common capture file descriptor.
pub(crate) fn sst_to_capture_meta(meta: &SstMeta) -> CaptureFileMeta {
    CaptureFileMeta {
        file_path: meta.file_path.clone(),
        min_lsn: meta.base_lsn,
        max_lsn: meta.max_lsn,
        min_ts: meta.min_ts,
        max_ts: meta.max_ts,
        rows: meta.row_count as u64,
    }
}

/// Storage write-path hooks (SstMeta-native). Bridges into [`TableCaptureListener`].
pub trait TableCapturer: Send + Sync {
    fn on_insert(&self, lsn: u64, batch: RecordBatch);
    /// Memtable freeze watermark (`end_lsn` = inclusive max LSN of the sealed memtable).
    fn on_memtable_end(&self, end_lsn: u64);
    fn on_flush(&self, meta: &SstMeta);
    fn on_bulk_load(&self, meta: &SstMeta);
    fn on_compact(&self, inputs: &[SstMeta], output: &SstMeta);
}

/// Hard-link `original` into `pending_dir`, returning the pin path.
///
/// - Same-filesystem hard link only — **never** falls back to `copy` (avoids
///   blocking Flush/Compact threads on multi-GB SST copies).
/// - Concurrent linkers: `AlreadyExists` is treated as success (no TOCTOU
///   exists-then-link race that spuriously fails).
pub fn hard_link_into_pending(pending_dir: &Path, original_path: &str) -> Result<PathBuf> {
    let original = Path::new(original_path);
    let file_name = original.file_name().ok_or_else(|| {
        TsdbError::Storage(format!("stream pin: invalid SST path {original_path}"))
    })?;
    std::fs::create_dir_all(pending_dir).map_err(|e| {
        TsdbError::Storage(format!(
            "create stream pending {}: {e}",
            pending_dir.display()
        ))
    })?;
    let link_path = pending_dir.join(file_name);
    match std::fs::hard_link(original, &link_path) {
        Ok(()) => Ok(link_path),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(link_path),
        Err(link_err) => Err(TsdbError::Storage(format!(
            "stream hard_link fatal for {original_path} → {} ({link_err}); \
             pending/ must share the same filesystem mount as engine data (no copy fallback)",
            pending_dir.display()
        ))),
    }
}

/// Immutable listener set published via [`ArcSwap`] (copy-on-write).
///
/// Hot path (`on_insert`) only `load()`s an `Arc` and iterates — no locks.
/// DDL (`set_listener` / remove) clones the vec under a rare update mutex, then
/// atomically swaps the pointer.
#[derive(Clone)]
struct ListenerSnapshot {
    entries: Vec<(String, Arc<dyn TableCaptureListener>)>,
}

impl ListenerSnapshot {
    fn empty() -> Arc<Self> {
        Arc::new(Self {
            entries: Vec::new(),
        })
    }
}

/// Fan-out capturer: notify every subscribed listener (no progress filtering).
pub struct RegisteredTableCapturer {
    listeners: ArcSwap<ListenerSnapshot>,
    /// Serializes rare COW publishes (CREATE/DROP STREAM).
    publish: Mutex<()>,
}

impl RegisteredTableCapturer {
    fn new() -> Self {
        Self {
            listeners: ArcSwap::from(ListenerSnapshot::empty()),
            publish: Mutex::new(()),
        }
    }

    #[inline]
    fn has_subscribers(&self) -> bool {
        !self.listeners.load().entries.is_empty()
    }

    fn is_subscribed(&self, subscriber_id: &str) -> bool {
        self.listeners
            .load()
            .entries
            .iter()
            .any(|(id, _)| id == subscriber_id)
    }

    pub fn set_listener(&self, subscriber_id: &str, listener: Arc<dyn TableCaptureListener>) {
        let _guard = self.publish.lock();
        let prev = self.listeners.load_full();
        let mut entries = prev.entries.clone();
        if let Some((_, slot)) = entries.iter_mut().find(|(id, _)| id == subscriber_id) {
            *slot = listener;
        } else {
            entries.push((subscriber_id.to_string(), listener));
        }
        self.listeners.store(Arc::new(ListenerSnapshot { entries }));
    }

    fn remove_subscriber(&self, subscriber_id: &str) -> bool {
        let _guard = self.publish.lock();
        let prev = self.listeners.load_full();
        let entries: Vec<_> = prev
            .entries
            .iter()
            .filter(|(id, _)| id != subscriber_id)
            .cloned()
            .collect();
        let empty = entries.is_empty();
        self.listeners.store(Arc::new(ListenerSnapshot { entries }));
        empty
    }

    #[inline]
    fn for_each_listener(&self, mut f: impl FnMut(&dyn TableCaptureListener)) {
        let snap = self.listeners.load_full();
        for (_, listener) in &snap.entries {
            f(listener.as_ref());
        }
    }
}

impl TableCapturer for RegisteredTableCapturer {
    fn on_insert(&self, lsn: u64, batch: RecordBatch) {
        if !self.has_subscribers() {
            return;
        }
        let snap = self.listeners.load_full();
        // Skip Arrow clone entirely when no subscriber wants WAL/log capture.
        if !snap.entries.iter().any(|(_, l)| l.capture_wal()) {
            return;
        }
        for (_, listener) in &snap.entries {
            if listener.capture_wal() {
                listener.on_insert(lsn, lsn, batch.clone());
            }
        }
    }

    fn on_memtable_end(&self, end_lsn: u64) {
        if !self.has_subscribers() || end_lsn == 0 {
            return;
        }
        self.for_each_listener(|listener| {
            listener.on_memtable_end(end_lsn);
        });
    }

    fn on_flush(&self, meta: &SstMeta) {
        if !self.has_subscribers() || !meta.has_lsn_bounds() {
            return;
        }
        let cm = sst_to_capture_meta(meta);
        self.for_each_listener(|listener| {
            listener.on_flush(&cm);
        });
    }

    fn on_bulk_load(&self, meta: &SstMeta) {
        if !self.has_subscribers() || !meta.has_lsn_bounds() {
            return;
        }
        let cm = sst_to_capture_meta(meta);
        self.for_each_listener(|listener| {
            listener.on_bulk_load(&cm);
        });
    }

    fn on_compact(&self, inputs: &[SstMeta], output: &SstMeta) {
        if !self.has_subscribers() || !output.has_lsn_bounds() {
            return;
        }
        let inputs: Vec<_> = inputs.iter().map(sst_to_capture_meta).collect();
        let out = sst_to_capture_meta(output);
        self.for_each_listener(|listener| {
            listener.on_compact(&inputs, &out);
        });
    }
}

struct TableCaptureEntry {
    capturer: Arc<RegisteredTableCapturer>,
}

/// Engine-wide hub: one [`RegisteredTableCapturer`] per table.
pub struct TableCaptureHub {
    inner: DashMap<String, TableCaptureEntry>,
}

impl TableCaptureHub {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    /// `capacity` / `pin_root` kept for call-site compatibility; listeners own their sandboxes.
    pub fn with_pin_root(capacity: usize, _pin_root: impl Into<PathBuf>) -> Self {
        let _ = capacity.max(16);
        Self::new()
    }

    pub fn is_registered(&self, table: &str) -> bool {
        self.inner
            .get(table)
            .is_some_and(|e| e.capturer.has_subscribers())
    }

    pub fn has_subscriber(&self, table: &str, subscriber_id: &str) -> bool {
        self.inner
            .get(table)
            .is_some_and(|e| e.capturer.is_subscribed(subscriber_id))
    }

    /// Atomically attach a real listener (no Noop placeholder — avoids event black holes).
    ///
    /// Uses [`DashMap::entry`] so concurrent first-time registrations for the same table
    /// share one capturer instead of overwriting each other (check-then-act race).
    pub fn set_listener(
        &self,
        table: &str,
        subscriber_id: &str,
        listener: Arc<dyn TableCaptureListener>,
    ) -> Arc<RegisteredTableCapturer> {
        let capturer = {
            let entry = self
                .inner
                .entry(table.to_string())
                .or_insert_with(|| TableCaptureEntry {
                    capturer: Arc::new(RegisteredTableCapturer::new()),
                });
            Arc::clone(&entry.capturer)
        };
        // Drop shard RefMut before COW publish so other tables / ops are not blocked.
        capturer.set_listener(subscriber_id, listener);
        capturer
    }

    pub fn unregister_subscriber(&self, table: &str, subscriber_id: &str) -> bool {
        // 1. Drop the subscriber from the COW snapshot (may leave table entry empty).
        let is_empty = if let Some(entry) = self.inner.get(table) {
            entry.capturer.remove_subscriber(subscriber_id)
        } else {
            return true;
        };

        // 2. Only remove the table key if still empty under the DashMap write lock —
        //    so a concurrent set_listener that re-populated subscribers is not wiped out.
        if is_empty {
            self.inner
                .remove_if(table, |_, entry| !entry.capturer.has_subscribers());
            true
        } else {
            false
        }
    }

    pub fn unregister_stream(&self, stream: &str, table: &str) -> bool {
        let id = format!("{stream}::log::{table}");
        self.unregister_subscriber(table, &id)
    }
}

impl Default for TableCaptureHub {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use parking_lot::Mutex;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64])),
                Arc::new(Int64Array::from(vec![1_i64])),
            ],
        )
        .unwrap()
    }

    fn meta(path: &str, base: u64, max: u64) -> SstMeta {
        SstMeta {
            file_path: path.into(),
            min_ts: 0,
            max_ts: 10,
            row_count: 1,
            file_size: 10,
            creation_time_ms: 0,
            inner_compaction_count: 0,
            cross_compaction_count: 0,
            base_lsn: base,
            max_lsn: max,
        }
    }

    struct PinListener {
        pending: PathBuf,
        inserts: Mutex<u64>,
        links: Mutex<Vec<PathBuf>>,
    }

    impl TableCaptureListener for PinListener {
        fn on_insert(&self, _min_lsn: u64, _max_lsn: u64, _batch: RecordBatch) {
            *self.inserts.lock() += 1;
        }
        fn on_flush(&self, meta: &CaptureFileMeta) {
            let link = hard_link_into_pending(&self.pending, &meta.file_path).unwrap();
            self.links.lock().push(link);
        }
        fn on_bulk_load(&self, meta: &CaptureFileMeta) {
            self.on_flush(meta);
        }
        fn on_compact(&self, _inputs: &[CaptureFileMeta], output: &CaptureFileMeta) {
            let link = hard_link_into_pending(&self.pending, &output.file_path).unwrap();
            self.links.lock().push(link);
        }
    }

    #[test]
    fn insert_flush_compact_pins_in_callback() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        let pending = tmp.path().join("pending");
        std::fs::create_dir_all(&data).unwrap();
        let a = data.join("100-1-1-0-0.parquet");
        let b = data.join("200-1-1-1-0.parquet");
        std::fs::write(&a, b"aaaa").unwrap();
        std::fs::write(&b, b"bbbb").unwrap();

        let listener = Arc::new(PinListener {
            pending: pending.clone(),
            inserts: Mutex::new(0),
            links: Mutex::new(Vec::new()),
        });
        let hub = TableCaptureHub::new();
        let cap = hub.set_listener("t0", "s1::log::t0", Arc::clone(&listener) as _);

        TableCapturer::on_insert(cap.as_ref(), 1, batch());
        TableCapturer::on_flush(cap.as_ref(), &meta(a.to_str().unwrap(), 1, 1));
        TableCapturer::on_compact(
            cap.as_ref(),
            &[meta(a.to_str().unwrap(), 1, 1)],
            &meta(b.to_str().unwrap(), 1, 1),
        );

        assert_eq!(*listener.inserts.lock(), 1);
        let links = listener.links.lock().clone();
        assert_eq!(links.len(), 2);
        assert!(links[0].exists());
        std::fs::remove_file(&a).unwrap();
        assert_eq!(std::fs::read(&links[0]).unwrap(), b"aaaa");
        assert!(links[1].exists());
    }

    #[test]
    fn multi_subscriber_pins_are_isolated() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let sst = data.join("50-3-3-0-0.parquet");
        std::fs::write(&sst, b"sst").unwrap();

        let hub = TableCaptureHub::new();
        let a = Arc::new(PinListener {
            pending: tmp.path().join("a"),
            inserts: Mutex::new(0),
            links: Mutex::new(Vec::new()),
        });
        let b = Arc::new(PinListener {
            pending: tmp.path().join("b"),
            inserts: Mutex::new(0),
            links: Mutex::new(Vec::new()),
        });
        let cap = hub.set_listener("t0", "stream_a", Arc::clone(&a) as _);
        hub.set_listener("t0", "stream_b", Arc::clone(&b) as _);
        TableCapturer::on_flush(cap.as_ref(), &meta(sst.to_str().unwrap(), 3, 3));

        let la = a.links.lock()[0].clone();
        let lb = b.links.lock()[0].clone();
        assert_ne!(la, lb);
        assert!(la.exists() && lb.exists());
        std::fs::remove_file(&la).unwrap();
        assert_eq!(std::fs::read(&lb).unwrap(), b"sst");
    }

    #[test]
    fn skip_insert_when_capture_wal_disabled() {
        let inserts = Arc::new(Mutex::new(0u64));
        struct BatchOnly {
            inserts: Arc<Mutex<u64>>,
        }
        impl TableCaptureListener for BatchOnly {
            fn capture_wal(&self) -> bool {
                false
            }
            fn on_insert(&self, _: u64, _: u64, _: RecordBatch) {
                *self.inserts.lock() += 1;
            }
            fn on_flush(&self, _: &CaptureFileMeta) {}
            fn on_bulk_load(&self, _: &CaptureFileMeta) {}
            fn on_compact(&self, _: &[CaptureFileMeta], _: &CaptureFileMeta) {}
        }

        let hub = TableCaptureHub::new();
        let listener = Arc::new(BatchOnly {
            inserts: Arc::clone(&inserts),
        });
        let cap = hub.set_listener("t0", "s1::batch::t0", listener as _);
        TableCapturer::on_insert(cap.as_ref(), 1, batch());
        assert_eq!(*inserts.lock(), 0);
    }

    #[test]
    fn hub_allocates_only_after_real_listener() {
        let hub = TableCaptureHub::new();
        assert!(!hub.is_registered("t0"));
        struct Counting;
        impl TableCaptureListener for Counting {
            fn on_insert(&self, _: u64, _: u64, _: RecordBatch) {}
            fn on_flush(&self, _: &CaptureFileMeta) {}
            fn on_bulk_load(&self, _: &CaptureFileMeta) {}
            fn on_compact(&self, _: &[CaptureFileMeta], _: &CaptureFileMeta) {}
        }
        hub.set_listener("t0", "s1::log::t0", Arc::new(Counting));
        assert!(hub.is_registered("t0"));
        assert!(hub.unregister_stream("s1", "t0"));
        assert!(!hub.is_registered("t0"));
    }

    #[test]
    fn unregister_empty_table_does_not_wipe_concurrent_reregister() {
        // Regression for: last-subscriber unregister racing with a new set_listener —
        // remove_if must keep the table entry if has_subscribers() became true again.
        let hub = Arc::new(TableCaptureHub::new());
        struct Counting;
        impl TableCaptureListener for Counting {
            fn on_insert(&self, _: u64, _: u64, _: RecordBatch) {}
            fn on_flush(&self, _: &CaptureFileMeta) {}
            fn on_bulk_load(&self, _: &CaptureFileMeta) {}
            fn on_compact(&self, _: &[CaptureFileMeta], _: &CaptureFileMeta) {}
        }

        hub.set_listener("t0", "s1::log::t0", Arc::new(Counting));
        assert!(hub.is_registered("t0"));

        // Simulate the race window: remove last subscriber, then another stream
        // attaches before the outer map key is cleaned up.
        let entry = hub.inner.get("t0").unwrap();
        assert!(entry.capturer.remove_subscriber("s1::log::t0"));
        drop(entry);
        hub.set_listener("t0", "s2::log::t0", Arc::new(Counting));
        hub.inner
            .remove_if("t0", |_, e| !e.capturer.has_subscribers());

        assert!(
            hub.is_registered("t0"),
            "new Stream_2 must survive remove_if after empty check"
        );
        assert!(hub.has_subscriber("t0", "s2::log::t0"));
    }

    #[test]
    fn concurrent_first_register_does_not_overwrite() {
        // Regression for get-then-insert race: two streams attaching to an empty
        // table must land on the same capturer, not overwrite each other.
        let hub = Arc::new(TableCaptureHub::new());
        struct Counting;
        impl TableCaptureListener for Counting {
            fn on_insert(&self, _: u64, _: u64, _: RecordBatch) {}
            fn on_flush(&self, _: &CaptureFileMeta) {}
            fn on_bulk_load(&self, _: &CaptureFileMeta) {}
            fn on_compact(&self, _: &[CaptureFileMeta], _: &CaptureFileMeta) {}
        }

        let h1 = Arc::clone(&hub);
        let h2 = Arc::clone(&hub);
        let t1 =
            std::thread::spawn(move || h1.set_listener("t0", "s1::log::t0", Arc::new(Counting)));
        let t2 =
            std::thread::spawn(move || h2.set_listener("t0", "s2::log::t0", Arc::new(Counting)));
        let c1 = t1.join().unwrap();
        let c2 = t2.join().unwrap();
        assert!(
            Arc::ptr_eq(&c1, &c2),
            "both registrations must share one RegisteredTableCapturer"
        );
        assert!(hub.has_subscriber("t0", "s1::log::t0"));
        assert!(hub.has_subscriber("t0", "s2::log::t0"));
    }
}
