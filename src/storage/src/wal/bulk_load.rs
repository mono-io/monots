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

//! Bulk-load WAL: durable LSN-ordered hard links under `wal_segments/bulk_load/`.
//!
//! Regular DML lives in per-memtable `wal_segments/{id}/segment.wal`. Bulk load bypasses the
//! memtable, so each ingested SST is recorded here as a second WAL entry type:
//!
//! 1. Allocate a global LSN (when replication is on).
//! 2. Hard-link the SST into this directory (`{lsn:020}-{filename}`).
//! 3. Append a fsynced JSONL event (`entries.log`).
//!
//! Crash recovery can restore missing FileIndex entries from these links. Compaction must not
//! unlink a bulk-loaded SST while its WAL entry still exists; [`BulkLoadWal::gc_upto`] deletes
//! the WAL entry (hard link) first, then removes the original SST if it is no longer live.

use crate::compaction::sst::{FileIndex, SstMeta};
use crate::compaction::sst_id::parse_sst_filename;
use common::{FileAddEvent, Result, TsdbError, WAL_SEGMENTS_DIR};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

/// Directory name under `wal_segments/` (non-numeric so memtable WAL scanners skip it).
pub const BULK_LOAD_DIR_NAME: &str = "bulk_load";
/// Durable event log filename inside the bulk-load WAL directory.
pub const BULK_LOAD_LOG_NAME: &str = "entries.log";

/// JSONL event log shared by [`BulkLoadWal`] (also used historically as `addfile.log`).
pub struct FileEventLog {
    path: PathBuf,
    events: Mutex<Vec<FileAddEvent>>,
}

impl FileEventLog {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let events = if path.exists() {
            Self::scan(&path)?
        } else {
            Vec::new()
        };
        Ok(Self {
            path,
            events: Mutex::new(events),
        })
    }

    fn scan(path: &Path) -> Result<Vec<FileAddEvent>> {
        let reader = BufReader::new(fs::File::open(path)?);
        let mut out = Vec::new();
        for line in reader.lines() {
            let Ok(line) = line else { break };
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<FileAddEvent>(&line) {
                Ok(ev) => out.push(ev),
                Err(_) => break,
            }
        }
        out.sort_by_key(|e| e.lsn);
        Ok(out)
    }

    pub fn append(&self, event: FileAddEvent) -> Result<()> {
        let line = serde_json::to_string(&event).map_err(|e| TsdbError::Storage(e.to_string()))?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
        file.flush()?;
        file.sync_data()?;
        let mut events = self.events.lock();
        events.push(event);
        events.sort_by_key(|e| e.lsn);
        Ok(())
    }

    pub fn since(&self, lsn: u64) -> Vec<FileAddEvent> {
        self.events
            .lock()
            .iter()
            .filter(|e| e.lsn > lsn)
            .cloned()
            .collect()
    }

    pub fn all(&self) -> Vec<FileAddEvent> {
        self.events.lock().clone()
    }

    /// Drop events with `lsn <= retained_lsn`, delete their hard links, rewrite the log.
    /// Returns the expired events (caller may then unlink original SSTs no longer in FileIndex).
    pub fn gc_upto(&self, retained_lsn: u64) -> Result<Vec<FileAddEvent>> {
        let mut events = self.events.lock();
        let (expired, live): (Vec<_>, Vec<_>) =
            events.drain(..).partition(|e| e.lsn <= retained_lsn);
        if expired.is_empty() {
            *events = live;
            return Ok(expired);
        }
        for e in &expired {
            let link = Path::new(&e.link_path);
            if link.exists() {
                let _ = fs::remove_file(link);
            }
        }
        *events = live;
        Self::rewrite(&self.path, &events)?;
        Ok(expired)
    }

    fn rewrite(path: &Path, events: &[FileAddEvent]) -> Result<()> {
        let tmp = path.with_extension("log.tmp");
        {
            let mut f = fs::File::create(&tmp)?;
            for e in events {
                let line =
                    serde_json::to_string(e).map_err(|err| TsdbError::Storage(err.to_string()))?;
                f.write_all(line.as_bytes())?;
                f.write_all(b"\n")?;
            }
            f.flush()?;
            f.sync_data()?;
        }
        fs::rename(&tmp, path)?;
        Ok(())
    }
}

/// Bulk-load WAL rooted at `{table}/wal_segments/bulk_load/`.
pub struct BulkLoadWal {
    dir: PathBuf,
    events: FileEventLog,
}

impl BulkLoadWal {
    pub fn dir_for(table_data_dir: &Path) -> PathBuf {
        table_data_dir
            .join(WAL_SEGMENTS_DIR)
            .join(BULK_LOAD_DIR_NAME)
    }

    pub fn open(table_data_dir: &Path) -> Result<ArcBulkLoadWal> {
        let dir = Self::dir_for(table_data_dir);
        fs::create_dir_all(&dir)?;
        let events = FileEventLog::open(dir.join(BULK_LOAD_LOG_NAME))?;
        Ok(std::sync::Arc::new(Self { dir, events }))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Hard-link `meta` into the WAL dir and append a durable event at `lsn`.
    pub fn record(&self, lsn: u64, meta: &SstMeta) -> Result<FileAddEvent> {
        fs::create_dir_all(&self.dir)?;
        let file_name = Path::new(&meta.file_path)
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| format!("{lsn}.parquet"));
        let link_path = self.dir.join(format!("{lsn:020}-{file_name}"));
        if !link_path.exists() {
            hard_link_or_copy(Path::new(&meta.file_path), &link_path)?;
        }
        let event = FileAddEvent {
            lsn,
            original_path: meta.file_path.clone(),
            link_path: link_path.to_string_lossy().into_owned(),
            min_ts: meta.min_ts,
            max_ts: meta.max_ts,
            rows: meta.row_count as u64,
        };
        self.events.append(event.clone())?;
        Ok(event)
    }

    pub fn since(&self, lsn: u64) -> Vec<FileAddEvent> {
        self.events.since(lsn)
    }

    pub fn all(&self) -> Vec<FileAddEvent> {
        self.events.all()
    }

    pub fn pins_path(&self, original_path: &str) -> bool {
        self.events
            .all()
            .iter()
            .any(|e| e.original_path == original_path)
    }

    /// Reclaim WAL entries at/below `retained_lsn`. After the WAL hard links are gone, delete
    /// original SST paths that are no longer in the live FileIndex (compaction already dropped
    /// them from the manifest but deferred unlink while pinned).
    pub fn gc_upto(&self, retained_lsn: u64, live_paths: &HashSet<String>) -> Result<()> {
        let expired = self.events.gc_upto(retained_lsn)?;
        for e in expired {
            if live_paths.contains(&e.original_path) {
                continue;
            }
            let orig = Path::new(&e.original_path);
            if orig.exists() {
                if let Err(err) = fs::remove_file(orig) {
                    tracing::warn!(
                        path = %e.original_path,
                        error = %err,
                        "failed to remove bulk-load SST after BulkLoad WAL GC"
                    );
                }
            }
        }
        Ok(())
    }

    /// Crash recovery: restore missing originals from hard links and insert into FileIndex.
    pub fn recover_into_index(&self, file_index: &FileIndex) -> Result<u64> {
        let live: HashSet<String> = file_index
            .snapshot()
            .into_iter()
            .map(|m| m.file_path)
            .collect();
        for ev in self.events.all() {
            let orig = PathBuf::from(&ev.original_path);
            let link = Path::new(&ev.link_path);
            if !orig.exists() {
                if link.exists() {
                    if let Some(parent) = orig.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    hard_link_or_copy(link, &orig)?;
                } else {
                    tracing::warn!(
                        lsn = ev.lsn,
                        path = %ev.original_path,
                        "bulk-load WAL entry has neither original nor hard link; skipping"
                    );
                    continue;
                }
            }

            let file_name = orig.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
                TsdbError::Storage(format!("invalid bulk-load SST path: {}", orig.display()))
            })?;
            let identity = parse_sst_filename(file_name)?;
            // Sequence watermark is no longer tied to SST identity (LSN-only); keep 0.
            let _ = identity;

            if live.contains(&ev.original_path) {
                continue;
            }

            let file_size = fs::metadata(&orig)?.len();
            let meta = SstMeta::from_identity(
                identity,
                ev.original_path.clone(),
                ev.min_ts,
                ev.max_ts,
                ev.rows as usize,
                file_size,
            );
            meta.validate()?;
            file_index.insert(meta);
            tracing::info!(
                lsn = ev.lsn,
                path = %ev.original_path,
                "recovered bulk-load SST into FileIndex from BulkLoad WAL"
            );
        }
        Ok(0)
    }
}

/// Shared handle used by [`crate::table::LsmTable`] and replication.
pub type ArcBulkLoadWal = std::sync::Arc<BulkLoadWal>;

fn hard_link_or_copy(src: &Path, dst: &Path) -> Result<()> {
    if dst.exists() {
        return Ok(());
    }
    if fs::hard_link(src, dst).is_err() {
        fs::copy(src, dst)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn catalog_schema() -> arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(common::TIMESTAMP_COLUMN, DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ]))
    }

    fn write_sst(dir: &Path, lsn: u64) -> SstMeta {
        let schema = catalog_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10_i64, 20])),
                Arc::new(Int64Array::from(vec![1_i64, 2])),
            ],
        )
        .unwrap();
        let identity = crate::compaction::sst_id::SstIdentity::fresh_flush(lsn, lsn);
        crate::compaction::sst::write_sst(&identity, &batch, dir, 10, 20).unwrap()
    }

    #[test]
    fn record_hard_links_and_recovers_missing_index() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("t");
        fs::create_dir_all(&data_dir).unwrap();
        let meta = write_sst(&data_dir, 100);
        let wal = BulkLoadWal::open(&data_dir).unwrap();
        wal.record(100, &meta).unwrap();

        let events = wal.all();
        let link = Path::new(&events[0].link_path);
        assert!(link.exists());
        assert_eq!(
            fs::metadata(&meta.file_path).unwrap().len(),
            fs::metadata(link).unwrap().len()
        );

        // Simulate crash: FileIndex empty, original deleted, only hard link remains.
        fs::remove_file(&meta.file_path).unwrap();
        let index = FileIndex::new();
        let _max_id = wal.recover_into_index(&index).unwrap();
        assert!(Path::new(&meta.file_path).exists());
        assert_eq!(index.snapshot().len(), 1);
        assert_eq!(index.snapshot()[0].base_lsn, 100);
        assert_eq!(index.snapshot()[0].max_lsn, 100);
    }

    #[test]
    fn gc_removes_wal_then_orphaned_sst() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("t");
        fs::create_dir_all(&data_dir).unwrap();
        let meta = write_sst(&data_dir, 50);
        let wal = BulkLoadWal::open(&data_dir).unwrap();
        wal.record(50, &meta).unwrap();

        // Compaction dropped it from the live index but deferred unlink while pinned.
        let live = HashSet::new();
        assert!(wal.pins_path(&meta.file_path));
        wal.gc_upto(50, &live).unwrap();
        assert!(!wal.pins_path(&meta.file_path));
        assert!(!Path::new(&meta.file_path).exists());
        assert!(wal.all().is_empty());
    }

    #[test]
    fn gc_keeps_sst_still_in_file_index() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("t");
        fs::create_dir_all(&data_dir).unwrap();
        let meta = write_sst(&data_dir, 60);
        let wal = BulkLoadWal::open(&data_dir).unwrap();
        wal.record(60, &meta).unwrap();

        let mut live = HashSet::new();
        live.insert(meta.file_path.clone());
        wal.gc_upto(u64::MAX, &live).unwrap();
        assert!(Path::new(&meta.file_path).exists());
        assert!(wal.all().is_empty());
    }
}
