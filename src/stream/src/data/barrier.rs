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

//! Checkpoint barrier (2PC txn boundary) and async pending-file GC.
//!
//! Progress for Parquet is owned by [`crate::data::worker::SinkWorker`].
//! The barrier only carries watermark hints + pending paths to unlink after commit.

use std::path::PathBuf;

use crate::model::event::DataEvent;

/// Fire-and-forget background deletion (replaces global PendingUnlinkHub).
pub fn async_gc_paths(paths: Vec<PathBuf>) {
    if paths.is_empty() {
        return;
    }
    tokio::spawn(async move {
        for path in paths {
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {
                    tracing::debug!(path = %path.display(), "GC: unlinked pending CDC parquet")
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "GC: unlink failed, will delay reclaim"
                    );
                }
            }
        }
    });
}

/// One open Mux→Sink transaction boundary.
#[derive(Debug)]
pub struct CheckpointBarrier {
    pub table: String,
    watermark_lsn: u64,
    /// Pending Parquet paths — GC only after Sink commit succeeds.
    pub files_to_unlink: Vec<PathBuf>,
    events: usize,
}

impl CheckpointBarrier {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            watermark_lsn: 0,
            files_to_unlink: Vec::new(),
            events: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.events == 0 && self.files_to_unlink.is_empty()
    }

    pub fn record_event(&mut self, table: &str, event: &DataEvent) {
        if self.table.is_empty() {
            self.table = table.to_string();
        }
        self.events += 1;
        match event {
            DataEvent::Watermark { end_lsn } => {
                self.watermark_lsn = self.watermark_lsn.max(*end_lsn);
            }
            // Fresh Parquet commit point: GC after Sink commit.
            DataEvent::FlushFile { file_path, .. } => {
                self.files_to_unlink.push(PathBuf::from(file_path.as_ref()));
            }
            DataEvent::Insert { .. } => {}
        }
    }

    /// Queue a pending path for post-commit GC (stale / equal-cursor Parquet).
    pub fn record_unlink(&mut self, table: &str, file_path: impl AsRef<str>) {
        if self.table.is_empty() {
            self.table = table.to_string();
        }
        self.files_to_unlink.push(PathBuf::from(file_path.as_ref()));
    }

    /// Logical progress hint from Watermark only (Parquet progress lives in Sink).
    pub fn progress_lsn(&self) -> Option<u64> {
        if self.watermark_lsn > 0 {
            Some(self.watermark_lsn)
        } else {
            None
        }
    }

    pub fn async_gc_files(self) {
        async_gc_paths(self.files_to_unlink);
    }
}

/// Data-plane events between Dispatcher and SinkWorker.
pub enum PipelineEvent {
    Data(DataEvent),
    Barrier(CheckpointBarrier),
    /// `max_lsn < cursor`: expired replay — no sink write; unlink on Barrier commit.
    Stale {
        table: String,
        event: DataEvent,
    },
    /// `max_lsn == cursor`: already sunk — no fresh write; carries [`DataEvent::FlushFile`]
    /// so open WAL Inserts can degrade to this file and commit-retry can replay it.
    /// Unlink still happens on Barrier commit.
    Degrade {
        table: String,
        event: DataEvent,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use common::LsnRange;
    use std::sync::Arc;

    fn batch() -> arrow::record_batch::RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64])),
                Arc::new(Int64Array::from(vec![1_i64])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn insert_only_has_no_progress_lsn() {
        let mut b = CheckpointBarrier::new("t0");
        b.record_event(
            "t0",
            &DataEvent::insert(LsnRange::new(10, 20), vec![batch()]),
        );
        assert!(!b.is_empty());
        assert_eq!(b.progress_lsn(), None);
    }

    #[test]
    fn parquet_queues_gc_but_does_not_set_barrier_progress() {
        let mut b = CheckpointBarrier::new("t0");
        b.record_event(
            "t0",
            &DataEvent::insert(LsnRange::new(50, 100), vec![batch()]),
        );
        b.record_event(
            "t0",
            &DataEvent::FlushFile {
                lsn: LsnRange::new(1, 40),
                file_path: "/pending/flush/1-1-40-0-0.parquet".into(),
                rows: 10,
            },
        );
        assert_eq!(b.progress_lsn(), None);
        assert_eq!(b.files_to_unlink.len(), 1);
    }

    #[test]
    fn watermark_sets_barrier_progress() {
        let mut b = CheckpointBarrier::new("t0");
        b.record_event("t0", &DataEvent::Watermark { end_lsn: 42 });
        assert_eq!(b.progress_lsn(), Some(42));
    }

    #[tokio::test]
    async fn async_gc_deletes_files() {
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("1-1-1-0-0.parquet");
        let b = tmp.path().join("2-2-2-0-0.parquet");
        std::fs::write(&a, b"a").unwrap();
        std::fs::write(&b, b"b").unwrap();
        async_gc_paths(vec![a.clone(), b.clone()]);
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert!(!a.exists());
        assert!(!b.exists());
    }
}
