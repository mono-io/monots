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

//! Unified Arrow loader for stream sink writes.
//!
//! Routing (never mix sources):
//! - [`DataEvent::Insert`] + [`InsertArrow::Deferred`] → WAL by LSN
//! - [`DataEvent::Insert`] + [`InsertArrow::Resident`] → already in memory
//! - [`DataEvent::FlushFile`] → read Parquet path directly (**never WAL**)

use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common::{LogEvent, LsnRange, Result, TsdbError};
use monots_storage::{
    materialize_log_event_with_cache, read_parquet_file, LsmEngine, LsmTable, ParquetReadOptions,
    WalLoadCache,
};

use crate::model::event::{DataEvent, InsertArrow};

/// Loads stream Arrow batches on demand (sink side), not at Source dequeue.
#[derive(Clone)]
pub struct StreamArrowLoader {
    table: Arc<LsmTable>,
    cache: Arc<WalLoadCache>,
}

impl StreamArrowLoader {
    pub fn new(table: Arc<LsmTable>, cache: Arc<WalLoadCache>) -> Self {
        Self { table, cache }
    }

    pub fn from_engine(engine: &LsmEngine, table_name: &str) -> Result<Self> {
        let table = engine
            .get_table(table_name)
            .ok_or_else(|| TsdbError::TableNotFound(table_name.to_string()))?;
        Ok(Self::new(table, engine.wal_load_cache()))
    }

    pub fn table_schema(&self) -> SchemaRef {
        self.table.schema()
    }

    /// Stream Arrow batches for an Insert LSN range from WAL.
    pub fn load_wal_insert(&self, lsn: LsnRange) -> Result<Vec<RecordBatch>> {
        let log = LogEvent::from_lsn_range(lsn.base_lsn, lsn.max_lsn);
        let filled = materialize_log_event_with_cache(
            self.table.as_ref(),
            Some(Arc::clone(&self.cache)),
            log,
        )
        .map_err(|e| {
            TsdbError::Storage(format!(
                "WAL load failed for Insert LSN [{}, {}]: {e}",
                lsn.base_lsn, lsn.max_lsn
            ))
        })?;
        Ok(filled.batches.unwrap_or_default())
    }

    /// Stream Arrow batches from a Parquet SST / pending file path (no WAL).
    pub fn load_parquet_batches(&self, path: impl AsRef<Path>) -> Result<Vec<RecordBatch>> {
        let path = path.as_ref();
        read_parquet_file(path, self.table.schema(), &ParquetReadOptions::default()).map_err(|e| {
            TsdbError::Storage(format!("Parquet load failed for {}: {e}", path.display()))
        })
    }

    /// Resolve Arrow rows for any data event.
    ///
    /// - Deferred Insert → WAL  
    /// - Resident Insert → clone batches  
    /// - FlushFile → Parquet file directly (optimization: skip WAL entirely)
    pub fn load_event_batches(&self, event: &DataEvent) -> Result<Vec<RecordBatch>> {
        match event {
            DataEvent::Insert {
                lsn,
                arrow: InsertArrow::Deferred,
            } => self.load_wal_insert(*lsn),
            DataEvent::Insert {
                arrow: InsertArrow::Resident { batches, .. },
                ..
            } => Ok(batches.clone()),
            DataEvent::FlushFile { file_path, .. } => self.load_parquet_batches(file_path.as_ref()),
            DataEvent::Watermark { .. } => Ok(Vec::new()),
        }
    }

    /// Prepare an event for sink write that consumes in-memory Insert Arrow.
    ///
    /// FlushFile is left as a path (filesystem sinks copy the file; no Arrow needed).
    /// Only Deferred Insert is filled from WAL.
    pub fn ensure_for_write(&self, event: &DataEvent) -> Result<DataEvent> {
        match event {
            DataEvent::Insert {
                lsn,
                arrow: InsertArrow::Deferred,
            } => {
                let batches = self.load_wal_insert(*lsn)?;
                Ok(DataEvent::Insert {
                    lsn: *lsn,
                    arrow: InsertArrow::resident(batches),
                })
            }
            // Parquet path events: keep path; Arrow sinks call [`Self::load_event_batches`].
            other => Ok(other.clone()),
        }
    }

    /// Alias kept for call sites that only care about Insert materialization.
    #[inline]
    pub fn ensure_insert_arrow(&self, event: &DataEvent) -> Result<DataEvent> {
        self.ensure_for_write(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::LsnRange;

    #[test]
    fn flush_file_is_classified_as_parquet_not_wal() {
        let ev = DataEvent::FlushFile {
            lsn: LsnRange::new(1, 2),
            file_path: "/pending/flush/a.parquet".into(),
            rows: 10,
        };
        assert!(!ev.insert_needs_load());
        // Routing contract: FlushFile must not be treated as WAL deferred Insert.
        assert!(matches!(ev, DataEvent::FlushFile { .. }));
    }

    #[test]
    fn deferred_insert_needs_wal_load() {
        let ev = DataEvent::insert_deferred(LsnRange::new(1, 2));
        assert!(ev.insert_needs_load());
    }
}
