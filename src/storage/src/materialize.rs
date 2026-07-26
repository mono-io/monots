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

//! Materialize [`LogEvent`] Arrow from WAL by [`LsnRange`] when `batches` is absent.

use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use common::{CdcEvent, LogEvent, LsnRange, Result, TsdbError};

use crate::wal::format::numbered_wal_path;
use crate::wal::{
    find_wal_file_for_lsn as find_memtable_for_lsn_on_disk,
    next_wal_file_after as next_memtable_after_on_disk, WalFrameCursor, WalLoadCache, WalLoadKey,
};
use crate::LsmTable;

/// Fill [`LogEvent::batches`] from WAL frames whose LSN falls in `event.lsn`.
pub fn materialize_log_event_with_cache(
    table: &LsmTable,
    cache: Option<Arc<WalLoadCache>>,
    mut event: LogEvent,
) -> Result<LogEvent> {
    if event.has_batches() {
        return Ok(event);
    }

    let key = WalLoadKey::new(Arc::clone(&table.name), event.lsn);
    if let Some(cache) = cache.as_ref() {
        if let Some(batches) = cache.get_batches(&key) {
            event.set_batches((*batches).clone());
            return Ok(event);
        }
    }

    let batches = read_wal_batches_for_lsn_range(&table.wal_root(), table.data_dir(), event.lsn)?;
    if batches.is_empty() {
        return Err(TsdbError::Storage(format!(
            "no WAL frames for LSN range [{}, {}]",
            event.lsn.base_lsn, event.lsn.max_lsn
        )));
    }

    let batches = if let Some(cache) = cache.as_ref() {
        (*cache.put_batches(key, batches)).clone()
    } else {
        batches
    };
    event.set_batches(batches);
    Ok(event)
}

/// Materialize log payloads inside a [`CdcEvent`]; file events pass through.
pub fn materialize_logical_event_with_cache(
    table: &LsmTable,
    cache: Option<Arc<WalLoadCache>>,
    event: CdcEvent,
) -> Result<CdcEvent> {
    match event {
        CdcEvent::Insert(log) => Ok(CdcEvent::Insert(materialize_log_event_with_cache(
            table, cache, log,
        )?)),
        other => Ok(other),
    }
}

/// Read all WAL batch frames with `lsn ∈ [range.base_lsn, range.max_lsn]` (LSN order).
pub fn read_wal_batches_for_lsn_range(
    wal_root: &Path,
    table_data_dir: &Path,
    range: LsnRange,
) -> Result<Vec<RecordBatch>> {
    let mut out = Vec::new();
    let Some(mut mid) = find_memtable_for_lsn_on_disk(table_data_dir, range.base_lsn)? else {
        return Ok(out);
    };

    loop {
        let (frames, exceeded_max) = read_memtable_frames_in_range(wal_root, mid, range)?;
        out.extend(frames);
        if exceeded_max {
            break;
        }
        match next_memtable_after_on_disk(table_data_dir, mid)? {
            Some(next) => mid = next,
            None => break,
        }
    }
    Ok(out)
}

/// Returns `(batches in range, saw_frame_with_lsn_gt_max)`.
fn read_memtable_frames_in_range(
    wal_root: &Path,
    file_id: u64,
    range: LsnRange,
) -> Result<(Vec<RecordBatch>, bool)> {
    let path = numbered_wal_path(wal_root, file_id);
    let mut out = Vec::new();
    let Some(mut cursor) = WalFrameCursor::open(&path, file_id)? else {
        return Ok((out, false));
    };
    while let Some(ev) = cursor.next_batch()? {
        if ev.lsn < range.base_lsn {
            continue;
        }
        if ev.lsn > range.max_lsn {
            return Ok((out, true));
        }
        out.push(ev.batch);
    }
    Ok((out, false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryController;
    use crate::memtable::{DEFAULT_MEMTABLE_BATCH_MAX_BYTES, DEFAULT_MEMTABLE_BATCH_MAX_ROWS};
    use crate::wal::format::{numbered_wal_path, FramedSegmentWriter};
    use crate::wal::{WalDurabilityMode, WalWriterOptions};
    use crate::LsmTable;
    use arrow::array::{AsArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use common::WAL_SEGMENTS_DIR;
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
                Arc::new(Int64Array::from(vec![ts])),
            ],
        )
        .unwrap()
    }

    fn open_table(dir: &std::path::Path) -> Arc<LsmTable> {
        let memory = Arc::new(MemoryController::new(64 * 1024 * 1024));
        LsmTable::open(
            "metrics",
            dir,
            schema(),
            1024 * 1024,
            DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
            DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
            memory,
            vec![],
            WalWriterOptions::for_test_table(WalDurabilityMode::Async),
        )
        .unwrap()
    }

    fn write_sealed_wal(table_dir: &std::path::Path, file_id: u64, lsns: &[u64]) {
        let wal_root = table_dir.join(WAL_SEGMENTS_DIR);
        std::fs::create_dir_all(&wal_root).unwrap();
        let path = numbered_wal_path(&wal_root, file_id);
        let mut w = FramedSegmentWriter::create(path, file_id, schema()).unwrap();
        for &lsn in lsns {
            w.append_batch(&batch(lsn as i64), lsn, true).unwrap();
        }
    }

    #[test]
    fn read_wal_batches_filters_inclusive_lsn_range() {
        let dir = tempfile::tempdir().unwrap();
        write_sealed_wal(dir.path(), 9, &[10, 20, 30, 40]);
        let batches = read_wal_batches_for_lsn_range(
            &dir.path().join(WAL_SEGMENTS_DIR),
            dir.path(),
            LsnRange::new(20, 30),
        )
        .unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches[0].column(0).as_primitive::<Int64Type>().value(0),
            20
        );
        assert_eq!(
            batches[1].column(0).as_primitive::<Int64Type>().value(0),
            30
        );
    }

    #[test]
    fn materialize_populates_and_hits_cache_even_after_wal_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let table = open_table(dir.path());
        // Use a high file id so it does not collide with the active writer segment.
        write_sealed_wal(table.data_dir(), 100, &[100, 101, 102]);

        let cache = WalLoadCache::new(32 << 20);
        let range = LsnRange::new(100, 101);
        let filled = materialize_log_event_with_cache(
            &table,
            Some(Arc::clone(&cache)),
            LogEvent::from_lsn_range(range.base_lsn, range.max_lsn),
        )
        .unwrap();
        assert_eq!(filled.batches.as_ref().unwrap().len(), 2);
        assert!(cache.used_bytes() > 0);
        assert!(cache
            .get_batches(&WalLoadKey::new(Arc::clone(&table.name), range))
            .is_some());

        // Delete the sealed WAL file — second materialize must still succeed via cache.
        let path = numbered_wal_path(&table.wal_root(), 100);
        std::fs::remove_file(&path).unwrap();
        assert!(!path.exists());

        let again = materialize_log_event_with_cache(
            &table,
            Some(Arc::clone(&cache)),
            LogEvent::from_lsn_range(range.base_lsn, range.max_lsn),
        )
        .unwrap();
        assert_eq!(again.batches.as_ref().unwrap().len(), 2);
        assert_eq!(
            again.batches.as_ref().unwrap()[0]
                .column(0)
                .as_primitive::<Int64Type>()
                .value(0),
            100
        );
    }

    #[test]
    fn materialize_without_cache_fails_after_wal_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let table = open_table(dir.path());
        write_sealed_wal(table.data_dir(), 100, &[50]);
        let filled =
            materialize_log_event_with_cache(&table, None, LogEvent::from_lsn(50)).unwrap();
        assert_eq!(filled.batches.as_ref().unwrap().len(), 1);

        std::fs::remove_file(numbered_wal_path(&table.wal_root(), 100)).unwrap();
        let err = materialize_log_event_with_cache(&table, None, LogEvent::from_lsn(50))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no WAL frames") || err.contains("WAL"),
            "{err}"
        );
    }

    /// Compare cold WAL decode vs warm `WalLoadCache` hit on a multi-frame sealed segment.
    ///
    /// Run with `-- --nocapture` to print timings. Asserts a clear hit speedup so
    /// regressions in the cache path fail CI.
    #[test]
    fn materialize_cache_hit_is_much_faster_than_wal_miss() {
        use std::hint::black_box;
        use std::time::Instant;

        const FRAMES: u64 = 2_000;
        const ITERS: u32 = 40;
        // Hit should be at least this many times faster than miss on a healthy machine.
        const MIN_SPEEDUP: f64 = 5.0;

        let dir = tempfile::tempdir().unwrap();
        let table = open_table(dir.path());
        let lsns: Vec<u64> = (1..=FRAMES).collect();
        write_sealed_wal(table.data_dir(), 200, &lsns);

        // Mid-range load forces scanning through the sealed file on every miss.
        let range = LsnRange::new(FRAMES / 4, FRAMES / 4 + 63);
        let cache = WalLoadCache::new(64 << 20);

        // Warmup miss (populate cache) + hit.
        let _ = materialize_log_event_with_cache(
            &table,
            Some(Arc::clone(&cache)),
            LogEvent::from_lsn_range(range.base_lsn, range.max_lsn),
        )
        .unwrap();
        let _ = materialize_log_event_with_cache(
            &table,
            Some(Arc::clone(&cache)),
            LogEvent::from_lsn_range(range.base_lsn, range.max_lsn),
        )
        .unwrap();

        let miss_ns = {
            let t0 = Instant::now();
            for _ in 0..ITERS {
                let filled = materialize_log_event_with_cache(
                    &table,
                    None,
                    LogEvent::from_lsn_range(range.base_lsn, range.max_lsn),
                )
                .unwrap();
                black_box(filled.batches.as_ref().map(|b| b.len()));
            }
            t0.elapsed().as_nanos() as f64 / f64::from(ITERS)
        };

        let hit_ns = {
            let t0 = Instant::now();
            for _ in 0..ITERS {
                let filled = materialize_log_event_with_cache(
                    &table,
                    Some(Arc::clone(&cache)),
                    LogEvent::from_lsn_range(range.base_lsn, range.max_lsn),
                )
                .unwrap();
                black_box(filled.batches.as_ref().map(|b| b.len()));
            }
            t0.elapsed().as_nanos() as f64 / f64::from(ITERS)
        };

        let speedup = miss_ns / hit_ns.max(1.0);
        eprintln!(
            "wal_load_cache efficiency: frames={FRAMES} range=[{},{}] \
             miss_avg={miss_ns:.0}ns hit_avg={hit_ns:.0}ns speedup={speedup:.1}x",
            range.base_lsn, range.max_lsn
        );

        assert!(
            speedup >= MIN_SPEEDUP,
            "expected cache hit ≥{MIN_SPEEDUP}x faster than WAL miss, got {speedup:.2}x \
             (miss={miss_ns:.0}ns hit={hit_ns:.0}ns)"
        );
        assert!(
            hit_ns < miss_ns,
            "hit ({hit_ns:.0}ns) should be faster than miss ({miss_ns:.0}ns)"
        );
    }
}
