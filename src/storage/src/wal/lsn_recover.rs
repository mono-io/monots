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

//! Recover the global LSN high-water mark and replay unflushed WAL batches into SST.
//!
//! # Recovery model
//!
//! - Online flush is driven by **MemTable memory size**, sealed with a durable
//!   [`RecordType::MemTableEnd`] WAL frame (fsynced) at freeze time.
//! - Durable truth after flush is **SST `[base_lsn, max_lsn]`**.
//! - On crash: scan WAL batch frames and rebuild only those with `lsn > sst_max_lsn`
//!   (or `lsn == 0` when SST has no files yet — single-node / replication-off).
//! - [`RecordType::MemTableEnd`] splits recover partitions so each sealed memtable
//!   becomes its own Parquet (matching online freeze boundaries).
//! - **Unclosed tails** (batches after the last `MemTableEnd`, or EOF without one) are flushed
//!   to Parquet on recovery, then open segments get a synthetic durable `MemTableEnd` (fsync).

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use arrow::record_batch::RecordBatch;
use common::{Result, WAL_SEGMENTS_DIR};

use crate::compaction::sst::{FileIndex, SstMeta};
use crate::wal::bulk_load::{BulkLoadWal, BULK_LOAD_DIR_NAME};
use crate::wal::format::{
    crc32, decode_batch_ipc, decode_memtable_end_payload, decode_schema_ipc,
    list_numbered_wal_paths, list_wal_file_ids, numbered_wal_path, wal_segment_tail, FrameHeader,
    FramedSegmentWriter, RecordType, SegmentHeader, PAYLOAD_FORMAT_ARROW_IPC,
    SCHEMA_FRAME_SEQUENCE,
};

/// One recover partition emitted at a [`RecordType::MemTableEnd`] boundary.
#[derive(Debug)]
pub struct WalRecoverPartition {
    pub batches: Vec<RecordBatch>,
    pub base_lsn: u64,
    pub max_lsn: u64,
}

/// Max LSN stamped on any complete batch frame in one segment (header-only; skips payloads).
pub fn max_lsn_in_segment(path: &Path) -> Result<u64> {
    Ok(lsn_range_in_segment(path)?.1)
}

/// Inclusive `(min_lsn, max_lsn)` of batch frames with **real** LSN `> 0`. `(0,0)` if none.
pub fn lsn_range_in_segment(path: &Path) -> Result<(u64, u64)> {
    if !path.exists() {
        return Ok((0, 0));
    }
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    match SegmentHeader::decode(&mut reader) {
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "skip unreadable WAL segment for LSN recover");
            return Ok((0, 0));
        }
    }

    let mut min_lsn = u64::MAX;
    let mut max_lsn = 0u64;
    loop {
        let frame = match FrameHeader::decode(&mut reader) {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => {
                tracing::debug!(path = %path.display(), error = %e, "stop LSN scan at torn WAL tail");
                break;
            }
        };
        if matches!(frame.record_type, RecordType::Batch) && frame.lsn > 0 {
            min_lsn = min_lsn.min(frame.lsn);
            max_lsn = max_lsn.max(frame.lsn);
        }
        if matches!(frame.record_type, RecordType::Footer) {
            break;
        }
        let len = frame.body_len() as u64;
        if len > 0 {
            if let Err(e) = reader.seek(SeekFrom::Current(len as i64)) {
                tracing::debug!(path = %path.display(), error = %e, "stop LSN scan: cannot skip payload");
                break;
            }
        }
    }
    if max_lsn == 0 {
        Ok((0, 0))
    } else {
        Ok((min_lsn, max_lsn))
    }
}

/// Whether the segment contains at least one complete batch frame (LSN may be 0).
pub fn segment_has_batches(path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    if SegmentHeader::decode(&mut reader).is_err() {
        return Ok(false);
    }
    loop {
        let frame = match FrameHeader::decode(&mut reader) {
            Ok(Some(f)) => f,
            _ => break,
        };
        if matches!(frame.record_type, RecordType::Batch) {
            return Ok(true);
        }
        if matches!(frame.record_type, RecordType::Footer) {
            break;
        }
        let len = frame.body_len() as u64;
        if len > 0 {
            let _ = reader.seek(SeekFrom::Current(len as i64));
        }
    }
    Ok(false)
}

/// Max LSN across numbered WAL files + BulkLoad WAL for one table.
pub fn max_lsn_in_table_wals(table_data_dir: &Path) -> Result<u64> {
    let mut max_lsn = 0u64;
    let wal_root = table_data_dir.join(WAL_SEGMENTS_DIR);
    if wal_root.is_dir() {
        for path in list_numbered_wal_paths(&wal_root)? {
            max_lsn = max_lsn.max(max_lsn_in_segment(&path)?);
        }
    }
    max_lsn = max_lsn.max(max_bulk_load_lsn(table_data_dir)?);
    Ok(max_lsn)
}

/// Compat: LSN span across the flat WAL chain (no per-memtable Bind ownership).
pub fn max_lsn_in_memtable_wal(table_data_dir: &Path, _memtable_id: u64) -> Result<u64> {
    Ok(lsn_range_in_memtable_wal(table_data_dir, _memtable_id)?.1)
}

/// Compat: LSN span across the flat WAL chain.
pub fn lsn_range_in_memtable_wal(table_data_dir: &Path, _memtable_id: u64) -> Result<(u64, u64)> {
    let wal_root = table_data_dir.join(WAL_SEGMENTS_DIR);
    let mut min_lsn = u64::MAX;
    let mut max_lsn = 0u64;
    for path in list_numbered_wal_paths(&wal_root)? {
        let (lo, hi) = lsn_range_in_segment(&path)?;
        if hi == 0 {
            continue;
        }
        min_lsn = min_lsn.min(lo);
        max_lsn = max_lsn.max(hi);
    }
    if max_lsn == 0 {
        Ok((0, 0))
    } else {
        Ok((min_lsn, max_lsn))
    }
}

fn max_bulk_load_lsn(table_data_dir: &Path) -> Result<u64> {
    let path = table_data_dir
        .join(WAL_SEGMENTS_DIR)
        .join(BULK_LOAD_DIR_NAME);
    if !path.is_dir() {
        return Ok(0);
    }
    Ok(BulkLoadWal::open(table_data_dir)?
        .all()
        .into_iter()
        .map(|e| e.lsn)
        .max()
        .unwrap_or(0))
}

pub fn max_lsn_in_sst_metas(metas: &[SstMeta]) -> u64 {
    metas.iter().map(|m| m.max_lsn).max().unwrap_or(0)
}

pub fn sst_has_lsn_watermark(file_index: &FileIndex) -> bool {
    file_index.snapshot().iter().any(|m| m.has_lsn_bounds())
}

/// Numbered files that contain at least one batch frame.
pub fn data_bearing_wal_file_ids(table_data_dir: &Path) -> Result<Vec<u64>> {
    let wal_root = table_data_dir.join(WAL_SEGMENTS_DIR);
    let mut out = Vec::new();
    for id in list_wal_file_ids(&wal_root)? {
        let path = numbered_wal_path(&wal_root, id);
        if segment_has_batches(&path)? {
            out.push(id);
        }
    }
    Ok(out)
}

/// Compat name retained; returns empty (ownership is by LSN, not memtable id).
pub fn data_bearing_memtable_wal_ids(_table_data_dir: &Path) -> Result<Vec<u64>> {
    Ok(vec![])
}

pub fn has_recoverable_memtable_wal(table_data_dir: &Path) -> Result<bool> {
    Ok(!data_bearing_wal_file_ids(table_data_dir)?.is_empty())
}

/// Whether this batch frame still needs crash rebuild given durable SST frontier.
#[inline]
fn batch_needs_recover(frame_lsn: u64, sst_max_lsn: u64, sst_has_files: bool) -> bool {
    if frame_lsn > 0 {
        frame_lsn > sst_max_lsn
    } else {
        // Replication-off (LSN=0): rebuild only when no SST exists yet. After recover flush,
        // SST presence means WAL content is already durable — do not re-inflate.
        !sst_has_files
    }
}

/// Drop sealed WAL when its max LSN is covered by SST (or file has no real LSN / no batches).
///
/// Keeps at least one data-bearing file when SST has no LSN watermark (allocator fallback).
pub fn can_drop_wal_file(
    table_data_dir: &Path,
    file_id: u64,
    file_index: &FileIndex,
) -> Result<bool> {
    let path = numbered_wal_path(&table_data_dir.join(WAL_SEGMENTS_DIR), file_id);
    let (_, hi) = lsn_range_in_segment(&path)?;
    if hi > 0 {
        if !file_index.covers_lsn(hi) {
            return Ok(false);
        }
        return Ok(true);
    }
    // No real LSN on this file.
    if !segment_has_batches(&path)? {
        return Ok(true);
    }
    if sst_has_lsn_watermark(file_index) {
        // SST already carries the clock; LSN=0 sealed files are redundant.
        return Ok(true);
    }
    if !file_index.snapshot().is_empty() {
        // SST exists without LSN (replication-off recover) — sealed LSN=0 WAL is redundant.
        return Ok(true);
    }
    // Keep one data-bearing file as the sole watermark seed.
    let bearing = data_bearing_wal_file_ids(table_data_dir)?;
    Ok(bearing.iter().filter(|&&id| id != file_id).count() > 0)
}

/// Compat: after a memtable flush, sealed GC uses [`can_drop_wal_file`].
pub fn can_drop_wal_for_lsn_watermark(
    table_data_dir: &Path,
    _memtable_id: u64,
    file_index: &FileIndex,
) -> Result<bool> {
    if sst_has_lsn_watermark(file_index) {
        return Ok(true);
    }
    Ok(data_bearing_wal_file_ids(table_data_dir)?.len() > 1)
}

/// Locate the numbered WAL file whose LSN span contains `lsn`.
pub fn find_wal_file_for_lsn(table_data_dir: &Path, lsn: u64) -> Result<Option<u64>> {
    let wal_root = table_data_dir.join(WAL_SEGMENTS_DIR);
    let ids = list_wal_file_ids(&wal_root)?;
    let mut best: Option<u64> = None;
    for id in &ids {
        let path = numbered_wal_path(&wal_root, *id);
        let (base, max) = lsn_range_in_segment(&path)?;
        if max == 0 {
            continue;
        }
        let base = if base == 0 { max } else { base };
        if base <= lsn && lsn <= max {
            return Ok(Some(*id));
        }
        if base <= lsn {
            best = Some(*id);
        }
    }
    Ok(best.or_else(|| ids.into_iter().next()))
}

/// Next numbered WAL file id strictly greater than `file_id`.
pub fn next_wal_file_after(table_data_dir: &Path, file_id: u64) -> Result<Option<u64>> {
    Ok(list_wal_file_ids(&table_data_dir.join(WAL_SEGMENTS_DIR))?
        .into_iter()
        .find(|id| *id > file_id))
}

/// Walk WAL batches that still need recovery, emitting partitions at
/// [`RecordType::MemTableEnd`] boundaries and at unclosed segment tails.
///
/// - `sst_max_lsn`: max LSN already durable in SST (frames with `lsn > sst_max_lsn` are rebuilt).
/// - `sst_has_files`: when true, LSN=0 frames are skipped (replication-off already recovered).
/// - Unclosed open segments: flush recoverable batches to Parquet, then append `MemTableEnd` + fsync.
pub fn walk_unflushed_partitions(
    table_data_dir: &Path,
    sst_max_lsn: u64,
    sst_has_files: bool,
    _soft_limit_bytes: usize,
    mut on_partition: impl FnMut(WalRecoverPartition) -> Result<()>,
) -> Result<()> {
    let wal_root = table_data_dir.join(WAL_SEGMENTS_DIR);
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut base_lsn = 0u64;
    let mut max_lsn = 0u64;

    let mut emit =
        |batches: &mut Vec<RecordBatch>, base_lsn: &mut u64, max_lsn: &mut u64| -> Result<()> {
            if batches.is_empty() {
                *base_lsn = 0;
                *max_lsn = 0;
                return Ok(());
            }
            on_partition(WalRecoverPartition {
                batches: std::mem::take(batches),
                base_lsn: *base_lsn,
                max_lsn: *max_lsn,
            })?;
            *base_lsn = 0;
            *max_lsn = 0;
            Ok(())
        };

    let discard_open = |batches: &mut Vec<RecordBatch>, base_lsn: &mut u64, max_lsn: &mut u64| {
        batches.clear();
        *base_lsn = 0;
        *max_lsn = 0;
    };

    for path in list_numbered_wal_paths(&wal_root)? {
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "skip unreadable WAL during recover walk");
                continue;
            }
        };
        let mut reader = BufReader::new(file);
        let header = match SegmentHeader::decode(&mut reader) {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "skip WAL header during recover walk");
                continue;
            }
        };

        let mut logical_active = header.memtable_id;
        let mut open_tail_has_batches = false;
        let mut open_tail_max_lsn = 0u64;
        let mut segment_complete = false;

        let mut schema = None;
        let mut payload_buf = Vec::new();
        loop {
            let frame = match FrameHeader::decode(&mut reader) {
                Ok(Some(f)) => f,
                Ok(None) => break,
                Err(e) => {
                    tracing::debug!(path = %path.display(), error = %e, "stop recover walk at torn tail");
                    break;
                }
            };
            payload_buf.resize(frame.body_len(), 0);
            if reader.read_exact(&mut payload_buf).is_err() {
                tracing::debug!(path = %path.display(), "stop recover walk at truncated payload");
                break;
            }
            if crc32(&payload_buf) != frame.crc32 {
                tracing::warn!(path = %path.display(), "stop recover walk at crc mismatch");
                break;
            }

            match frame.record_type {
                RecordType::Schema => {
                    if frame.payload_format == PAYLOAD_FORMAT_ARROW_IPC
                        && frame.sequence == SCHEMA_FRAME_SEQUENCE
                    {
                        if let Ok(s) = decode_schema_ipc(&payload_buf) {
                            schema = Some(s);
                        }
                    }
                }
                RecordType::Batch => {
                    open_tail_has_batches = true;
                    if frame.lsn > 0 {
                        open_tail_max_lsn = open_tail_max_lsn.max(frame.lsn);
                    }
                    if !batch_needs_recover(frame.lsn, sst_max_lsn, sst_has_files) {
                        continue;
                    }
                    match decode_batch_ipc(schema.as_ref(), &payload_buf) {
                        Ok(batch) => {
                            if frame.lsn > 0 {
                                if base_lsn == 0 || frame.lsn < base_lsn {
                                    base_lsn = frame.lsn;
                                }
                                max_lsn = max_lsn.max(frame.lsn);
                            }
                            batches.push(batch);
                        }
                        Err(e) => {
                            tracing::warn!(
                                path = %path.display(),
                                error = %e,
                                "stop recover walk at bad batch"
                            );
                            break;
                        }
                    }
                }
                RecordType::Footer => {
                    segment_complete = true;
                    break;
                }
                RecordType::MemTableEnd => {
                    let (_closed, new_id) = decode_memtable_end_payload(&payload_buf);
                    if new_id > 0 {
                        logical_active = new_id;
                    }
                    open_tail_has_batches = false;
                    open_tail_max_lsn = 0;
                    emit(&mut batches, &mut base_lsn, &mut max_lsn)?;
                }
                RecordType::Unknown(_) => {}
            }
        }

        if open_tail_has_batches {
            emit(&mut batches, &mut base_lsn, &mut max_lsn)?;
            if segment_complete {
                tracing::debug!(
                    path = %path.display(),
                    end_lsn = open_tail_max_lsn,
                    closed_memtable_id = logical_active,
                    "open WAL tail before footer cannot be sealed in place"
                );
            } else {
                let closed = logical_active;
                let new_id = closed.saturating_add(1);
                seal_open_wal_tail(&path, open_tail_max_lsn, closed, new_id)?;
            }
        } else {
            discard_open(&mut batches, &mut base_lsn, &mut max_lsn);
        }
    }

    Ok(())
}

/// Append a durable `MemTableEnd` to an in-progress WAL segment (crash recovery only).
fn seal_open_wal_tail(
    path: &Path,
    end_lsn: u64,
    closed_memtable_id: u64,
    new_memtable_id: u64,
) -> Result<()> {
    let tail = wal_segment_tail(path, closed_memtable_id)?;
    if tail.has_footer {
        tracing::debug!(
            path = %path.display(),
            "skip recovery WAL seal: segment already has footer"
        );
        return Ok(());
    }
    let mut writer = FramedSegmentWriter::resume_any(path.to_path_buf())?;
    writer.append_memtable_end(end_lsn, closed_memtable_id, new_memtable_id)?;
    tracing::info!(
        path = %path.display(),
        end_lsn,
        closed_memtable_id,
        new_memtable_id,
        "recovery sealed open WAL memtable tail with MemTableEnd"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::format::{numbered_wal_path, FramedSegmentWriter};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(Int64Array::from(vec![2i64])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn scans_max_lsn_from_segment_headers() {
        let dir = tempdir().unwrap();
        let wal_root = dir.path().join(WAL_SEGMENTS_DIR);
        std::fs::create_dir_all(&wal_root).unwrap();
        let path = numbered_wal_path(&wal_root, 1);
        let schema = batch().schema();
        let mut w = FramedSegmentWriter::create(path.clone(), 1, schema).unwrap();
        w.append_batch(&batch(), 10, true).unwrap();
        w.append_batch(&batch(), 42, true).unwrap();
        w.finish().unwrap();
        assert_eq!(max_lsn_in_segment(&path).unwrap(), 42);
        assert_eq!(max_lsn_in_table_wals(dir.path()).unwrap(), 42);
    }

    #[test]
    fn lsn_zero_batches_do_not_invent_watermark() {
        let dir = tempdir().unwrap();
        let wal_root = dir.path().join(WAL_SEGMENTS_DIR);
        std::fs::create_dir_all(&wal_root).unwrap();
        let path = numbered_wal_path(&wal_root, 1);
        let schema = batch().schema();
        let mut w = FramedSegmentWriter::create(path.clone(), 1, schema).unwrap();
        w.append_batch(&batch(), 0, true).unwrap();
        w.finish().unwrap();
        assert_eq!(lsn_range_in_segment(&path).unwrap(), (0, 0));
        assert!(segment_has_batches(&path).unwrap());
    }

    #[test]
    fn refuses_to_drop_last_data_wal_without_sst_lsn() {
        let dir = tempdir().unwrap();
        let wal_root = dir.path().join(WAL_SEGMENTS_DIR);
        std::fs::create_dir_all(&wal_root).unwrap();
        let path = numbered_wal_path(&wal_root, 7);
        let schema = batch().schema();
        let mut w = FramedSegmentWriter::create(path, 7, schema).unwrap();
        w.append_batch(&batch(), 5, true).unwrap();
        w.finish().unwrap();

        let index = FileIndex::new();
        assert!(!can_drop_wal_file(dir.path(), 7, &index).unwrap());
        assert!(!can_drop_wal_for_lsn_watermark(dir.path(), 7, &index).unwrap());
    }

    fn memtable_end_frames(path: &Path) -> Vec<(u64, u64, u64)> {
        let file = File::open(path).unwrap();
        let mut reader = BufReader::new(file);
        SegmentHeader::decode(&mut reader).unwrap();
        let mut out = Vec::new();
        let mut payload_buf = Vec::new();
        loop {
            let frame = match FrameHeader::decode(&mut reader) {
                Ok(Some(f)) => f,
                _ => break,
            };
            payload_buf.resize(frame.body_len(), 0);
            if reader.read_exact(&mut payload_buf).is_err() {
                break;
            }
            if frame.record_type == RecordType::MemTableEnd {
                let (closed, new) = decode_memtable_end_payload(&payload_buf);
                out.push((frame.lsn, closed, new));
            }
        }
        out
    }

    #[test]
    fn walk_flushes_open_tail_to_parquet_and_seals_wal() {
        let dir = tempdir().unwrap();
        let wal_root = dir.path().join(WAL_SEGMENTS_DIR);
        std::fs::create_dir_all(&wal_root).unwrap();
        let path = numbered_wal_path(&wal_root, 1);
        let schema = batch().schema();
        let mut w = FramedSegmentWriter::create(path.clone(), 1, schema).unwrap();
        w.append_batch(&batch(), 10, true).unwrap();
        w.append_batch(&batch(), 20, true).unwrap();
        w.append_batch(&batch(), 30, true).unwrap();
        // Leave segment open (crash) — recovery flushes unclosed tail + seals MemTableEnd.

        let mut parts = Vec::new();
        walk_unflushed_partitions(dir.path(), 20, true, usize::MAX, |p| {
            parts.push((p.batches.len(), p.base_lsn, p.max_lsn));
            Ok(())
        })
        .unwrap();
        assert_eq!(
            parts,
            vec![(1, 30, 30)],
            "only batches past SST frontier are rebuilt"
        );
        assert_eq!(
            memtable_end_frames(&path),
            vec![(30, 1, 2)],
            "recovery must seal open WAL tail"
        );
    }

    #[test]
    fn walk_flushes_at_memtable_end_and_open_tail() {
        let dir = tempdir().unwrap();
        let wal_root = dir.path().join(WAL_SEGMENTS_DIR);
        std::fs::create_dir_all(&wal_root).unwrap();
        let path = numbered_wal_path(&wal_root, 1);
        let schema = batch().schema();
        let mut w = FramedSegmentWriter::create(path.clone(), 1, schema).unwrap();
        w.append_batch(&batch(), 10, true).unwrap();
        w.append_batch(&batch(), 20, true).unwrap();
        w.append_memtable_end(20, 1, 2).unwrap();
        w.append_batch(&batch(), 30, true).unwrap();

        let mut parts = Vec::new();
        walk_unflushed_partitions(dir.path(), 0, false, usize::MAX, |p| {
            parts.push((p.batches.len(), p.base_lsn, p.max_lsn));
            Ok(())
        })
        .unwrap();
        assert_eq!(parts, vec![(2, 10, 20), (1, 30, 30)]);
        assert_eq!(
            memtable_end_frames(&path),
            vec![(20, 1, 2), (30, 2, 3)],
            "recovery flushes trailing open memtable and seals WAL"
        );
    }

    #[test]
    fn scans_table_including_bulk_load() {
        let dir = tempdir().unwrap();
        let wal_root = dir.path().join(WAL_SEGMENTS_DIR);
        std::fs::create_dir_all(&wal_root).unwrap();
        let path = numbered_wal_path(&wal_root, 1);
        let schema = batch().schema();
        let mut w = FramedSegmentWriter::create(path, 1, schema).unwrap();
        w.append_batch(&batch(), 3, true).unwrap();
        w.finish().unwrap();
        assert_eq!(max_lsn_in_table_wals(dir.path()).unwrap(), 3);
    }
}
