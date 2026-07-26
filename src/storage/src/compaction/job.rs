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

use crate::compaction::reader::BatchAligner;
use crate::compaction::sst::{
    compact_tmp_dir, promote_sst_from_compact_tmp, write_sst_streaming, FileIndex, SstMeta,
};
use crate::compaction::sst_id::SstIdentity;
use crate::disk_space::DiskSpaceController;
use crate::memory::MemoryController;
use arrow::array::UInt32Array;
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common::{time_column_index, time_value_at, Result, TsdbError};
use dashmap::DashMap;
use parking_lot::RwLock;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::{sleep, Duration};

/// Bounded queue of pending compaction events; overflow is dropped and reclaimed by the ticker.
pub const COMPACTION_EVENT_QUEUE_CAPACITY: usize = 4096;
/// Default cap on concurrently running merge jobs across all tables.
pub const DEFAULT_COMPACTION_MAX_CONCURRENT_JOBS: usize = 4;

/// Output batch size for streaming compaction (bounds transient heap usage per merge window).
const STREAM_MERGE_BATCH_ROWS: usize = 8192;

/// Rough in-memory expansion of Parquet-on-disk bytes once decoded to Arrow (compression +
/// dictionary/encoding overhead). Used only for transient memory accounting, not correctness.
const PARQUET_INFLATE_FACTOR: usize = 4;

/// Upper bound on how many contiguous files one merge collapses when none is configured.
pub const DEFAULT_COMPACTION_MAX_MERGE_FILES: usize = 8;

/// How the compactor chooses which **contiguous** run of files to merge.
///
/// The candidate is always a contiguous slice of the time-sorted file list, which keeps the merge a
/// linear k-way pass and avoids rewriting non-adjacent history. Strategies differ only in *which*
/// contiguous run they favour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionStrategy {
    /// Merge the contiguous run that collapses the most files while staying under the size
    /// threshold (tie-break: cheapest total bytes). Balanced default for steady ingest.
    SizeTiered,
    /// Merge the longest contiguous run of *small* files (each below `threshold / max_merge_files`).
    /// Best when ingest produces many tiny SSTs and read amplification is the concern.
    FileCount,
    /// Prefer the contiguous run with the most time-range overlap (duplicate timestamps), reclaiming
    /// the most space via dedup. Falls back to `SizeTiered` when nothing overlaps.
    Overlap,
}

impl Default for CompactionStrategy {
    fn default() -> Self {
        Self::SizeTiered
    }
}

impl CompactionStrategy {
    /// Parse a config string; unknown values fall back to the default `SizeTiered`.
    pub fn from_str_lenient(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "file_count" | "filecount" | "count" => Self::FileCount,
            "overlap" | "time_overlap" | "dedup" => Self::Overlap,
            _ => Self::SizeTiered,
        }
    }
}

/// Pick a contiguous run `(start, len)` (with `len >= 2`) to merge, or `None` if nothing qualifies.
///
/// Runs must also be **LSN-contiguous** (no hole between sealed spans). Otherwise a merge would
/// claim `[min_lsn, max_lsn]` while skipping an interleaved BulkLoad / other SST — breaking CDC.
pub fn pick_compaction(
    files: &[SstMeta],
    strategy: CompactionStrategy,
    threshold_bytes: u64,
    max_merge_files: usize,
) -> Option<(usize, usize)> {
    let max_merge_files = max_merge_files.max(2);
    if files.len() < 2 {
        return None;
    }
    let raw = match strategy {
        CompactionStrategy::SizeTiered => pick_size_tiered(files, threshold_bytes, max_merge_files),
        CompactionStrategy::FileCount => pick_file_count(files, threshold_bytes, max_merge_files),
        CompactionStrategy::Overlap => pick_overlap(files, threshold_bytes, max_merge_files),
    }?;
    shrink_to_lsn_contiguous(files, raw)
}

/// True if sealed LSN spans in `metas` form one contiguous coverage (holes ⇒ false).
pub fn lsn_spans_contiguous(metas: &[SstMeta]) -> bool {
    let mut spans: Vec<(u64, u64)> = metas
        .iter()
        .filter(|m| m.has_lsn_bounds())
        .map(|m| (m.base_lsn, m.max_lsn))
        .collect();
    if spans.len() < 2 {
        return true;
    }
    spans.sort_by_key(|(b, _)| *b);
    let mut end = spans[0].1;
    for &(base, max) in &spans[1..] {
        if base > end.saturating_add(1) {
            return false;
        }
        end = end.max(max);
    }
    true
}

/// Shrink or reject a pick so the merged run has no LSN holes.
fn shrink_to_lsn_contiguous(
    files: &[SstMeta],
    (start, len): (usize, usize),
) -> Option<(usize, usize)> {
    if len < 2 || start + len > files.len() {
        return None;
    }
    if lsn_spans_contiguous(&files[start..start + len]) {
        return Some((start, len));
    }
    // Try longest LSN-contiguous prefix of the pick with len >= 2.
    for try_len in (2..len).rev() {
        if lsn_spans_contiguous(&files[start..start + try_len]) {
            return Some((start, try_len));
        }
    }
    // Try sliding windows inside the pick.
    for s in start..start + len {
        for try_len in (2..=(start + len - s)).rev() {
            if lsn_spans_contiguous(&files[s..s + try_len]) {
                return Some((s, try_len));
            }
        }
    }
    None
}

/// Longest under-threshold run (tie-break: cheapest bytes). Files at or above the threshold on their
/// own are never merged (they are already "large enough").
fn pick_size_tiered(
    files: &[SstMeta],
    threshold_bytes: u64,
    max_merge_files: usize,
) -> Option<(usize, usize)> {
    let mut best: Option<(usize, usize, u64)> = None;
    for start in 0..files.len() {
        let mut total = files[start].file_size;
        let mut len = 1usize;
        let mut j = start + 1;
        while j < files.len() && len < max_merge_files {
            let next = total.saturating_add(files[j].file_size);
            if next >= threshold_bytes {
                break;
            }
            total = next;
            len += 1;
            j += 1;
        }
        if len >= 2 {
            let better = match best {
                None => true,
                Some((_, blen, btotal)) => len > blen || (len == blen && total < btotal),
            };
            if better {
                best = Some((start, len, total));
            }
        }
    }
    best.map(|(start, len, _)| (start, len))
}

/// Longest contiguous run of small files (read-amplification reduction).
fn pick_file_count(
    files: &[SstMeta],
    threshold_bytes: u64,
    max_merge_files: usize,
) -> Option<(usize, usize)> {
    let small = (threshold_bytes / max_merge_files as u64).max(1);
    let mut best: Option<(usize, usize)> = None;
    let mut i = 0;
    while i < files.len() {
        if files[i].file_size >= small {
            i += 1;
            continue;
        }
        let mut j = i;
        let mut len = 0usize;
        while j < files.len() && files[j].file_size < small && len < max_merge_files {
            len += 1;
            j += 1;
        }
        if len >= 2 && best.map_or(true, |(_, bl)| len > bl) {
            best = Some((i, len));
        }
        i = j.max(i + 1);
    }
    best
}

/// Under-threshold run with the most overlapping adjacent pairs; falls back to size-tiered.
fn pick_overlap(
    files: &[SstMeta],
    threshold_bytes: u64,
    max_merge_files: usize,
) -> Option<(usize, usize)> {
    let mut best: Option<(usize, usize, usize)> = None;
    for start in 0..files.len() {
        let mut total = files[start].file_size;
        let mut len = 1usize;
        let mut overlaps = 0usize;
        let mut j = start + 1;
        while j < files.len() && len < max_merge_files {
            let next = total.saturating_add(files[j].file_size);
            if next >= threshold_bytes {
                break;
            }
            if files[j - 1].max_ts >= files[j].min_ts {
                overlaps += 1;
            }
            total = next;
            len += 1;
            j += 1;
        }
        if len >= 2 && overlaps > 0 && best.map_or(true, |(_, _, bo)| overlaps > bo) {
            best = Some((start, len, overlaps));
        }
    }
    best.map(|(start, len, _)| (start, len))
        .or_else(|| pick_size_tiered(files, threshold_bytes, max_merge_files))
}

/// Releases a transient memory reservation on drop (panic-safe).
struct MergeMemGuard {
    memory: Arc<MemoryController>,
    bytes: usize,
}

impl Drop for MergeMemGuard {
    fn drop(&mut self) {
        self.memory.release(self.bytes);
    }
}

pub struct Compactor {
    file_index: Arc<FileIndex>,
    data_dir: PathBuf,
    threshold_bytes: u64,
    interval_secs: u64,
    strategy: CompactionStrategy,
    max_merge_files: usize,
    memory: Arc<MemoryController>,
    disk: RwLock<Option<Arc<DiskSpaceController>>>,
    target_schema: RwLock<SchemaRef>,
    cancelled: Arc<AtomicBool>,
    on_merge: parking_lot::RwLock<Option<Box<dyn Fn(Vec<SstMeta>, SstMeta) + Send + Sync>>>,
    /// Called **before** `file_index.replace_range` (table capturer enqueue).
    on_pre_replace: parking_lot::RwLock<Option<Box<dyn Fn(Vec<SstMeta>, SstMeta) + Send + Sync>>>,
    /// When true, compacted input SST must stay on disk (e.g. BulkLoad WAL still pins it).
    retain_input: parking_lot::RwLock<Option<Arc<dyn Fn(&str) -> bool + Send + Sync>>>,
}

impl Compactor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        file_index: Arc<FileIndex>,
        data_dir: PathBuf,
        threshold_bytes: u64,
        interval_secs: u64,
        target_schema: SchemaRef,
        strategy: CompactionStrategy,
        max_merge_files: usize,
        memory: Arc<MemoryController>,
    ) -> Self {
        Self {
            file_index,
            data_dir,
            threshold_bytes,
            interval_secs,
            strategy,
            max_merge_files: max_merge_files.max(2),
            memory,
            disk: RwLock::new(None),
            target_schema: RwLock::new(target_schema),
            cancelled: Arc::new(AtomicBool::new(false)),
            on_merge: parking_lot::RwLock::new(None),
            on_pre_replace: parking_lot::RwLock::new(None),
            retain_input: parking_lot::RwLock::new(None),
        }
    }

    pub fn set_disk_space(&self, disk: Arc<DiskSpaceController>) {
        *self.disk.write() = Some(disk);
    }

    pub fn set_on_merge(&self, callback: Box<dyn Fn(Vec<SstMeta>, SstMeta) + Send + Sync>) {
        *self.on_merge.write() = Some(callback);
    }

    /// Hook before FileIndex swap — hard-link output for lagging streams first.
    pub fn set_on_pre_replace(&self, callback: Box<dyn Fn(Vec<SstMeta>, SstMeta) + Send + Sync>) {
        *self.on_pre_replace.write() = Some(callback);
    }

    pub fn set_retain_input(&self, callback: Arc<dyn Fn(&str) -> bool + Send + Sync>) {
        *self.retain_input.write() = Some(callback);
    }

    pub fn set_target_schema(&self, schema: SchemaRef) {
        *self.target_schema.write() = schema;
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, AtomicOrdering::Relaxed);
    }

    pub fn start_background_task(self: Arc<Self>) {
        let interval = self.interval_secs;
        let cancelled = self.cancelled.clone();
        tokio::spawn(async move {
            loop {
                if cancelled.load(AtomicOrdering::Relaxed) {
                    break;
                }
                sleep(Duration::from_secs(interval)).await;
                if cancelled.load(AtomicOrdering::Relaxed) {
                    break;
                }
                let compactor = Arc::clone(&self);
                match tokio::task::spawn_blocking(move || compactor.run_compaction_pass()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => tracing::error!(error = %e, "compaction failed"),
                    Err(e) => tracing::error!(error = %e, "compaction task join failed"),
                }
            }
        });
    }

    pub fn run_compaction_pass(&self) -> Result<()> {
        if self.cancelled.load(AtomicOrdering::Relaxed) {
            return Ok(());
        }

        if let Some(disk) = self.disk.read().clone() {
            let usage = disk.refresh_if_due()?;
            if usage.read_only {
                tracing::info!(
                    free_bytes = usage.free_bytes,
                    total_bytes = usage.total_bytes,
                    free_ratio = usage.free_ratio,
                    "compaction skipped: disk is read-only"
                );
                return Ok(());
            }
        }

        // Back off while memtables are under memory pressure: compaction is a background space
        // optimization and must not compete with the write path for the global budget.
        if self.memory.at_or_over_soft_threshold() {
            tracing::debug!(
                used = self.memory.used_bytes(),
                soft = self.memory.soft_threshold_bytes(),
                "compaction deferred under memory pressure"
            );
            return Ok(());
        }

        let files = self.file_index.snapshot();
        let Some((start, len)) = pick_compaction(
            &files,
            self.strategy,
            self.threshold_bytes,
            self.max_merge_files,
        ) else {
            return Ok(());
        };

        let inputs: Vec<SstMeta> = files[start..start + len].to_vec();
        tracing::info!(
            strategy = ?self.strategy,
            start,
            files = len,
            input_rows = inputs.iter().map(|f| f.row_count).sum::<usize>(),
            input_bytes = inputs.iter().map(|f| f.file_size).sum::<u64>(),
            threshold_bytes = self.threshold_bytes,
            "compaction started"
        );

        let merged = self.merge_files(&inputs)?;
        tracing::info!(
            output = %merged.file_path,
            merged_rows = merged.row_count,
            merged_bytes = merged.file_size,
            min_ts = merged.min_ts,
            max_ts = merged.max_ts,
            "compaction completed"
        );

        // Hard-link / capturer **before** updating the on-disk SST list.
        if let Some(cb) = self.on_pre_replace.read().as_ref() {
            cb(inputs.clone(), merged.clone());
        }
        self.file_index.replace_range(start, len, merged.clone());
        if let Some(cb) = self.on_merge.read().as_ref() {
            cb(inputs.clone(), merged);
        }
        let retain = self.retain_input.read().clone();
        for f in &inputs {
            if retain.as_ref().is_some_and(|r| r(&f.file_path)) {
                tracing::debug!(
                    path = %f.file_path,
                    "defer SST unlink until BulkLoad WAL is GC'd"
                );
                continue;
            }
            if let Err(e) = std::fs::remove_file(&f.file_path) {
                tracing::warn!(path = %f.file_path, error = %e, "failed to remove compacted input file");
            }
        }
        Ok(())
    }

    /// Estimate the transient in-memory footprint of merging `files` (one merge window plus one
    /// in-flight input batch per source), so the [`MemoryController`] accounts for compaction too.
    fn estimate_merge_bytes(&self, files: &[SstMeta]) -> usize {
        let total_bytes: u64 = files.iter().map(|f| f.file_size).sum();
        let total_rows: usize = files.iter().map(|f| f.row_count).sum();
        let bytes_per_row = if total_rows > 0 {
            (total_bytes as usize / total_rows).max(1)
        } else {
            64
        };
        // One output window + one input batch per cursor, inflated for Arrow decode overhead.
        bytes_per_row
            .saturating_mul(PARQUET_INFLATE_FACTOR)
            .saturating_mul(
                STREAM_MERGE_BATCH_ROWS.saturating_add(files.len() * STREAM_MERGE_BATCH_ROWS),
            )
    }

    /// Streaming k-way merge of a contiguous run: timestamp ASC, later file in the run wins on equal
    /// timestamp (newest-wins), consecutive duplicate timestamps collapsed.
    fn merge_files(&self, files: &[SstMeta]) -> Result<SstMeta> {
        if files.len() < 2 {
            return Err(TsdbError::Storage(
                "compaction requires at least two files".into(),
            ));
        }
        let schema = self.target_schema.read().clone();

        // Charge (and auto-release on return) the transient merge footprint.
        let est = self.estimate_merge_bytes(files);
        self.memory.reserve_unchecked(est);
        let _guard = MergeMemGuard {
            memory: self.memory.clone(),
            bytes: est,
        };

        let mut cursors: Vec<ParquetCursor> = Vec::with_capacity(files.len());
        for (idx, f) in files.iter().enumerate() {
            cursors.push(ParquetCursor::open(&f.file_path, schema.clone(), idx)?);
        }

        let identities: Vec<SstIdentity> = files.iter().map(|f| f.identity()).collect();
        let identity = SstIdentity::after_inner_merge_run(&identities);

        let mut heap: BinaryHeap<HeapKey> = BinaryHeap::with_capacity(cursors.len());
        for (idx, cursor) in cursors.iter().enumerate() {
            if let Some(ts) = cursor.peek_ts() {
                heap.push(HeapKey {
                    ts,
                    source_idx: idx,
                });
            }
        }

        let mut chunk = MergeChunk::new(schema.clone());
        let mut out_batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows = 0usize;
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        let mut last_ts: Option<i64> = None;

        while let Some(HeapKey { ts, source_idx }) = heap.pop() {
            // First row seen for this timestamp comes from the highest source index (newest wins);
            // any later row with the same ts (older source or within-file duplicate) is dropped.
            if Some(ts) != last_ts {
                let (batch, row, key) = cursors[source_idx].current_row_owned()?;
                min_ts = min_ts.min(ts);
                max_ts = max_ts.max(ts);
                chunk.push(batch, row, key);
                total_rows += 1;
                last_ts = Some(ts);

                if chunk.len() >= STREAM_MERGE_BATCH_ROWS {
                    out_batches.push(chunk.build()?);
                    chunk = MergeChunk::new(schema.clone());
                }
            }

            cursors[source_idx].advance();
            if let Some(next_ts) = cursors[source_idx].peek_ts() {
                heap.push(HeapKey {
                    ts: next_ts,
                    source_idx,
                });
            }
        }

        if chunk.len() > 0 {
            out_batches.push(chunk.build()?);
        }

        if total_rows == 0 {
            return Err(TsdbError::Storage("empty merge result".into()));
        }

        // Write under `.compact_tmp/` first so readers / FileIndex never observe a partial merge
        // SST; promote into `data_dir/` only after the Parquet writer finishes successfully.
        let tmp_dir = compact_tmp_dir(&self.data_dir);
        std::fs::create_dir_all(&tmp_dir)?;
        let staged = write_sst_streaming(
            &identity,
            &tmp_dir,
            min_ts,
            max_ts,
            total_rows,
            schema,
            out_batches,
        );
        let meta = match staged {
            Ok(meta) => promote_sst_from_compact_tmp(meta, &self.data_dir)?,
            Err(e) => {
                let staging = tmp_dir.join(identity.filename());
                let _ = std::fs::remove_file(staging);
                return Err(e);
            }
        };
        Ok(meta)
    }
}

/// Min-heap key over cursor heads: pop smallest timestamp first; on ties, the highest source index
/// (newest file in the run) pops first so newest-wins dedup keeps the right row.
#[derive(PartialEq, Eq)]
struct HeapKey {
    ts: i64,
    source_idx: usize,
}

impl Ord for HeapKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap: "greatest" pops first. Greatest == smallest ts, then largest idx.
        other
            .ts
            .cmp(&self.ts)
            .then_with(|| self.source_idx.cmp(&other.source_idx))
    }
}

impl PartialOrd for HeapKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Streams one Parquet file batch-at-a-time with schema alignment.
struct ParquetCursor {
    reader: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    schema: SchemaRef,
    current: Option<RecordBatch>,
    row: usize,
    source_idx: usize,
    /// Increments on every loaded batch so each distinct batch gets a stable, unique key even though
    /// `current` is reused in place (its address alone is not a safe identity).
    load_count: u64,
}

impl ParquetCursor {
    fn open(path: &str, schema: SchemaRef, source_idx: usize) -> Result<Self> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;
        let mut cursor = Self {
            reader,
            schema,
            current: None,
            row: 0,
            source_idx,
            load_count: 0,
        };
        cursor.load_next_batch()?;
        Ok(cursor)
    }

    fn load_next_batch(&mut self) -> Result<()> {
        loop {
            match self.reader.next() {
                Some(Ok(batch)) => {
                    let aligned = BatchAligner::align(batch, self.schema.clone())?;
                    if aligned.num_rows() > 0 {
                        self.current = Some(aligned);
                        self.row = 0;
                        self.load_count += 1;
                        return Ok(());
                    }
                }
                Some(Err(e)) => return Err(e.into()),
                None => {
                    self.current = None;
                    self.row = 0;
                    return Ok(());
                }
            }
        }
    }

    fn peek_ts(&self) -> Option<i64> {
        let batch = self.current.as_ref()?;
        timestamp_at(batch, self.row).ok()
    }

    /// Current row as an owned (cheap Arc-clone) batch plus a globally unique batch key.
    fn current_row_owned(&self) -> Result<(RecordBatch, usize, u64)> {
        let batch = self
            .current
            .as_ref()
            .ok_or_else(|| TsdbError::Storage("merge cursor exhausted".into()))?;
        let key = ((self.source_idx as u64) << 40) | self.load_count;
        Ok((batch.clone(), self.row, key))
    }

    fn advance(&mut self) {
        self.row += 1;
        if self
            .current
            .as_ref()
            .is_some_and(|b| self.row >= b.num_rows())
        {
            let _ = self.load_next_batch();
        }
    }
}

fn timestamp_at(batch: &RecordBatch, row: usize) -> Result<i64> {
    let ts_idx = time_column_index(batch.schema())?;
    time_value_at(batch.column(ts_idx), row)
}

/// Collects row references from streaming cursors in **emission order**, materializing them lazily
/// into a single batch at [`MergeChunk::build`].
struct MergeChunk {
    schema: SchemaRef,
    emission: Vec<(usize, u32)>,
    sources: Vec<RecordBatch>,
    source_index: HashMap<u64, usize>,
    len: usize,
}

impl MergeChunk {
    fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            emission: Vec::new(),
            sources: Vec::new(),
            source_index: HashMap::new(),
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn push(&mut self, batch: RecordBatch, row: usize, key: u64) {
        let batch_idx = if let Some(&idx) = self.source_index.get(&key) {
            idx
        } else {
            let idx = self.sources.len();
            self.sources.push(batch);
            self.source_index.insert(key, idx);
            idx
        };
        self.emission.push((batch_idx, row as u32));
        self.len += 1;
    }

    fn build(self) -> Result<RecordBatch> {
        if self.len == 0 {
            return Err(TsdbError::Storage("empty merge chunk".into()));
        }

        let mut pieces = Vec::with_capacity(self.emission.len());
        for (batch_idx, row) in self.emission {
            let batch = &self.sources[batch_idx];
            let idx_array = UInt32Array::from(vec![row]);
            let mut columns = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                columns.push(take(batch.column(col_idx).as_ref(), &idx_array, None)?);
            }
            pieces.push(RecordBatch::try_new(self.schema.clone(), columns)?);
        }

        if pieces.len() == 1 {
            return Ok(pieces.into_iter().next().unwrap());
        }

        Ok(arrow::compute::concat_batches(&self.schema, &pieces)?)
    }
}

/// Global compaction scheduler shared by every table.
///
/// Replaces per-table background loops (one sleeping task + uncoordinated IO storms per table) with
/// a single dispatcher fed by table events, plus one periodic sweep. A [`Semaphore`] caps how many
/// merge jobs run at once so a burst of tables crossing their thresholds cannot saturate disk IO
/// and stall live queries.
pub struct GlobalCompactor {
    event_tx: mpsc::Sender<Arc<Compactor>>,
    registry: Arc<DashMap<String, Arc<Compactor>>>,
}

impl GlobalCompactor {
    pub fn start(max_concurrent_jobs: usize, tick_interval_secs: u64) -> Arc<Self> {
        let (event_tx, mut event_rx) =
            mpsc::channel::<Arc<Compactor>>(COMPACTION_EVENT_QUEUE_CAPACITY);
        let registry: Arc<DashMap<String, Arc<Compactor>>> = Arc::new(DashMap::new());
        let semaphore = Arc::new(Semaphore::new(max_concurrent_jobs.max(1)));

        let dispatch_sem = semaphore.clone();
        tokio::spawn(async move {
            while let Some(compactor) = event_rx.recv().await {
                let Ok(permit) = dispatch_sem.clone().acquire_owned().await else {
                    break;
                };
                tokio::task::spawn_blocking(move || {
                    if let Err(e) = compactor.run_compaction_pass() {
                        tracing::error!(error = %e, "compaction pass failed");
                    }
                    drop(permit);
                });
            }
        });

        let this = Arc::new(Self { event_tx, registry });

        if tick_interval_secs > 0 {
            let weak: Weak<Self> = Arc::downgrade(&this);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(tick_interval_secs));
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    let Some(this) = weak.upgrade() else {
                        break;
                    };
                    let compactors: Vec<Arc<Compactor>> =
                        this.registry.iter().map(|e| e.value().clone()).collect();
                    for compactor in compactors {
                        this.notify(compactor);
                    }
                }
            });
        }

        this
    }

    pub fn register(&self, name: impl Into<String>, compactor: Arc<Compactor>) {
        self.registry.insert(name.into(), compactor);
    }

    pub fn deregister(&self, name: &str) {
        self.registry.remove(name);
    }

    /// Enqueue a merge evaluation for `compactor`; drops silently if the queue is saturated
    /// (the periodic sweep will retry).
    pub fn notify(&self, compactor: Arc<Compactor>) {
        let _ = self.event_tx.try_send(compactor);
    }

    /// Enqueue a merge evaluation for a registered table by name.
    pub fn notify_table(&self, name: &str) {
        if let Some(compactor) = self.registry.get(name).map(|e| e.value().clone()) {
            self.notify(compactor);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use common::TIMESTAMP_COLUMN;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn ts_value_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COLUMN, DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn write_test_parquet(path: &std::path::Path, timestamps: &[i64], values: &[i64]) -> u64 {
        let schema = ts_value_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .unwrap();
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        std::fs::metadata(path).unwrap().len()
    }

    fn test_meta(
        path: &std::path::Path,
        min_ts: i64,
        max_ts: i64,
        rows: usize,
        size: u64,
        version: u64,
    ) -> SstMeta {
        SstMeta {
            file_path: path.to_string_lossy().to_string(),
            min_ts,
            max_ts,
            row_count: rows,
            file_size: size,
            creation_time_ms: 1_000,
            inner_compaction_count: 0,
            cross_compaction_count: 0,
            base_lsn: version,
            max_lsn: version,
        }
    }

    fn test_compactor(dir: &TempDir, schema: SchemaRef, threshold: u64) -> Compactor {
        Compactor::new(
            Arc::new(FileIndex::new()),
            dir.path().to_path_buf(),
            threshold,
            60,
            schema,
            CompactionStrategy::SizeTiered,
            DEFAULT_COMPACTION_MAX_MERGE_FILES,
            Arc::new(MemoryController::new(1 << 30)),
        )
    }

    fn size_meta(min_ts: i64, max_ts: i64, size: u64) -> SstMeta {
        test_meta(std::path::Path::new("x"), min_ts, max_ts, 1, size, 1)
    }

    #[test]
    fn size_tiered_picks_longest_run_under_threshold() {
        let files = vec![
            size_meta(0, 9, 10),
            size_meta(10, 19, 10),
            size_meta(20, 29, 10),
            size_meta(30, 39, 100), // large: stops the run
            size_meta(40, 49, 10),
            size_meta(50, 59, 10),
        ];
        // threshold 35 => first run [0,1,2]=30 (<35, len 3), later run [4,5]=20 (len 2).
        let pick = pick_compaction(&files, CompactionStrategy::SizeTiered, 35, 8).unwrap();
        assert_eq!(pick, (0, 3));
    }

    #[test]
    fn pick_skips_run_with_lsn_hole() {
        let mut a = size_meta(0, 9, 10);
        a.base_lsn = 1;
        a.max_lsn = 2;
        let mut b = size_meta(10, 19, 10);
        b.base_lsn = 4;
        b.max_lsn = 5; // hole at LSN 3 (e.g. BulkLoad)
        let files = vec![a, b];
        assert!(
            pick_compaction(&files, CompactionStrategy::SizeTiered, 100, 8).is_none(),
            "must not merge across LSN hole — would fake-cover BulkLoad"
        );
        assert!(!lsn_spans_contiguous(&files));
    }

    #[test]
    fn size_tiered_respects_max_merge_files() {
        let files: Vec<_> = (0..10).map(|i| size_meta(i * 10, i * 10 + 9, 1)).collect();
        let pick = pick_compaction(&files, CompactionStrategy::SizeTiered, 1_000, 3).unwrap();
        assert_eq!(pick.1, 3);
    }

    #[test]
    fn file_count_picks_longest_small_run() {
        let files = vec![
            size_meta(0, 9, 1),
            size_meta(10, 19, 1),
            size_meta(20, 29, 500), // not small
            size_meta(30, 39, 1),
            size_meta(40, 49, 1),
            size_meta(50, 59, 1),
        ];
        // small threshold = 1000/8 = 125; longest small run is [3,4,5] len 3.
        let pick = pick_compaction(&files, CompactionStrategy::FileCount, 1_000, 8).unwrap();
        assert_eq!(pick, (3, 3));
    }

    #[test]
    fn overlap_prefers_overlapping_run() {
        let files = vec![
            size_meta(0, 9, 1), // no overlap with next
            size_meta(20, 29, 1),
            size_meta(25, 35, 1), // overlaps prev
            size_meta(34, 40, 1), // overlaps prev
        ];
        let pick = pick_compaction(&files, CompactionStrategy::Overlap, 1_000, 8).unwrap();
        assert_eq!(pick, (0, 4)); // whole run picked, 2 overlapping pairs inside
    }

    #[test]
    fn pick_returns_none_for_single_file() {
        let files = vec![size_meta(0, 9, 1)];
        assert!(pick_compaction(&files, CompactionStrategy::SizeTiered, 1_000, 8).is_none());
    }

    #[test]
    fn merge_two_files_newest_wins_on_duplicate_timestamp() {
        let dir = TempDir::new().unwrap();
        let schema = ts_value_schema();
        let f1_path = dir.path().join("f1.parquet");
        let f2_path = dir.path().join("f2.parquet");
        let f1_size = write_test_parquet(&f1_path, &[100, 200], &[1, 2]);
        let f2_size = write_test_parquet(&f2_path, &[100, 300], &[9, 3]);

        let f1 = test_meta(&f1_path, 100, 200, 2, f1_size, 1);
        let f2 = test_meta(&f2_path, 100, 300, 2, f2_size, 2);
        let compactor = test_compactor(&dir, schema, u64::MAX);

        let merged = compactor.merge_files(&[f1, f2]).unwrap();
        assert_eq!(merged.row_count, 3);
        assert_eq!(
            std::path::Path::new(&merged.file_path).parent(),
            Some(dir.path()),
            "merged SST must live in data_dir, not .compact_tmp"
        );
        let compact_tmp = crate::compaction::sst::compact_tmp_dir(dir.path());
        if compact_tmp.exists() {
            let leftovers: Vec<_> = std::fs::read_dir(&compact_tmp)
                .unwrap()
                .filter_map(|e| e.ok())
                .collect();
            assert!(
                leftovers.is_empty(),
                "compact_tmp must be empty after successful merge: {leftovers:?}"
            );
        }

        let mut reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&merged.file_path).unwrap())
                .unwrap()
                .build()
                .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let ts = batch.column(0).as_primitive::<Int64Type>().values();
        let val = batch.column(1).as_primitive::<Int64Type>().values();
        assert_eq!(ts, &[100, 200, 300]);
        assert_eq!(val, &[9, 2, 3]);
    }

    #[test]
    fn merge_two_files_dedupes_within_file_disorder() {
        let dir = TempDir::new().unwrap();
        let schema = ts_value_schema();
        let f1_path = dir.path().join("f1.parquet");
        let f2_path = dir.path().join("f2.parquet");
        let f1_size = write_test_parquet(&f1_path, &[100, 100, 200], &[1, 5, 2]);
        let f2_size = write_test_parquet(&f2_path, &[150, 200, 200], &[7, 20, 99]);

        let f1 = test_meta(&f1_path, 100, 200, 3, f1_size, 1);
        let f2 = test_meta(&f2_path, 150, 200, 3, f2_size, 2);
        let compactor = test_compactor(&dir, schema, u64::MAX);

        let merged = compactor.merge_files(&[f1, f2]).unwrap();
        assert_eq!(merged.row_count, 3);

        let mut reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&merged.file_path).unwrap())
                .unwrap()
                .build()
                .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let ts = batch.column(0).as_primitive::<Int64Type>().values();
        let val = batch.column(1).as_primitive::<Int64Type>().values();
        assert_eq!(ts, &[100, 150, 200]);
        assert_eq!(val, &[1, 7, 20]); // 200 from f2 wins; internal dupes collapsed
    }

    #[test]
    fn merge_three_files_kway_newest_wins() {
        let dir = TempDir::new().unwrap();
        let schema = ts_value_schema();
        let f1_path = dir.path().join("f1.parquet");
        let f2_path = dir.path().join("f2.parquet");
        let f3_path = dir.path().join("f3.parquet");
        let f1_size = write_test_parquet(&f1_path, &[100, 400], &[1, 4]);
        let f2_size = write_test_parquet(&f2_path, &[100, 200], &[9, 2]);
        let f3_size = write_test_parquet(&f3_path, &[200, 300], &[99, 3]);

        let f1 = test_meta(&f1_path, 100, 400, 2, f1_size, 1);
        let f2 = test_meta(&f2_path, 100, 200, 2, f2_size, 2);
        let f3 = test_meta(&f3_path, 200, 300, 2, f3_size, 3);
        let compactor = test_compactor(&dir, schema, u64::MAX);

        let merged = compactor.merge_files(&[f1, f2, f3]).unwrap();
        assert_eq!(merged.row_count, 4);

        let mut reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&merged.file_path).unwrap())
                .unwrap()
                .build()
                .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let ts = batch.column(0).as_primitive::<Int64Type>().values();
        let val = batch.column(1).as_primitive::<Int64Type>().values();
        // ts 100 -> f2 wins over f1 (higher index); ts 200 -> f3 wins over f2.
        assert_eq!(ts, &[100, 200, 300, 400]);
        assert_eq!(val, &[9, 99, 3, 4]);
    }

    #[test]
    fn merge_crosses_batch_boundaries_without_key_collision() {
        // Two row groups per file force the cursor to reload batches within one merge window,
        // exercising the unique batch-key path (regression for pointer-identity reuse).
        let dir = TempDir::new().unwrap();
        let schema = ts_value_schema();
        let f1_path = dir.path().join("f1.parquet");
        let f2_path = dir.path().join("f2.parquet");

        let write_multi = |path: &std::path::Path, groups: &[(Vec<i64>, Vec<i64>)]| -> u64 {
            let file = File::create(path).unwrap();
            let props = parquet::file::properties::WriterProperties::builder()
                .set_max_row_group_size(2)
                .build();
            let mut writer = ArrowWriter::try_new(file, ts_value_schema(), Some(props)).unwrap();
            for (ts, val) in groups {
                let batch = RecordBatch::try_new(
                    ts_value_schema(),
                    vec![
                        Arc::new(Int64Array::from(ts.clone())),
                        Arc::new(Int64Array::from(val.clone())),
                    ],
                )
                .unwrap();
                writer.write(&batch).unwrap();
            }
            writer.close().unwrap();
            std::fs::metadata(path).unwrap().len()
        };

        let f1_size = write_multi(
            &f1_path,
            &[(vec![1, 2], vec![10, 20]), (vec![3, 4], vec![30, 40])],
        );
        let f2_size = write_multi(
            &f2_path,
            &[(vec![5, 6], vec![50, 60]), (vec![7, 8], vec![70, 80])],
        );

        let f1 = test_meta(&f1_path, 1, 4, 4, f1_size, 1);
        let f2 = test_meta(&f2_path, 5, 8, 4, f2_size, 2);
        let compactor = test_compactor(&dir, schema.clone(), u64::MAX);

        let merged = compactor.merge_files(&[f1, f2]).unwrap();
        assert_eq!(merged.row_count, 8);

        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&merged.file_path).unwrap())
                .unwrap()
                .build()
                .unwrap();
        let mut all_ts = Vec::new();
        let mut all_val = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            all_ts.extend_from_slice(batch.column(0).as_primitive::<Int64Type>().values());
            all_val.extend_from_slice(batch.column(1).as_primitive::<Int64Type>().values());
        }
        assert_eq!(all_ts, vec![1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(all_val, vec![10, 20, 30, 40, 50, 60, 70, 80]);
    }
}
