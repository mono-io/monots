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

//! Deduplicate rows by timestamp: newer layers / later writes win (FIFO layer order).

use arrow::array::{Array, BooleanArray, UInt32Array};
use arrow::compute::{concat_batches, filter_record_batch, interleave, take};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common::{time_column_index, time_value_at, time_values_slice, Result, TsdbError};
use std::collections::{HashMap, HashSet};

/// Rows per output batch when materializing the sorted flush path incrementally.
pub const FLUSH_WINDOW_ROWS: usize = 8192;

/// Tunables for flush / dedupe windowing (exposed to storage engine config).
#[derive(Debug, Clone)]
pub struct DedupeConfig {
    pub flush_window_rows: usize,
}

impl Default for DedupeConfig {
    fn default() -> Self {
        Self {
            flush_window_rows: FLUSH_WINDOW_ROWS,
        }
    }
}

/// Merge batches in **oldest → newest** order within one layer; keep the newest row per timestamp.
/// Output is a single sorted, unique batch (for flush / compaction).
pub fn prepare_flush_batch(batches: &[RecordBatch], schema: SchemaRef) -> Result<RecordBatch> {
    let deduped = dedupe_batches_newest_wins(batches, schema)?;
    deduped
        .into_iter()
        .next()
        .ok_or_else(|| TsdbError::Storage("nothing to flush after dedupe".into()))
}

/// How a memtable's chunk list should be written to an SST.
pub enum FlushPlan {
    /// Chunks are globally, strictly ascending by timestamp: write each chunk as-is,
    /// one at a time (zero-copy — no full-memtable concat, bounded transient memory).
    Streaming(Vec<RecordBatch>),
    /// Chunks overlap or contain duplicate/disordered timestamps: rows are reordered via a
    /// timestamp-sorted coordinate index and materialized window-by-window with `interleave`,
    /// so peak transient memory is one output window rather than a full-dataset concat.
    Sorted(SortedFlush),
}

/// Decide the flush strategy for a memtable's frozen chunks.
///
/// The common ingest pattern (monotonic timestamps) yields the zero-copy
/// [`FlushPlan::Streaming`] path; out-of-order / duplicate timestamps fall back to the
/// index-sorted, windowed [`FlushPlan::Sorted`] path (no full concat copy).
pub fn plan_flush(chunks: &[RecordBatch], schema: SchemaRef) -> Result<FlushPlan> {
    let non_empty: Vec<RecordBatch> = chunks
        .iter()
        .filter(|b| b.num_rows() > 0)
        .cloned()
        .collect();
    if non_empty.is_empty() {
        return Err(TsdbError::Storage("nothing to flush".into()));
    }
    if chunks_strictly_ordered(&non_empty)? {
        return Ok(FlushPlan::Streaming(non_empty));
    }
    SortedFlush::build(non_empty, schema).map(FlushPlan::Sorted)
}

/// A disorder-path flush: source chunks plus a timestamp-sorted, newest-wins deduped list of
/// `(chunk_idx, row_idx)` coordinates into them. Output batches are produced lazily so only one
/// window's worth of rows is ever materialized at a time.
pub struct SortedFlush {
    chunks: Vec<RecordBatch>,
    /// `(chunk_idx, row_idx)` coordinates in ascending-timestamp order, duplicates removed
    /// keeping the newest row (later chunk / later row within a chunk wins).
    coords: Vec<(u32, u32)>,
    schema: SchemaRef,
}

impl SortedFlush {
    fn build(chunks: Vec<RecordBatch>, schema: SchemaRef) -> Result<Self> {
        let total: usize = chunks.iter().map(|b| b.num_rows()).sum();
        // (time, chunk_idx, row_idx). Sorting ascending by all three puts, for any run of
        // equal timestamps, the newest row (largest chunk/row coordinate) last.
        let mut entries: Vec<(i64, u32, u32)> = Vec::with_capacity(total);
        for (chunk_idx, batch) in chunks.iter().enumerate() {
            let ts_idx = time_column_index(batch.schema())?;
            let values = time_values_slice(batch.column(ts_idx))?;
            for (row, &ts) in values.iter().enumerate() {
                entries.push((ts, chunk_idx as u32, row as u32));
            }
        }
        if entries.is_empty() {
            return Err(TsdbError::Storage("nothing to flush after dedupe".into()));
        }
        entries.sort_unstable();

        let mut coords: Vec<(u32, u32)> = Vec::with_capacity(entries.len());
        let mut i = 0;
        while i < entries.len() {
            let ts = entries[i].0;
            let mut last = i;
            while last + 1 < entries.len() && entries[last + 1].0 == ts {
                last += 1;
            }
            coords.push((entries[last].1, entries[last].2));
            i = last + 1;
        }

        Ok(Self {
            chunks,
            coords,
            schema,
        })
    }

    /// Approximate transient bytes held while writing: the coordinate index (kept for the whole
    /// write) plus one output window's share of the row data.
    pub fn transient_estimate(&self, window: usize) -> usize {
        let coords_bytes = self.coords.len() * std::mem::size_of::<(u32, u32)>();
        let total_rows: usize = self.chunks.iter().map(|b| b.num_rows()).sum();
        let total_bytes: usize = self.chunks.iter().map(|b| b.get_array_memory_size()).sum();
        let window_bytes = if total_rows == 0 {
            0
        } else {
            total_bytes.saturating_mul(window.min(self.coords.len())) / total_rows
        };
        coords_bytes + window_bytes
    }

    /// Lazily materialize output batches of at most `window` rows via `interleave`, so the whole
    /// deduped dataset is never held in memory at once.
    pub fn window_batches(self, window: usize) -> SortedFlushBatches {
        let window = window.max(1);
        SortedFlushBatches {
            chunks: self.chunks,
            coords: self.coords,
            schema: self.schema,
            window,
            cursor: 0,
        }
    }
}

/// Iterator over the windowed output batches of a [`SortedFlush`].
pub struct SortedFlushBatches {
    chunks: Vec<RecordBatch>,
    coords: Vec<(u32, u32)>,
    schema: SchemaRef,
    window: usize,
    cursor: usize,
}

impl Iterator for SortedFlushBatches {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.coords.len() {
            return None;
        }
        let end = (self.cursor + self.window).min(self.coords.len());
        let indices: Vec<(usize, usize)> = self.coords[self.cursor..end]
            .iter()
            .map(|&(c, r)| (c as usize, r as usize))
            .collect();
        self.cursor = end;

        let num_cols = self.schema.fields().len();
        let mut columns = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let arrays: Vec<&dyn Array> = self
                .chunks
                .iter()
                .map(|b| b.column(col_idx).as_ref())
                .collect();
            match interleave(&arrays, &indices) {
                Ok(array) => columns.push(array),
                Err(e) => return Some(Err(TsdbError::Storage(format!("interleave flush: {e}")))),
            }
        }
        Some(
            RecordBatch::try_new(self.schema.clone(), columns)
                .map_err(|e| TsdbError::Storage(e.to_string())),
        )
    }
}

/// True when every chunk is strictly increasing internally and chunk boundaries strictly
/// increase across chunks — i.e. concatenating the chunks as-is is already globally sorted
/// and duplicate-free, so no sort or copy is needed to produce a valid SST.
pub fn chunks_strictly_ordered(chunks: &[RecordBatch]) -> Result<bool> {
    let mut prev_last: Option<i64> = None;
    for batch in chunks {
        if batch.num_rows() == 0 {
            continue;
        }
        let ts_idx = time_column_index(batch.schema())?;
        let values = time_values_slice(batch.column(ts_idx))?;
        if !values.windows(2).all(|w| w[0] < w[1]) {
            return Ok(false);
        }
        let first = values[0];
        if let Some(prev) = prev_last {
            if first <= prev {
                return Ok(false);
            }
        }
        prev_last = Some(values[values.len() - 1]);
    }
    Ok(true)
}

/// Prepare memtable **chunk list** for query scan: filter/time-range already applied per chunk.
/// Avoids merging all chunks into one batch when insert order is time-monotonic.
pub fn prepare_scan_batches(
    batches: &[RecordBatch],
    schema: SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let non_empty: Vec<RecordBatch> = batches
        .iter()
        .filter(|batch| batch.num_rows() > 0)
        .cloned()
        .collect();
    if non_empty.is_empty() {
        return Ok(vec![]);
    }
    if non_empty.len() == 1 {
        // A single chunk can still contain duplicate timestamps (e.g. repeated inserts coalesced
        // into one builder); dedupe (newest wins) before returning, only sorting when unique.
        if batch_has_duplicate_timestamps(&non_empty[0])? {
            return dedupe_batches_newest_wins(&non_empty, schema);
        }
        return Ok(vec![sort_batch_by_timestamp(non_empty[0].clone())?]);
    }
    if batches_are_time_ordered(&non_empty)? {
        return non_empty.into_iter().map(sort_batch_by_timestamp).collect();
    }
    dedupe_batches_newest_wins(&non_empty, schema)
}

/// True when batches are non-empty, each chunk is time-monotonic, and chunk boundaries increase.
pub fn batches_are_time_ordered(batches: &[RecordBatch]) -> Result<bool> {
    let mut last_ts: Option<i64> = None;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        if batch_has_duplicate_timestamps(batch)? {
            return Ok(false);
        }
        let ts_idx = time_column_index(batch.schema())?;
        let ts_col = batch.column(ts_idx);
        let first = time_value_at(ts_col, 0)?;
        let chunk_last = time_value_at(ts_col, batch.num_rows() - 1)?;
        if first > chunk_last {
            return Ok(false);
        }
        if let Some(prev) = last_ts {
            if first <= prev {
                return Ok(false);
            }
        }
        last_ts = Some(chunk_last);
    }
    Ok(true)
}

/// Layered merge: `layers[0]` oldest, `layers[last]` newest. Avoids concatenating the full dataset.
pub fn merge_sst_layers(
    layers: &[Vec<RecordBatch>],
    schema: SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let non_empty_layers: Vec<&Vec<RecordBatch>> = layers
        .iter()
        .filter(|layer| layer.iter().any(|b| b.num_rows() > 0))
        .collect();
    if non_empty_layers.is_empty() {
        return Ok(vec![]);
    }
    if non_empty_layers.len() == 1 {
        return dedupe_batches_newest_wins(non_empty_layers[0], schema);
    }

    let mut seen = HashSet::new();
    let mut kept: Vec<(usize, usize, usize)> = Vec::new();

    for layer_idx in (0..layers.len()).rev() {
        let layer = &layers[layer_idx];
        for batch_idx in (0..layer.len()).rev() {
            let batch = &layer[batch_idx];
            if batch.num_rows() == 0 {
                continue;
            }
            let ts_idx = time_column_index(batch.schema())?;
            let ts_col = batch.column(ts_idx);
            for row in (0..batch.num_rows()).rev() {
                if seen.insert(time_value_at(ts_col, row)?) {
                    kept.push((layer_idx, batch_idx, row));
                }
            }
        }
    }

    if kept.is_empty() {
        return Ok(vec![]);
    }

    materialize_kept_rows(layers, &kept, schema)
}

/// Merge batches ordered **oldest → newest**; for duplicate timestamps keep the newest row.
pub fn dedupe_batches_newest_wins(
    batches: &[RecordBatch],
    schema: SchemaRef,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }

    let non_empty: Vec<RecordBatch> = batches
        .iter()
        .filter(|b| b.num_rows() > 0)
        .cloned()
        .collect();
    if non_empty.is_empty() {
        return Ok(vec![]);
    }
    if non_empty.len() == 1 && !batch_has_duplicate_timestamps(&non_empty[0])? {
        return Ok(vec![sort_batch_by_timestamp(non_empty[0].clone())?]);
    }

    let merged = concat_batches(&schema, &non_empty)
        .map_err(|e| TsdbError::Storage(format!("concat for dedupe: {e}")))?;
    dedupe_sorted_batch(merged)
}

/// When scanning LSM layers **newest → oldest**, keep rows whose timestamp is not yet in `seen_ts`.
pub fn filter_batch_skip_seen_timestamps(
    batch: &RecordBatch,
    seen_ts: &mut HashSet<i64>,
) -> Result<Option<RecordBatch>> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let ts_idx = time_column_index(batch.schema())?;
    let ts_col = batch.column(ts_idx);

    let mut keep = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        keep.push(seen_ts.insert(time_value_at(ts_col, row)?));
    }
    if !keep.iter().any(|&k| k) {
        return Ok(None);
    }
    let mask = BooleanArray::from(keep);
    let filtered = filter_record_batch(batch, &mask)
        .map_err(|e| TsdbError::Storage(format!("layer filter: {e}")))?;
    Ok(Some(filtered))
}

/// True when layered merge is required (multiple sources or duplicate timestamps in a single batch).
pub fn needs_layer_dedupe(layers: &[Vec<RecordBatch>]) -> Result<bool> {
    let mut active_layers = 0usize;
    let mut total_batches = 0usize;
    for layer in layers {
        let non_empty: usize = layer.iter().filter(|b| b.num_rows() > 0).count();
        if non_empty > 0 {
            active_layers += 1;
            total_batches += non_empty;
        }
    }
    if active_layers > 1 || total_batches > 1 {
        return Ok(true);
    }
    if let Some(layer) = layers.iter().find(|l| l.iter().any(|b| b.num_rows() > 0)) {
        if let Some(batch) = layer.iter().find(|b| b.num_rows() > 0) {
            return batch_has_duplicate_timestamps(batch);
        }
    }
    Ok(false)
}

fn batch_has_duplicate_timestamps(batch: &RecordBatch) -> Result<bool> {
    let ts_idx = time_column_index(batch.schema())?;
    let values = time_values_slice(batch.column(ts_idx))?;
    if values.len() < 2 {
        return Ok(false);
    }
    // Fast path: monotonic ingest — O(N) adjacent scan only.
    if values.windows(2).all(|w| w[0] < w[1]) {
        return Ok(false);
    }
    // Slow path: disorder or adjacent duplicates — O(N) hash check, no sort/copy.
    let mut seen = HashSet::with_capacity(values.len());
    for &v in values {
        if !seen.insert(v) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn dedupe_sorted_batch(merged: RecordBatch) -> Result<Vec<RecordBatch>> {
    if merged.num_rows() == 0 {
        return Ok(vec![]);
    }
    if merged.num_rows() == 1 {
        return Ok(vec![merged]);
    }

    let ts_idx = time_column_index(merged.schema())?;
    let values = time_values_slice(merged.column(ts_idx))?;
    let n = values.len();

    // Sort by (timestamp, original_row). Secondary key preserves concat order so that for equal
    // timestamps the *later* (newer) row sorts last — newest-wins without a HashSet.
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_unstable_by_key(|&i| (values[i], i));

    // Adjacent compare: keep the last row of each equal-timestamp run.
    let mut keep_rows: Vec<u32> = Vec::with_capacity(n);
    for idx in 0..n {
        let is_last_of_run = idx + 1 == n || values[order[idx]] != values[order[idx + 1]];
        if is_last_of_run {
            keep_rows.push(order[idx] as u32);
        }
    }
    let indices = UInt32Array::from(keep_rows);
    let filtered = take_batch_by_indices(&merged, &indices)?;
    if filtered.num_rows() == 0 {
        return Ok(vec![]);
    }
    Ok(vec![filtered])
}

fn take_batch_by_indices(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for i in 0..batch.num_columns() {
        let col = take(batch.column(i).as_ref(), indices, None)
            .map_err(|e| TsdbError::Storage(format!("dedupe take: {e}")))?;
        columns.push(col);
    }
    RecordBatch::try_new(batch.schema(), columns).map_err(|e| TsdbError::Storage(e.to_string()))
}

fn materialize_kept_rows(
    layers: &[Vec<RecordBatch>],
    kept: &[(usize, usize, usize)],
    schema: SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let mut groups: HashMap<(usize, usize), Vec<u32>> = HashMap::new();
    for (layer_idx, batch_idx, row) in kept {
        groups
            .entry((*layer_idx, *batch_idx))
            .or_default()
            .push(*row as u32);
    }

    let mut pieces = Vec::with_capacity(groups.len());
    for ((layer_idx, batch_idx), indices) in groups {
        let batch = &layers[layer_idx][batch_idx];
        let idx_array = UInt32Array::from(indices);
        let mut columns = Vec::with_capacity(batch.num_columns());
        for col_idx in 0..batch.num_columns() {
            let taken = take(batch.column(col_idx).as_ref(), &idx_array, None)
                .map_err(|e| TsdbError::Storage(e.to_string()))?;
            columns.push(taken);
        }
        pieces.push(
            RecordBatch::try_new(schema.clone(), columns)
                .map_err(|e| TsdbError::Storage(e.to_string()))?,
        );
    }

    if pieces.is_empty() {
        return Ok(vec![]);
    }
    if pieces.len() == 1 {
        return Ok(vec![sort_batch_by_timestamp(
            pieces.into_iter().next().unwrap(),
        )?]);
    }

    let merged = concat_batches(&schema, &pieces)
        .map_err(|e| TsdbError::Storage(format!("concat deduped pieces: {e}")))?;
    Ok(vec![sort_batch_by_timestamp(merged)?])
}

pub fn sort_batch_by_timestamp(batch: RecordBatch) -> Result<RecordBatch> {
    common::sort_batch_by_time(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use std::sync::Arc;

    fn ts_value_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn batch(ts: &[i64], values: &[i64]) -> RecordBatch {
        let schema = ts_value_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ts.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn keeps_newest_row_for_duplicate_timestamp() {
        let schema = ts_value_schema();
        let old = batch(&[10, 20], &[1, 2]);
        let new = batch(&[10, 30], &[100, 3]);
        let out = dedupe_batches_newest_wins(&[old, new], schema).unwrap();
        assert_eq!(out.len(), 1);
        let ts = out[0].column(0).as_primitive::<Int64Type>();
        let val = out[0].column(1).as_primitive::<Int64Type>();
        assert_eq!(ts.values(), &[10, 20, 30]);
        assert_eq!(val.values(), &[100, 2, 3]);
    }

    #[test]
    fn layered_dedupe_keeps_newest_layer() {
        let schema = ts_value_schema();
        let layer_old = vec![batch(&[100], &[1])];
        let layer_new = vec![batch(&[100], &[2])];
        let out = merge_sst_layers(&[layer_old, layer_new], schema).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].column(1).as_primitive::<Int64Type>().value(0), 2);
    }

    #[test]
    fn prepare_flush_batch_dedupes_within_memtable() {
        let schema = ts_value_schema();
        let merged = prepare_flush_batch(&[batch(&[5, 5], &[1, 9])], schema).unwrap();
        assert_eq!(merged.num_rows(), 1);
        assert_eq!(merged.column(1).as_primitive::<Int64Type>().value(0), 9);
    }

    #[test]
    fn needs_dedupe_for_single_batch_with_duplicate_ts() {
        let layers = vec![vec![batch(&[1, 1], &[1, 2])]];
        assert!(needs_layer_dedupe(&layers).unwrap());
        let unique = vec![vec![batch(&[1, 2], &[1, 2])]];
        assert!(!needs_layer_dedupe(&unique).unwrap());
    }

    #[test]
    fn skips_dedupe_for_single_unique_batch() {
        let layers = vec![vec![batch(&[1, 2], &[1, 2])]];
        assert!(!needs_layer_dedupe(&layers).unwrap());
        assert!(needs_layer_dedupe(&[vec![batch(&[1], &[1])], vec![batch(&[2], &[2])],]).unwrap());
    }

    #[test]
    fn empty_input_returns_empty() {
        let schema = ts_value_schema();
        assert!(dedupe_batches_newest_wins(&[], schema).unwrap().is_empty());
    }

    #[test]
    fn prepare_scan_batches_keeps_monotonic_chunks_separate() {
        let schema = ts_value_schema();
        let chunks = vec![
            batch(&[1, 2, 3], &[10, 20, 30]),
            batch(&[4, 5, 6], &[40, 50, 60]),
        ];
        assert!(batches_are_time_ordered(&chunks).unwrap());
        let out = prepare_scan_batches(&chunks, schema).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].num_rows(), 3);
        assert_eq!(out[1].num_rows(), 3);
    }

    #[test]
    fn prepare_scan_batches_dedupes_single_chunk_with_duplicate_ts() {
        let schema = ts_value_schema();
        // Repeated inserts coalesced into one chunk: newest (later row) must win.
        let chunks = vec![batch(&[100, 100, 200], &[1, 99, 5])];
        let out = prepare_scan_batches(&chunks, schema).unwrap();
        assert_eq!(out.len(), 1);
        let ts = out[0].column(0).as_primitive::<Int64Type>();
        let val = out[0].column(1).as_primitive::<Int64Type>();
        assert_eq!(ts.values(), &[100, 200]);
        assert_eq!(val.values(), &[99, 5]);
    }

    #[test]
    fn prepare_scan_batches_dedupes_when_chunks_overlap() {
        let schema = ts_value_schema();
        let chunks = vec![batch(&[1, 2], &[10, 20]), batch(&[2, 3], &[200, 30])];
        assert!(!batches_are_time_ordered(&chunks).unwrap());
        let out = prepare_scan_batches(&chunks, schema).unwrap();
        assert_eq!(out.len(), 1);
        let ts = out[0].column(0).as_primitive::<Int64Type>();
        assert_eq!(ts.values(), &[1, 2, 3]);
    }

    #[test]
    fn plan_flush_streams_strictly_ordered_chunks_without_merge() {
        let schema = ts_value_schema();
        let chunks = vec![
            batch(&[1, 2, 3], &[10, 20, 30]),
            batch(&[4, 5, 6], &[40, 50, 60]),
        ];
        match plan_flush(&chunks, schema).unwrap() {
            FlushPlan::Streaming(out) => {
                assert_eq!(out.len(), 2);
                assert_eq!(out[0].num_rows(), 3);
                assert_eq!(out[1].num_rows(), 3);
            }
            FlushPlan::Sorted(_) => panic!("expected streaming plan for ordered chunks"),
        }
    }

    fn collect_sorted(plan: FlushPlan, window: usize) -> RecordBatch {
        match plan {
            FlushPlan::Sorted(sorted) => {
                let schema = sorted.schema.clone();
                let batches: Vec<RecordBatch> =
                    sorted.window_batches(window).map(|b| b.unwrap()).collect();
                concat_batches(&schema, &batches).unwrap()
            }
            FlushPlan::Streaming(_) => panic!("expected sorted plan for disordered chunks"),
        }
    }

    #[test]
    fn plan_flush_sorts_and_dedupes_overlapping_chunks() {
        let schema = ts_value_schema();
        let chunks = vec![batch(&[1, 2], &[10, 20]), batch(&[2, 3], &[200, 30])];
        // Window smaller than the row count to exercise multi-window interleave streaming.
        let out = collect_sorted(plan_flush(&chunks, schema).unwrap(), 2);
        let ts = out.column(0).as_primitive::<Int64Type>();
        let val = out.column(1).as_primitive::<Int64Type>();
        assert_eq!(ts.values(), &[1, 2, 3]);
        assert_eq!(val.values(), &[10, 200, 30]);
    }

    #[test]
    fn plan_flush_sorts_single_chunk_with_duplicate_timestamps() {
        let schema = ts_value_schema();
        let chunks = vec![batch(&[5, 5], &[1, 9])];
        let out = collect_sorted(plan_flush(&chunks, schema).unwrap(), FLUSH_WINDOW_ROWS);
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out.column(1).as_primitive::<Int64Type>().value(0), 9);
    }

    #[test]
    fn plan_flush_sorts_fully_reversed_chunks_across_windows() {
        let schema = ts_value_schema();
        let chunks = vec![batch(&[9, 7], &[90, 70]), batch(&[8, 6], &[80, 60])];
        let out = collect_sorted(plan_flush(&chunks, schema).unwrap(), 1);
        let ts = out.column(0).as_primitive::<Int64Type>();
        let val = out.column(1).as_primitive::<Int64Type>();
        assert_eq!(ts.values(), &[6, 7, 8, 9]);
        assert_eq!(val.values(), &[60, 70, 80, 90]);
    }

    #[test]
    fn filter_batch_skip_seen_timestamps_newest_layer_wins() {
        let mut seen = HashSet::new();
        let newer = batch(&[100, 200], &[2, 20]);
        let older = batch(&[100, 300], &[1, 30]);

        let kept_new = filter_batch_skip_seen_timestamps(&newer, &mut seen)
            .unwrap()
            .unwrap();
        assert_eq!(kept_new.num_rows(), 2);

        let kept_old = filter_batch_skip_seen_timestamps(&older, &mut seen)
            .unwrap()
            .unwrap();
        assert_eq!(kept_old.num_rows(), 1);
        assert_eq!(kept_old.column(1).as_primitive::<Int64Type>().value(0), 30);
    }
}
