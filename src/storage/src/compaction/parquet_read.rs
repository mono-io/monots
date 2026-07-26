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

//! Parquet read helpers: column projection and row-group pruning.

use crate::compaction::reader::BatchAligner;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common::{time_column_index, time_value_at, Result, TsdbError, TIMESTAMP_COLUMN};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use parquet::schema::types::SchemaDescriptor;
use std::fs::File;
use std::path::Path;

#[derive(Debug, Clone, Default)]
pub struct ParquetReadOptions {
    pub min_ts: Option<i64>,
    pub max_ts: Option<i64>,
    /// Column indices in the catalog schema; `timestamp` is always included.
    pub projection: Option<Vec<usize>>,
}

impl ParquetReadOptions {
    pub fn overlaps_file(&self, file_min: i64, file_max: i64) -> bool {
        if let Some(min) = self.min_ts {
            if file_max < min {
                return false;
            }
        }
        if let Some(max) = self.max_ts {
            if file_min > max {
                return false;
            }
        }
        true
    }
}

pub fn read_parquet_schema(path: impl AsRef<Path>) -> Result<SchemaRef> {
    let file = File::open(path.as_ref())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| TsdbError::Storage(e.to_string()))?;
    Ok(builder.schema().clone())
}

pub fn read_parquet_file(
    path: impl AsRef<Path>,
    catalog_schema: SchemaRef,
    opts: &ParquetReadOptions,
) -> Result<Vec<RecordBatch>> {
    let path = path.as_ref();
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| TsdbError::Storage(e.to_string()))?;

    let parquet_schema = builder.parquet_schema();
    let metadata = builder.metadata().clone();

    let row_groups = select_row_groups(&metadata, &parquet_schema, opts);
    if row_groups.is_empty() {
        return Ok(vec![]);
    }

    let projection = build_projection_mask(&parquet_schema, catalog_schema.as_ref(), opts)?;
    let mut reader = builder
        .with_projection(projection)
        .with_row_groups(row_groups)
        .build()
        .map_err(|e| TsdbError::Storage(e.to_string()))?;

    let mut out = Vec::new();
    while let Some(batch) = reader.next() {
        let batch = batch.map_err(|e| TsdbError::Storage(e.to_string()))?;
        if batch.num_rows() == 0 {
            continue;
        }
        let aligned = BatchAligner::align(batch, catalog_schema.clone())?;
        if let Some(filtered) = filter_batch_by_time_inner(&aligned, opts)? {
            out.push(filtered);
        }
    }
    Ok(out)
}

fn build_projection_mask(
    parquet_schema: &SchemaDescriptor,
    catalog_schema: &arrow::datatypes::Schema,
    opts: &ParquetReadOptions,
) -> Result<ProjectionMask> {
    let mut roots = Vec::new();
    roots.push(
        parquet_column_index(parquet_schema, TIMESTAMP_COLUMN)
            .ok_or_else(|| TsdbError::Storage("parquet missing timestamp column".into()))?,
    );

    if let Some(projection) = &opts.projection {
        for idx in projection {
            let name = catalog_schema.field(*idx).name();
            if name == TIMESTAMP_COLUMN {
                continue;
            }
            if let Some(col_idx) = parquet_column_index(parquet_schema, name) {
                if !roots.contains(&col_idx) {
                    roots.push(col_idx);
                }
            }
        }
    } else {
        for i in 0..parquet_schema.num_columns() {
            if !roots.contains(&i) {
                roots.push(i);
            }
        }
    }

    Ok(ProjectionMask::roots(parquet_schema, roots))
}

fn parquet_column_index(parquet_schema: &SchemaDescriptor, name: &str) -> Option<usize> {
    parquet_schema
        .columns()
        .iter()
        .position(|c| c.name() == name)
}

fn select_row_groups(
    metadata: &parquet::file::metadata::ParquetMetaData,
    parquet_schema: &SchemaDescriptor,
    opts: &ParquetReadOptions,
) -> Vec<usize> {
    if opts.min_ts.is_none() && opts.max_ts.is_none() {
        return (0..metadata.row_groups().len()).collect();
    }

    let mut selected = Vec::new();
    for (idx, rg) in metadata.row_groups().iter().enumerate() {
        match row_group_time_range(rg, parquet_schema) {
            Some((rg_min, rg_max)) if opts.overlaps_file(rg_min, rg_max) => selected.push(idx),
            None => selected.push(idx),
            Some(_) => {}
        }
    }
    selected
}

fn row_group_time_range(
    rg: &RowGroupMetaData,
    parquet_schema: &SchemaDescriptor,
) -> Option<(i64, i64)> {
    let col_idx = parquet_column_index(parquet_schema, TIMESTAMP_COLUMN)?;
    let col = rg.column(col_idx);
    let stats = col.statistics()?;
    match stats {
        Statistics::Int64(s) => {
            let min = s.min_opt()?;
            let max = s.max_opt()?;
            Some((*min, *max))
        }
        _ => None,
    }
}

/// O(1) time bounds from Parquet footer row-group statistics (no data pages read).
///
/// Returns `None` when the time column is missing or statistics are absent — callers may
/// fall back to a projected data scan.
pub fn parquet_file_time_bounds(path: impl AsRef<Path>) -> Result<Option<(i64, i64, usize)>> {
    let path = path.as_ref();
    if !path.is_file() {
        return Ok(None);
    }
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| TsdbError::Storage(format!("Failed to open parquet metadata: {e}")))?;
    let metadata = builder.metadata();
    let parquet_schema = builder.parquet_schema();
    let row_count = metadata.file_metadata().num_rows() as usize;
    if row_count == 0 {
        return Ok(Some((0, 0, 0)));
    }

    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut saw_stats = false;
    for rg in metadata.row_groups() {
        if let Some((rg_min, rg_max)) = row_group_time_range(rg, parquet_schema) {
            min_ts = min_ts.min(rg_min);
            max_ts = max_ts.max(rg_max);
            saw_stats = true;
        }
    }
    if !saw_stats {
        return Ok(None);
    }
    Ok(Some((min_ts, max_ts, row_count)))
}

pub fn filter_batch_by_time(
    batch: &RecordBatch,
    opts: &ParquetReadOptions,
) -> Result<Option<RecordBatch>> {
    filter_batch_by_time_inner(batch, opts)
}

fn filter_batch_by_time_inner(
    batch: &RecordBatch,
    opts: &ParquetReadOptions,
) -> Result<Option<RecordBatch>> {
    if opts.min_ts.is_none() && opts.max_ts.is_none() {
        return Ok(Some(batch.clone()));
    }

    use arrow::array::BooleanArray;
    use arrow::compute::filter_record_batch;

    let ts_idx = time_column_index(batch.schema())?;

    let mut keep = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let ts = time_value_at(batch.column(ts_idx), row)?;
        let ok = opts.min_ts.map(|min| ts >= min).unwrap_or(true)
            && opts.max_ts.map(|max| ts <= max).unwrap_or(true);
        keep.push(ok);
    }

    if keep.iter().all(|v| *v) {
        return Ok(Some(batch.clone()));
    }
    if keep.iter().all(|v| !*v) {
        return Ok(None);
    }

    let mask = BooleanArray::from(keep);
    let filtered = filter_record_batch(batch, &mask)
        .map_err(|e| TsdbError::Storage(format!("time filter: {e}")))?;
    if filtered.num_rows() == 0 {
        Ok(None)
    } else {
        Ok(Some(filtered))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    fn write_test_parquet(path: &Path, timestamps: &[i64], values: &[i64]) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]));
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
    }

    #[test]
    fn projection_reads_subset_of_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proj.parquet");
        write_test_parquet(&path, &[1, 2], &[10, 20]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]));
        let batches = read_parquet_file(
            &path,
            schema,
            &ParquetReadOptions {
                projection: Some(vec![0]),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 2);
    }
}
