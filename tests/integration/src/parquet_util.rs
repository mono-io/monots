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

//! Shared Parquet / SST helpers for integration tests.

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

/// Write a simple `(time BIGINT, value BIGINT)` Parquet file.
pub fn write_i64_parquet(path: &Path, timestamps: &[i64], values: &[i64]) {
    assert_eq!(
        timestamps.len(),
        values.len(),
        "timestamps and values length mismatch"
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let vals: Vec<Option<i64>> = values.iter().copied().map(Some).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(timestamps.to_vec())),
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .unwrap();
    let file = fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// List `*.parquet` file names under a table data directory.
pub fn list_sst_files(table_dir: &Path) -> Vec<String> {
    match fs::read_dir(table_dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.ends_with(".parquet"))
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Flip bytes near the middle of a file to simulate on-disk corruption.
pub fn corrupt_file_mid(path: &Path) {
    let meta = fs::metadata(path).unwrap();
    let len = meta.len();
    assert!(len > 16, "file too small to corrupt: {}", path.display());
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let offset = len / 2;
    f.seek(SeekFrom::Start(offset)).unwrap();
    let mut buf = [0u8; 8];
    let n = f.read(&mut buf).unwrap();
    assert!(n > 0);
    for b in &mut buf[..n] {
        *b ^= 0xFF;
    }
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.write_all(&buf[..n]).unwrap();
    f.flush().unwrap();
}
