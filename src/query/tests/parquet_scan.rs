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

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use monots_storage::{write_sst, SstIdentity};
use std::sync::Arc;

fn ts_value_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn sorted_batch(n: i64) -> arrow::record_batch::RecordBatch {
    let schema = ts_value_schema();
    let ts: Vec<i64> = (0..n).collect();
    let val: Vec<i64> = (0..n).collect();
    arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ts)),
            Arc::new(Int64Array::from(val)),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn parquet_exec_reads_all_rows_from_local_sst() {
    let dir = tempfile::tempdir().unwrap();
    let batch = sorted_batch(34_000);
    let identity = SstIdentity::fresh_flush(1, 1);
    let meta = write_sst(&identity, &batch, dir.path(), 0, 33_999).unwrap();

    let schema = ts_value_schema();
    let path = std::path::Path::new(&meta.file_path);
    let location = object_store::path::Path::from_filesystem_path(path).unwrap();
    let fs_meta = std::fs::metadata(path).unwrap();
    let partitioned_file = PartitionedFile {
        object_meta: object_store::ObjectMeta {
            location,
            last_modified: fs_meta
                .modified()
                .map(chrono::DateTime::<chrono::Utc>::from)
                .unwrap(),
            size: fs_meta.len() as usize,
            e_tag: None,
            version: None,
        },
        partition_values: vec![],
        range: None,
        statistics: None,
        extensions: None,
    };

    let config =
        FileScanConfig::new(ObjectStoreUrl::local_filesystem(), schema).with_file(partitioned_file);
    let exec = ParquetExec::builder(config).build();
    let ctx = SessionContext::new();
    let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();

    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        rows += batch.unwrap().num_rows();
    }
    assert_eq!(rows, 34_000);
}

#[tokio::test]
async fn partitioned_file_new_truncates_local_scan() {
    let dir = tempfile::tempdir().unwrap();
    let batch = sorted_batch(34_000);
    let identity = SstIdentity::fresh_flush(2, 2);
    let meta = write_sst(&identity, &batch, dir.path(), 0, 33_999).unwrap();

    let schema = ts_value_schema();
    let config = FileScanConfig::new(ObjectStoreUrl::local_filesystem(), schema)
        .with_file(PartitionedFile::new(meta.file_path.clone(), meta.file_size));
    let exec = ParquetExec::builder(config).build();
    let ctx = SessionContext::new();
    let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();

    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        rows += batch.unwrap().num_rows();
    }
    assert_eq!(rows, 34_000);
}
