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
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_physical_expr::PhysicalExpr;
use futures::{StreamExt, TryStreamExt};
use monots_query::{LsmLayer, LsmLayeredScanExec};
use monots_storage::{write_sst, SstIdentity};
use std::sync::Arc;

fn ts_value_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn sorted_batch(start: i64, n: i64) -> arrow::record_batch::RecordBatch {
    let schema = ts_value_schema();
    let ts: Vec<i64> = (start..start + n).collect();
    let val: Vec<i64> = ts.clone();
    arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ts)),
            Arc::new(Int64Array::from(val)),
        ],
    )
    .unwrap()
}

fn parquet_layer(path: &str, schema: Arc<Schema>, layer_priority: usize) -> LsmLayer {
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
    use object_store::path::Path as ObjectPath;

    let p = std::path::Path::new(path);
    let location = ObjectPath::from_filesystem_path(p).unwrap();
    let fs_meta = std::fs::metadata(p).unwrap();
    let file = PartitionedFile {
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
    let config = FileScanConfig::new(ObjectStoreUrl::local_filesystem(), schema).with_file(file);
    let plan = ParquetExec::builder(config).build();
    LsmLayer {
        plan: Arc::new(plan),
        layer_priority,
    }
}

#[tokio::test]
async fn lsm_layered_scan_reads_all_parquet_layers() {
    let dir = tempfile::tempdir().unwrap();
    let sizes = [12_000_i64, 34_000, 34_000];
    let mut paths = Vec::new();
    let mut offset = 0_i64;
    for (i, &n) in sizes.iter().enumerate() {
        let batch = sorted_batch(offset, n);
        let meta = write_sst(
            &SstIdentity::fresh_flush(i as u64 + 1, i as u64 + 1),
            &batch,
            dir.path(),
            offset,
            offset + n - 1,
        )
        .unwrap();
        offset += n;
        paths.push(meta.file_path);
    }

    let schema = ts_value_schema();
    let layers: Vec<LsmLayer> = paths
        .iter()
        .rev()
        .enumerate()
        .map(|(idx, p)| parquet_layer(p, schema.clone(), idx))
        .collect();
    let exec = LsmLayeredScanExec::new(layers, schema);
    let ctx = SessionContext::new();
    let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();

    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        rows += batch.unwrap().num_rows();
    }
    assert_eq!(rows, 80_000);
}

#[tokio::test]
async fn lsm_layered_scan_with_projection_collects_all_rows() {
    let dir = tempfile::tempdir().unwrap();
    let sizes = [12_000_i64, 34_000, 34_000];
    let mut paths = Vec::new();
    let mut offset = 0_i64;
    for (i, &n) in sizes.iter().enumerate() {
        let batch = sorted_batch(offset, n);
        let meta = write_sst(
            &SstIdentity::fresh_flush(i as u64 + 1, i as u64 + 1),
            &batch,
            dir.path(),
            offset,
            offset + n - 1,
        )
        .unwrap();
        offset += n;
        paths.push(meta.file_path);
    }

    let schema = ts_value_schema();
    let layers: Vec<LsmLayer> = paths
        .iter()
        .rev()
        .enumerate()
        .map(|(idx, p)| parquet_layer(p, schema.clone(), idx))
        .collect();
    let scan = Arc::new(LsmLayeredScanExec::new(layers, schema.clone()));
    let projection = Arc::new(
        ProjectionExec::try_new(
            vec![(
                Arc::new(Column::new("time", 0)) as Arc<dyn PhysicalExpr>,
                "time".to_string(),
            )],
            scan,
        )
        .unwrap(),
    );
    let ctx = SessionContext::new();
    let task_ctx = ctx.task_ctx();
    let stream = projection.execute(0, task_ctx).unwrap();
    let batches = stream.try_collect::<Vec<_>>().await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 80_000);
}

#[tokio::test]
async fn table_provider_scan_exec_reads_all_rows() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
    ]));
    let dir = tempfile::tempdir().unwrap();
    let memory = Arc::new(monots_storage::MemoryController::new(8 * 1024 * 1024));
    let engine = Arc::new(monots_storage::LsmEngine::new(dir.path()).unwrap());
    engine.disable_disk_watermark_for_tests();
    let table = monots_storage::table::LsmTable::open(
        "t",
        dir.path().join("t"),
        schema.clone(),
        512 * 1024,
        monots_storage::DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
        monots_storage::DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
        memory,
        vec![],
        monots_storage::WalWriterOptions::with_durability(monots_storage::WalDurabilityMode::Async),
    )
    .unwrap();
    engine.register_table("t", table.clone()).unwrap();

    let mut ts = 1_700_000_000_000_i64;
    for batch_idx in 0..40 {
        let mut ts_vals = Vec::with_capacity(2000);
        let mut f_vals = Vec::with_capacity(2000);
        for i in 0..2000 {
            ts_vals.push(ts + i);
            f_vals.push((batch_idx * 2000 + i) as f64);
        }
        ts += 2000;
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ts_vals)),
                Arc::new(arrow::array::Float64Array::from(f_vals)),
            ],
        )
        .unwrap();
        table.put_batch(batch).await.unwrap();
    }
    table.flush_all().unwrap();

    let provider = monots_query::LsmTableProvider {
        name: "t".into(),
        schema: schema.clone(),
        table: table.clone(),
    };
    let ctx = SessionContext::new();
    let state = ctx.state();
    let scan = provider
        .scan(&state, Some(&vec![0]), &[], None)
        .await
        .unwrap();
    let plan = Arc::new(
        ProjectionExec::try_new(
            vec![(
                Arc::new(Column::new("time", 0)) as Arc<dyn PhysicalExpr>,
                "time".to_string(),
            )],
            scan,
        )
        .unwrap(),
    );
    let stream = plan.execute(0, ctx.task_ctx()).unwrap();
    let rows: usize = stream
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 80_000);
}
