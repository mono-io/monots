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

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use monots_query::LsmTableProvider;
use monots_storage::table::LsmTable;
use monots_storage::{LsmEngine, MemoryController};
use std::sync::Arc;

fn ts_col_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
    ]))
}

#[tokio::test]
async fn table_provider_sql_collects_all_flushed_rows() {
    let dir = tempfile::tempdir().unwrap();
    let schema = ts_col_schema();
    let memory = Arc::new(MemoryController::new(8 * 1024 * 1024));
    let engine = Arc::new(LsmEngine::new(dir.path()).unwrap());
    engine.disable_disk_watermark_for_tests();
    let table = LsmTable::open(
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

    let mut ts_base = 1_700_000_000_000_i64;
    for batch_idx in 0..40 {
        let mut ts_vals = Vec::with_capacity(2000);
        let mut f_vals = Vec::with_capacity(2000);
        for i in 0..2000 {
            ts_vals.push(ts_base + i);
            f_vals.push((batch_idx * 2000 + i) as f64);
        }
        ts_base += 2000;
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(ts_vals)),
                Arc::new(arrow::array::Float64Array::from(f_vals)),
            ],
        )
        .unwrap();
        table.put_batch(batch).await.unwrap();
    }
    table.flush_all().unwrap();

    let provider = Arc::new(LsmTableProvider {
        name: "t".into(),
        schema: schema.clone(),
        table: table.clone(),
    });
    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();

    let rows: usize = ctx
        .sql("SELECT time FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 80_000);

    let count = ctx
        .sql("SELECT COUNT(*) AS c FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let count_val = count[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count_val, 80_000);

    let stream = ctx
        .sql("SELECT time FROM t")
        .await
        .unwrap()
        .execute_stream()
        .await
        .unwrap();
    let stream_rows: usize = stream
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(stream_rows, 80_000);
}
