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

use arrow::array::{Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::is_sorted_by_time;
use monots_core::config::EngineConfig;
use monots_core::engine::TsdbEngine;
use monots_core::metadata::catalog::ColumnDef;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

fn ts_col() -> ColumnDef {
    ColumnDef {
        name: "time".into(),
        data_type: "Int64".into(),
        nullable: false,
    }
}

#[tokio::test]
async fn write_batches_resorts_unsorted_input_on_engine() {
    let dir = TempDir::new().unwrap();
    let config = EngineConfig {
        data_dir: PathBuf::from(dir.path()),
        ..EngineConfig::default()
    };
    let engine = TsdbEngine::open(config).await.unwrap();

    engine
        .create_table_and_load(
            "t",
            vec![
                ts_col(),
                ColumnDef {
                    name: "v".into(),
                    data_type: "Int32".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("v", DataType::Int32, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![5000, 1000, 3000])),
            Arc::new(Int32Array::from(vec![50, 10, 30])),
        ],
    )
    .unwrap();
    assert!(!is_sorted_by_time(&batch).unwrap());

    engine.write_batches("t", vec![batch]).await.unwrap();

    let rows = engine
        .query_sql("SELECT v FROM t ORDER BY time")
        .await
        .unwrap();
    let v = rows[0]
        .column_by_name("v")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(v.values(), &[10, 30, 50]);
}
