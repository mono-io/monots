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

use monots_core::config::EngineConfig;
use monots_core::engine::TsdbEngine;
use monots_core::metadata::catalog::ColumnDef;
use std::path::PathBuf;
use tempfile::TempDir;

fn ts_col() -> ColumnDef {
    ColumnDef {
        name: "time".into(),
        data_type: "Int64".into(),
        nullable: false,
    }
}

#[tokio::test]
async fn count_many_flushed_rows() {
    let dir = TempDir::new().unwrap();
    let config = EngineConfig {
        data_dir: PathBuf::from(dir.path()),
        memtable_max_bytes: 256 * 1024,
        ..EngineConfig::default()
    };
    let engine = TsdbEngine::open(config).await.unwrap();
    engine
        .create_table_and_load(
            "t",
            vec![
                ts_col(),
                ColumnDef {
                    name: "value".into(),
                    data_type: "Float64".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();

    let mut ts = 1_700_000_000_000_i64;
    let mut inserted = 0_u64;
    for batch in 0..20 {
        let mut values = String::new();
        for i in 0..500 {
            if i > 0 {
                values.push(',');
            }
            values.push_str(&format!("({}, {})", ts + i, batch * 500 + i));
        }
        ts += 500;
        inserted += engine
            .execute_no_query(&format!("INSERT INTO t (time, value) VALUES {values}"))
            .await
            .unwrap();
    }
    assert_eq!(inserted, 10_000);

    let rows: usize = engine
        .query_sql("SELECT time FROM t")
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 10_000);
}

#[tokio::test]
async fn count_eighty_k_rows_under_small_memtable() {
    let dir = TempDir::new().unwrap();
    let config = EngineConfig {
        data_dir: PathBuf::from(dir.path()),
        memtable_max_bytes: 512 * 1024,
        global_memory_limit_bytes: 8 * 1024 * 1024,
        ..EngineConfig::default()
    };
    let engine = TsdbEngine::open(config).await.unwrap();
    engine
        .create_table_and_load(
            "t",
            vec![
                ts_col(),
                ColumnDef {
                    name: "value".into(),
                    data_type: "Float64".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();

    let mut ts = 1_700_000_000_000_i64;
    let mut inserted = 0_u64;
    for batch in 0..40 {
        let mut values = String::new();
        for i in 0..2000 {
            if i > 0 {
                values.push(',');
            }
            values.push_str(&format!("({}, {})", ts + i, batch * 500 + i));
        }
        ts += 2000;
        inserted += engine
            .execute_no_query(&format!("INSERT INTO t (time, value) VALUES {values}"))
            .await
            .unwrap();
    }
    assert_eq!(inserted, 80_000);

    engine.execute_no_query("FLUSH TABLE t").await.unwrap();

    let rows: usize = engine
        .query_sql("SELECT time FROM t")
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 80_000);

    let count_batches = engine
        .query_sql("SELECT COUNT(*) AS c FROM t")
        .await
        .unwrap();
    let count = count_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 80_000);

    let mut stream = engine.query_sql_stream("SELECT time FROM t").await.unwrap();
    use futures::StreamExt;
    let mut stream_rows = 0usize;
    while let Some(batch) = stream.next().await.transpose().unwrap() {
        stream_rows += batch.num_rows();
    }
    assert_eq!(stream_rows, 80_000);
}
