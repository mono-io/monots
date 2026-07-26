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
async fn count_star_over_memtable_with_dedup() {
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
                    name: "value".into(),
                    data_type: "Int64".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();

    engine
        .execute_no_query("INSERT INTO t (time, value) VALUES (100, 1)")
        .await
        .unwrap();
    engine
        .execute_no_query("INSERT INTO t (time, value) VALUES (100, 99)")
        .await
        .unwrap();

    let point = engine
        .query_sql("SELECT value FROM t WHERE time = 100")
        .await
        .unwrap();
    assert_eq!(point.len(), 1);

    let count = engine.query_sql("SELECT COUNT(*) AS c FROM t").await;
    if let Err(e) = &count {
        eprintln!("collect count query failed: {e}");
    }
    count.unwrap();

    let stream = engine.query_sql_stream("SELECT COUNT(*) AS c FROM t").await;
    if let Err(e) = &stream {
        eprintln!("stream count query failed: {e}");
    }
    let mut stream = stream.unwrap();
    use futures::StreamExt;
    while stream.next().await.transpose().unwrap().is_some() {}
}
