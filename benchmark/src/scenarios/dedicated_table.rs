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

use super::{make_write_batch, Scenario};
use crate::config::BenchConfig;
use common::Result;
use monots_catalog::catalog::ColumnDef;
use monots_core::TsdbEngine;
use std::sync::Arc;

fn table_name(index: usize) -> String {
    format!("bench_t_{index:04}")
}

pub struct DedicatedTableScenario;

#[async_trait::async_trait]
impl Scenario for DedicatedTableScenario {
    fn name(&self) -> &'static str {
        "dedicated_table"
    }

    fn label(&self, config: &BenchConfig) -> String {
        format!(
            "1 thread → 1 table ({} tables, {} workers)",
            config.tables,
            config.threads.min(config.tables)
        )
    }

    async fn setup(&self, engine: &Arc<TsdbEngine>, config: &BenchConfig) -> Result<()> {
        let columns = vec![
            ColumnDef {
                name: "time".into(),
                data_type: "Int64".into(),
                nullable: false,
            },
            ColumnDef {
                name: "value".into(),
                data_type: "Int64".into(),
                nullable: true,
            },
        ];
        for i in 0..config.tables {
            engine
                .create_table_and_load(&table_name(i), columns.clone())
                .await?;
        }
        Ok(())
    }

    async fn run(&self, engine: &Arc<TsdbEngine>, config: &BenchConfig) -> Result<u64> {
        let workers = config.threads.min(config.tables);
        let mut handles = Vec::with_capacity(workers);

        for thread_id in 0..workers {
            let engine = engine.clone();
            let table = table_name(thread_id);
            let batches = config.batches_per_thread;
            let rows = config.rows_per_batch;
            handles.push(tokio::spawn(async move {
                let mut written = 0u64;
                for batch_idx in 0..batches {
                    let batch = make_write_batch(thread_id, batch_idx, rows);
                    written += engine.write_batches(&table, vec![batch]).await?;
                }
                Ok::<u64, common::TsdbError>(written)
            }));
        }

        let mut total = 0u64;
        for handle in handles {
            total += handle
                .await
                .map_err(|e| common::TsdbError::Storage(e.to_string()))??;
        }
        Ok(total)
    }
}
