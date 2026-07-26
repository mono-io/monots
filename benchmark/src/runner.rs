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

use crate::config::BenchConfig;
use crate::report::BenchReport;
use crate::scenarios::{DedicatedTableScenario, Scenario, SharedTableScenario};
use common::Result;
use monots_core::TsdbEngine;
use std::sync::Arc;
use std::time::Instant;

pub async fn run_all(
    shared: Option<BenchConfig>,
    dedicated: Option<BenchConfig>,
) -> Result<Vec<BenchReport>> {
    let mut reports = Vec::new();
    if let Some(cfg) = shared {
        reports.push(run_scenario(&SharedTableScenario, cfg).await?);
    }
    if let Some(cfg) = dedicated {
        reports.push(run_scenario(&DedicatedTableScenario, cfg).await?);
    }
    Ok(reports)
}

async fn run_scenario(scenario: &dyn Scenario, config: BenchConfig) -> Result<BenchReport> {
    let engine = Arc::new(TsdbEngine::open(config.engine.clone()).await?);
    engine.storage().disable_disk_watermark_for_tests();
    scenario.setup(&engine, &config).await?;

    let start = Instant::now();
    let total_rows = scenario.run(&engine, &config).await?;
    engine.flush_all_wal()?;
    let duration = start.elapsed();

    Ok(BenchReport {
        scenario: scenario.label(&config),
        threads: config.threads,
        tables: config.tables,
        batches_per_thread: config.batches_per_thread,
        rows_per_batch: config.rows_per_batch,
        total_rows,
        duration,
    })
}
