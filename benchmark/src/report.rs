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

use comfy_table::{Cell, Table};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct BenchReport {
    pub scenario: String,
    pub threads: usize,
    pub tables: usize,
    pub batches_per_thread: usize,
    pub rows_per_batch: usize,
    pub total_rows: u64,
    pub duration: Duration,
}

impl BenchReport {
    pub fn rows_per_sec(&self) -> f64 {
        if self.duration.as_secs_f64() > 0.0 {
            self.total_rows as f64 / self.duration.as_secs_f64()
        } else {
            0.0
        }
    }

    pub fn batches_per_sec(&self) -> f64 {
        let total_batches = (self.batches_per_thread * self.threads) as f64;
        if self.duration.as_secs_f64() > 0.0 {
            total_batches / self.duration.as_secs_f64()
        } else {
            0.0
        }
    }
}

pub fn print_reports(reports: &[BenchReport]) {
    let mut table = Table::new();
    table.set_header(vec![
        "Scenario",
        "Threads",
        "Tables",
        "Total Rows",
        "Duration",
        "Rows/s",
        "Batches/s",
    ]);

    for r in reports {
        table.add_row(vec![
            Cell::new(&r.scenario),
            Cell::new(r.threads),
            Cell::new(r.tables),
            Cell::new(r.total_rows),
            Cell::new(format!("{:.3}s", r.duration.as_secs_f64())),
            Cell::new(format!("{:.0}", r.rows_per_sec())),
            Cell::new(format!("{:.1}", r.batches_per_sec())),
        ]);
    }

    println!("{table}");
}
