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

mod batch;
mod dedicated_table;
mod shared_table;

pub use dedicated_table::DedicatedTableScenario;
pub use shared_table::SharedTableScenario;

use crate::config::BenchConfig;
use common::Result;
use monots_core::TsdbEngine;
use std::sync::Arc;

#[async_trait::async_trait]
pub trait Scenario: Send + Sync {
    fn name(&self) -> &'static str;
    fn label(&self, config: &BenchConfig) -> String;
    async fn setup(&self, engine: &Arc<TsdbEngine>, config: &BenchConfig) -> Result<()>;
    async fn run(&self, engine: &Arc<TsdbEngine>, config: &BenchConfig) -> Result<u64>;
}

pub use batch::make_write_batch;
