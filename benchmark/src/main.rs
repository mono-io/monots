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

use clap::Parser;
use monots_benchmark::{print_reports, run_all, Cli, Command, ScenarioKind, WriteArgs};
use std::path::PathBuf;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

// Production allocator: jemalloc with aggressive page decay so freed builder/batch
// buffers are returned to the OS quickly, keeping RSS close to the live heap under
// the high allocation churn of the write path.
#[cfg(all(not(feature = "dhat-heap"), not(target_env = "msvc")))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// tikv-jemalloc builds with the `_rjem_` symbol prefix, so the tuning knob must use
// that exact export name to be picked up at startup.
#[cfg(all(not(feature = "dhat-heap"), not(target_env = "msvc")))]
#[allow(non_upper_case_globals)]
// Cap arena count (default is 4*ncpu, each caching dirty pages → large RSS) and purge
// freed pages back to the OS immediately to keep RSS near the live heap.
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: &[u8] = b"narenas:4,dirty_decay_ms:0,muzzy_decay_ms:0\0";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Heap profiler guard: on drop it writes dhat-heap.json (peak/`t-gmax` breakdown).
    #[cfg(feature = "dhat-heap")]
    let _dhat = dhat::Profiler::new_heap();

    let cli = Cli::parse();
    match cli.command {
        Command::Write(args) => run_write_bench(args).await?,
        Command::Profile => monots_benchmark::profile::run_profile().await?,
        Command::Soak(args) => monots_benchmark::soak::run_soak(args).await?,
    }
    Ok(())
}

async fn run_write_bench(args: WriteArgs) -> common::Result<()> {
    let data_dir = resolve_data_dir(&args)?;
    let base = args.to_bench_config(data_dir);

    println!(
        "MonoTS write benchmark\n  threads={}  batches/thread={}  rows/batch={}  wal={:?}  data_dir={}\n",
        base.threads,
        base.batches_per_thread,
        base.rows_per_batch,
        base.engine.wal_durability,
        base.data_dir.display(),
    );

    let (shared, dedicated) = match args.scenario {
        ScenarioKind::Shared => {
            let mut cfg = base.clone();
            cfg.tables = 1;
            (Some(cfg), None)
        }
        ScenarioKind::Dedicated => {
            let mut cfg = base.clone();
            cfg.threads = cfg.tables;
            (None, Some(cfg))
        }
        ScenarioKind::All => {
            let mut shared_cfg = base.clone();
            shared_cfg.tables = 1;

            let mut dedicated_cfg = base;
            dedicated_cfg.threads = dedicated_cfg.tables;
            (Some(shared_cfg), Some(dedicated_cfg))
        }
    };

    let reports = run_all(shared, dedicated).await?;
    print_reports(&reports);
    Ok(())
}

fn resolve_data_dir(args: &WriteArgs) -> common::Result<PathBuf> {
    if let Some(dir) = &args.data_dir {
        std::fs::create_dir_all(dir)?;
        return Ok(dir.clone());
    }
    let tmp = tempfile::tempdir().map_err(|e| common::TsdbError::Storage(e.to_string()))?;
    Ok(tmp.keep())
}
