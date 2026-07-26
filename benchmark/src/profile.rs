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

//! Break down single-thread async write path latency.

use crate::scenarios::make_write_batch;
use common::Result;
use monots_catalog::catalog::CatalogManager;
use monots_catalog::catalog::ColumnDef;
use monots_core::{EngineConfig, TsdbEngine, WalDurabilityMode};
use monots_storage::memtable::MemTable;
use monots_storage::{
    LsmTable, MemoryController, WalBacklogBudget, WalDurabilityMode as StorageWalMode,
    WalWriterOptions,
};
use std::sync::Arc;
use std::time::Instant;

const N: usize = 10_000;

pub async fn run_profile() -> Result<()> {
    let tmp = tempfile::tempdir().map_err(|e| common::TsdbError::Storage(e.to_string()))?;
    let data_dir = tmp.keep();

    println!("=== Single-thread Async profile ({N} writes, 1 row each) ===\n");

    // 1) batch construction
    let t0 = Instant::now();
    let batches: Vec<_> = (0..N).map(|i| make_write_batch(0, i, 1)).collect();
    let build_ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!(
        "1. make_write_batch x{N}:       {build_ms:6.1} ms  ({:>8.0}/s)",
        N as f64 / (build_ms / 1000.0)
    );

    // 2) full engine path (write_batches)
    let engine = open_engine(&data_dir).await?;
    engine
        .create_table_and_load("t_engine", table_columns())
        .await?;

    let t0 = Instant::now();
    for batch in &batches {
        engine
            .write_batches("t_engine", vec![batch.clone()])
            .await?;
    }
    let engine_loop_ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!(
        "2. write_batches loop x{N}:     {engine_loop_ms:6.1} ms  ({:>8.0}/s)",
        N as f64 / (engine_loop_ms / 1000.0)
    );

    let t0 = Instant::now();
    engine.flush_all_wal()?;
    let engine_flush_ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!("3. flush_all_wal (after #2):   {engine_flush_ms:6.1} ms");

    // 4) put_batch direct (storage only, with replication attached)
    let schema = CatalogManager::build_arrow_schema(&table_columns())?;
    let storage = Arc::new(monots_storage::LsmEngine::new(&data_dir)?);
    storage.disable_disk_watermark_for_tests();
    let wal_backlog = Arc::new(WalBacklogBudget::new(
        monots_storage::DEFAULT_WAL_GLOBAL_BACKLOG_MAX_BYTES,
        monots_storage::DEFAULT_WAL_TABLE_BACKLOG_MAX_BYTES,
    ));
    let table_backlog = wal_backlog.new_table_backlog();
    let table = LsmTable::open(
        "t_direct",
        &data_dir.join("t_direct"),
        schema,
        512 * 1024 * 1024,
        monots_storage::DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
        monots_storage::DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
        Arc::new(MemoryController::new(2 * 1024 * 1024 * 1024)),
        vec![],
        WalWriterOptions {
            durability: StorageWalMode::Async,
            micro_batch_max_bytes: monots_storage::DEFAULT_WAL_MICRO_BATCH_MAX_BYTES,
            segment_max_bytes: monots_storage::DEFAULT_WAL_SEGMENT_MAX_BYTES,
            block_max_bytes: monots_storage::DEFAULT_WAL_BLOCK_MAX_BYTES,
            backlog: wal_backlog,
            table_backlog,
            table_name: None,
            notify: None,
        },
    )?;
    storage.register_table("t_direct", table.clone())?;
    let batches2: Vec<_> = (0..N).map(|i| make_write_batch(0, i, 1)).collect();

    let t0 = Instant::now();
    for batch in batches2 {
        table.put_batch(batch).await?;
    }
    let put_ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!(
        "4. put_batch direct x{N}:       {put_ms:6.1} ms  ({:>8.0}/s)",
        N as f64 / (put_ms / 1000.0)
    );

    let t0 = Instant::now();
    table.flush_wal()?;
    let put_flush_ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!("5. flush_wal (after #4):        {put_flush_ms:6.1} ms");

    // 6) memtable only
    let mem = MemTable::new(
        1,
        CatalogManager::build_arrow_schema(&table_columns())?,
        512 * 1024 * 1024,
        Arc::new(MemoryController::new(2 * 1024 * 1024 * 1024)),
        monots_storage::DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
        monots_storage::DEFAULT_MEMTABLE_BATCH_MAX_BYTES,
    );
    let batches3: Vec<_> = (0..N).map(|i| make_write_batch(0, i, 1)).collect();
    let t0 = Instant::now();
    for batch in batches3 {
        mem.insert(Arc::new(batch))?;
    }
    let mem_ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!(
        "6. memtable insert only x{N}:   {mem_ms:6.1} ms  ({:>8.0}/s)",
        N as f64 / (mem_ms / 1000.0)
    );

    println!("\n--- overhead estimate (per write, 1 row) ---");
    let engine_per_us = engine_loop_ms * 1000.0 / N as f64;
    let put_per_us = put_ms * 1000.0 / N as f64;
    let mem_per_us = mem_ms * 1000.0 / N as f64;
    println!(
        "engine overhead vs put_batch: {:.1} µs/write",
        engine_per_us - put_per_us
    );
    println!(
        "put_batch vs mem-only:        {:.1} µs/write",
        put_per_us - mem_per_us
    );
    println!(
        "WAL drain (flush after put):  {:.1} µs/write",
        put_flush_ms * 1000.0 / N as f64
    );

    Ok(())
}

fn table_columns() -> Vec<ColumnDef> {
    vec![
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
    ]
}

async fn open_engine(data_dir: &std::path::Path) -> Result<Arc<TsdbEngine>> {
    Ok(Arc::new(
        TsdbEngine::open(EngineConfig {
            data_dir: data_dir.to_path_buf(),
            memtable_max_bytes: 512 * 1024 * 1024,
            compaction_threshold_bytes: u64::MAX,
            compaction_interval_secs: u64::MAX,
            global_memory_limit_bytes: 2 * 1024 * 1024 * 1024,
            metadata_memory_limit_bytes: 64 * 1024 * 1024,
            wal_durability: WalDurabilityMode::Async,
            ..EngineConfig::default()
        })
        .await?,
    ))
}
