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

//! Sustained write soak test with periodic process memory sampling.

use crate::config::SoakArgs;
use crate::scenarios::make_write_batch;
use comfy_table::{Cell, Table};
use common::{set_process_name, Result, TsdbError, DEFAULT_PROCESS_NAME};
use monots_catalog::catalog::ColumnDef;
use monots_core::TsdbEngine;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::sync::watch;

fn table_name(index: usize) -> String {
    format!("soak_t_{index:04}")
}

fn soak_columns() -> Vec<ColumnDef> {
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

#[derive(Debug, Clone)]
struct MemorySample {
    elapsed_secs: f64,
    rss_bytes: u64,
    engine_used_bytes: usize,
    engine_limit_bytes: usize,
    pending_memtable_bytes: usize,
    wal_used_bytes: usize,
    wal_limit_bytes: usize,
    rows_written: u64,
    memory_limit_errors: u64,
}

#[derive(Debug, Default)]
struct SoakStats {
    memory_limit_errors: AtomicU64,
    per_thread_errors: Vec<AtomicU64>,
}

impl SoakStats {
    fn new(worker_count: usize) -> Self {
        Self {
            memory_limit_errors: AtomicU64::new(0),
            per_thread_errors: (0..worker_count).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn record_memory_limit(&self, thread_id: usize) {
        self.memory_limit_errors.fetch_add(1, Ordering::Relaxed);
        if let Some(counter) = self.per_thread_errors.get(thread_id) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn total_errors(&self) -> u64 {
        self.memory_limit_errors.load(Ordering::Relaxed)
    }
}

/// Write one batch, retrying on global memory hard-limit rejections until success.
async fn write_batch_with_memory_retry(
    engine: &TsdbEngine,
    table: &str,
    batch: arrow::record_batch::RecordBatch,
    stats: &SoakStats,
    thread_id: usize,
    backoff: Duration,
) -> Result<u64> {
    loop {
        match engine.write_batches(table, vec![batch.clone()]).await {
            Ok(n) => return Ok(n),
            Err(e) if e.is_memory_limit_exceeded() => {
                stats.record_memory_limit(thread_id);
                if !backoff.is_zero() {
                    tokio::time::sleep(backoff).await;
                } else {
                    tokio::task::yield_now().await;
                }
            }
            Err(e) => return Err(e),
        }
    }
}

pub async fn run_soak(args: SoakArgs) -> Result<()> {
    let process_name = args.process_name.as_deref().unwrap_or(DEFAULT_PROCESS_NAME);
    set_process_name(process_name);

    let data_dir = if let Some(dir) = &args.data_dir {
        std::fs::create_dir_all(dir)?;
        dir.clone()
    } else {
        let tmp = tempfile::tempdir().map_err(|e| TsdbError::Storage(e.to_string()))?;
        tmp.keep()
    };

    let config = args.to_bench_config(data_dir.clone());
    let duration = Duration::from_secs(args.duration_secs);
    let sample_interval = Duration::from_secs(args.sample_interval_secs);
    let table_count = config.tables;
    let worker_count = config.threads;
    let retry_backoff = Duration::from_micros(args.memory_retry_backoff_us);

    println!(
        "MonoTS soak test (process name: {process_name})\n  \
         duration={}s  sample_every={}s  tables={}  threads={}  rows/batch={}  \
         global_mem={}  soft_ratio={}  retry_backoff={}us  \
         wal={:?}  data_dir={}\n",
        args.duration_secs,
        args.sample_interval_secs,
        table_count,
        worker_count,
        config.rows_per_batch,
        format_bytes(args.global_memory_limit_bytes as u64),
        args.global_memory_soft_threshold_ratio,
        args.memory_retry_backoff_us,
        config.engine.wal_durability,
        data_dir.display(),
    );

    let engine = Arc::new(TsdbEngine::open(config.engine.clone()).await?);
    // Bench machines may sit near the default 10% free-disk watermark.
    engine.storage().disable_disk_watermark_for_tests();
    let columns = soak_columns();
    for i in 0..table_count {
        engine
            .create_table_and_load(&table_name(i), columns.clone())
            .await?;
    }

    let rows_written = Arc::new(AtomicU64::new(0));
    let stats = Arc::new(SoakStats::new(worker_count));
    let (stop_tx, stop_rx) = watch::channel(false);
    let samples = Arc::new(parking_lot::Mutex::new(Vec::<MemorySample>::new()));

    let sampler = {
        let engine = engine.clone();
        let rows_written = rows_written.clone();
        let stats = stats.clone();
        let samples = samples.clone();
        let mut stop_rx = stop_rx.clone();
        let start = Instant::now();
        tokio::spawn(async move {
            let mut sys = System::new();
            let pid = Pid::from_u32(std::process::id());
            let mut ticker = tokio::time::interval(sample_interval);
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let sample = capture_sample(
                            &mut sys,
                            pid,
                            &engine,
                            &rows_written,
                            &stats,
                            start.elapsed(),
                        );
                        println!(
                            "[{:.0}s] RSS={}  memtable={}/{}  pending={}  WAL={}/{}  rows={}  mem_errors={}",
                            sample.elapsed_secs,
                            format_bytes(sample.rss_bytes),
                            format_bytes(sample.engine_used_bytes as u64),
                            format_bytes(sample.engine_limit_bytes as u64),
                            format_bytes(sample.pending_memtable_bytes as u64),
                            format_bytes(sample.wal_used_bytes as u64),
                            format_bytes(sample.wal_limit_bytes as u64),
                            sample.rows_written,
                            sample.memory_limit_errors,
                        );
                        samples.lock().push(sample);
                    }
                    changed = stop_rx.changed() => {
                        if changed.is_ok() && *stop_rx.borrow() {
                            break;
                        }
                    }
                }
            }

            let sample = capture_sample(
                &mut sys,
                pid,
                &engine,
                &rows_written,
                &stats,
                start.elapsed(),
            );
            samples.lock().push(sample);
        })
    };

    let start = Instant::now();
    let deadline = start + duration;
    let mut handles = Vec::with_capacity(worker_count);
    for thread_id in 0..worker_count {
        let engine = engine.clone();
        let rows_written = rows_written.clone();
        let stats = stats.clone();
        let table = table_name(thread_id % table_count);
        let rows = config.rows_per_batch;
        handles.push(tokio::spawn(async move {
            let mut batch_idx = 0usize;
            while Instant::now() < deadline {
                let batch = make_write_batch(thread_id, batch_idx, rows);
                batch_idx = batch_idx.wrapping_add(1);
                let n = write_batch_with_memory_retry(
                    &engine,
                    &table,
                    batch,
                    &stats,
                    thread_id,
                    retry_backoff,
                )
                .await?;
                rows_written.fetch_add(n, Ordering::Relaxed);
            }
            Ok::<(), TsdbError>(())
        }));
    }

    for handle in handles {
        handle
            .await
            .map_err(|e| TsdbError::Storage(e.to_string()))??;
    }

    let elapsed = start.elapsed();
    engine.flush_all_wal()?;
    let _ = stop_tx.send(true);
    sampler
        .await
        .map_err(|e| TsdbError::Storage(e.to_string()))?;

    let final_rows = rows_written.load(Ordering::Relaxed);
    print_summary(
        process_name,
        table_count,
        worker_count,
        config.rows_per_batch,
        elapsed,
        final_rows,
        &stats,
        &samples.lock(),
        &data_dir,
    );

    Ok(())
}

fn capture_sample(
    sys: &mut System,
    pid: Pid,
    engine: &TsdbEngine,
    rows_written: &AtomicU64,
    stats: &SoakStats,
    elapsed: Duration,
) -> MemorySample {
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_memory(),
    );
    let rss_bytes = sys.process(pid).map(|p| p.memory()).unwrap_or(0);

    let (engine_used_bytes, engine_limit_bytes, pending_memtable_bytes) = engine.memory_stats();
    let (wal_used_bytes, wal_limit_bytes) = engine.wal_backlog_stats();

    MemorySample {
        elapsed_secs: elapsed.as_secs_f64(),
        rss_bytes,
        engine_used_bytes,
        engine_limit_bytes,
        pending_memtable_bytes,
        wal_used_bytes,
        wal_limit_bytes,
        rows_written: rows_written.load(Ordering::Relaxed),
        memory_limit_errors: stats.total_errors(),
    }
}

fn print_summary(
    process_name: &str,
    table_count: usize,
    worker_count: usize,
    rows_per_batch: usize,
    elapsed: Duration,
    total_rows: u64,
    stats: &SoakStats,
    samples: &[MemorySample],
    data_dir: &std::path::Path,
) {
    let rss_values: Vec<u64> = samples.iter().map(|s| s.rss_bytes).collect();
    let engine_values: Vec<usize> = samples.iter().map(|s| s.engine_used_bytes).collect();
    let wal_values: Vec<usize> = samples.iter().map(|s| s.wal_used_bytes).collect();

    let min_rss = rss_values.iter().copied().min().unwrap_or(0);
    let max_rss = rss_values.iter().copied().max().unwrap_or(0);
    let avg_rss = if rss_values.is_empty() {
        0
    } else {
        rss_values.iter().sum::<u64>() / rss_values.len() as u64
    };

    let min_engine = engine_values.iter().copied().min().unwrap_or(0);
    let max_engine = engine_values.iter().copied().max().unwrap_or(0);
    let avg_engine = if engine_values.is_empty() {
        0
    } else {
        engine_values.iter().sum::<usize>() / engine_values.len()
    };

    let min_wal = wal_values.iter().copied().min().unwrap_or(0);
    let max_wal = wal_values.iter().copied().max().unwrap_or(0);
    let avg_wal = if wal_values.is_empty() {
        0
    } else {
        wal_values.iter().sum::<usize>() / wal_values.len()
    };

    let rows_per_sec = if elapsed.as_secs_f64() > 0.0 {
        total_rows as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    let total_mem_errors = stats.total_errors();
    let errors_per_thread: Vec<u64> = stats
        .per_thread_errors
        .iter()
        .map(|c| c.load(Ordering::Relaxed))
        .collect();
    let max_thread_errors = errors_per_thread.iter().copied().max().unwrap_or(0);
    let min_thread_errors = errors_per_thread.iter().copied().min().unwrap_or(0);
    let avg_thread_errors = if errors_per_thread.is_empty() {
        0.0
    } else {
        total_mem_errors as f64 / errors_per_thread.len() as f64
    };

    println!("\n=== Soak summary ({process_name}) ===");
    println!("Tables:       {table_count}");
    println!("Threads:      {worker_count}");
    println!("Duration:     {:.1}s", elapsed.as_secs_f64());
    println!("Total rows:   {total_rows}");
    println!("Throughput:   {rows_per_sec:.0} rows/s");
    println!("Mem errors:   total={total_mem_errors}  per-thread min={min_thread_errors} avg={avg_thread_errors:.1} max={max_thread_errors}");
    println!("Data dir:     {}", data_dir.display());
    println!("Samples:      {}", samples.len());
    println!(
        "Process RSS:  min={} avg={} max={}",
        format_bytes(min_rss),
        format_bytes(avg_rss),
        format_bytes(max_rss),
    );
    let engine_limit = samples.first().map(|s| s.engine_limit_bytes).unwrap_or(0);
    let last_pending = samples
        .last()
        .map(|s| s.pending_memtable_bytes)
        .unwrap_or(0);

    println!(
        "Memtable:     charged={} avg={} max={} / {}  pending={}",
        format_bytes(min_engine as u64),
        format_bytes(avg_engine as u64),
        format_bytes(max_engine as u64),
        format_bytes(engine_limit as u64),
        format_bytes(last_pending as u64),
    );
    println!(
        "WAL backlog:  min={} avg={} max={}",
        format_bytes(min_wal as u64),
        format_bytes(avg_wal as u64),
        format_bytes(max_wal as u64),
    );

    if samples.len() >= 2 {
        let first_rss = samples.first().map(|s| s.rss_bytes).unwrap_or(0);
        let last_rss = samples.last().map(|s| s.rss_bytes).unwrap_or(0);
        if last_rss > first_rss.saturating_add(first_rss / 10) {
            println!(
                "RSS growth:   +{} over run ({:.1}%)",
                format_bytes(last_rss.saturating_sub(first_rss)),
                (last_rss as f64 - first_rss as f64) / first_rss.max(1) as f64 * 100.0,
            );
        } else {
            println!("RSS growth:   stable");
        }
    }

    if !errors_per_thread.is_empty() && errors_per_thread.len() <= 64 {
        print_thread_error_table(table_count, &errors_per_thread);
    }

    let csv_bytes = csv_equivalent_bytes(worker_count, rows_per_batch, total_rows);
    let (on_disk_total, on_disk_sst, on_disk_wal) = measure_data_dir_bytes(data_dir);
    if on_disk_total > 0 && csv_bytes > 0 {
        println!(
            "Compression:  on_disk={} (SST={} WAL={})  csv_equiv={}  ratio={:.2}x",
            format_bytes(on_disk_total),
            format_bytes(on_disk_sst),
            format_bytes(on_disk_wal),
            format_bytes(csv_bytes),
            csv_bytes as f64 / on_disk_total as f64,
        );
    }

    let mut table = Table::new();
    table.set_header(vec![
        "Elapsed",
        "RSS",
        "Mem Charged",
        "Pending",
        "WAL Used",
        "Rows",
        "Mem Errors",
    ]);
    for sample in samples {
        table.add_row(vec![
            Cell::new(format!("{:.0}s", sample.elapsed_secs)),
            Cell::new(format_bytes(sample.rss_bytes)),
            Cell::new(format_bytes(sample.engine_used_bytes as u64)),
            Cell::new(format_bytes(sample.pending_memtable_bytes as u64)),
            Cell::new(format_bytes(sample.wal_used_bytes as u64)),
            Cell::new(sample.rows_written),
            Cell::new(sample.memory_limit_errors),
        ]);
    }
    println!("\n{table}");
}

fn print_thread_error_table(table_count: usize, errors_per_thread: &[u64]) {
    let mut table = Table::new();
    table.set_header(vec!["Thread", "Table", "Mem Errors"]);
    let mut any = false;
    for (thread_id, count) in errors_per_thread.iter().enumerate() {
        if *count == 0 {
            continue;
        }
        any = true;
        table.add_row(vec![
            Cell::new(thread_id),
            Cell::new(table_name(thread_id % table_count)),
            Cell::new(*count),
        ]);
    }
    if any {
        println!("\n=== Memory-limit errors by thread ===\n{table}");
    }
}

/// Hypothetical CSV export size for rows written by [`make_write_batch`].
fn csv_equivalent_bytes(worker_count: usize, rows_per_batch: usize, total_rows: u64) -> u64 {
    if total_rows == 0 || worker_count == 0 || rows_per_batch == 0 {
        return 0;
    }
    let rows_per_thread = total_rows / worker_count as u64;
    let extra = (total_rows % worker_count as u64) as usize;
    let mut bytes = b"time,value\n".len() as u64;
    for thread_id in 0..worker_count {
        let thread_rows = rows_per_thread + u64::from(thread_id < extra);
        let full_batches = thread_rows / rows_per_batch as u64;
        for batch_idx in 0..full_batches {
            bytes += csv_batch_row_bytes(thread_id, batch_idx, rows_per_batch);
        }
        let partial = (thread_rows % rows_per_batch as u64) as usize;
        if partial > 0 {
            bytes += csv_batch_row_bytes(thread_id, full_batches, partial);
        }
    }
    bytes
}

fn csv_batch_row_bytes(thread_id: usize, batch_idx: u64, rows: usize) -> u64 {
    let mut bytes = 0u64;
    for i in 0..rows {
        let ts =
            (thread_id as i64) * 1_000_000_000_000 + (batch_idx as i64) * rows as i64 + i as i64;
        let val = (thread_id * 10_000 + i) as i64;
        bytes += format!("{ts},{val}\n").len() as u64;
    }
    bytes
}

fn measure_data_dir_bytes(data_dir: &std::path::Path) -> (u64, u64, u64) {
    let mut total = 0u64;
    let mut sst = 0u64;
    let mut wal = 0u64;
    let mut stack = vec![data_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
            total = total.saturating_add(size);
            let path_str = path.to_string_lossy();
            if path_str.ends_with(".parquet") {
                sst = sst.saturating_add(size);
            } else if path_str.ends_with(".wal") {
                wal = wal.saturating_add(size);
            }
        }
    }
    (total, sst, wal)
}

fn format_bytes(bytes: u64) -> String {
    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::{csv_equivalent_bytes, format_bytes};

    #[test]
    fn format_bytes_scales() {
        assert_eq!(format_bytes(512), "512 B");
        assert!(format_bytes(2048).contains("KiB"));
        assert!(format_bytes(5 * 1024 * 1024).contains("MiB"));
    }

    #[test]
    fn csv_equivalent_includes_header_and_rows() {
        let bytes = csv_equivalent_bytes(1, 2, 2);
        assert_eq!(
            bytes,
            b"time,value\n".len() as u64 + "0,0\n1,1\n".len() as u64
        );
    }
}
