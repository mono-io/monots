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

mod auth;
mod service;

use clap::Parser;
use common::{print_banner, set_process_name, LogConfig, LogGuard, DEFAULT_PROCESS_NAME};
use monots_core::config::{AppConfig, ResolvedServerConfig};
use monots_core::engine::TsdbEngine;
use parking_lot::RwLock;
use proto::api::edge_tsdb_server::EdgeTsdbServer;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tonic::transport::Server;

#[derive(Parser, Debug)]
#[command(name = "monots-server", about = "Edge time-series database server")]
struct Args {
    /// YAML config file (`MONOTS_CONF` / `conf/config.yaml` in dist package).
    #[arg(long, env = "MONOTS_CONF")]
    config: Option<PathBuf>,

    #[arg(long)]
    listen: Option<String>,

    #[arg(long)]
    data_dir: Option<PathBuf>,

    #[arg(long)]
    username: Option<String>,

    #[arg(long)]
    password: Option<String>,

    #[arg(long)]
    memtable_max_bytes: Option<usize>,

    #[arg(long)]
    global_memory_limit_bytes: Option<usize>,

    /// Soft threshold ratio (0.0–1.0) for proactive largest-memtable flush.
    #[arg(long)]
    global_memory_soft_threshold_ratio: Option<f64>,

    #[arg(long)]
    sync_max_pending: Option<usize>,
}

fn config_base(config_path: &Path) -> PathBuf {
    if let Ok(home) = std::env::var("MONOTS_HOME") {
        return PathBuf::from(home);
    }
    config_path
        .parent()
        .and_then(|p| p.parent())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    print_banner();
    set_process_name(DEFAULT_PROCESS_NAME);
    let args = Args::parse();

    let (resolved, log_config, log_base) = if let Some(path) = AppConfig::resolve_path(args.config)
    {
        let app = AppConfig::load(&path)?;
        app.apply_lake_env();
        let base = config_base(&path);
        let log_dir = app.resolve_log_dir(&base);
        LogGuard::init(&app.logging, &log_dir);
        tracing::info!(config = %path.display(), "loaded configuration");
        let resolved = ResolvedServerConfig::from_yaml_and_overrides(
            &app,
            &path,
            args.listen,
            args.data_dir,
            args.username,
            args.password,
            args.memtable_max_bytes,
            args.global_memory_limit_bytes,
            args.global_memory_soft_threshold_ratio,
            args.sync_max_pending,
        );
        (resolved, app.logging.clone(), base)
    } else {
        let app = AppConfig::default();
        LogGuard::init(&LogConfig::default(), Path::new("logs"));
        tracing::warn!("no config file found; using defaults (pass --config conf/config.yaml)");
        let resolved = ResolvedServerConfig::from_yaml_and_overrides(
            &app,
            Path::new("conf/config.yaml"),
            args.listen,
            args.data_dir,
            args.username,
            args.password,
            args.memtable_max_bytes,
            args.global_memory_limit_bytes,
            args.global_memory_soft_threshold_ratio,
            args.sync_max_pending,
        );
        (resolved, LogConfig::default(), PathBuf::from("."))
    };

    tracing::info!(
        listen = %resolved.listen,
        data_dir = %resolved.data_dir.display(),
        log_dir = %log_config.resolve_directory(&log_base).display(),
        memtable_max_bytes = resolved.engine.memtable_max_bytes,
        global_memory_limit_bytes = resolved.engine.global_memory_limit_bytes,
        global_memory_soft_threshold_ratio = resolved.engine.global_memory_soft_threshold_ratio,
        compaction_threshold_bytes = resolved.engine.compaction_threshold_bytes,
        "monots server starting"
    );

    let engine = TsdbEngine::open(resolved.engine).await?;
    let table_count = engine.catalog().list_tables().len();
    tracing::info!(tables = table_count, "engine opened");

    let engine = Arc::new(tokio::sync::RwLock::new(engine));
    let tsdb_service = service::TsdbService {
        active_tokens: Arc::new(RwLock::new(HashSet::new())),
        engine: engine.clone(),
        username: resolved.username,
        password: resolved.password,
    };

    let svc = EdgeTsdbServer::new(tsdb_service);
    let addr = resolved.listen.parse()?;
    tracing::info!(%addr, "monots server listening");

    Server::builder()
        .add_service(svc)
        .serve_with_shutdown(addr, async move {
            shutdown_signal().await;
            tracing::info!("shutdown signal received, flushing WAL");
            let tables = {
                let guard = engine.read().await;
                guard.snapshot_tables_for_wal_flush()
            };
            let flush = tokio::task::spawn_blocking(move || {
                for table in tables {
                    table.flush_wal()?;
                }
                Ok::<(), common::TsdbError>(())
            });
            match flush.await {
                Ok(Ok(())) => tracing::info!("WAL flush on shutdown complete"),
                Ok(Err(e)) => tracing::error!(error = %e, "WAL flush on shutdown failed"),
                Err(e) => tracing::error!(error = %e, "WAL flush task join failed"),
            }
            tracing::info!("monots server stopped");
        })
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if tokio::signal::ctrl_c().await.is_err() {
            tracing::warn!("failed to listen for Ctrl+C");
        }
    };

    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = sigterm.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await;
    }
}
