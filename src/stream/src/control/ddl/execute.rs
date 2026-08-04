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

//! Execute CREATE / DROP / SHOW stream DDL (runtime side).
//!
//! Metadata mutations stay in the DDL handler; runtime wiring is encapsulated.
//! Failed CREATE allocation rolls back stream metadata.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tracing::{error, info, warn};

use common::{Result, TsdbError};
use monots_catalog::catalog::CatalogManager;
use monots_storage::LsmEngine;

use crate::control::context::StreamContext;
use crate::control::meta::StreamStore;
use crate::control::orchestrator::{
    drop_stream_capture_progress, mark_inactive, StreamRuntimeManager,
};
use crate::model::def::{parse_stream_def, stream_plan, StreamDef};

pub struct StreamDdlContext {
    pub catalog: Arc<CatalogManager>,
    pub streams: Arc<StreamStore>,
    /// Optional so metadata-only tests can run without a full data plane.
    pub storage: Option<Arc<LsmEngine>>,
    pub stream_engine: Option<Arc<StreamRuntimeManager>>,
    pub stream_context: Option<StreamContext>,
}

impl StreamDdlContext {
    fn active_runtime(
        &self,
    ) -> Option<(&StreamContext, &Arc<LsmEngine>, &Arc<StreamRuntimeManager>)> {
        match (&self.stream_context, &self.storage, &self.stream_engine) {
            (Some(ctx), Some(storage), Some(engine)) => Some((ctx, storage, engine)),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamMutatingOutcome {
    Created { name: String },
    Dropped { name: String },
}

static SHOW_STREAMS_SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
static SHOW_STREAM_SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
static SHOW_STATUS_SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();

fn show_streams_schema() -> Arc<Schema> {
    SHOW_STREAMS_SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("stream_name", DataType::Utf8, false),
                Field::new("connector_type", DataType::Utf8, false),
                Field::new("cdc_mode", DataType::Utf8, false),
                Field::new("source_tables", DataType::Utf8, false),
            ]))
        })
        .clone()
}

fn show_stream_schema() -> Arc<Schema> {
    SHOW_STREAM_SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("stream_name", DataType::Utf8, false),
                Field::new("create_statement", DataType::Utf8, false),
                Field::new("stream_plan", DataType::Utf8, false),
            ]))
        })
        .clone()
}

fn show_status_schema() -> Arc<Schema> {
    SHOW_STATUS_SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("stream_name", DataType::Utf8, false),
                Field::new("phase", DataType::Utf8, false),
                Field::new("current_step", DataType::Utf8, false),
                Field::new("cdc_mode", DataType::Utf8, false),
                Field::new("auto_end", DataType::Utf8, false),
                Field::new("batch_files_done", DataType::Int64, false),
                Field::new("batch_files_total", DataType::Int64, false),
                Field::new("acked_lsn", DataType::Int64, false),
                Field::new("log_channel_opened_ms", DataType::Int64, false),
            ]))
        })
        .clone()
}

pub async fn create_stream(
    ctx: &StreamDdlContext,
    name: String,
    if_not_exists: bool,
    options: HashMap<String, String>,
) -> Result<StreamMutatingOutcome> {
    if ctx.streams.get(&name).is_some() {
        if if_not_exists {
            return Ok(StreamMutatingOutcome::Created { name });
        }
        return Err(TsdbError::Schema(format!("stream {name} already exists")));
    }

    if ctx.storage.is_some() && ctx.active_runtime().is_none() {
        return Err(TsdbError::Storage(
            "CREATE STREAM requires stream_context and stream_engine when storage is configured"
                .into(),
        ));
    }

    let def = parse_stream_def(name.clone(), &options, now_ms())?;
    for table in &def.source_tables {
        if ctx.catalog.get_table(table).is_none() {
            return Err(TsdbError::TableNotFound(table.clone()));
        }
    }

    ctx.streams.put(def.clone()).await?;
    info!(stream = %name, "Stream metadata created successfully");

    if let Some((rt_ctx, storage, orchestrator)) = ctx.active_runtime() {
        if let Err(e) = allocate_stream_runtime(&def, rt_ctx, storage).await {
            error!(
                stream = %name,
                error = %e,
                "Failed to allocate stream runtime, rolling back metadata"
            );
            let _ = teardown_stream_runtime(&def, rt_ctx, storage, orchestrator).await;
            let _ = ctx.streams.remove(&name).await;
            return Err(e);
        }

        if let Err(e) = orchestrator.start_stream(rt_ctx.clone(), &name).await {
            warn!(
                stream = %name,
                error = %e,
                "Stream created but failed to start supervisor"
            );
        }
    }

    Ok(StreamMutatingOutcome::Created { name })
}

pub async fn drop_stream(ctx: &StreamDdlContext, name: String) -> Result<StreamMutatingOutcome> {
    let def = ctx
        .streams
        .get(&name)
        .ok_or_else(|| TsdbError::TableNotFound(format!("stream {name}")))?;

    if let Some((rt_ctx, storage, orchestrator)) = ctx.active_runtime() {
        teardown_stream_runtime(&def, rt_ctx, storage, orchestrator).await?;
    } else if let Some(storage) = &ctx.storage {
        for table in &def.source_tables {
            let _ = storage.unregister_stream_table_capture(&name, table);
        }
    }

    ctx.streams.remove(&name).await?;
    info!(stream = %name, "Stream successfully dropped");
    Ok(StreamMutatingOutcome::Dropped { name })
}

pub fn show_streams(ctx: &StreamDdlContext) -> Result<RecordBatch> {
    let streams = ctx.streams.list();
    let mut names = Vec::with_capacity(streams.len());
    let mut connectors = Vec::with_capacity(streams.len());
    let mut modes = Vec::with_capacity(streams.len());
    let mut tables = Vec::with_capacity(streams.len());

    for s in streams {
        connectors.push(s.connector_type().as_str().to_string());
        modes.push(s.capture_mode.as_cdc_mode_str().to_string());
        tables.push(s.source_tables.join(","));
        names.push(s.name);
    }

    RecordBatch::try_new(
        show_streams_schema(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(connectors)),
            Arc::new(StringArray::from(modes)),
            Arc::new(StringArray::from(tables)),
        ],
    )
    .map_err(TsdbError::from)
}

pub fn show_stream(ctx: &StreamDdlContext, name: &str) -> Result<RecordBatch> {
    let def = ctx
        .streams
        .get(name)
        .ok_or_else(|| TsdbError::TableNotFound(format!("stream {name}")))?;

    let ddl = format_stream_ddl(&def);
    let plan = stream_plan(&def).join(" -> ");

    RecordBatch::try_new(
        show_stream_schema(),
        vec![
            Arc::new(StringArray::from(vec![def.name])),
            Arc::new(StringArray::from(vec![ddl])),
            Arc::new(StringArray::from(vec![plan])),
        ],
    )
    .map_err(TsdbError::from)
}

pub fn show_stream_status(ctx: &StreamDdlContext, name: &str) -> Result<RecordBatch> {
    let def = ctx
        .streams
        .get(name)
        .ok_or_else(|| TsdbError::TableNotFound(format!("stream {name}")))?;

    let status = ctx
        .stream_context
        .as_ref()
        .map(|rt| rt.runtime_states.get(name).as_stream_status())
        .unwrap_or_default();
    let phase = format!("{:?}", status.phase).to_ascii_lowercase();

    RecordBatch::try_new(
        show_status_schema(),
        vec![
            Arc::new(StringArray::from(vec![def.name])),
            Arc::new(StringArray::from(vec![phase])),
            Arc::new(StringArray::from(vec![status.current_step])),
            Arc::new(StringArray::from(vec![def
                .capture_mode
                .as_cdc_mode_str()
                .to_string()])),
            Arc::new(StringArray::from(vec![def.auto_end.to_string()])),
            Arc::new(Int64Array::from(vec![status.batch_files_done as i64])),
            Arc::new(Int64Array::from(vec![status.batch_files_total as i64])),
            Arc::new(Int64Array::from(vec![status.acked_lsn as i64])),
            Arc::new(Int64Array::from(vec![status.log_channel_opened_ms])),
        ],
    )
    .map_err(TsdbError::from)
}

async fn allocate_stream_runtime(
    def: &StreamDef,
    rt_ctx: &StreamContext,
    storage: &Arc<LsmEngine>,
) -> Result<()> {
    let table = def.source_tables.first().map(|s| s.as_str()).unwrap_or("");
    rt_ctx.runtime_states.ensure(&def.name, table);
    rt_ctx.sources.ensure_stream(&def.name);

    let progress = &rt_ctx.progress;
    let arrow_block = {
        let block = rt_ctx.alloc_arrow_block();
        rt_ctx
            .sources
            .set_arrow_block(&def.name, Arc::clone(&block));
        block
    };

    let manager =
        crate::data::ingress::StreamSourceManager::new(storage.base_dir(), Arc::clone(storage));

    for tbl in &def.source_tables {
        let progress_id = crate::control::progress::capture_progress_id(&def.name, tbl);
        progress.progress().register(&progress_id, 0)?;

        let source = manager
            .load_or_create_source(
                &def.name,
                tbl,
                def.capture_mode,
                Some(Arc::clone(&arrow_block)),
            )
            .await?;
        rt_ctx.sources.insert(&def.name, tbl, source);
    }
    Ok(())
}

async fn teardown_stream_runtime(
    def: &StreamDef,
    rt_ctx: &StreamContext,
    storage: &Arc<LsmEngine>,
    orchestrator: &Arc<StreamRuntimeManager>,
) -> Result<()> {
    orchestrator.abort_stream(&def.name).await?;

    let _ = mark_inactive(rt_ctx, &def.name);
    rt_ctx.runtime_states.remove(&def.name);
    rt_ctx.sources.remove_stream(&def.name);

    for table in &def.source_tables {
        let _ = storage.unregister_stream_table_capture(&def.name, table);
    }

    drop_stream_capture_progress(
        storage,
        rt_ctx.progress.progress(),
        &def.name,
        &def.source_tables,
    )?;
    Ok(())
}

pub fn format_stream_ddl(def: &StreamDef) -> String {
    let mut parts = vec![
        format!("'sink.type' = '{}'", def.connector_type().as_str()),
        format!("'source.table' = '{}'", def.source_tables.join(",")),
        format!("'cdc.mode' = '{}'", def.capture_mode.as_cdc_mode_str()),
        format!("'sink.format' = '{}'", def.delivery_format()),
    ];

    match &def.sink_config {
        crate::model::SinkConfig::Delta {
            path,
            endpoint,
            options,
        } => {
            parts.push(format!("'sink.delta.path' = '{path}'"));
            if let Some(ep) = endpoint.as_ref().filter(|s| !s.is_empty()) {
                parts.push(format!("'sink.delta.endpoint' = '{ep}'"));
            }
            for (k, v) in options.ddl_pairs(endpoint.as_deref()) {
                parts.push(format!("'{k}' = '{v}'"));
            }
        }
        crate::model::SinkConfig::Filesystem { path } => {
            parts.push(format!("'sink.filesystem.path' = '{path}'"));
        }
        crate::model::SinkConfig::Kafka { brokers, topic, .. } => {
            parts.push(format!("'sink.kafka.brokers' = '{brokers}'"));
            parts.push(format!("'sink.kafka.topic' = '{topic}'"));
        }
    }
    if def.auto_end {
        parts.push(format!("'cdc.auto_end' = '{}'", def.auto_end));
    }

    format!(
        "CREATE STREAM {} WITH (\n  {}\n)",
        def.name,
        parts.join(",\n  ")
    )
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Idempotent start after CREATE when the control plane is fully wired.
pub async fn start_stream_if_ready(ctx: &StreamDdlContext, stream_name: &str) {
    let Some((rt, _, engine)) = ctx.active_runtime() else {
        return;
    };
    if ctx.streams.get(stream_name).is_some() {
        let _ = engine.start_stream(rt.clone(), stream_name).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DeltaSinkOptions, SinkConfig, StreamDef};
    use common::StreamCaptureMode;

    #[test]
    fn format_delta_ddl_always_emits_industrial_defaults() {
        let def = StreamDef {
            name: "metrics_out".into(),
            source_tables: vec!["metrics".into()],
            capture_mode: StreamCaptureMode::Batch,
            sink_config: SinkConfig::Delta {
                path: "s3://b/t".into(),
                endpoint: Some("http://127.0.0.1:9000".into()),
                options: DeltaSinkOptions::default(),
            },
            created_at_ms: 0,
            auto_end: false,
        };
        let ddl = format_stream_ddl(&def);
        assert!(ddl.contains("'sink.delta.path' = 's3://b/t'"));
        assert!(ddl.contains("'sink.delta.endpoint' = 'http://127.0.0.1:9000'"));
        assert!(ddl.contains("'sink.delta.region' = 'us-east-1'"));
        assert!(ddl.contains("'sink.delta.path.style.access' = 'true'"));
        assert!(ddl.contains("'sink.delta.connection.maximum' = '500'"));
        assert!(ddl.contains("'sink.delta.connection.timeout' = '200s'"));
        assert!(ddl.contains("'sink.delta.attempts.maximum' = '20'"));
        assert!(!ddl.contains("rolling-policy"));
        assert!(!ddl.contains("autoOptimize"));
        assert!(!ddl.contains("logRetentionDuration"));
        assert!(!ddl.contains("deletedFileRetentionDuration"));
    }
}
