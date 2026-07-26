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

use crate::config::EngineConfig;
use crate::sql::insert::build_insert_batch;
use crate::sql::route::{ensure_no_query, ensure_query, route_sql, NoQueryKind};
use crate::sql::stream_ddl::{self, execute_mutating, execute_show};
use crate::sql::types::sql_type_name;
use crate::stream::{
    start_stream_if_ready, StreamContext, StreamDdlContext, StreamEngine, StreamMutatingOutcome,
    StreamStore,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common::{Result, TsdbError};
use dashmap::DashMap;
use datafusion::physical_plan::SendableRecordBatchStream;
use monots_catalog::catalog::{CatalogManager, ColumnDef};
use monots_query::{LsmTableProvider, QuerySession};
use monots_storage::{
    validate_write_batch, BatchAligner, BulkLoadResult, Compactor, FileIndex, GlobalCompactor,
    LsmEngine, LsmTable, MemoryController, TableOpenOptions, WalBacklogBudget,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

struct TableHandle {
    compactor: Arc<Compactor>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FlushStats {
    pub files_flushed: u32,
    pub rows_flushed: u64,
}

pub struct TsdbEngine {
    config: EngineConfig,
    catalog: Arc<CatalogManager>,
    streams: Arc<StreamStore>,
    stream_engine: Arc<StreamEngine>,
    /// Shared Stream Source registry (CREATE / resume → StreamTaskRunner).
    stream_sources: monots_stream::StreamSourceRegistry,
    /// Stream-owned capture progress (WAL retention pin).
    stream_progress: std::sync::Arc<monots_stream::ProgressManager>,
    /// In-memory stream phase / metrics (not persisted in streams/*.pb).
    stream_runtime_states: monots_stream::RuntimeStateRegistry,
    /// Process-wide CDC Arrow memory pool for Stream Source buffers.
    stream_arrow_pool: Arc<monots_stream::StreamArrowMemoryPool>,
    memory: Arc<MemoryController>,
    wal_backlog: Arc<WalBacklogBudget>,
    storage: Arc<LsmEngine>,
    query: Arc<QuerySession>,
    compactors: DashMap<String, TableHandle>,
    global_compactor: Arc<GlobalCompactor>,
}

impl TsdbEngine {
    pub async fn open(config: EngineConfig) -> Result<Self> {
        fs::create_dir_all(&config.data_dir)?;
        let catalog = Arc::new(CatalogManager::new(
            &config.data_dir,
            config.metadata_memory_limit_bytes,
        )?);
        let streams = StreamStore::open(&config.data_dir).await?;
        let memory = Arc::new(MemoryController::with_soft_threshold(
            config.global_memory_limit_bytes,
            config.global_memory_soft_threshold_ratio,
        ));
        let wal_backlog = Arc::new(WalBacklogBudget::new(
            config.wal_global_backlog_max_bytes,
            config.wal_table_backlog_max_bytes,
        ));
        let storage = Arc::new(LsmEngine::with_cdc_limits(
            config.data_dir.clone(),
            config.sync_wal_load_cache_max_bytes,
        )?);
        storage
            .disk_space()
            .set_min_free_ratio(config.disk_min_free_ratio);
        storage.install_memory_reclaim_handler(memory.clone());
        let provider_map: Arc<DashMap<String, Arc<LsmTableProvider>>> = Arc::new(DashMap::new());
        let query = Arc::new(QuerySession::new(
            provider_map,
            config.query_memory_limit_bytes,
            config.data_dir.join("query_spill"),
        ));

        let global_compactor = GlobalCompactor::start(
            config.compaction_max_concurrent_jobs,
            config.compaction_interval_secs,
        );

        let stream_progress = monots_stream::ProgressManager::open(
            &config.data_dir,
            common::CommitDurability::Async,
        )?;
        stream_progress.bind_engine(Arc::clone(&storage))?;
        storage
            .replication()
            .set_retention_pin(stream_progress.as_retention_pin());

        let stream_arrow_pool = monots_stream::StreamArrowMemoryPool::new(
            monots_stream::DEFAULT_STREAM_ARROW_POOL_BYTES,
        );

        // Boot the Stream data-plane Tokio pool before any ingress/supervisor tasks.
        monots_stream::init_executor(monots_stream::ExecutorConfig::default());

        let engine = Self {
            config,
            catalog,
            streams,
            stream_engine: Arc::new(StreamEngine::new()),
            stream_sources: monots_stream::StreamSourceRegistry::new(),
            stream_progress,
            stream_runtime_states: monots_stream::RuntimeStateRegistry::new(),
            stream_arrow_pool,
            memory,
            wal_backlog,
            storage,
            query,
            compactors: DashMap::new(),
            global_compactor,
        };

        // Starting: mount tables in memory only (catalog SST index; no WAL→SST yet).
        for table_name in engine.catalog.list_tables() {
            engine.mount_table(&table_name).await?;
        }

        // Attach / recover Stream capture while LSM is still Starting so recovery
        // flushes can be observed by capturers.
        engine.prepare_stream_captures().await?;

        // Recovering: replay sealed memtable WAL → SST (capturers already attached).
        engine.storage.begin_disk_recovery()?;
        for table_name in engine.catalog.list_tables() {
            engine.recover_table_disk(&table_name).await?;
        }

        // Running: start stream supervisors and accept writes.
        engine.start_stream_supervisors().await?;
        engine.storage.mark_running()?;

        Ok(engine)
    }

    /// Lazy / DDL path: mount table and recover disk immediately.
    async fn load_table(&self, table_name: &str) -> Result<()> {
        self.mount_table_inner(table_name, TableOpenOptions::default())
            .await?;
        Ok(())
    }

    /// Boot path: memory + catalog SST only; disk recovery runs later.
    async fn mount_table(&self, table_name: &str) -> Result<()> {
        self.mount_table_inner(table_name, TableOpenOptions::deferred())
            .await
    }

    async fn mount_table_inner(&self, table_name: &str, open_opts: TableOpenOptions) -> Result<()> {
        let meta = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| TsdbError::TableNotFound(table_name.to_string()))?;
        let schema = self
            .catalog
            .get_schema(table_name)
            .ok_or_else(|| TsdbError::Schema(format!("missing schema for {table_name}")))?;

        let data_dir = PathBuf::from(&meta.data_dir);
        let table_backlog = self.wal_backlog.new_table_backlog();
        let wal_options = self.config.wal_writer_options_for_table(
            table_name,
            self.wal_backlog.clone(),
            table_backlog,
            self.storage.wal_hub(),
        );
        let table = LsmTable::open_with_options(
            table_name,
            &data_dir,
            schema.clone(),
            self.config.memtable_max_bytes,
            self.config.memtable_batch_max_rows,
            self.config.memtable_batch_max_bytes,
            self.memory.clone(),
            meta.runtime.parquet_files.clone(),
            wal_options,
            open_opts,
        )?;

        table.set_sst_flush_options(monots_storage::SstFlushOptions::from_sizes(
            self.config.flush_window_rows,
            self.config.sst_max_row_group_size,
        ));

        self.storage.attach_table_replication(&table)?;

        if !table.needs_disk_recovery() {
            if let Err(e) = self
                .catalog
                .update_runtime(table_name, table.file_index().snapshot())
                .await
            {
                tracing::error!("catalog sync after WAL recovery failed: {e}");
            }
        }

        self.finish_table_registration(table_name, &data_dir, schema, table)
            .await
    }

    async fn recover_table_disk(&self, table_name: &str) -> Result<()> {
        let table = self
            .storage
            .get_table(table_name)
            .ok_or_else(|| TsdbError::TableNotFound(table_name.to_string()))?;
        table.recover_disk()?;
        if let Err(e) = self
            .catalog
            .update_runtime(table_name, table.file_index().snapshot())
            .await
        {
            tracing::error!("catalog sync after disk recovery failed: {e}");
        }
        Ok(())
    }

    async fn finish_table_registration(
        &self,
        table_name: &str,
        data_dir: &Path,
        schema: SchemaRef,
        table: Arc<LsmTable>,
    ) -> Result<()> {
        let flush_store = self.catalog.meta_store();
        let table_name_owned = table_name.to_string();
        let file_index = table.file_index();
        table.set_on_flush(Arc::new(move |_| {
            if let Err(e) = flush_store.set_manifest(&table_name_owned, file_index.snapshot()) {
                tracing::error!("flush catalog update failed: {e}");
            }
        }));
        table.spawn_background_flush();

        let compactor = Arc::new(Compactor::new(
            table.file_index(),
            data_dir.to_path_buf(),
            self.config.compaction_threshold_bytes,
            self.config.compaction_interval_secs,
            schema.clone(),
            self.config.compaction_strategy,
            self.config.compaction_max_merge_files,
            self.memory.clone(),
        ));
        // Capturer notify **before** FileIndex replace (see Compactor::on_pre_replace).
        let table_for_shadow = table.clone();
        compactor.set_on_pre_replace(Box::new(move |inputs, merged| {
            table_for_shadow.notify_compaction(&inputs, &merged);
        }));
        let compact_store = self.catalog.meta_store();
        let table_for_compact = table_name.to_string();
        let file_index_for_compact = table.file_index();
        let bulk_wal_for_retain = table.bulk_wal();
        // After replace_range: persist catalog manifest.
        compactor.set_on_merge(Box::new(move |_inputs, _merged| {
            if let Err(e) =
                compact_store.set_manifest(&table_for_compact, file_index_for_compact.snapshot())
            {
                tracing::error!("compaction catalog update failed: {e}");
            }
        }));
        compactor.set_retain_input(Arc::new(move |path: &str| {
            bulk_wal_for_retain.pins_path(path)
        }));
        compactor.set_disk_space(self.storage.disk_space());
        self.global_compactor
            .register(table_name, compactor.clone());

        let provider = Arc::new(LsmTableProvider {
            name: table_name.to_string(),
            schema: schema.clone(),
            table: table.clone(),
        });

        self.storage.register_table(table_name, table)?;
        self.query.register_table(table_name, provider).await?;
        self.compactors
            .insert(table_name.to_string(), TableHandle { compactor });
        Ok(())
    }

    pub async fn create_table(&self, table_name: &str, columns: Vec<ColumnDef>) -> Result<()> {
        self.catalog
            .create_table(table_name, columns, &self.config.data_dir)
            .await?;
        Ok(())
    }

    pub async fn ensure_table_loaded(&self, table_name: &str) -> Result<()> {
        let needs_load =
            !self.storage.contains(table_name) && self.catalog.get_table(table_name).is_some();
        if needs_load {
            // During Starting, keep deferred mount so Stream can attach before disk recover.
            if self.storage.lifecycle() == monots_storage::EngineLifecycle::Starting {
                self.mount_table(table_name).await?;
            } else {
                self.load_table(table_name).await?;
            }
        }
        Ok(())
    }

    pub async fn create_table_and_load(
        &self,
        table_name: &str,
        columns: Vec<ColumnDef>,
    ) -> Result<()> {
        self.create_table(table_name, columns).await?;
        self.load_table(table_name).await
    }

    pub async fn add_column(&self, table_name: &str, column: ColumnDef) -> Result<()> {
        self.ensure_table_loaded(table_name).await?;

        if let Some(table) = self.storage.get_table(table_name) {
            {
                let _write_guard = table.block_writes().await;

                self.catalog.add_column(table_name, column).await?;
                let new_schema = self
                    .catalog
                    .get_schema(table_name)
                    .ok_or_else(|| TsdbError::Schema("schema missing after add column".into()))?;

                let table_for_flush = table.clone();
                let flushed = tokio::task::spawn_blocking(move || table_for_flush.flush_all())
                    .await
                    .map_err(|e| TsdbError::Storage(format!("flush task join failed: {e}")))??;

                let table_for_evolve = table.clone();
                let evolve_schema = new_schema.clone();
                tokio::task::spawn_blocking(move || {
                    table_for_evolve.apply_schema_evolution(evolve_schema)
                })
                .await
                .map_err(|e| {
                    TsdbError::Storage(format!("schema evolution task join failed: {e}"))
                })??;

                if !flushed.is_empty() {
                    self.catalog
                        .update_runtime(table_name, table.file_index().snapshot())
                        .await?;
                }

                if let Some(handle) = self.compactors.get(table_name) {
                    handle.compactor.set_target_schema(new_schema.clone());
                }
                let provider = Arc::new(LsmTableProvider {
                    name: table_name.to_string(),
                    schema: new_schema.clone(),
                    table: table.clone(),
                });
                self.query.unregister_table(table_name).await?;
                self.query.register_table(table_name, provider).await?;
            }
            return Ok(());
        }

        self.catalog.add_column(table_name, column).await
    }

    pub async fn drop_table(&self, table_name: &str, if_exists: bool) -> Result<()> {
        if self.catalog.get_table(table_name).is_none() {
            if if_exists {
                return Ok(());
            }
            return Err(TsdbError::TableNotFound(table_name.to_string()));
        }

        if self.storage.contains(table_name) {
            let _ = self.flush_table(table_name).await;
            self.unload_table(table_name).await?;
        }

        let data_dir = self
            .catalog
            .get_table(table_name)
            .map(|m| PathBuf::from(&m.data_dir))
            .ok_or_else(|| TsdbError::TableNotFound(table_name.to_string()))?;

        self.catalog.drop_table(table_name).await?;

        if data_dir.exists() {
            fs::remove_dir_all(&data_dir)?;
        }

        Ok(())
    }

    async fn unload_table(&self, table_name: &str) -> Result<()> {
        if let Some(table) = self.storage.remove_table(table_name) {
            table.release_memory();
        }
        self.global_compactor.deregister(table_name);
        if let Some((_, handle)) = self.compactors.remove(table_name) {
            handle.compactor.cancel();
        }
        self.query.unregister_table(table_name).await?;
        Ok(())
    }

    pub async fn write_batches(&self, table_name: &str, batches: Vec<RecordBatch>) -> Result<u64> {
        self.ensure_table_loaded(table_name).await?;
        let schema = self
            .catalog
            .get_schema(table_name)
            .ok_or_else(|| TsdbError::TableNotFound(table_name.to_string()))?;

        let mut total_rows = 0u64;
        for batch in batches {
            let batch = common::ensure_sorted_by_time(batch)?;
            let aligned = BatchAligner::align(batch, schema.clone())?;
            validate_write_batch(&aligned, schema.as_ref())?;
            total_rows += aligned.num_rows() as u64;
            self.storage.write_to_table(table_name, aligned).await?;
        }
        Ok(total_rows)
    }

    pub async fn flush_table(&self, table_name: &str) -> Result<FlushStats> {
        let table = self
            .storage
            .get_table(table_name)
            .ok_or_else(|| TsdbError::TableNotFound(table_name.to_string()))?;

        let table_for_flush = table.clone();
        let flushed = tokio::task::spawn_blocking(move || table_for_flush.flush_all())
            .await
            .map_err(|e| TsdbError::Storage(format!("flush task join failed: {e}")))??;
        let stats = FlushStats {
            files_flushed: flushed.len() as u32,
            rows_flushed: flushed.iter().map(|m| m.row_count as u64).sum(),
        };
        if !flushed.is_empty() {
            self.catalog
                .update_runtime(table_name, table.file_index().snapshot())
                .await?;
        }
        Ok(stats)
    }

    async fn handle_flush_sql(&self, sql: &str) -> Result<u64> {
        match crate::sql::flush::parse_flush(sql)? {
            None => {
                let mut total = 0u64;
                for name in self.catalog.list_tables() {
                    if self.storage.contains(&name) {
                        total += self.flush_table(&name).await?.rows_flushed;
                    }
                }
                Ok(total)
            }
            Some(table_name) => {
                self.ensure_table_loaded(&table_name).await?;
                Ok(self.flush_table(&table_name).await?.rows_flushed)
            }
        }
    }

    pub async fn execute_no_query(&self, sql: &str) -> Result<u64> {
        let kind = ensure_no_query(route_sql(sql)?)?;
        match kind {
            NoQueryKind::StreamDdl => {
                let outcome = execute_mutating(&self.stream_ctx(), sql).await?;
                match outcome {
                    StreamMutatingOutcome::Created { name } => {
                        for table in self
                            .streams
                            .get(&name)
                            .map(|d| d.source_tables.clone())
                            .unwrap_or_default()
                        {
                            self.ensure_table_loaded(&table).await?;
                        }
                        start_stream_if_ready(&self.stream_ctx(), &name).await;
                    }
                    StreamMutatingOutcome::Dropped { name: _ } => {}
                }
                Ok(0)
            }
            NoQueryKind::CreateTable => {
                self.handle_create_table_sql(sql).await?;
                Ok(0)
            }
            NoQueryKind::AddColumn => {
                self.handle_add_column_sql(sql).await?;
                Ok(0)
            }
            NoQueryKind::DropTable => {
                self.handle_drop_table_sql(sql).await?;
                Ok(0)
            }
            NoQueryKind::Insert => self.handle_insert_sql(sql).await,
            NoQueryKind::BulkLoad => self.handle_bulk_load_sql(sql).await,
            NoQueryKind::FlushTable => self.handle_flush_sql(sql).await,
        }
    }

    pub async fn query_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        ensure_query(route_sql(sql)?)?;
        if stream_ddl::is_stream_show(sql) {
            return execute_show(&self.stream_ctx(), sql);
        }
        if let Some(table_name) = crate::sql::show::parse_show_create_table(sql)? {
            return Ok(vec![crate::sql::show::create_table_batch(
                &self.catalog,
                &table_name,
            )?]);
        }
        if crate::sql::show::is_show_tables(sql) {
            return Ok(vec![crate::sql::show::tables_batch(&self.catalog)?]);
        }
        self.query.execute_collect(sql).await
    }

    pub async fn query_sql_stream(&self, sql: &str) -> Result<SendableRecordBatchStream> {
        ensure_query(route_sql(sql)?)?;
        if stream_ddl::is_stream_show(sql) {
            let batches = execute_show(&self.stream_ctx(), sql)?;
            return QuerySession::batches_to_stream(batches);
        }
        if let Some(table_name) = crate::sql::show::parse_show_create_table(sql)? {
            let batch = crate::sql::show::create_table_batch(&self.catalog, &table_name)?;
            return QuerySession::batches_to_stream(vec![batch]);
        }
        if crate::sql::show::is_show_tables(sql) {
            let batch = crate::sql::show::tables_batch(&self.catalog)?;
            return QuerySession::batches_to_stream(vec![batch]);
        }
        self.query.execute_stream(sql).await
    }

    async fn handle_create_table_sql(&self, sql: &str) -> Result<()> {
        let stmt = self.query.parse_statement(sql)?;

        if let datafusion::sql::parser::Statement::Statement(stmt) = stmt {
            if let datafusion::sql::sqlparser::ast::Statement::CreateTable(create_table) = *stmt {
                let table_name = create_table.name.to_string();
                let cols: Vec<ColumnDef> = create_table
                    .columns
                    .iter()
                    .map(|c| {
                        let dt = sql_type_name(&c.data_type)?;
                        Ok(ColumnDef {
                            name: c.name.value.clone(),
                            data_type: dt,
                            nullable: true,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                self.create_table_and_load(&table_name, cols).await?;
                return Ok(());
            }
        }
        Err(TsdbError::Query("unsupported CREATE TABLE syntax".into()))
    }

    async fn handle_add_column_sql(&self, sql: &str) -> Result<()> {
        let stmt = self.query.parse_statement(sql)?;

        if let datafusion::sql::parser::Statement::Statement(stmt) = stmt {
            if let datafusion::sql::sqlparser::ast::Statement::AlterTable {
                name, operations, ..
            } = *stmt
            {
                let table_name = name.to_string();
                for op in operations {
                    if let datafusion::sql::sqlparser::ast::AlterTableOperation::AddColumn {
                        column_def,
                        ..
                    } = op
                    {
                        let dt = sql_type_name(&column_def.data_type)?;
                        self.add_column(
                            &table_name,
                            ColumnDef {
                                name: column_def.name.value.clone(),
                                data_type: dt,
                                nullable: true,
                            },
                        )
                        .await?;
                    }
                }
                return Ok(());
            }
        }
        Err(TsdbError::Query("unsupported ALTER TABLE syntax".into()))
    }

    async fn handle_drop_table_sql(&self, sql: &str) -> Result<()> {
        let trimmed = sql.trim().trim_end_matches(';');
        let stmt = self.query.parse_statement(trimmed)?;

        if let datafusion::sql::parser::Statement::Statement(stmt) = stmt {
            if let datafusion::sql::sqlparser::ast::Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } = *stmt
            {
                if !matches!(
                    object_type,
                    datafusion::sql::sqlparser::ast::ObjectType::Table
                ) {
                    return Err(TsdbError::Query("only DROP TABLE is supported".into()));
                }
                if names.len() != 1 {
                    return Err(TsdbError::Query(
                        "only single-table DROP TABLE is supported".into(),
                    ));
                }
                let table_name = names[0].to_string();
                self.drop_table(&table_name, if_exists).await?;
                return Ok(());
            }
        }
        Err(TsdbError::Query("unsupported DROP TABLE syntax".into()))
    }

    async fn handle_insert_sql(&self, sql: &str) -> Result<u64> {
        let stmt = self.query.parse_statement(sql)?;

        if let datafusion::sql::parser::Statement::Statement(stmt) = stmt {
            if let datafusion::sql::sqlparser::ast::Statement::Insert(insert) = *stmt {
                let table_name = insert.table_name.to_string();
                self.ensure_table_loaded(&table_name).await?;
                let schema = self
                    .catalog
                    .get_schema(&table_name)
                    .ok_or_else(|| TsdbError::TableNotFound(table_name.clone()))?;

                let batch = build_insert_batch(&insert, schema)?;
                return self.write_batches(&table_name, vec![batch]).await;
            }
        }
        Err(TsdbError::Query("unsupported INSERT syntax".into()))
    }

    async fn handle_bulk_load_sql(&self, sql: &str) -> Result<u64> {
        let (path, table_name) = crate::sql::bulk_load::parse_bulk_load(sql)?;
        let result = self
            .bulk_load_parquet(&table_name, Path::new(&path))
            .await?;
        Ok(result.rows_loaded)
    }

    pub async fn bulk_load_parquet(&self, table_name: &str, path: &Path) -> Result<BulkLoadResult> {
        self.ensure_table_loaded(table_name).await?;
        let table = self
            .storage
            .get_table(table_name)
            .ok_or_else(|| TsdbError::TableNotFound(table_name.to_string()))?;

        let result = table
            .bulk_load_parquet_paths_async(&[path.to_path_buf()])
            .await?;
        self.catalog
            .update_runtime(table_name, table.file_index().snapshot())
            .await?;
        Ok(result)
    }

    pub async fn bulk_load_parquet_files(
        &self,
        table_name: &str,
        paths: &[PathBuf],
    ) -> Result<BulkLoadResult> {
        self.ensure_table_loaded(table_name).await?;
        let table = self
            .storage
            .get_table(table_name)
            .ok_or_else(|| TsdbError::TableNotFound(table_name.to_string()))?;

        let result = table.bulk_load_parquet_paths_async(paths).await?;
        self.catalog
            .update_runtime(table_name, table.file_index().snapshot())
            .await?;
        Ok(result)
    }

    pub async fn load_file(&self, table_name: &str, file_path: &str, format: &str) -> Result<u64> {
        self.ensure_table_loaded(table_name).await?;

        match format.to_lowercase().as_str() {
            "parquet" => {
                let result = self
                    .bulk_load_parquet(table_name, Path::new(file_path))
                    .await?;
                Ok(result.rows_loaded)
            }
            "csv" => {
                let schema = self
                    .catalog
                    .get_schema(table_name)
                    .ok_or_else(|| TsdbError::TableNotFound(table_name.to_string()))?;
                let batches = self.read_csv(file_path, schema)?;
                self.write_batches(table_name, batches).await
            }
            other => Err(TsdbError::Storage(format!("unsupported format: {other}"))),
        }
    }

    fn read_csv(
        &self,
        file_path: &str,
        schema: arrow::datatypes::SchemaRef,
    ) -> Result<Vec<RecordBatch>> {
        use arrow::csv::ReaderBuilder;
        let file = fs::File::open(file_path)?;
        let mut reader = ReaderBuilder::new(schema)
            .with_header(true)
            .build(file)
            .map_err(|e| TsdbError::Storage(e.to_string()))?;
        let mut batches = Vec::new();
        while let Some(batch) = reader.next() {
            batches.push(batch.map_err(|e| TsdbError::Storage(e.to_string()))?);
        }
        Ok(batches)
    }

    pub fn storage(&self) -> Arc<LsmEngine> {
        Arc::clone(&self.storage)
    }

    pub fn catalog(&self) -> Arc<CatalogManager> {
        self.catalog.clone()
    }

    pub fn table_file_index(&self, table_name: &str) -> Result<Arc<FileIndex>> {
        self.storage
            .get_table(table_name)
            .map(|t| t.file_index())
            .ok_or_else(|| TsdbError::TableNotFound(table_name.to_string()))
    }

    pub fn memory_stats(&self) -> (usize, usize, usize) {
        (
            self.memory.used_bytes(),
            self.memory.limit_bytes(),
            self.storage.total_pending_memtable_bytes(),
        )
    }

    pub fn wal_backlog_stats(&self) -> (usize, usize) {
        (
            self.wal_backlog.global_used(),
            self.wal_backlog.global_limit(),
        )
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn flush_all_wal(&self) -> Result<()> {
        self.storage.flush_all_wal()
    }

    pub fn snapshot_tables_for_wal_flush(&self) -> Vec<Arc<LsmTable>> {
        self.storage.snapshot_tables()
    }

    pub async fn force_flush_all(&self) -> Result<FlushStats> {
        let mut total = FlushStats::default();
        for name in self.catalog.list_tables() {
            if self.storage.contains(&name) {
                let stats = self.flush_table(&name).await?;
                total.files_flushed += stats.files_flushed;
                total.rows_flushed += stats.rows_flushed;
            }
        }
        Ok(total)
    }

    pub fn data_dir(&self) -> &Path {
        &self.config.data_dir
    }

    fn stream_ctx(&self) -> StreamDdlContext {
        StreamDdlContext {
            catalog: Arc::clone(&self.catalog),
            streams: Arc::clone(&self.streams),
            storage: Some(Arc::clone(&self.storage)),
            stream_engine: Some(Arc::clone(&self.stream_engine)),
            stream_context: Some(self.stream_context()),
        }
    }

    fn stream_context(&self) -> StreamContext {
        let lake_endpoint = std::env::var("MONOTS_LAKE_ENDPOINT")
            .ok()
            .filter(|s| !s.is_empty());
        let runtime = monots_stream::StreamRuntimeConfig::new()
            .with_poll_ms(self.config.sync_realtime_poll_ms)
            .with_queue_capacity(monots_stream::DEFAULT_QUEUE_CAPACITY)
            .with_lake_endpoint(lake_endpoint)
            .with_arrow_pool_bytes(self.stream_arrow_pool.limit_bytes());
        StreamContext {
            engine: Arc::clone(&self.storage),
            store: Arc::clone(&self.streams),
            sources: self.stream_sources.clone(),
            progress: Arc::clone(&self.stream_progress),
            runtime_states: self.stream_runtime_states.clone(),
            runtime,
            arrow_pool: Arc::clone(&self.stream_arrow_pool),
        }
    }

    async fn prepare_stream_captures(&self) -> Result<()> {
        for def in self.streams.list() {
            let table = def.source_tables.first().map(|s| s.as_str()).unwrap_or("");
            self.stream_runtime_states.ensure(&def.name, table);
            if !monots_stream::should_run_phase(self.stream_runtime_states.phase(&def.name)) {
                continue;
            }
            self.stream_sources.ensure_stream(&def.name);
            let manager = monots_stream::StreamSourceManager::new(
                self.storage.base_dir(),
                std::sync::Arc::clone(&self.storage),
            );
            let arrow_block = {
                let ctx = self.stream_context();
                let block = ctx.alloc_arrow_block();
                self.stream_sources
                    .set_arrow_block(&def.name, std::sync::Arc::clone(&block));
                block
            };
            for table in &def.source_tables {
                self.ensure_table_loaded(table).await?;
                let progress_id = monots_stream::capture_progress_id(&def.name, table);
                self.stream_progress.progress().register(&progress_id, 0)?;
                let source = manager
                    .load_or_create_source(
                        &def.name,
                        table,
                        def.capture_mode,
                        Some(std::sync::Arc::clone(&arrow_block)),
                    )
                    .await?;
                self.stream_sources.insert(&def.name, table, source);
            }
        }
        Ok(())
    }

    async fn start_stream_supervisors(&self) -> Result<()> {
        self.stream_engine.resume_all(self.stream_context()).await;
        Ok(())
    }

    pub fn stream_engine(&self) -> Arc<StreamEngine> {
        Arc::clone(&self.stream_engine)
    }

    pub fn stream_store(&self) -> Arc<StreamStore> {
        Arc::clone(&self.streams)
    }
}
