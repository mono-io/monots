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

use crate::provider::LsmTableProvider;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use common::{Result, TsdbError};
use dashmap::DashMap;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeConfig;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionConfig;
use datafusion_execution::memory_pool::FairSpillPool;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

pub struct QuerySession {
    ctx: SessionContext,
    tables: Arc<DashMap<String, Arc<LsmTableProvider>>>,
}

impl QuerySession {
    pub fn new(
        tables: Arc<DashMap<String, Arc<LsmTableProvider>>>,
        memory_limit_bytes: usize,
        spill_dir: PathBuf,
    ) -> Self {
        let _ = std::fs::create_dir_all(&spill_dir);
        let runtime = Arc::new(
            RuntimeConfig::new()
                .with_temp_file_path(spill_dir)
                .with_memory_pool(Arc::new(FairSpillPool::new(memory_limit_bytes.max(1))))
                .build()
                .expect("query RuntimeEnv"),
        );

        let config = SessionConfig::new()
            .with_information_schema(false)
            .with_batch_size(8192)
            .with_target_partitions(1);

        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .build();
        Self {
            ctx: SessionContext::new_with_state(state),
            tables,
        }
    }

    pub async fn register_table(&self, name: &str, table: Arc<LsmTableProvider>) -> Result<()> {
        self.tables.insert(name.to_string(), table.clone());
        self.ctx
            .register_table(name, table)
            .map_err(|e| TsdbError::Query(e.to_string()))?;
        Ok(())
    }

    pub async fn unregister_table(&self, name: &str) -> Result<()> {
        self.tables.remove(name);
        self.ctx
            .deregister_table(name)
            .map_err(|e| TsdbError::Query(e.to_string()))?;
        Ok(())
    }

    pub fn parse_statement(&self, sql: &str) -> Result<datafusion::sql::parser::Statement> {
        self.ctx
            .state()
            .sql_to_statement(sql, "generic")
            .map_err(|e| TsdbError::Query(e.to_string()))
    }

    /// Encode a single batch as a self-contained Arrow IPC stream chunk (for gRPC streaming).
    pub fn encode_batch_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buffer, batch.schema().as_ref())?;
        writer.write(batch)?;
        writer.finish()?;
        Ok(buffer)
    }

    /// Encode multiple batches into one IPC payload (small result sets / tests only).
    pub fn encode_arrow_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        if batches.is_empty() {
            return Ok(buffer);
        }
        let schema = batches[0].schema();
        let mut writer = StreamWriter::try_new(&mut buffer, &schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
        Ok(buffer)
    }

    pub fn decode_arrow_ipc(payload: &[u8]) -> Result<Vec<RecordBatch>> {
        if payload.is_empty() {
            return Ok(vec![]);
        }
        let cursor = Cursor::new(payload);
        let reader = StreamReader::try_new(cursor, None)?;
        reader.map(|batch| batch.map_err(TsdbError::from)).collect()
    }

    /// Wrap collected batches as a single-partition stream (for SHOW / small results).
    pub fn batches_to_stream(batches: Vec<RecordBatch>) -> Result<SendableRecordBatchStream> {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::stream;
        if batches.is_empty() {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(arrow::datatypes::Schema::empty()),
                stream::empty(),
            )));
        }
        let schema = batches[0].schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(batches.into_iter().map(Ok)),
        )))
    }

    /// Stream query results batch-by-batch (bounded memory).
    pub async fn execute_stream(&self, sql: &str) -> Result<SendableRecordBatchStream> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| TsdbError::Query(format!("SQL parse/plan error: {e}")))?;
        df.execute_stream()
            .await
            .map_err(|e| TsdbError::Query(format!("SQL execute error: {e}")))
    }

    /// Collect all batches (for tests and small result sets).
    pub async fn execute_collect(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| TsdbError::Query(e.to_string()))?;
        df.collect()
            .await
            .map_err(|e| TsdbError::Query(e.to_string()))
    }
}
