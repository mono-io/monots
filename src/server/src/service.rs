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

use crate::auth;
use arrow::record_batch::RecordBatch;
use monots_core::engine::TsdbEngine;
use monots_core::metadata::catalog::ColumnDef;
use monots_core::query::QuerySession;
use parking_lot::RwLock;
use proto::api::edge_tsdb_server::EdgeTsdb;
use proto::api::{
    AddColumnRequest, AddColumnResponse, BulkLoadRequest, BulkLoadResponse, CreateTableRequest,
    CreateTableResponse, DropTableRequest, DropTableResponse, GetMemoryStatsRequest,
    GetMemoryStatsResponse, LoadFileRequest, LoadFileResponse, LoginRequest, LoginResponse,
    NoQueryRequest, NoQueryResponse, QueryRequest, QueryResponse, WriteRequest, WriteResponse,
};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

pub struct TsdbService {
    pub active_tokens: Arc<RwLock<HashSet<String>>>,
    pub engine: Arc<tokio::sync::RwLock<TsdbEngine>>,
    pub username: String,
    pub password: String,
}

#[tonic::async_trait]
impl EdgeTsdb for TsdbService {
    async fn login(
        &self,
        request: Request<LoginRequest>,
    ) -> Result<Response<LoginResponse>, Status> {
        let req = request.into_inner();
        if req.username == self.username && req.password == self.password {
            let token = format!("edge-{}", Uuid::new_v4());
            self.active_tokens.write().insert(token.clone());
            Ok(Response::new(LoginResponse {
                success: true,
                token,
                message: "login successful".into(),
            }))
        } else {
            Ok(Response::new(LoginResponse {
                success: false,
                token: String::new(),
                message: "invalid credentials".into(),
            }))
        }
    }

    async fn write_data(
        &self,
        request: Request<Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        auth::require_auth(&request, &self.active_tokens)?;
        let mut stream = request.into_inner();
        let mut total_rows = 0u64;
        let mut current_table: Option<String> = None;
        let mut batches = Vec::new();

        while let Some(req) = stream.message().await? {
            if current_table.as_deref() != Some(&req.table_name) {
                if let Some(table) = current_table.take() {
                    if !batches.is_empty() {
                        total_rows += write_batches_or_response(
                            &self.engine,
                            &table,
                            std::mem::take(&mut batches),
                        )
                        .await?;
                    }
                }
                current_table = Some(req.table_name.clone());
            }

            if !req.payload.is_empty() {
                let decoded =
                    QuerySession::decode_arrow_ipc(&req.payload).map_err(status_from_err)?;
                batches.extend(decoded);
            }
        }

        if let Some(table) = current_table {
            if !batches.is_empty() {
                total_rows += write_batches_or_response(&self.engine, &table, batches).await?;
            }
        }

        Ok(Response::new(WriteResponse {
            success: true,
            rows_inserted: total_rows,
            message: "batch written successfully".into(),
        }))
    }

    type QueryStream = ReceiverStream<Result<QueryResponse, Status>>;

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        auth::require_auth(&request, &self.active_tokens)?;
        let sql = request.into_inner().sql;
        let (tx, rx) = mpsc::channel(8);

        let stream_result = {
            let engine = self.engine.read().await;
            engine.query_sql_stream(&sql).await
        };

        tokio::spawn(async move {
            match stream_result {
                Ok(mut stream) => {
                    while let Some(item) = stream.next().await {
                        match item {
                            Ok(batch) => match QuerySession::encode_batch_ipc(&batch) {
                                Ok(payload) => {
                                    if tx.send(Ok(QueryResponse { payload })).await.is_err() {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    let _ = tx.send(Err(status_from_err(e))).await;
                                    break;
                                }
                            },
                            Err(e) => {
                                let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(status_from_err(e))).await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn no_query(
        &self,
        request: Request<NoQueryRequest>,
    ) -> Result<Response<NoQueryResponse>, Status> {
        auth::require_auth(&request, &self.active_tokens)?;
        let sql = request.into_inner().sql;
        let rows = match self.engine.read().await.execute_no_query(&sql).await {
            Ok(rows) => rows,
            Err(e) if e.is_memory_limit_exceeded() => {
                return Ok(Response::new(NoQueryResponse {
                    success: false,
                    rows_affected: 0,
                    message: e.to_string(),
                }));
            }
            Err(e) => return Err(status_from_err(e)),
        };

        Ok(Response::new(NoQueryResponse {
            success: true,
            rows_affected: rows,
            message: "ok".into(),
        }))
    }

    async fn load_file(
        &self,
        request: Request<LoadFileRequest>,
    ) -> Result<Response<LoadFileResponse>, Status> {
        auth::require_auth(&request, &self.active_tokens)?;
        let req = request.into_inner();
        let rows = self
            .engine
            .write()
            .await
            .load_file(&req.table_name, &req.file_path, &req.format)
            .await
            .map_err(status_from_err)?;

        Ok(Response::new(LoadFileResponse {
            success: true,
            rows_loaded: rows,
            message: "file loaded successfully".into(),
        }))
    }

    async fn bulk_load(
        &self,
        request: Request<BulkLoadRequest>,
    ) -> Result<Response<BulkLoadResponse>, Status> {
        auth::require_auth(&request, &self.active_tokens)?;
        let req = request.into_inner();
        if req.format.to_lowercase() != "parquet" {
            return Err(Status::invalid_argument(
                "only parquet bulk load is supported",
            ));
        }
        if req.paths.is_empty() {
            return Err(Status::invalid_argument("paths must not be empty"));
        }

        let paths: Vec<std::path::PathBuf> =
            req.paths.iter().map(std::path::PathBuf::from).collect();
        let result = self
            .engine
            .write()
            .await
            .bulk_load_parquet_files(&req.table_name, &paths)
            .await
            .map_err(status_from_err)?;

        Ok(Response::new(BulkLoadResponse {
            success: true,
            rows_loaded: result.rows_loaded,
            files_loaded: result.files_loaded,
            message: format!(
                "bulk loaded {} file(s), {} rows",
                result.files_loaded, result.rows_loaded
            ),
        }))
    }

    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        auth::require_auth(&request, &self.active_tokens)?;
        let req = request.into_inner();
        let columns = req
            .columns
            .into_iter()
            .map(|c| ColumnDef {
                name: c.name,
                data_type: c.data_type,
                nullable: c.nullable,
            })
            .collect();

        self.engine
            .write()
            .await
            .create_table_and_load(&req.table_name, columns)
            .await
            .map_err(status_from_err)?;

        Ok(Response::new(CreateTableResponse {
            success: true,
            message: format!("table {} created", req.table_name),
        }))
    }

    async fn add_column(
        &self,
        request: Request<AddColumnRequest>,
    ) -> Result<Response<AddColumnResponse>, Status> {
        auth::require_auth(&request, &self.active_tokens)?;
        let req = request.into_inner();
        let column = req
            .column
            .ok_or_else(|| Status::invalid_argument("missing column definition"))?;
        self.engine
            .write()
            .await
            .add_column(
                &req.table_name,
                ColumnDef {
                    name: column.name,
                    data_type: column.data_type,
                    nullable: column.nullable,
                },
            )
            .await
            .map_err(status_from_err)?;

        Ok(Response::new(AddColumnResponse {
            success: true,
            message: "column added".into(),
        }))
    }

    async fn drop_table(
        &self,
        request: Request<DropTableRequest>,
    ) -> Result<Response<DropTableResponse>, Status> {
        auth::require_auth(&request, &self.active_tokens)?;
        let req = request.into_inner();
        self.engine
            .write()
            .await
            .drop_table(&req.table_name, req.if_exists)
            .await
            .map_err(status_from_err)?;

        Ok(Response::new(DropTableResponse {
            success: true,
            message: format!("table {} dropped", req.table_name),
        }))
    }

    async fn get_memory_stats(
        &self,
        request: Request<GetMemoryStatsRequest>,
    ) -> Result<Response<GetMemoryStatsResponse>, Status> {
        auth::require_auth(&request, &self.active_tokens)?;
        let engine = self.engine.read().await;
        let (used, limit, _pending) = engine.memory_stats();
        Ok(Response::new(GetMemoryStatsResponse {
            used_bytes: used as u64,
            limit_bytes: limit as u64,
            memtable_max_bytes: engine.config().memtable_max_bytes as u64,
        }))
    }
}

fn status_from_err(err: common::TsdbError) -> Status {
    if err.is_memory_limit_exceeded() {
        return Status::resource_exhausted(err.to_string());
    }
    Status::internal(err.to_string())
}

async fn write_batches_or_response(
    engine: &Arc<tokio::sync::RwLock<TsdbEngine>>,
    table: &str,
    batches: Vec<RecordBatch>,
) -> Result<u64, Status> {
    match engine.write().await.write_batches(table, batches).await {
        Ok(rows) => Ok(rows),
        Err(e) if e.is_memory_limit_exceeded() => Err(Status::resource_exhausted(e.to_string())),
        Err(e) => Err(status_from_err(e)),
    }
}
