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

pub use common::{ensure_sorted_by_time, is_sorted_by_time, sort_batch_by_time};

use arrow::record_batch::RecordBatch;
use common::{Result, TsdbError};
use monots_core::metadata::catalog::ColumnDef;
use monots_core::query::QuerySession;
use proto::api::edge_tsdb_client::EdgeTsdbClient;
use proto::api::{
    AddColumnRequest, BulkLoadRequest, ColumnDef as ProtoColumnDef, CreateTableRequest,
    DropTableRequest, LoginRequest, NoQueryRequest, QueryRequest, WriteRequest,
};
use std::time::Duration;
use tokio_stream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Streaming};

pub struct Client {
    inner: EdgeTsdbClient<Channel>,
    token: Option<String>,
}

impl Client {
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        let endpoint = Endpoint::from_shared(url.into())
            .map_err(|e| TsdbError::Network(e.to_string()))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30));
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| TsdbError::Network(e.to_string()))?;
        Ok(Self {
            inner: EdgeTsdbClient::new(channel),
            token: None,
        })
    }

    pub async fn login(&mut self, username: &str, password: &str) -> Result<()> {
        let response = self
            .inner
            .login(LoginRequest {
                username: username.to_string(),
                password: password.to_string(),
            })
            .await
            .map_err(|e| TsdbError::Auth(e.message().to_string()))?
            .into_inner();

        if response.success {
            self.token = Some(response.token);
            Ok(())
        } else {
            Err(TsdbError::Auth(response.message))
        }
    }

    pub async fn create_table(&mut self, table_name: &str, columns: Vec<ColumnDef>) -> Result<()> {
        let request = self.auth_request(CreateTableRequest {
            table_name: table_name.to_string(),
            columns: columns
                .into_iter()
                .map(|c| ProtoColumnDef {
                    name: c.name,
                    data_type: c.data_type,
                    nullable: c.nullable,
                })
                .collect(),
        })?;
        self.inner
            .create_table(request)
            .await
            .map_err(|e| TsdbError::Query(e.message().to_string()))?;
        Ok(())
    }

    pub async fn add_column(&mut self, table_name: &str, column: ColumnDef) -> Result<()> {
        let request = self.auth_request(AddColumnRequest {
            table_name: table_name.to_string(),
            column: Some(ProtoColumnDef {
                name: column.name,
                data_type: column.data_type,
                nullable: column.nullable,
            }),
        })?;
        self.inner
            .add_column(request)
            .await
            .map_err(|e| TsdbError::Schema(e.message().to_string()))?;
        Ok(())
    }

    pub async fn drop_table(&mut self, table_name: &str, if_exists: bool) -> Result<()> {
        let request = self.auth_request(DropTableRequest {
            table_name: table_name.to_string(),
            if_exists,
        })?;
        self.inner
            .drop_table(request)
            .await
            .map_err(|e| TsdbError::Schema(e.message().to_string()))?;
        Ok(())
    }

    pub async fn no_query(&mut self, sql: &str) -> Result<u64> {
        let request = self.auth_request(NoQueryRequest {
            sql: sql.to_string(),
        })?;
        let response = self
            .inner
            .no_query(request)
            .await
            .map_err(map_rpc_error)?
            .into_inner();
        if response.success {
            Ok(response.rows_affected)
        } else {
            Err(TsdbError::Storage(response.message))
        }
    }

    /// Write Arrow batches via gRPC `WriteData`. Each batch is sorted by `time` before send.
    pub async fn write_batches(
        &mut self,
        table_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<u64> {
        if batches.is_empty() {
            return Ok(0);
        }

        let table = table_name.to_string();
        let mut requests = Vec::with_capacity(batches.len());
        for batch in batches {
            let batch = ensure_sorted_by_time(batch)?;
            let payload = QuerySession::encode_batch_ipc(&batch)?;
            requests.push(WriteRequest {
                table_name: table.clone(),
                payload,
            });
        }

        let stream = tokio_stream::iter(requests);

        let request = self.auth_request(stream)?;
        let response = self
            .inner
            .write_data(request)
            .await
            .map_err(map_rpc_error)?
            .into_inner();

        if response.success {
            Ok(response.rows_inserted)
        } else {
            Err(TsdbError::Storage(response.message))
        }
    }

    /// Convenience wrapper for a single batch. See [`Self::write_batches`].
    pub async fn write_batch(&mut self, table_name: &str, batch: RecordBatch) -> Result<u64> {
        self.write_batches(table_name, vec![batch]).await
    }

    pub async fn query(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        let request = self.auth_request(QueryRequest {
            sql: sql.to_string(),
        })?;
        let mut stream: Streaming<proto::api::QueryResponse> = self
            .inner
            .query(request)
            .await
            .map_err(|e| TsdbError::Query(e.message().to_string()))?
            .into_inner();

        let mut all_batches = Vec::new();
        while let Some(msg) = stream
            .message()
            .await
            .map_err(|e| TsdbError::Query(e.message().to_string()))?
        {
            if !msg.payload.is_empty() {
                all_batches.extend(QuerySession::decode_arrow_ipc(&msg.payload)?);
            }
        }
        Ok(all_batches)
    }

    pub async fn bulk_load(&mut self, table_name: &str, paths: Vec<String>) -> Result<(u64, u32)> {
        let request = self.auth_request(BulkLoadRequest {
            table_name: table_name.to_string(),
            paths,
            format: "parquet".to_string(),
        })?;
        let response = self
            .inner
            .bulk_load(request)
            .await
            .map_err(|e| TsdbError::Storage(e.message().to_string()))?
            .into_inner();
        if response.success {
            Ok((response.rows_loaded, response.files_loaded))
        } else {
            Err(TsdbError::Storage(response.message))
        }
    }

    fn auth_request<T>(&self, message: T) -> Result<Request<T>> {
        let token = self
            .token
            .as_ref()
            .ok_or_else(|| TsdbError::Auth("not logged in".into()))?;
        let mut request = Request::new(message);
        let value = MetadataValue::try_from(format!("Bearer {token}"))
            .map_err(|e| TsdbError::Auth(e.to_string()))?;
        request.metadata_mut().insert("authorization", value);
        Ok(request)
    }
}

fn map_rpc_error(status: tonic::Status) -> TsdbError {
    if status.code() == tonic::Code::ResourceExhausted {
        TsdbError::Storage(status.message().to_string())
    } else {
        TsdbError::Query(status.message().to_string())
    }
}
