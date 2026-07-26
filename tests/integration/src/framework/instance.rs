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

use std::time::Duration;

use monots_core::metadata::catalog::ColumnDef;
use sdk::Client;

use super::path::PathManager;
use super::process::MonotsProcess;
use super::utils::{find_free_port, read_tail, wait_for_port};
use super::workspace::InstanceWorkspace;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_USER: &str = "admin";
const DEFAULT_PASS: &str = "admin";

/// Facade for a single MonoTS server used in integration tests.
pub struct MonotsInstance {
    host: String,
    port: u16,
    username: String,
    password: String,
    workspace: InstanceWorkspace,
    process: MonotsProcess,
    memtable_max_bytes: Option<usize>,
    global_memory_limit_bytes: Option<usize>,
    /// Extra env vars passed to `monots-server` (e.g. AWS_* / MONOTS_LAKE_ENDPOINT).
    extra_env: Vec<(String, String)>,
}

impl MonotsInstance {
    pub fn with_memory_limits(
        test_name: impl Into<String>,
        memtable_max_bytes: usize,
        global_memory_limit_bytes: usize,
    ) -> Result<Self, String> {
        let mut inst = Self::new(test_name)?;
        inst.memtable_max_bytes = Some(memtable_max_bytes);
        inst.global_memory_limit_bytes = Some(global_memory_limit_bytes);
        Ok(inst)
    }
    pub fn new(test_name: impl Into<String>) -> Result<Self, String> {
        let test_name = test_name.into();
        let host = DEFAULT_HOST.to_string();
        let port = find_free_port(&host)?;
        let binary = PathManager::server_binary()?;
        let workspace = InstanceWorkspace::new(&PathManager::target_dir(), &test_name, port);
        let process = MonotsProcess::new(binary, workspace.clone_refs());

        Ok(Self {
            host,
            port,
            username: DEFAULT_USER.into(),
            password: DEFAULT_PASS.into(),
            workspace,
            process,
            memtable_max_bytes: None,
            global_memory_limit_bytes: None,
            extra_env: Vec::new(),
        })
    }

    /// Attach env vars for the next [`Self::start`] (AWS credentials for MinIO, etc.).
    /// Prefer DDL `sink.delta.endpoint` for the S3 API URL — do not put endpoint in env.
    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_env.push((key.into(), value.into()));
        self
    }

    pub fn grpc_url(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }

    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub async fn start(&mut self) -> Result<(), String> {
        self.workspace.setup()?;
        self.process.start(
            &self.listen_addr(),
            &self.username,
            &self.password,
            self.memtable_max_bytes,
            self.global_memory_limit_bytes,
            &self.extra_env,
        )?;

        let addr = format!("{}:{}", self.host, self.port);
        if !wait_for_port(&addr, Duration::from_secs(30), Duration::from_millis(200)).await {
            let stderr = read_tail(&self.workspace.stderr_file, 2000);
            self.process.kill();
            return Err(format!(
                "server did not become ready on {addr} within 30s.\nstderr tail:\n{stderr}"
            ));
        }
        Ok(())
    }

    pub fn data_dir(&self) -> &std::path::Path {
        &self.workspace.data_dir
    }

    /// Stop the server process but keep on-disk data (for restart/recovery tests).
    pub fn shutdown(&mut self) {
        self.process.stop();
        // Allow graceful WAL flush (SIGTERM handler in monots-server).
        std::thread::sleep(Duration::from_millis(500));
    }

    /// SIGKILL the server process but keep on-disk data (crash / unclean shutdown).
    pub fn kill_keep_data(&mut self) {
        self.process.kill();
    }

    /// Stop the server, then start it again on the same data directory.
    pub async fn restart(&mut self) -> Result<(), String> {
        self.shutdown();
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.start().await
    }

    /// Hard-kill (SIGKILL) then start again on the same data directory (WAL crash recovery).
    pub async fn restart_after_hard_kill(&mut self) -> Result<(), String> {
        self.kill_keep_data();
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.start().await
    }

    pub fn stop(&mut self) {
        self.process.stop();
        self.workspace.cleanup();
    }

    pub fn kill(&mut self) {
        self.process.kill();
        self.workspace.cleanup();
    }

    pub async fn client(&self) -> Result<Client, String> {
        Client::connect(self.grpc_url())
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn authenticated_client(&self) -> Result<Client, String> {
        let mut client = self.client().await?;
        client
            .login(&self.username, &self.password)
            .await
            .map_err(|e| e.to_string())?;
        Ok(client)
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<u64, String> {
        let mut client = self.authenticated_client().await?;
        client.no_query(sql).await.map_err(|e| e.to_string())
    }

    pub async fn query_sql(
        &self,
        sql: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
        let mut client = self.authenticated_client().await?;
        client.query(sql).await.map_err(|e| e.to_string())
    }
}

impl Drop for MonotsInstance {
    fn drop(&mut self) {
        self.kill();
    }
}

// Workspace needs to be cloneable for process holder — use a thin wrapper.
impl InstanceWorkspace {
    pub fn clone_refs(&self) -> Self {
        Self {
            root_dir: self.root_dir.clone(),
            data_dir: self.data_dir.clone(),
            log_dir: self.log_dir.clone(),
            conf_dir: self.conf_dir.clone(),
            config_file: self.config_file.clone(),
            stdout_file: self.stdout_file.clone(),
            stderr_file: self.stderr_file.clone(),
        }
    }
}

pub fn unique_table(prefix: &str) -> String {
    format!(
        "{}_{}",
        prefix,
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    )
}

pub fn ts_col() -> ColumnDef {
    ColumnDef {
        name: "time".into(),
        data_type: "Int64".into(),
        nullable: false,
    }
}
