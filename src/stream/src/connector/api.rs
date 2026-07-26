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

//! Industrial-grade 2PC sink interface — single entry point for all physical connectors.

use crate::model::event::DataEvent;
use common::TsdbError;

#[derive(Debug, Clone)]
pub enum SinkError {
    /// Transient failure (network blip, broker election) — upper layer retries with backoff.
    Transient(String),
    /// Fatal failure (auth, bad config, disk full) — terminate the stream.
    Fatal(String),
}

impl std::fmt::Display for SinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transient(m) => write!(f, "transient: {m}"),
            Self::Fatal(m) => write!(f, "fatal: {m}"),
        }
    }
}

impl std::error::Error for SinkError {}

impl SinkError {
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::Fatal(_))
    }
}

impl From<TsdbError> for SinkError {
    fn from(err: TsdbError) -> Self {
        let msg = err.to_string();
        let lower = msg.to_lowercase();
        if lower.contains("timeout")
            || lower.contains("connection")
            || lower.contains("unavailable")
            || lower.contains("temporarily")
            || lower.contains("broken pipe")
            || lower.contains("reset by peer")
        {
            Self::Transient(msg)
        } else if lower.contains("schema")
            || lower.contains("auth")
            || lower.contains("permission")
            || lower.contains("not found")
            || lower.contains("unsupported")
        {
            Self::Fatal(msg)
        } else {
            Self::Transient(msg)
        }
    }
}

/// Every physical sink implements this 2PC protocol and consumes [`DataEvent`] directly.
#[async_trait::async_trait]
pub trait SinkConnector: Send + Sync + 'static {
    async fn begin_txn(&mut self) -> Result<(), SinkError>;
    async fn write(&mut self, event: &DataEvent) -> Result<(), SinkError>;
    async fn commit_txn(&mut self) -> Result<(), SinkError>;
    async fn abort_txn(&mut self) -> Result<(), SinkError>;

    /// Async teardown: flush buffers / send protocol Close. Default is a no-op.
    /// Prefer this over relying on sync [`Drop`] for network clients.
    async fn close(&mut self) -> Result<(), SinkError> {
        Ok(())
    }

    /// Idle keep-alive probe (e.g. `SELECT 1`, protocol ping). Default is a no-op.
    async fn ping(&mut self) -> Result<(), SinkError> {
        Ok(())
    }

    /// Recover from a transient session failure (default: abort open txn).
    async fn reset(&mut self) -> Result<(), SinkError> {
        self.abort_txn().await
    }
}

/// No-op sink for tests and dry-run.
#[derive(Debug, Default)]
pub struct NoopSink {
    open: bool,
}

#[async_trait::async_trait]
impl SinkConnector for NoopSink {
    async fn begin_txn(&mut self) -> Result<(), SinkError> {
        self.open = true;
        Ok(())
    }

    async fn write(&mut self, _event: &DataEvent) -> Result<(), SinkError> {
        if !self.open {
            return Err(SinkError::Fatal("write without begin_txn".into()));
        }
        Ok(())
    }

    async fn commit_txn(&mut self) -> Result<(), SinkError> {
        self.open = false;
        Ok(())
    }

    async fn abort_txn(&mut self) -> Result<(), SinkError> {
        self.open = false;
        Ok(())
    }
}
