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

//! Structured Delta sink errors — preserve source chains for retry classification.

use std::path::PathBuf;

use thiserror::Error;

use crate::connector::SinkError;

/// Typed failures inside the Delta plugin (converted to [`SinkError`] at the boundary).
#[derive(Debug, Error)]
pub enum DeltaSinkError {
    #[error("object storage network error on URI {uri}")]
    Network {
        uri: String,
        #[source]
        source: object_store::Error,
    },

    #[error("object storage auth error on URI {uri}")]
    Auth {
        uri: String,
        #[source]
        source: object_store::Error,
    },

    #[error("delta OCC conflict after {attempts} attempts on URI {uri}")]
    ConcurrentConflict {
        uri: String,
        attempts: usize,
        #[source]
        source: deltalake::DeltaTableError,
    },

    #[error("delta table error on URI {uri}")]
    Table {
        uri: String,
        #[source]
        source: deltalake::DeltaTableError,
    },

    #[error("IO error on path {path}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("{0}")]
    Fatal(String),

    #[error("{0}")]
    Transient(String),
}

impl DeltaSinkError {
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::Fatal(_))
            || matches!(self, Self::Table { source, .. } if is_fatal_delta(source))
    }

    /// Auth / expired STS — upper layer may refresh credentials and retry.
    pub fn is_auth(&self) -> bool {
        matches!(self, Self::Auth { .. }) || {
            let s = self.to_string().to_ascii_lowercase();
            s.contains("403")
                || s.contains("401")
                || s.contains("forbidden")
                || s.contains("accessdenied")
                || s.contains("expired")
                || s.contains("invalidtoken")
                || s.contains("expiredtoken")
        }
    }

    pub fn from_object_store(uri: impl Into<String>, source: object_store::Error) -> Self {
        let uri = uri.into();
        let msg = source.to_string().to_ascii_lowercase();
        if msg.contains("403")
            || msg.contains("401")
            || msg.contains("forbidden")
            || msg.contains("accessdenied")
            || msg.contains("expired")
            || msg.contains("invalidtoken")
            || msg.contains("signature")
        {
            Self::Auth { uri, source }
        } else {
            Self::Network { uri, source }
        }
    }
}

fn is_fatal_delta(err: &deltalake::DeltaTableError) -> bool {
    let s = err.to_string().to_ascii_lowercase();
    s.contains("schema") || s.contains("unsupported") || s.contains("not a table")
}

impl From<DeltaSinkError> for SinkError {
    fn from(err: DeltaSinkError) -> Self {
        // Prefer alternate formatting so `source` chains appear in logs / retry messages.
        let msg = format!("{err:#}");
        // Auth is Transient so SinkWorker can recover after refresh_credentials().
        if err.is_auth() {
            return SinkError::Transient(msg);
        }
        if err.is_fatal() {
            SinkError::Fatal(msg)
        } else {
            SinkError::Transient(msg)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_maps_to_transient_sink_error() {
        // Synthetic string path via Fatal/Transient helpers is enough for classification tests.
        let e = DeltaSinkError::Transient("403 Forbidden expired token".into());
        assert!(e.is_auth());
        let s: SinkError = e.into();
        assert!(!s.is_fatal());
    }
}
