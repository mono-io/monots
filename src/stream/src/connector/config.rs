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

//! Resolved physical sink configuration and DDL validation.

use std::path::PathBuf;

use common::{ConnectorType, Result, StreamCaptureMode, TsdbError};

use crate::model::StreamDef;

#[derive(Debug, Clone)]
pub enum ResolvedSinkConfig {
    Kafka {
        brokers: String,
        topic: String,
        format: String,
    },
    Filesystem {
        path: PathBuf,
        table: Option<String>,
    },
    Delta {
        /// Local path or object URI (`s3://…`, `file://…`, …). Kept as String so
        /// schemes like `s3://` are not mangled by [`PathBuf`].
        path: String,
        table: Option<String>,
        endpoint: Option<String>,
    },
}

impl ResolvedSinkConfig {
    pub fn resolve(def: &StreamDef) -> Result<Self> {
        let cfg = &def.sink_config;

        match def.connector_type() {
            ConnectorType::Kafka => {
                let brokers = cfg.kafka_brokers().unwrap_or_default();
                let topic = cfg.kafka_topic().unwrap_or_default();
                if brokers.is_empty() || topic.is_empty() {
                    return Err(TsdbError::Query(
                        "Kafka requires sink.kafka.brokers and sink.kafka.topic".into(),
                    ));
                }
                Ok(Self::Kafka {
                    brokers: brokers.to_string(),
                    topic: topic.to_string(),
                    format: cfg.delivery_format().to_string(),
                })
            }
            ConnectorType::Filesystem => {
                let path = cfg.sink_path().filter(|s| !s.is_empty()).ok_or_else(|| {
                    TsdbError::Query("filesystem sink requires sink.filesystem.path".into())
                })?;
                Ok(Self::Filesystem {
                    path: PathBuf::from(path),
                    table: def.source_tables.first().cloned(),
                })
            }
            ConnectorType::Delta => {
                let path = cfg.sink_path().filter(|s| !s.is_empty()).ok_or_else(|| {
                    TsdbError::Query("delta sink requires sink.delta.path".into())
                })?;
                let endpoint = match &def.sink_config {
                    crate::model::SinkConfig::Delta { endpoint, .. } => endpoint.clone(),
                    _ => None,
                };
                Ok(Self::Delta {
                    path: path.to_string(),
                    table: def.source_tables.first().cloned(),
                    endpoint,
                })
            }
        }
    }
}

/// Default capture mode when `cdc.mode` is omitted.
///
/// Kafka defaults to hybrid (batch backfill + WAL tail); filesystem / delta default to batch.
pub fn connector_capture_mode(connector: ConnectorType) -> StreamCaptureMode {
    match connector {
        ConnectorType::Kafka => StreamCaptureMode::Hybrid,
        ConnectorType::Delta | ConnectorType::Filesystem => StreamCaptureMode::Batch,
    }
}

pub fn connector_default_format(connector: ConnectorType) -> &'static str {
    match connector {
        ConnectorType::Kafka => "json",
        ConnectorType::Delta | ConnectorType::Filesystem => "parquet",
    }
}

fn supported_formats(connector: ConnectorType) -> &'static [&'static str] {
    match connector {
        ConnectorType::Kafka => &["json", "avro"],
        ConnectorType::Delta | ConnectorType::Filesystem => &["parquet"],
    }
}

/// Validate stream sink options (format, required paths, etc.).
pub fn validate_sink(def: &StreamDef) -> Result<()> {
    let formats = supported_formats(def.connector_type());
    let fmt = def.delivery_format();
    if !formats.iter().any(|f| *f == fmt) {
        return Err(TsdbError::Query(format!(
            "{} sink supports sink.format = {}",
            def.connector_type().as_str(),
            formats.join(" | ")
        )));
    }
    let _ = ResolvedSinkConfig::resolve(def)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SinkConfig;
    use common::StreamCaptureMode;

    fn minimal_def(connector: ConnectorType) -> StreamDef {
        let sink_config = match connector {
            ConnectorType::Kafka => SinkConfig::Kafka {
                brokers: "localhost:9092".into(),
                topic: "topic".into(),
                format: "json".into(),
            },
            ConnectorType::Delta => SinkConfig::Delta {
                path: "/tmp/lake".into(),
                endpoint: None,
            },
            ConnectorType::Filesystem => SinkConfig::Filesystem {
                path: "/tmp/fs".into(),
            },
        };
        StreamDef {
            name: "s".into(),
            source_tables: vec!["t".into()],
            capture_mode: connector_capture_mode(connector),
            sink_config,
            created_at_ms: 0,
            auto_end: false,
        }
    }

    #[test]
    fn capture_mode_by_connector() {
        assert_eq!(
            connector_capture_mode(ConnectorType::Kafka),
            StreamCaptureMode::Hybrid
        );
        assert_eq!(
            connector_capture_mode(ConnectorType::Delta),
            StreamCaptureMode::Batch
        );
    }

    #[test]
    fn validates_kafka_and_fs() {
        validate_sink(&minimal_def(ConnectorType::Kafka)).unwrap();
        validate_sink(&minimal_def(ConnectorType::Filesystem)).unwrap();
    }

    #[test]
    fn delta_requires_path() {
        let mut def = minimal_def(ConnectorType::Delta);
        def.sink_config = SinkConfig::Delta {
            path: String::new(),
            endpoint: None,
        };
        assert!(validate_sink(&def).is_err());
    }
}
