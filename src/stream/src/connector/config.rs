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

use common::{ConnectorType, Result, StreamCaptureMode, TsdbError};

use crate::model::StreamDef;

#[derive(Debug, Clone)]
pub enum ResolvedSinkConfig {
    Kafka {
        brokers: String,
        topic: String,
        format: String,
        options: crate::model::KafkaSinkOptions,
        /// Used to derive transactional.id when EOS is enabled and id is omitted.
        stream_name: String,
    },
    Filesystem {
        /// Local path or object URI (`s3://…`, `file://…`, …). Kept as String so
        /// schemes like `s3://` are not mangled by [`PathBuf`].
        path: String,
        table: Option<String>,
        endpoint: Option<String>,
        options: crate::model::DeltaSinkOptions,
    },
    Delta {
        /// Local path or object URI (`s3://…`, `file://…`, …). Kept as String so
        /// schemes like `s3://` are not mangled by [`PathBuf`].
        path: String,
        table: Option<String>,
        endpoint: Option<String>,
        options: crate::model::DeltaSinkOptions,
    },
    Iceberg {
        options: crate::model::IcebergSinkOptions,
    },
    Pulsar {
        format: String,
        options: crate::model::PulsarSinkOptions,
    },
    Mqtt {
        format: String,
        options: crate::model::MqttSinkOptions,
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
                let options = match &def.sink_config {
                    crate::model::SinkConfig::Kafka { options, .. } => options.clone(),
                    _ => crate::model::KafkaSinkOptions::default(),
                };
                Ok(Self::Kafka {
                    brokers: brokers.to_string(),
                    topic: topic.to_string(),
                    format: cfg.delivery_format().to_string(),
                    options,
                    stream_name: def.name.clone(),
                })
            }
            ConnectorType::Filesystem => {
                let path = cfg.sink_path().filter(|s| !s.is_empty()).ok_or_else(|| {
                    TsdbError::Query("filesystem sink requires sink.filesystem.path".into())
                })?;
                let (endpoint, options) = match &def.sink_config {
                    crate::model::SinkConfig::Filesystem {
                        endpoint, options, ..
                    } => (endpoint.clone(), options.clone()),
                    _ => (None, crate::model::DeltaSinkOptions::default()),
                };
                Ok(Self::Filesystem {
                    path: path.to_string(),
                    table: def.source_tables.first().cloned(),
                    endpoint,
                    options,
                })
            }
            ConnectorType::Delta => {
                let path = cfg.sink_path().filter(|s| !s.is_empty()).ok_or_else(|| {
                    TsdbError::Query("delta sink requires sink.delta.path".into())
                })?;
                let (endpoint, options) = match &def.sink_config {
                    crate::model::SinkConfig::Delta {
                        endpoint, options, ..
                    } => (endpoint.clone(), options.clone()),
                    _ => (None, crate::model::DeltaSinkOptions::default()),
                };
                Ok(Self::Delta {
                    path: path.to_string(),
                    table: def.source_tables.first().cloned(),
                    endpoint,
                    options,
                })
            }
            ConnectorType::Iceberg => {
                let options = match &def.sink_config {
                    crate::model::SinkConfig::Iceberg { options } => options.clone(),
                    _ => {
                        return Err(TsdbError::Query(
                            "iceberg sink requires sink.iceberg.* options".into(),
                        ))
                    }
                };
                Ok(Self::Iceberg { options })
            }
            ConnectorType::Pulsar => {
                let (format, options) = match &def.sink_config {
                    crate::model::SinkConfig::Pulsar { format, options } => {
                        (format.clone(), options.clone())
                    }
                    _ => {
                        return Err(TsdbError::Query(
                            "pulsar sink requires sink.pulsar.* options".into(),
                        ))
                    }
                };
                if options.topic.is_empty()
                    || options.service_url.is_empty()
                    || options.admin_url.is_empty()
                {
                    return Err(TsdbError::Query(
                        "Pulsar requires sink.pulsar.topic, sink.pulsar.service-url, and sink.pulsar.admin-url"
                            .into(),
                    ));
                }
                Ok(Self::Pulsar { format, options })
            }
            ConnectorType::Mqtt => {
                let (format, options) = match &def.sink_config {
                    crate::model::SinkConfig::Mqtt { format, options } => {
                        (format.clone(), options.clone())
                    }
                    _ => {
                        return Err(TsdbError::Query(
                            "mqtt sink requires sink.mqtt.* options".into(),
                        ))
                    }
                };
                if options.url.is_empty() || options.topic.is_empty() {
                    return Err(TsdbError::Query(
                        "MQTT requires sink.mqtt.url (or server-uri) and sink.mqtt.topic".into(),
                    ));
                }
                Ok(Self::Mqtt { format, options })
            }
        }
    }
}

/// Default capture mode when `cdc.mode` is omitted.
///
/// Kafka / Pulsar default to hybrid (batch backfill + WAL tail); filesystem / delta / iceberg default to batch.
pub fn connector_capture_mode(connector: ConnectorType) -> StreamCaptureMode {
    match connector {
        ConnectorType::Kafka | ConnectorType::Pulsar | ConnectorType::Mqtt => {
            StreamCaptureMode::Hybrid
        }
        ConnectorType::Delta | ConnectorType::Filesystem | ConnectorType::Iceberg => {
            StreamCaptureMode::Batch
        }
    }
}

pub fn connector_default_format(connector: ConnectorType) -> &'static str {
    match connector {
        ConnectorType::Kafka | ConnectorType::Pulsar | ConnectorType::Mqtt => "json",
        ConnectorType::Delta | ConnectorType::Filesystem | ConnectorType::Iceberg => "parquet",
    }
}

fn supported_formats(connector: ConnectorType) -> &'static [&'static str] {
    match connector {
        ConnectorType::Kafka | ConnectorType::Pulsar | ConnectorType::Mqtt => &["json"],
        ConnectorType::Delta | ConnectorType::Filesystem | ConnectorType::Iceberg => &["parquet"],
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
                options: crate::model::KafkaSinkOptions::default(),
            },
            ConnectorType::Delta => SinkConfig::Delta {
                path: "/tmp/lake".into(),
                endpoint: None,
                options: crate::model::DeltaSinkOptions::default(),
            },
            ConnectorType::Filesystem => SinkConfig::Filesystem {
                path: "/tmp/fs".into(),
                endpoint: None,
                options: crate::model::DeltaSinkOptions::default(),
            },
            ConnectorType::Iceberg => {
                let mut with = std::collections::HashMap::new();
                with.insert("sink.iceberg.catalog-type".into(), "hadoop".into());
                with.insert("sink.iceberg.catalog-name".into(), "c".into());
                with.insert("sink.iceberg.warehouse".into(), "/tmp/wh".into());
                with.insert("sink.iceberg.namespace".into(), "ns".into());
                with.insert("sink.iceberg.table".into(), "t".into());
                SinkConfig::Iceberg {
                    options: crate::model::IcebergSinkOptions::from_ddl(&with).unwrap(),
                }
            }
            ConnectorType::Pulsar => {
                let mut with = std::collections::HashMap::new();
                with.insert(
                    "sink.pulsar.topic".into(),
                    "persistent://public/default/t".into(),
                );
                with.insert(
                    "sink.pulsar.service-url".into(),
                    "pulsar://localhost:6650".into(),
                );
                with.insert(
                    "sink.pulsar.admin-url".into(),
                    "http://localhost:8080".into(),
                );
                SinkConfig::Pulsar {
                    format: "json".into(),
                    options: crate::model::PulsarSinkOptions::from_ddl(&with).unwrap(),
                }
            }
            ConnectorType::Mqtt => {
                let mut with = std::collections::HashMap::new();
                with.insert("sink.mqtt.url".into(), "tcp://127.0.0.1:1883".into());
                with.insert("sink.mqtt.topic".into(), "monots/cdc".into());
                SinkConfig::Mqtt {
                    format: "json".into(),
                    options: crate::model::MqttSinkOptions::from_ddl(&with).unwrap(),
                }
            }
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
            connector_capture_mode(ConnectorType::Pulsar),
            StreamCaptureMode::Hybrid
        );
        assert_eq!(
            connector_capture_mode(ConnectorType::Mqtt),
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
        validate_sink(&minimal_def(ConnectorType::Pulsar)).unwrap();
        validate_sink(&minimal_def(ConnectorType::Mqtt)).unwrap();
    }

    #[test]
    fn delta_requires_path() {
        let mut def = minimal_def(ConnectorType::Delta);
        def.sink_config = SinkConfig::Delta {
            path: String::new(),
            endpoint: None,
            options: crate::model::DeltaSinkOptions::default(),
        };
        assert!(validate_sink(&def).is_err());
    }
}
