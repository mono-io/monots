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

//! Connector factory — instantiate physical sinks from stream definitions.

use common::Result;
use monots_storage::LsmEngine;
use std::sync::Arc;

use crate::data::StreamArrowLoader;
use crate::model::StreamDef;

use super::api::SinkConnector;
use super::config::ResolvedSinkConfig;
use super::plugins::{
    DeltaSink, FilesystemSink, IcebergSink, KafkaSink, PayloadFormat, PulsarSink,
};
use super::plugins::pulsar::PayloadFormat as PulsarPayloadFormat;

/// Build the physical sink for a stream definition.
pub fn build_sink(def: &StreamDef) -> Result<Box<dyn SinkConnector>> {
    build_sink_with_engine(def, None)
}

/// Build sink; when `engine` is set, Kafka receives a [`StreamArrowLoader`] for Deferred Inserts.
pub fn build_sink_with_engine(
    def: &StreamDef,
    engine: Option<&Arc<LsmEngine>>,
) -> Result<Box<dyn SinkConnector>> {
    match ResolvedSinkConfig::resolve(def)? {
        ResolvedSinkConfig::Kafka {
            brokers,
            topic,
            format,
            options,
            stream_name,
        } => {
            let format = PayloadFormat::from_str_name(&format)
                .map_err(|e| common::TsdbError::Query(e.to_string()))?;
            let mut sink = KafkaSink::new(brokers, topic, format, options);
            if matches!(
                sink.delivery_guarantee(),
                crate::model::KafkaDeliveryGuarantee::ExactlyOnce
            ) {
                let txn_id = sink
                    .options()
                    .transactional_id
                    .clone()
                    .unwrap_or_else(|| format!("monots-stream-{stream_name}"));
                sink = sink.with_exactly_once(txn_id);
            }
            if let Some(engine) = engine {
                if let Ok(loader) =
                    StreamArrowLoader::from_engine(engine, def.source_tables[0].as_str())
                {
                    sink = sink.with_arrow_loader(loader);
                }
            }
            Ok(Box::new(sink))
        }
        ResolvedSinkConfig::Filesystem {
            path,
            table,
            endpoint,
            options,
        } => Ok(Box::new(FilesystemSink::new(
            path, table, endpoint, options,
        ))),
        ResolvedSinkConfig::Delta {
            path,
            table,
            endpoint,
            options,
        } => Ok(Box::new(DeltaSink::new(path, table, endpoint, options))),
        ResolvedSinkConfig::Iceberg { options } => Ok(Box::new(
            IcebergSink::new(options).map_err(|e| common::TsdbError::Query(e.to_string()))?,
        )),
        ResolvedSinkConfig::Pulsar { format, options } => {
            let format = PulsarPayloadFormat::from_str_name(&format)
                .map_err(|e| common::TsdbError::Query(e.to_string()))?;
            let mut sink = PulsarSink::new(format, options);
            if let Some(engine) = engine {
                if let Ok(loader) =
                    StreamArrowLoader::from_engine(engine, def.source_tables[0].as_str())
                {
                    sink = sink.with_arrow_loader(loader);
                }
            }
            Ok(Box::new(sink))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::api::NoopSink;
    use crate::model::SinkConfig;
    use common::ConnectorType;

    #[test]
    fn builds_each_protocol_plugin() {
        let kafka = StreamDef {
            name: "k".into(),
            source_tables: vec!["t".into()],
            capture_mode: common::StreamCaptureMode::Hybrid,
            sink_config: SinkConfig::Kafka {
                brokers: "b:9092".into(),
                topic: "cdc".into(),
                format: "json".into(),
                options: crate::model::KafkaSinkOptions::default(),
            },
            created_at_ms: 0,
            auto_end: false,
        };
        let _ = build_sink(&kafka).unwrap();

        let fs = StreamDef {
            sink_config: SinkConfig::Filesystem {
                path: "/tmp/out".into(),
                endpoint: None,
                options: crate::model::DeltaSinkOptions::default(),
            },
            capture_mode: common::StreamCaptureMode::Batch,
            ..kafka.clone()
        };
        let _ = build_sink(&fs).unwrap();

        let delta = StreamDef {
            sink_config: SinkConfig::Delta {
                path: "/tmp/lake".into(),
                endpoint: None,
                options: crate::model::DeltaSinkOptions::default(),
            },
            capture_mode: common::StreamCaptureMode::Batch,
            ..kafka.clone()
        };
        let _ = build_sink(&delta).unwrap();

        let mut iceberg_opts = std::collections::HashMap::new();
        iceberg_opts.insert("sink.iceberg.catalog-type".into(), "hadoop".into());
        iceberg_opts.insert("sink.iceberg.catalog-name".into(), "c".into());
        iceberg_opts.insert("sink.iceberg.warehouse".into(), "/tmp/wh".into());
        iceberg_opts.insert("sink.iceberg.namespace".into(), "ns".into());
        iceberg_opts.insert("sink.iceberg.table".into(), "t".into());
        let iceberg = StreamDef {
            sink_config: SinkConfig::Iceberg {
                options: crate::model::IcebergSinkOptions::from_ddl(&iceberg_opts).unwrap(),
            },
            capture_mode: common::StreamCaptureMode::Batch,
            ..kafka.clone()
        };
        let _ = build_sink(&iceberg).unwrap();

        let mut pulsar_opts = std::collections::HashMap::new();
        pulsar_opts.insert(
            "sink.pulsar.topic".into(),
            "persistent://public/default/t".into(),
        );
        pulsar_opts.insert(
            "sink.pulsar.service-url".into(),
            "pulsar://localhost:6650".into(),
        );
        pulsar_opts.insert(
            "sink.pulsar.admin-url".into(),
            "http://localhost:8080".into(),
        );
        let pulsar = StreamDef {
            sink_config: SinkConfig::Pulsar {
                format: "json".into(),
                options: crate::model::PulsarSinkOptions::from_ddl(&pulsar_opts).unwrap(),
            },
            capture_mode: common::StreamCaptureMode::Hybrid,
            ..kafka.clone()
        };
        let _ = build_sink(&pulsar).unwrap();

        let _ = NoopSink::default();
        let _ = ConnectorType::Kafka;
    }
}
