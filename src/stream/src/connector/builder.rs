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
use super::plugins::{DeltaSink, FilesystemSink, KafkaSink, PayloadFormat};

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
        } => {
            let format = PayloadFormat::from_str_name(&format)
                .map_err(|e| common::TsdbError::Query(e.to_string()))?;
            let mut sink = KafkaSink::new(brokers, topic, format);
            if let Some(engine) = engine {
                if let Ok(loader) =
                    StreamArrowLoader::from_engine(engine, def.source_tables[0].as_str())
                {
                    sink = sink.with_arrow_loader(loader);
                }
            }
            Ok(Box::new(sink))
        }
        ResolvedSinkConfig::Filesystem { path, table } => {
            Ok(Box::new(FilesystemSink::new(path, table)))
        }
        ResolvedSinkConfig::Delta {
            path,
            table,
            endpoint,
        } => Ok(Box::new(DeltaSink::new(path, table, endpoint))),
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
            },
            created_at_ms: 0,
            auto_end: false,
        };
        let _ = build_sink(&kafka).unwrap();

        let fs = StreamDef {
            sink_config: SinkConfig::Filesystem {
                path: "/tmp/out".into(),
            },
            capture_mode: common::StreamCaptureMode::Batch,
            ..kafka.clone()
        };
        let _ = build_sink(&fs).unwrap();

        let delta = StreamDef {
            sink_config: SinkConfig::Delta {
                path: "/tmp/lake".into(),
                endpoint: None,
            },
            capture_mode: common::StreamCaptureMode::Batch,
            ..kafka.clone()
        };
        let _ = build_sink(&delta).unwrap();

        let _ = NoopSink::default();
        let _ = ConnectorType::Kafka;
    }
}
