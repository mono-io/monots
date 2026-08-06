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

//! Encode / decode between in-memory stream types and versioned on-disk protobuf.
//!
//! Forward-compat: load refuses `min_reader_version > STREAM_SCHEMA_VERSION`;
//! persist refuses rewrite when on-disk `schema_version` is newer than this binary.

use common::{ConnectorType, StreamCaptureMode, StreamCheckpoint, TableCheckpoint, TsdbError};
use prost::Message;

use crate::model::{SinkConfig, StreamDef};

/// Schema version written by this binary.
pub const STREAM_SCHEMA_VERSION: u32 = 1;
/// Oldest reader this binary can still load (inclusive).
pub const STREAM_SCHEMA_MIN_SUPPORTED: u32 = 1;

/// Versioned on-disk envelope shared by stream def and checkpoint payloads.
#[derive(Debug, Clone)]
pub struct Versioned<T> {
    pub inner: T,
    pub disk_schema_version: u32,
    pub min_reader_version: u32,
}

impl<T> Versioned<T> {
    pub fn fresh(inner: T) -> Self {
        Self {
            inner,
            disk_schema_version: STREAM_SCHEMA_VERSION,
            min_reader_version: STREAM_SCHEMA_MIN_SUPPORTED,
        }
    }

    pub fn can_rewrite(&self) -> bool {
        self.disk_schema_version <= STREAM_SCHEMA_VERSION
    }

    fn ensure_rewritable(&self, identity: &str) -> Result<(), TsdbError> {
        if !self.can_rewrite() {
            Err(TsdbError::Storage(format!(
                "refusing to rewrite `{identity}`: on-disk schema_version {} > this binary's {} \
                 (would drop forward-compatible fields); upgrade the binary first",
                self.disk_schema_version, STREAM_SCHEMA_VERSION
            )))
        } else {
            Ok(())
        }
    }
}

pub fn encode_stream_def(def: &StreamDef) -> Result<Vec<u8>, TsdbError> {
    encode_versioned_stream_def(&Versioned::fresh(def.clone()))
}

pub fn encode_versioned_stream_def(v: &Versioned<StreamDef>) -> Result<Vec<u8>, TsdbError> {
    v.ensure_rewritable(&v.inner.name)?;
    Ok(proto::stream::StreamDefFile::from(v).encode_to_vec())
}

pub fn decode_stream_def(bytes: &[u8]) -> Result<StreamDef, TsdbError> {
    Ok(decode_versioned_stream_def(bytes)?.inner)
}

pub fn decode_versioned_stream_def(bytes: &[u8]) -> Result<Versioned<StreamDef>, TsdbError> {
    let file = proto::stream::StreamDefFile::decode(bytes)
        .map_err(|e| TsdbError::Storage(format!("StreamDef protobuf decode failed: {e}")))?;
    file.try_into()
}

pub fn encode_stream_checkpoint(cp: &StreamCheckpoint) -> Result<Vec<u8>, TsdbError> {
    encode_versioned_checkpoint(&Versioned::fresh(cp.clone()))
}

pub fn encode_versioned_checkpoint(v: &Versioned<StreamCheckpoint>) -> Result<Vec<u8>, TsdbError> {
    v.ensure_rewritable(&format!("{}/{}", v.inner.stream_name, v.inner.consumer_id))?;
    Ok(proto::stream::StreamCheckpointFile::from(v).encode_to_vec())
}

pub fn decode_stream_checkpoint(bytes: &[u8]) -> Result<StreamCheckpoint, TsdbError> {
    Ok(decode_versioned_checkpoint(bytes)?.inner)
}

pub fn decode_versioned_checkpoint(bytes: &[u8]) -> Result<Versioned<StreamCheckpoint>, TsdbError> {
    let file = proto::stream::StreamCheckpointFile::decode(bytes)
        .map_err(|e| TsdbError::Storage(format!("StreamCheckpoint protobuf decode failed: {e}")))?;
    file.try_into()
}

fn validate_schema(
    file_schema: u32,
    file_min_reader: u32,
    file_type: &str,
) -> Result<(u32, u32), TsdbError> {
    if file_schema == 0 {
        return Err(TsdbError::Storage(format!(
            "{file_type} file missing schema_version"
        )));
    }

    let min_reader = if file_min_reader == 0 {
        STREAM_SCHEMA_MIN_SUPPORTED
    } else {
        file_min_reader
    };

    if min_reader > STREAM_SCHEMA_VERSION {
        return Err(TsdbError::Storage(format!(
            "{file_type} file requires min_reader_version {min_reader}, but this binary only supports up to {}",
            STREAM_SCHEMA_VERSION
        )));
    }
    if file_schema < STREAM_SCHEMA_MIN_SUPPORTED {
        return Err(TsdbError::Storage(format!(
            "{file_type} file schema_version {file_schema} is older than supported minimum {}",
            STREAM_SCHEMA_MIN_SUPPORTED
        )));
    }

    Ok((file_schema, min_reader))
}

impl TryFrom<proto::stream::StreamDefFile> for Versioned<StreamDef> {
    type Error = TsdbError;

    fn try_from(file: proto::stream::StreamDefFile) -> Result<Self, Self::Error> {
        let (disk_schema_version, min_reader_version) =
            validate_schema(file.schema_version, file.min_reader_version, "StreamDef")?;
        let pb_def = file
            .def
            .ok_or_else(|| TsdbError::Storage("StreamDef file missing payload".into()))?;
        Ok(Versioned {
            inner: pb_def.try_into()?,
            disk_schema_version,
            min_reader_version,
        })
    }
}

impl From<&Versioned<StreamDef>> for proto::stream::StreamDefFile {
    fn from(v: &Versioned<StreamDef>) -> Self {
        Self {
            schema_version: STREAM_SCHEMA_VERSION,
            min_reader_version: STREAM_SCHEMA_MIN_SUPPORTED.max(v.min_reader_version),
            def: Some(proto::stream::StreamDef::from(&v.inner)),
        }
    }
}

impl TryFrom<proto::stream::StreamDef> for StreamDef {
    type Error = TsdbError;

    fn try_from(pb: proto::stream::StreamDef) -> Result<Self, Self::Error> {
        let connector = connector_from_pb(pb.connector_type)?;
        let capture_mode = capture_mode_from_pb(pb.capture_mode)?;
        let sink_config = match connector {
            ConnectorType::Kafka => SinkConfig::Kafka {
                brokers: pb.kafka_brokers.unwrap_or_default(),
                topic: pb.kafka_topic.unwrap_or_default(),
                format: pb.delivery_format,
                options: crate::model::KafkaSinkOptions::from_properties(&pb.properties)?,
            },
            ConnectorType::Delta => SinkConfig::Delta {
                path: pb.sink_path.unwrap_or_default(),
                endpoint: pb.sink_endpoint.filter(|s| !s.is_empty()),
                options: crate::model::DeltaSinkOptions::from_properties(&pb.properties)?,
            },
            ConnectorType::Filesystem => SinkConfig::Filesystem {
                path: pb.sink_path.unwrap_or_default(),
                endpoint: pb.sink_endpoint.filter(|s| !s.is_empty()),
                options: crate::model::DeltaSinkOptions::from_filesystem_properties(
                    &pb.properties,
                )?,
            },
            ConnectorType::Iceberg => SinkConfig::Iceberg {
                options: crate::model::IcebergSinkOptions::from_properties(&pb.properties)?,
            },
            ConnectorType::Pulsar => SinkConfig::Pulsar {
                format: pb.delivery_format,
                options: crate::model::PulsarSinkOptions::from_properties(&pb.properties)?,
            },
        };
        Ok(Self {
            name: pb.name,
            source_tables: pb.source_tables,
            capture_mode,
            sink_config,
            created_at_ms: pb.created_at_ms,
            auto_end: pb.auto_end,
        })
    }
}

impl From<&StreamDef> for proto::stream::StreamDef {
    fn from(def: &StreamDef) -> Self {
        Self {
            name: def.name.clone(),
            connector_type: connector_to_pb(def.connector_type()) as i32,
            source_tables: def.source_tables.clone(),
            capture_mode: capture_mode_to_pb(def.capture_mode) as i32,
            delivery_format: def.delivery_format().to_string(),
            delivery_files: def.delivery_files(),
            sink_path: def.sink_path().map(|s| s.to_string()),
            kafka_brokers: def.kafka_brokers().map(|s| s.to_string()),
            kafka_topic: def.kafka_topic().map(|s| s.to_string()),
            // Time-window fields retained on the wire for compatibility; unused by Source.
            from_timestamp: 0,
            to_timestamp: None,
            auto_end: def.auto_end,
            properties: match &def.sink_config {
                crate::model::SinkConfig::Delta { options, .. } => options.to_properties(),
                crate::model::SinkConfig::Filesystem { options, .. } => options
                    .to_properties_prefixed(crate::model::delta_options::FILESYSTEM_OPTION_PREFIX),
                crate::model::SinkConfig::Kafka { options, .. } => options.to_properties(),
                crate::model::SinkConfig::Iceberg { options } => options.to_properties(),
                crate::model::SinkConfig::Pulsar { options, .. } => options.to_properties(),
            },
            created_at_ms: def.created_at_ms,
            sink_endpoint: def.sink_endpoint().map(|s| s.to_string()),
        }
    }
}

impl TryFrom<proto::stream::StreamCheckpointFile> for Versioned<StreamCheckpoint> {
    type Error = TsdbError;

    fn try_from(file: proto::stream::StreamCheckpointFile) -> Result<Self, Self::Error> {
        let (disk_schema_version, min_reader_version) = validate_schema(
            file.schema_version,
            file.min_reader_version,
            "StreamCheckpoint",
        )?;
        let cp = file
            .checkpoint
            .ok_or_else(|| TsdbError::Storage("StreamCheckpoint file missing payload".into()))?;
        Ok(Versioned {
            inner: checkpoint_from_pb(cp),
            disk_schema_version,
            min_reader_version,
        })
    }
}

impl From<&Versioned<StreamCheckpoint>> for proto::stream::StreamCheckpointFile {
    fn from(v: &Versioned<StreamCheckpoint>) -> Self {
        Self {
            schema_version: STREAM_SCHEMA_VERSION,
            min_reader_version: STREAM_SCHEMA_MIN_SUPPORTED.max(v.min_reader_version),
            checkpoint: Some(checkpoint_to_pb(&v.inner)),
        }
    }
}

fn checkpoint_from_pb(pb: proto::stream::StreamCheckpoint) -> StreamCheckpoint {
    let tables = pb
        .tables
        .into_iter()
        .map(|(k, v)| {
            (
                k,
                TableCheckpoint {
                    acked_files: v.acked_files,
                    acked_lsn: v.acked_lsn,
                    batch_complete: v.batch_complete,
                    log_complete: v.log_complete,
                },
            )
        })
        .collect();
    StreamCheckpoint {
        stream_name: pb.stream_name,
        consumer_id: pb.consumer_id,
        log_channel_opened_ms: pb.log_channel_opened_ms,
        tables,
    }
}

fn checkpoint_to_pb(cp: &StreamCheckpoint) -> proto::stream::StreamCheckpoint {
    let tables = cp
        .tables
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                proto::stream::TableCheckpoint {
                    acked_files: v.acked_files.clone(),
                    acked_lsn: v.acked_lsn,
                    // Retained on the wire for compatibility; progress is LSN-only.
                    log_watermark: 0,
                    batch_complete: v.batch_complete,
                    log_complete: v.log_complete,
                },
            )
        })
        .collect();
    proto::stream::StreamCheckpoint {
        stream_name: cp.stream_name.clone(),
        consumer_id: cp.consumer_id.clone(),
        log_channel_opened_ms: cp.log_channel_opened_ms,
        tables,
    }
}

fn connector_to_pb(c: ConnectorType) -> proto::stream::ConnectorType {
    match c {
        ConnectorType::Delta => proto::stream::ConnectorType::Delta,
        ConnectorType::Kafka => proto::stream::ConnectorType::Kafka,
        ConnectorType::Filesystem => proto::stream::ConnectorType::Filesystem,
        ConnectorType::Iceberg => proto::stream::ConnectorType::IcebergV1,
        ConnectorType::Pulsar => proto::stream::ConnectorType::Pulsar,
    }
}

fn connector_from_pb(v: i32) -> Result<ConnectorType, TsdbError> {
    match proto::stream::ConnectorType::try_from(v) {
        Ok(proto::stream::ConnectorType::Delta) => Ok(ConnectorType::Delta),
        Ok(proto::stream::ConnectorType::Kafka) => Ok(ConnectorType::Kafka),
        Ok(proto::stream::ConnectorType::Filesystem) => Ok(ConnectorType::Filesystem),
        Ok(proto::stream::ConnectorType::IcebergV1) => Ok(ConnectorType::Iceberg),
        Ok(proto::stream::ConnectorType::Pulsar) => Ok(ConnectorType::Pulsar),
        Ok(proto::stream::ConnectorType::Unspecified) | Err(_) => Err(TsdbError::Storage(format!(
            "unsupported connector_type={v} in stream protobuf"
        ))),
    }
}

fn capture_mode_to_pb(m: StreamCaptureMode) -> proto::stream::StreamCaptureMode {
    match m {
        StreamCaptureMode::Batch => proto::stream::StreamCaptureMode::Batch,
        StreamCaptureMode::Log => proto::stream::StreamCaptureMode::Log,
        StreamCaptureMode::Hybrid => proto::stream::StreamCaptureMode::Hybrid,
    }
}

fn capture_mode_from_pb(v: i32) -> Result<StreamCaptureMode, TsdbError> {
    match proto::stream::StreamCaptureMode::try_from(v) {
        Ok(proto::stream::StreamCaptureMode::Batch) => Ok(StreamCaptureMode::Batch),
        Ok(proto::stream::StreamCaptureMode::Log) => Ok(StreamCaptureMode::Log),
        Ok(proto::stream::StreamCaptureMode::Hybrid) => Ok(StreamCaptureMode::Hybrid),
        Ok(proto::stream::StreamCaptureMode::Unspecified) | Err(_) => {
            tracing::warn!(
                capture_mode = v,
                "unknown stream capture_mode in protobuf; defaulting to hybrid"
            );
            Ok(StreamCaptureMode::Hybrid)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::StreamCaptureMode;
    use prost::Message;

    fn sample() -> StreamDef {
        StreamDef {
            name: "s1".into(),
            source_tables: vec!["t0".into()],
            capture_mode: StreamCaptureMode::Hybrid,
            sink_config: SinkConfig::Delta {
                path: "/tmp/x".into(),
                endpoint: None,
                options: crate::model::DeltaSinkOptions::default(),
            },
            created_at_ms: 1,
            auto_end: false,
        }
    }

    #[test]
    fn delta_endpoint_roundtrips_on_wire() {
        let bytes = encode_stream_def(&StreamDef {
            sink_config: SinkConfig::Delta {
                path: "s3://b/t".into(),
                endpoint: Some("http://127.0.0.1:9000".into()),
                options: crate::model::DeltaSinkOptions::default(),
            },
            ..sample()
        })
        .unwrap();
        let v = decode_versioned_stream_def(&bytes).unwrap();
        assert_eq!(v.inner.sink_endpoint(), Some("http://127.0.0.1:9000"));
        let opts = v.inner.delta_options().unwrap();
        assert_eq!(opts.region, "us-east-1");
        assert_eq!(opts.connection_maximum, 500);
    }

    #[test]
    fn envelope_roundtrip() {
        let bytes = encode_stream_def(&sample()).unwrap();
        let v = decode_versioned_stream_def(&bytes).unwrap();
        assert_eq!(v.inner.name, "s1");
        assert_eq!(v.disk_schema_version, STREAM_SCHEMA_VERSION);
        assert!(v.can_rewrite());
    }

    #[test]
    fn refuses_future_min_reader() {
        let file = proto::stream::StreamDefFile {
            schema_version: 99,
            min_reader_version: STREAM_SCHEMA_VERSION + 1,
            def: Some((&sample()).into()),
        };
        let bytes = file.encode_to_vec();
        let err = decode_versioned_stream_def(&bytes).unwrap_err().to_string();
        assert!(err.contains("min_reader_version"), "{err}");
    }

    #[test]
    fn refuses_rewrite_of_newer_schema() {
        let v = Versioned {
            inner: sample(),
            disk_schema_version: STREAM_SCHEMA_VERSION + 1,
            min_reader_version: STREAM_SCHEMA_MIN_SUPPORTED,
        };
        assert!(!v.can_rewrite());
        assert!(encode_versioned_stream_def(&v).is_err());
    }

    #[test]
    fn checkpoint_roundtrip() {
        let cp = StreamCheckpoint {
            stream_name: "s1".into(),
            consumer_id: "c1".into(),
            log_channel_opened_ms: 42,
            tables: [(
                "t0".into(),
                TableCheckpoint {
                    acked_files: vec!["f1".into()],
                    acked_lsn: 9,
                    batch_complete: false,
                    log_complete: true,
                },
            )]
            .into_iter()
            .collect(),
        };
        let bytes = encode_stream_checkpoint(&cp).unwrap();
        let decoded = decode_stream_checkpoint(&bytes).unwrap();
        assert_eq!(decoded.stream_name, "s1");
        assert_eq!(decoded.tables["t0"].acked_lsn, 9);
    }

    #[test]
    fn kafka_options_roundtrip_on_wire() {
        let mut options = crate::model::KafkaSinkOptions::default();
        options.key_format = Some("json".into());
        options.key_fields = vec!["order_id".into()];
        options.key_fields_prefix = "k_".into();
        options.delivery_guarantee = crate::model::KafkaDeliveryGuarantee::ExactlyOnce;
        options.compression_type = Some("lz4".into());
        options.transaction_timeout_ms = Some(900_000);

        let bytes = encode_stream_def(&StreamDef {
            sink_config: SinkConfig::Kafka {
                brokers: "b:9092".into(),
                topic: "orders".into(),
                format: "json".into(),
                options,
            },
            ..sample()
        })
        .unwrap();
        let v = decode_versioned_stream_def(&bytes).unwrap();
        let o = v.inner.kafka_options().unwrap();
        assert_eq!(o.key_fields, vec!["order_id".to_string()]);
        assert_eq!(o.key_fields_prefix, "k_");
        assert_eq!(o.delivery_guarantee.as_str(), "exactly-once");
        assert_eq!(o.compression_type.as_deref(), Some("lz4"));
        assert_eq!(o.transaction_timeout_ms, Some(900_000));
    }
}
