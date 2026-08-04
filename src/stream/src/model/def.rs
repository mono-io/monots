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

//! Pure Stream DDL definition — no runtime status.

use common::{ConnectorType, Result, StreamCaptureMode, TsdbError};

use super::delta_options::{DeltaSinkOptions, DELTA_OPTION_KEYS, FILESYSTEM_OPTION_KEYS};
use super::kafka_options::{KafkaSinkOptions, KAFKA_OPTION_KEYS};

/// Pure stream configuration (persisted in `streams/*.pb`).
#[derive(Debug, Clone)]
pub struct StreamDef {
    pub name: String,
    pub source_tables: Vec<String>,
    pub capture_mode: StreamCaptureMode,
    pub sink_config: SinkConfig,
    pub created_at_ms: i64,
    /// When true, stream plan ends with `complete` instead of staying `activate`.
    pub auto_end: bool,
}

/// Strongly-typed downstream sink config (Delta / Kafka / Filesystem).
#[derive(Debug, Clone)]
pub enum SinkConfig {
    Kafka {
        brokers: String,
        topic: String,
        format: String,
        options: KafkaSinkOptions,
    },
    Delta {
        path: String,
        endpoint: Option<String>,
        /// Industrial S3 / table-property defaults (always filled on parse).
        options: DeltaSinkOptions,
    },
    /// Parquet directory export (local / `file://` / `s3://` / `s3a://`).
    Filesystem {
        path: String,
        endpoint: Option<String>,
        /// S3 client knobs when `path` is an object URI (same industrial defaults as Delta).
        options: DeltaSinkOptions,
    },
}

impl SinkConfig {
    pub fn connector_type(&self) -> ConnectorType {
        match self {
            Self::Kafka { .. } => ConnectorType::Kafka,
            Self::Delta { .. } => ConnectorType::Delta,
            Self::Filesystem { .. } => ConnectorType::Filesystem,
        }
    }

    pub fn delivery_format(&self) -> &str {
        match self {
            Self::Kafka { format, .. } => format.as_str(),
            Self::Delta { .. } | Self::Filesystem { .. } => "parquet",
        }
    }

    pub fn sink_path(&self) -> Option<&str> {
        match self {
            Self::Delta { path, .. } | Self::Filesystem { path, .. } => Some(path.as_str()),
            Self::Kafka { .. } => None,
        }
    }

    pub fn kafka_brokers(&self) -> Option<&str> {
        match self {
            Self::Kafka { brokers, .. } => Some(brokers.as_str()),
            _ => None,
        }
    }

    pub fn kafka_topic(&self) -> Option<&str> {
        match self {
            Self::Kafka { topic, .. } => Some(topic.as_str()),
            _ => None,
        }
    }

    pub fn kafka_options(&self) -> Option<&KafkaSinkOptions> {
        match self {
            Self::Kafka { options, .. } => Some(options),
            _ => None,
        }
    }

    pub fn sink_endpoint(&self) -> Option<&str> {
        match self {
            Self::Delta {
                endpoint: Some(ep), ..
            }
            | Self::Filesystem {
                endpoint: Some(ep), ..
            } if !ep.is_empty() => Some(ep.as_str()),
            _ => None,
        }
    }

    pub fn delta_options(&self) -> Option<&DeltaSinkOptions> {
        match self {
            Self::Delta { options, .. } | Self::Filesystem { options, .. } => Some(options),
            _ => None,
        }
    }
}

impl StreamDef {
    pub fn connector_type(&self) -> ConnectorType {
        self.sink_config.connector_type()
    }

    pub fn delivery_format(&self) -> &str {
        self.sink_config.delivery_format()
    }

    pub fn delivery_files(&self) -> bool {
        self.capture_mode.includes_batch()
    }

    pub fn sink_path(&self) -> Option<&str> {
        self.sink_config.sink_path()
    }

    pub fn kafka_brokers(&self) -> Option<&str> {
        self.sink_config.kafka_brokers()
    }

    pub fn kafka_topic(&self) -> Option<&str> {
        self.sink_config.kafka_topic()
    }

    pub fn kafka_options(&self) -> Option<&KafkaSinkOptions> {
        self.sink_config.kafka_options()
    }

    pub fn sink_endpoint(&self) -> Option<&str> {
        self.sink_config.sink_endpoint()
    }

    pub fn delta_options(&self) -> Option<&DeltaSinkOptions> {
        self.sink_config.delta_options()
    }

    /// Validation enforcing single source table binding.
    pub fn ensure_single_source_table(&self) -> Result<()> {
        match self.source_tables.as_slice() {
            [_] => Ok(()),
            [] => Err(TsdbError::Query(format!(
                "Stream {} requires exactly one source table",
                self.name
            ))),
            _ => Err(TsdbError::Query(format!(
                "Stream {} currently only supports binding to a single table",
                self.name
            ))),
        }
    }

    /// Fill lake/object-store endpoint from a process-wide default when the stream omitted it.
    /// Stream DDL (`sink.delta.endpoint` / `sink.filesystem.endpoint`) always wins.
    pub fn with_lake_endpoint(mut self, endpoint: Option<String>) -> Self {
        match &mut self.sink_config {
            SinkConfig::Delta { endpoint: e, .. } | SinkConfig::Filesystem { endpoint: e, .. } => {
                if e.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
                    if let Some(ep) = endpoint.filter(|s| !s.is_empty()) {
                        *e = Some(ep);
                    }
                }
            }
            _ => {}
        }
        self
    }
}

pub use common::{StreamPhase, StreamStatus, TableCaptureStatus, TableStreamStatus};

use std::collections::HashMap;

use crate::connector::{connector_capture_mode, connector_default_format, validate_sink};

/// Look up the first non-empty value among candidate DDL keys.
fn opt_keyed<'a>(options: &'a HashMap<String, String>, keys: &[&str]) -> Option<&'a str> {
    for k in keys {
        if let Some(v) = options.get(*k) {
            if !v.is_empty() {
                return Some(v.as_str());
            }
        }
    }
    None
}

fn has_any_key(options: &HashMap<String, String>, keys: &[&str]) -> bool {
    keys.iter().any(|k| options.contains_key(*k))
}

/// Flat / alias keys that are not part of the v1 DDL surface.
const REJECTED_LEGACY_KEYS: &[&str] = &[
    "sink.path",
    "sink.endpoint",
    "sink.fs.path",
    "sink.brokers",
    "sink.topic",
];

fn reject_legacy_keys(options: &HashMap<String, String>) -> Result<()> {
    let present: Vec<&str> = REJECTED_LEGACY_KEYS
        .iter()
        .copied()
        .filter(|k| options.contains_key(*k))
        .collect();
    if present.is_empty() {
        return Ok(());
    }
    Err(TsdbError::Query(format!(
        "unsupported options {} (use sink.delta.|sink.filesystem.|sink.kafka. prefixed keys)",
        present.join(", ")
    )))
}

fn reject_foreign_sink_keys(
    connector: ConnectorType,
    options: &HashMap<String, String>,
) -> Result<()> {
    reject_legacy_keys(options)?;

    let delta_keys = ["sink.delta.path", "sink.delta.endpoint"];
    let fs_keys = ["sink.filesystem.path", "sink.filesystem.endpoint"];
    let kafka_keys = ["sink.kafka.brokers", "sink.kafka.topic"];

    let bad = match connector {
        ConnectorType::Delta => {
            let mut v = Vec::new();
            if has_any_key(options, &fs_keys) {
                v.extend_from_slice(&fs_keys);
            }
            if has_any_key(options, FILESYSTEM_OPTION_KEYS) {
                v.extend_from_slice(FILESYSTEM_OPTION_KEYS);
            }
            if has_any_key(options, &kafka_keys) {
                v.extend_from_slice(&kafka_keys);
            }
            if has_any_key(options, KAFKA_OPTION_KEYS) {
                v.extend_from_slice(KAFKA_OPTION_KEYS);
            }
            v
        }
        ConnectorType::Filesystem => {
            let mut v = Vec::new();
            if has_any_key(options, &delta_keys) {
                v.extend_from_slice(&delta_keys);
            }
            if has_any_key(options, DELTA_OPTION_KEYS) {
                v.extend_from_slice(DELTA_OPTION_KEYS);
            }
            if has_any_key(options, &kafka_keys) {
                v.extend_from_slice(&kafka_keys);
            }
            if has_any_key(options, KAFKA_OPTION_KEYS) {
                v.extend_from_slice(KAFKA_OPTION_KEYS);
            }
            v
        }
        ConnectorType::Kafka => {
            let mut v = Vec::new();
            if has_any_key(options, &delta_keys) {
                v.extend_from_slice(&delta_keys);
            }
            if has_any_key(options, DELTA_OPTION_KEYS) {
                v.extend_from_slice(DELTA_OPTION_KEYS);
            }
            if has_any_key(options, &fs_keys) {
                v.extend_from_slice(&fs_keys);
            }
            if has_any_key(options, FILESYSTEM_OPTION_KEYS) {
                v.extend_from_slice(FILESYSTEM_OPTION_KEYS);
            }
            v
        }
    };

    let present: Vec<&str> = bad
        .into_iter()
        .filter(|k| options.contains_key(*k))
        .collect();
    if present.is_empty() {
        return Ok(());
    }
    Err(TsdbError::Query(format!(
        "options {} are not valid for sink.type = {} (use sink.{}.… for sink-specific keys)",
        present.join(", "),
        connector.as_str(),
        match connector {
            ConnectorType::Delta => "delta",
            ConnectorType::Filesystem => "filesystem",
            ConnectorType::Kafka => "kafka",
        }
    )))
}

pub fn parse_stream_def(
    name: String,
    options: &HashMap<String, String>,
    created_at_ms: i64,
) -> Result<StreamDef> {
    let connector_raw = options
        .get("sink.type")
        .ok_or_else(|| TsdbError::Query("sink.type is required".into()))?;
    let connector_type = ConnectorType::parse(connector_raw)?;
    reject_foreign_sink_keys(connector_type, options)?;

    let source_tables = parse_source_tables(options)?;

    // User may set `cdc.mode` to `batch` or `hybrid` only (`log` is rejected).
    let capture_mode = match options.get("cdc.mode") {
        Some(raw) => StreamCaptureMode::parse(raw)?,
        None => connector_capture_mode(connector_type),
    };

    let delivery_format = options
        .get("sink.format")
        .cloned()
        .unwrap_or_else(|| connector_default_format(connector_type).to_string());

    if options.contains_key("sink.consumer") {
        return Err(TsdbError::Query(
            "sink.consumer is not supported; checkpoint identity is always stream::<name>".into(),
        ));
    }

    if options.contains_key("cdc.from_timestamp") || options.contains_key("cdc.to_timestamp") {
        return Err(TsdbError::Query(
            "cdc.from_timestamp / cdc.to_timestamp are not supported; stream source does not filter by time"
                .into(),
        ));
    }

    let auto_end = options
        .get("cdc.auto_end")
        .map(|s| parse_bool(s))
        .transpose()?
        .unwrap_or(false);

    let sink_config = match connector_type {
        ConnectorType::Kafka => {
            let brokers = opt_keyed(options, &["sink.kafka.brokers"])
                .unwrap_or_default()
                .to_string();
            let topic = opt_keyed(options, &["sink.kafka.topic"])
                .unwrap_or_default()
                .to_string();
            if brokers.is_empty() || topic.is_empty() {
                return Err(TsdbError::Query(
                    "kafka sink requires sink.kafka.brokers and sink.kafka.topic".into(),
                ));
            }
            SinkConfig::Kafka {
                brokers,
                topic,
                format: delivery_format,
                options: KafkaSinkOptions::from_ddl(options)?,
            }
        }
        ConnectorType::Delta => {
            let path = opt_keyed(options, &["sink.delta.path"])
                .ok_or_else(|| TsdbError::Query("delta sink requires sink.delta.path".into()))?
                .to_string();
            let endpoint = opt_keyed(options, &["sink.delta.endpoint"]).map(|s| s.to_string());
            let delta_opts = DeltaSinkOptions::from_ddl(options)?;
            SinkConfig::Delta {
                path,
                endpoint,
                options: delta_opts,
            }
        }
        ConnectorType::Filesystem => {
            let path = opt_keyed(options, &["sink.filesystem.path"])
                .ok_or_else(|| {
                    TsdbError::Query("filesystem sink requires sink.filesystem.path".into())
                })?
                .to_string();
            let endpoint = opt_keyed(options, &["sink.filesystem.endpoint"]).map(|s| s.to_string());
            let fs_opts = DeltaSinkOptions::from_filesystem_ddl(options)?;
            SinkConfig::Filesystem {
                path,
                endpoint,
                options: fs_opts,
            }
        }
    };

    let def = StreamDef {
        name,
        source_tables,
        capture_mode,
        sink_config,
        created_at_ms,
        auto_end,
    };

    validate_sink(&def)?;
    def.ensure_single_source_table()?;
    Ok(def)
}

pub fn ensure_single_source_table(def: &StreamDef) -> Result<()> {
    def.ensure_single_source_table()
}

fn parse_source_tables(options: &HashMap<String, String>) -> Result<Vec<String>> {
    let raw = options
        .get("source.table")
        .ok_or_else(|| TsdbError::Query("source.table is required".into()))?;
    let table = raw.trim();
    if table.is_empty() {
        return Err(TsdbError::Query("source.table must not be empty".into()));
    }
    if table.contains(',') {
        return Err(TsdbError::Query(
            "a stream can only bind one source.table".into(),
        ));
    }
    Ok(vec![table.to_string()])
}

fn parse_bool(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        other => Err(TsdbError::Query(format!("invalid boolean: {other}"))),
    }
}

pub fn stream_plan(def: &StreamDef) -> Vec<&'static str> {
    let mut steps = Vec::new();
    if def.capture_mode.includes_log() {
        steps.push("pin_capture_progress");
    }
    if def.capture_mode.includes_batch() {
        steps.push("sync_batch_parquet");
    }
    if def.capture_mode.includes_log() {
        steps.push("tail_log_wal");
    }
    if def.auto_end {
        steps.push("complete");
    } else {
        steps.push("activate");
    }
    steps
}

/// Durable checkpoint / worker identity for a stream (always derived from the name).
pub fn stream_worker_id(def: &StreamDef) -> String {
    format!("stream::{}", def.name)
}

pub fn should_run_phase(phase: StreamPhase) -> bool {
    !matches!(
        phase,
        StreamPhase::Completed | StreamPhase::Failed | StreamPhase::Suspended
    )
}

pub fn should_run_stream(_def: &StreamDef) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn opts(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn source_table_singular_is_accepted() {
        let def = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "delta"),
                ("source.table", "t0"),
                ("sink.delta.path", "/tmp/x"),
            ]),
            0,
        )
        .unwrap();
        assert_eq!(def.source_tables, vec!["t0".to_string()]);
        assert!(matches!(def.sink_config, SinkConfig::Delta { .. }));
    }

    #[test]
    fn multi_table_stream_is_rejected() {
        let err = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "delta"),
                ("source.table", "t0,t1"),
                ("sink.delta.path", "/tmp/x"),
            ]),
            0,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("one source.table"), "{err}");
    }

    #[test]
    fn source_table_is_required() {
        let err = parse_stream_def(
            "s".into(),
            &opts(&[("sink.type", "delta"), ("sink.delta.path", "/tmp/x")]),
            0,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("source.table"), "{err}");
    }

    #[test]
    fn unsupported_connector_types_are_rejected() {
        for connector in ["monots", "json", "iceberg"] {
            let err = parse_stream_def(
                "s".into(),
                &opts(&[
                    ("sink.type", connector),
                    ("source.table", "t"),
                    ("sink.delta.path", "/tmp/x"),
                ]),
                0,
            )
            .unwrap_err()
            .to_string();
            assert!(err.contains("unsupported sink.type"), "{err}");
        }
    }

    #[test]
    fn delta_defaults_to_batch_mode() {
        let def = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "delta"),
                ("source.table", "t"),
                ("sink.delta.path", "/tmp/x"),
            ]),
            0,
        )
        .unwrap();
        assert_eq!(def.capture_mode, StreamCaptureMode::Batch);
        assert_eq!(def.delivery_format(), "parquet");
    }

    #[test]
    fn filesystem_defaults_to_batch_parquet() {
        let def = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "filesystem"),
                ("source.table", "t"),
                ("sink.filesystem.path", "/tmp/fs"),
            ]),
            0,
        )
        .unwrap();
        assert_eq!(def.capture_mode, StreamCaptureMode::Batch);
        assert_eq!(def.delivery_format(), "parquet");
    }

    #[test]
    fn kafka_defaults_to_hybrid_json() {
        let def = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "kafka"),
                ("source.table", "t"),
                ("sink.kafka.brokers", "localhost:9092"),
                ("sink.kafka.topic", "t"),
            ]),
            0,
        )
        .unwrap();
        assert_eq!(def.capture_mode, StreamCaptureMode::Hybrid);
        assert_eq!(def.delivery_format(), "json");
        let o = def.kafka_options().unwrap();
        assert_eq!(o.delivery_guarantee.as_str(), "at-least-once");
        assert_eq!(o.partitioner.as_str(), "default");
    }

    #[test]
    fn kafka_full_flink_style_options_parse() {
        let def = parse_stream_def(
            "orders".into(),
            &opts(&[
                ("sink.type", "kafka"),
                ("source.table", "t"),
                ("sink.kafka.brokers", "kafka-broker1:9093,kafka-broker2:9093"),
                ("sink.kafka.topic", "dwd_order_events"),
                ("sink.format", "json"),
                ("sink.kafka.key.format", "json"),
                ("sink.kafka.key.fields", "order_id"),
                ("sink.kafka.key.fields-prefix", "k_"),
                ("sink.kafka.partitioner", "default"),
                ("sink.kafka.delivery-guarantee", "exactly-once"),
                ("sink.kafka.transaction.timeout.ms", "900000"),
                ("sink.kafka.compression.type", "lz4"),
                ("sink.kafka.batch.size", "65536"),
                ("sink.kafka.linger.ms", "20"),
                ("sink.kafka.acks", "all"),
                ("sink.kafka.retries", "10"),
                ("sink.kafka.security.protocol", "SASL_SSL"),
                ("sink.kafka.sasl.mechanism", "PLAIN"),
                (
                    "sink.kafka.sasl.jaas.config",
                    r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="your_password";"#,
                ),
                ("sink.kafka.ssl.truststore.location", "/opt/certs/ca.pem"),
                ("sink.kafka.ssl.keystore.location", "/opt/certs/client.p12"),
                ("sink.kafka.ssl.keystore.password", "keystore_password"),
            ]),
            0,
        )
        .unwrap();
        assert_eq!(def.sink_path(), None);
        assert_eq!(def.kafka_topic(), Some("dwd_order_events"));
        let o = def.kafka_options().unwrap();
        assert_eq!(o.key_fields, vec!["order_id".to_string()]);
        assert_eq!(o.key_fields_prefix, "k_");
        assert_eq!(o.delivery_guarantee.as_str(), "exactly-once");
        assert_eq!(o.compression_type.as_deref(), Some("lz4"));
        assert_eq!(o.sasl_username.as_deref(), Some("admin"));
        assert_eq!(o.ssl_ca_location.as_deref(), Some("/opt/certs/ca.pem"));
    }

    #[test]
    fn sink_format_is_honored_for_kafka() {
        let def = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "kafka"),
                ("source.table", "t"),
                ("sink.kafka.brokers", "localhost:9092"),
                ("sink.kafka.topic", "t"),
                ("sink.format", "json"),
                ("cdc.mode", "batch"),
            ]),
            0,
        )
        .unwrap();
        assert_eq!(def.delivery_format(), "json");
        assert_eq!(def.capture_mode, StreamCaptureMode::Batch);
    }

    #[test]
    fn legacy_connector_keys_are_rejected() {
        let err = parse_stream_def(
            "s".into(),
            &opts(&[
                ("connector.type", "kafka"),
                ("source.table", "t"),
                ("connector.brokers", "localhost:9092"),
                ("connector.topic", "t"),
            ]),
            0,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("sink.type is required"), "{err}");
    }

    #[test]
    fn cdc_mode_batch_and_hybrid_are_accepted() {
        let batch = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "kafka"),
                ("source.table", "t"),
                ("sink.kafka.brokers", "localhost:9092"),
                ("sink.kafka.topic", "t"),
                ("cdc.mode", "batch"),
            ]),
            0,
        )
        .unwrap();
        assert_eq!(batch.capture_mode, StreamCaptureMode::Batch);

        let hybrid = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "delta"),
                ("source.table", "t"),
                ("sink.delta.path", "/tmp/x"),
                ("cdc.mode", "hybrid"),
            ]),
            0,
        )
        .unwrap();
        assert_eq!(hybrid.capture_mode, StreamCaptureMode::Hybrid);
    }

    #[test]
    fn sink_consumer_is_rejected() {
        let err = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "delta"),
                ("source.table", "t"),
                ("sink.delta.path", "/tmp/x"),
                ("sink.consumer", "job-1"),
            ]),
            0,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("sink.consumer"), "{err}");
    }

    #[test]
    fn stream_worker_id_is_derived_from_name() {
        let def = parse_stream_def(
            "metrics_out".into(),
            &opts(&[
                ("sink.type", "delta"),
                ("source.table", "t"),
                ("sink.delta.path", "/tmp/x"),
            ]),
            0,
        )
        .unwrap();
        assert_eq!(stream_worker_id(&def), "stream::metrics_out");
    }

    #[test]
    fn delta_sink_endpoint_from_ddl() {
        let def = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "delta"),
                ("source.table", "t"),
                ("sink.delta.path", "s3://bucket/metrics"),
                ("sink.delta.endpoint", "http://127.0.0.1:9000"),
            ]),
            0,
        )
        .unwrap();
        assert_eq!(
            def.sink_endpoint(),
            Some("http://127.0.0.1:9000"),
            "DDL sink.delta.endpoint should bind onto DeltaSink"
        );
        match &def.sink_config {
            SinkConfig::Delta {
                path,
                endpoint,
                options,
            } => {
                assert_eq!(path, "s3://bucket/metrics");
                assert_eq!(endpoint.as_deref(), Some("http://127.0.0.1:9000"));
                assert_eq!(options.region, crate::model::delta_options::DEFAULT_REGION);
                assert_eq!(
                    options.connection_maximum,
                    crate::model::delta_options::DEFAULT_CONNECTION_MAXIMUM
                );
            }
            other => panic!("expected Delta sink, got {other:?}"),
        }
    }

    #[test]
    fn delta_sink_defaults_always_materialized() {
        let def = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "delta"),
                ("source.table", "t"),
                ("sink.delta.path", "/tmp/x"),
            ]),
            0,
        )
        .unwrap();
        let opts = def.delta_options().expect("delta options");
        assert_eq!(opts.region, "us-east-1");
        assert_eq!(opts.connection_timeout_ms, 200_000);
        assert_eq!(opts.attempts_maximum, 20);
        assert_eq!(opts.connection_maximum, 500);
    }

    #[test]
    fn delta_connection_timeout_parses_sql_duration() {
        let def = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "delta"),
                ("source.table", "t"),
                ("sink.delta.path", "/tmp/x"),
                ("sink.delta.connection.timeout", "3 min"),
            ]),
            0,
        )
        .unwrap();
        assert_eq!(def.delta_options().unwrap().connection_timeout_ms, 180_000);
    }

    #[test]
    fn flat_legacy_sink_keys_are_rejected() {
        let err = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "delta"),
                ("source.table", "t"),
                ("sink.path", "s3://b/legacy"),
                ("sink.endpoint", "http://legacy"),
            ]),
            0,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("unsupported options"), "{err}");
        assert!(
            err.contains("sink.path") || err.contains("sink.endpoint"),
            "{err}"
        );
    }

    #[test]
    fn filesystem_s3_ddl_parses_client_options() {
        let def = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "filesystem"),
                ("source.table", "t"),
                ("sink.filesystem.path", "s3a://bucket/export"),
                ("sink.filesystem.endpoint", "http://127.0.0.1:9000"),
                ("sink.filesystem.region", "us-west-2"),
                ("sink.filesystem.connection.timeout", "3 min"),
            ]),
            0,
        )
        .unwrap();
        assert_eq!(def.sink_path(), Some("s3a://bucket/export"));
        assert_eq!(def.sink_endpoint(), Some("http://127.0.0.1:9000"));
        let opts = def.delta_options().expect("fs options");
        assert_eq!(opts.region, "us-west-2");
        assert_eq!(opts.connection_timeout_ms, 180_000);
    }

    #[test]
    fn sink_endpoint_rejected_for_non_delta() {
        let err = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "filesystem"),
                ("source.table", "t"),
                ("sink.filesystem.path", "/tmp/x"),
                ("sink.delta.endpoint", "http://127.0.0.1:9000"),
            ]),
            0,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("sink.delta.endpoint"), "{err}");
        assert!(err.contains("filesystem"), "{err}");
    }

    #[test]
    fn ddl_endpoint_wins_over_lake_default() {
        let def = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "delta"),
                ("source.table", "t"),
                ("sink.delta.path", "s3://b/t"),
                ("sink.delta.endpoint", "http://ddl-endpoint"),
            ]),
            0,
        )
        .unwrap()
        .with_lake_endpoint(Some("http://env-endpoint".into()));
        assert_eq!(def.sink_endpoint(), Some("http://ddl-endpoint"));
    }

    #[test]
    fn cdc_mode_log_is_rejected() {
        let err = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "delta"),
                ("source.table", "t"),
                ("sink.delta.path", "/tmp/x"),
                ("cdc.mode", "log"),
            ]),
            0,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("not supported"), "{err}");
        assert!(err.contains("batch") || err.contains("hybrid"), "{err}");
    }

    #[test]
    fn batch_mode_plan_is_parquet_only() {
        let def = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "delta"),
                ("source.table", "t"),
                ("sink.delta.path", "/tmp/x"),
            ]),
            0,
        )
        .unwrap();
        assert_eq!(stream_plan(&def), vec!["sync_batch_parquet", "activate"]);
    }

    #[test]
    fn hybrid_plan_pins_progress_then_parquet_then_wal() {
        let def = StreamDef {
            name: "s".into(),
            source_tables: vec!["t".into()],
            capture_mode: StreamCaptureMode::Hybrid,
            sink_config: SinkConfig::Delta {
                path: "/tmp/x".into(),
                endpoint: None,
                options: DeltaSinkOptions::default(),
            },
            created_at_ms: 0,
            auto_end: false,
        };
        assert_eq!(
            stream_plan(&def),
            vec![
                "pin_capture_progress",
                "sync_batch_parquet",
                "tail_log_wal",
                "activate"
            ]
        );
    }

    #[test]
    fn kafka_plan_is_hybrid_by_default() {
        let def = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "kafka"),
                ("source.table", "t"),
                ("sink.kafka.brokers", "localhost:9092"),
                ("sink.kafka.topic", "t"),
            ]),
            0,
        )
        .unwrap();
        assert_eq!(
            stream_plan(&def),
            vec![
                "pin_capture_progress",
                "sync_batch_parquet",
                "tail_log_wal",
                "activate"
            ]
        );
    }

    #[test]
    fn rejects_time_filter_options() {
        let err = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "delta"),
                ("source.table", "t"),
                ("sink.delta.path", "/tmp/x"),
                ("cdc.to_timestamp", "100"),
            ]),
            0,
        )
        .unwrap_err();
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn auto_end_option() {
        let def = parse_stream_def(
            "s".into(),
            &opts(&[
                ("sink.type", "delta"),
                ("source.table", "t"),
                ("sink.delta.path", "/tmp/x"),
                ("cdc.auto_end", "true"),
            ]),
            0,
        )
        .unwrap();
        assert!(def.auto_end);
        assert_eq!(stream_plan(&def), vec!["sync_batch_parquet", "complete"]);
    }
}
