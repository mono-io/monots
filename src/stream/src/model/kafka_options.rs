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

//! Kafka sink options (delivery, keying, producer tuning, SASL/SSL).
//!
//! DDL keys use the `sink.kafka.*` prefix. librdkafka property names are used
//! at runtime (not Java `properties.*`). JAAS strings are parsed into username /
//! password; Java JKS truststore paths are accepted as aliases for PEM CA files.

use std::collections::HashMap;

use common::{Result, TsdbError};

pub const KAFKA_OPTION_PREFIX: &str = "sink.kafka";

/// All optional Kafka DDL keys (excludes required brokers/topic).
pub const KAFKA_OPTION_KEYS: &[&str] = &[
    "sink.kafka.key.format",
    "sink.kafka.key.fields",
    "sink.kafka.key.fields-prefix",
    "sink.kafka.partitioner",
    "sink.kafka.delivery-guarantee",
    "sink.kafka.transactional.id",
    "sink.kafka.transaction.timeout.ms",
    "sink.kafka.compression.type",
    "sink.kafka.batch.size",
    "sink.kafka.linger.ms",
    "sink.kafka.acks",
    "sink.kafka.retries",
    "sink.kafka.security.protocol",
    "sink.kafka.sasl.mechanism",
    "sink.kafka.sasl.jaas.config",
    "sink.kafka.sasl.username",
    "sink.kafka.sasl.password",
    "sink.kafka.ssl.ca.location",
    "sink.kafka.ssl.truststore.location",
    "sink.kafka.ssl.truststore.password",
    "sink.kafka.ssl.certificate.location",
    "sink.kafka.ssl.key.location",
    "sink.kafka.ssl.key.password",
    "sink.kafka.ssl.keystore.location",
    "sink.kafka.ssl.keystore.password",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KafkaDeliveryGuarantee {
    #[default]
    AtLeastOnce,
    ExactlyOnce,
}

impl KafkaDeliveryGuarantee {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "at-least-once" | "at_least_once" | "alo" => Ok(Self::AtLeastOnce),
            "exactly-once" | "exactly_once" | "eos" => Ok(Self::ExactlyOnce),
            other => Err(TsdbError::Query(format!(
                "invalid sink.kafka.delivery-guarantee: {other} (use at-least-once | exactly-once)"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::AtLeastOnce => "at-least-once",
            Self::ExactlyOnce => "exactly-once",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KafkaPartitioner {
    /// librdkafka default (key hash when key present).
    #[default]
    Default,
    /// Force round-robin regardless of key.
    RoundRobin,
    /// Always partition 0.
    Fixed,
}

impl KafkaPartitioner {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "default" => Ok(Self::Default),
            "round-robin" | "round_robin" => Ok(Self::RoundRobin),
            "fixed" => Ok(Self::Fixed),
            other => Err(TsdbError::Query(format!(
                "invalid sink.kafka.partitioner: {other} (use default | round-robin | fixed)"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::RoundRobin => "round-robin",
            Self::Fixed => "fixed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaSinkOptions {
    pub key_format: Option<String>,
    pub key_fields: Vec<String>,
    pub key_fields_prefix: String,
    pub partitioner: KafkaPartitioner,
    pub delivery_guarantee: KafkaDeliveryGuarantee,
    pub transactional_id: Option<String>,
    pub transaction_timeout_ms: Option<u64>,
    pub compression_type: Option<String>,
    pub batch_size: Option<u32>,
    pub linger_ms: Option<u64>,
    pub acks: Option<String>,
    pub retries: Option<u32>,
    pub security_protocol: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    /// PEM CA bundle (also accepts Flink `ssl.truststore.location` as alias).
    pub ssl_ca_location: Option<String>,
    pub ssl_truststore_password: Option<String>,
    pub ssl_certificate_location: Option<String>,
    pub ssl_key_location: Option<String>,
    pub ssl_key_password: Option<String>,
    /// PKCS#12 client keystore (librdkafka).
    pub ssl_keystore_location: Option<String>,
    pub ssl_keystore_password: Option<String>,
}

impl Default for KafkaSinkOptions {
    fn default() -> Self {
        Self {
            key_format: None,
            key_fields: Vec::new(),
            key_fields_prefix: String::new(),
            partitioner: KafkaPartitioner::Default,
            delivery_guarantee: KafkaDeliveryGuarantee::AtLeastOnce,
            transactional_id: None,
            transaction_timeout_ms: None,
            compression_type: None,
            batch_size: None,
            linger_ms: None,
            acks: None,
            retries: None,
            security_protocol: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            ssl_ca_location: None,
            ssl_truststore_password: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ssl_key_password: None,
            ssl_keystore_location: None,
            ssl_keystore_password: None,
        }
    }
}

impl KafkaSinkOptions {
    pub fn from_ddl(options: &HashMap<String, String>) -> Result<Self> {
        reject_unknown_kafka_option_keys(options)?;

        let mut opts = Self::default();

        if let Some(v) = non_empty(options, "sink.kafka.key.format") {
            let fmt = v.to_ascii_lowercase();
            if fmt != "json" {
                return Err(TsdbError::Query(format!(
                    "unsupported sink.kafka.key.format: {v} (only json)"
                )));
            }
            opts.key_format = Some("json".into());
        }
        if let Some(v) = non_empty(options, "sink.kafka.key.fields") {
            opts.key_fields = v
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
        if let Some(v) = options.get("sink.kafka.key.fields-prefix") {
            opts.key_fields_prefix = v.clone();
        }
        if let Some(v) = non_empty(options, "sink.kafka.partitioner") {
            opts.partitioner = KafkaPartitioner::parse(v)?;
        }
        if let Some(v) = non_empty(options, "sink.kafka.delivery-guarantee") {
            opts.delivery_guarantee = KafkaDeliveryGuarantee::parse(v)?;
        }
        if let Some(v) = non_empty(options, "sink.kafka.transactional.id") {
            opts.transactional_id = Some(v.to_string());
        }
        if let Some(v) = non_empty(options, "sink.kafka.transaction.timeout.ms") {
            opts.transaction_timeout_ms = Some(parse_u64(v, "sink.kafka.transaction.timeout.ms")?);
        }
        if let Some(v) = non_empty(options, "sink.kafka.compression.type") {
            opts.compression_type = Some(v.to_ascii_lowercase());
        }
        if let Some(v) = non_empty(options, "sink.kafka.batch.size") {
            opts.batch_size = Some(parse_u32(v, "sink.kafka.batch.size")?);
        }
        if let Some(v) = non_empty(options, "sink.kafka.linger.ms") {
            opts.linger_ms = Some(parse_u64(v, "sink.kafka.linger.ms")?);
        }
        if let Some(v) = non_empty(options, "sink.kafka.acks") {
            opts.acks = Some(v.to_string());
        }
        if let Some(v) = non_empty(options, "sink.kafka.retries") {
            opts.retries = Some(parse_u32(v, "sink.kafka.retries")?);
        }
        if let Some(v) = non_empty(options, "sink.kafka.security.protocol") {
            opts.security_protocol = Some(v.to_string());
        }
        if let Some(v) = non_empty(options, "sink.kafka.sasl.mechanism") {
            opts.sasl_mechanism = Some(v.to_string());
        }
        if let Some(v) = non_empty(options, "sink.kafka.sasl.username") {
            opts.sasl_username = Some(v.to_string());
        }
        if let Some(v) = non_empty(options, "sink.kafka.sasl.password") {
            opts.sasl_password = Some(v.to_string());
        }
        if let Some(jaas) = non_empty(options, "sink.kafka.sasl.jaas.config") {
            let (user, pass) = parse_jaas_credentials(jaas)?;
            if opts.sasl_username.is_none() {
                opts.sasl_username = Some(user);
            }
            if opts.sasl_password.is_none() {
                opts.sasl_password = Some(pass);
            }
        }

        // PEM CA: prefer explicit ca.location; accept Flink truststore.location as alias.
        if let Some(v) = non_empty(options, "sink.kafka.ssl.ca.location") {
            opts.ssl_ca_location = Some(v.to_string());
        } else if let Some(v) = non_empty(options, "sink.kafka.ssl.truststore.location") {
            opts.ssl_ca_location = Some(v.to_string());
        }
        if let Some(v) = non_empty(options, "sink.kafka.ssl.truststore.password") {
            opts.ssl_truststore_password = Some(v.to_string());
        }
        if let Some(v) = non_empty(options, "sink.kafka.ssl.certificate.location") {
            opts.ssl_certificate_location = Some(v.to_string());
        }
        if let Some(v) = non_empty(options, "sink.kafka.ssl.key.location") {
            opts.ssl_key_location = Some(v.to_string());
        }
        if let Some(v) = non_empty(options, "sink.kafka.ssl.key.password") {
            opts.ssl_key_password = Some(v.to_string());
        }
        if let Some(v) = non_empty(options, "sink.kafka.ssl.keystore.location") {
            opts.ssl_keystore_location = Some(v.to_string());
        }
        if let Some(v) = non_empty(options, "sink.kafka.ssl.keystore.password") {
            opts.ssl_keystore_password = Some(v.to_string());
        }

        opts.validate()?;
        Ok(opts)
    }

    pub fn from_properties(props: &HashMap<String, String>) -> Result<Self> {
        Self::from_ddl(props)
    }

    fn validate(&self) -> Result<()> {
        if !self.key_fields.is_empty() {
            let fmt = self.key_format.as_deref().unwrap_or("json");
            if fmt != "json" {
                return Err(TsdbError::Query(
                    "sink.kafka.key.fields requires sink.kafka.key.format = json".into(),
                ));
            }
        }
        if self.delivery_guarantee == KafkaDeliveryGuarantee::ExactlyOnce
            && self.transaction_timeout_ms.is_none()
        {
            // Default applied at runtime; ok to omit.
        }
        if let Some(acks) = &self.acks {
            if self.delivery_guarantee == KafkaDeliveryGuarantee::ExactlyOnce
                && acks != "all"
                && acks != "-1"
            {
                return Err(TsdbError::Query(
                    "sink.kafka.acks must be all (or -1) when delivery-guarantee = exactly-once"
                        .into(),
                ));
            }
        }
        Ok(())
    }

    /// Whether key payloads should be produced.
    pub fn has_key(&self) -> bool {
        !self.key_fields.is_empty()
    }

    pub fn to_properties(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        if let Some(v) = &self.key_format {
            m.insert("sink.kafka.key.format".into(), v.clone());
        }
        if !self.key_fields.is_empty() {
            m.insert("sink.kafka.key.fields".into(), self.key_fields.join(","));
        }
        if !self.key_fields_prefix.is_empty() {
            m.insert(
                "sink.kafka.key.fields-prefix".into(),
                self.key_fields_prefix.clone(),
            );
        }
        m.insert(
            "sink.kafka.partitioner".into(),
            self.partitioner.as_str().into(),
        );
        m.insert(
            "sink.kafka.delivery-guarantee".into(),
            self.delivery_guarantee.as_str().into(),
        );
        if let Some(v) = &self.transactional_id {
            m.insert("sink.kafka.transactional.id".into(), v.clone());
        }
        if let Some(v) = self.transaction_timeout_ms {
            m.insert("sink.kafka.transaction.timeout.ms".into(), v.to_string());
        }
        if let Some(v) = &self.compression_type {
            m.insert("sink.kafka.compression.type".into(), v.clone());
        }
        if let Some(v) = self.batch_size {
            m.insert("sink.kafka.batch.size".into(), v.to_string());
        }
        if let Some(v) = self.linger_ms {
            m.insert("sink.kafka.linger.ms".into(), v.to_string());
        }
        if let Some(v) = &self.acks {
            m.insert("sink.kafka.acks".into(), v.clone());
        }
        if let Some(v) = self.retries {
            m.insert("sink.kafka.retries".into(), v.to_string());
        }
        if let Some(v) = &self.security_protocol {
            m.insert("sink.kafka.security.protocol".into(), v.clone());
        }
        if let Some(v) = &self.sasl_mechanism {
            m.insert("sink.kafka.sasl.mechanism".into(), v.clone());
        }
        if let Some(v) = &self.sasl_username {
            m.insert("sink.kafka.sasl.username".into(), v.clone());
        }
        if let Some(v) = &self.sasl_password {
            m.insert("sink.kafka.sasl.password".into(), v.clone());
        }
        if let Some(v) = &self.ssl_ca_location {
            m.insert("sink.kafka.ssl.ca.location".into(), v.clone());
        }
        if let Some(v) = &self.ssl_truststore_password {
            m.insert("sink.kafka.ssl.truststore.password".into(), v.clone());
        }
        if let Some(v) = &self.ssl_certificate_location {
            m.insert("sink.kafka.ssl.certificate.location".into(), v.clone());
        }
        if let Some(v) = &self.ssl_key_location {
            m.insert("sink.kafka.ssl.key.location".into(), v.clone());
        }
        if let Some(v) = &self.ssl_key_password {
            m.insert("sink.kafka.ssl.key.password".into(), v.clone());
        }
        if let Some(v) = &self.ssl_keystore_location {
            m.insert("sink.kafka.ssl.keystore.location".into(), v.clone());
        }
        if let Some(v) = &self.ssl_keystore_password {
            m.insert("sink.kafka.ssl.keystore.password".into(), v.clone());
        }
        m
    }

    /// Ordered pairs for SHOW CREATE (omit secrets unless set; always emit semantic defaults).
    pub fn ddl_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        if let Some(v) = &self.key_format {
            pairs.push(("sink.kafka.key.format".into(), v.clone()));
        }
        if !self.key_fields.is_empty() {
            pairs.push(("sink.kafka.key.fields".into(), self.key_fields.join(",")));
        }
        if !self.key_fields_prefix.is_empty() {
            pairs.push((
                "sink.kafka.key.fields-prefix".into(),
                self.key_fields_prefix.clone(),
            ));
        }
        pairs.push((
            "sink.kafka.partitioner".into(),
            self.partitioner.as_str().into(),
        ));
        pairs.push((
            "sink.kafka.delivery-guarantee".into(),
            self.delivery_guarantee.as_str().into(),
        ));
        if let Some(v) = &self.transactional_id {
            pairs.push(("sink.kafka.transactional.id".into(), v.clone()));
        }
        if let Some(v) = self.transaction_timeout_ms {
            pairs.push(("sink.kafka.transaction.timeout.ms".into(), v.to_string()));
        }
        if let Some(v) = &self.compression_type {
            pairs.push(("sink.kafka.compression.type".into(), v.clone()));
        }
        if let Some(v) = self.batch_size {
            pairs.push(("sink.kafka.batch.size".into(), v.to_string()));
        }
        if let Some(v) = self.linger_ms {
            pairs.push(("sink.kafka.linger.ms".into(), v.to_string()));
        }
        if let Some(v) = &self.acks {
            pairs.push(("sink.kafka.acks".into(), v.clone()));
        }
        if let Some(v) = self.retries {
            pairs.push(("sink.kafka.retries".into(), v.to_string()));
        }
        if let Some(v) = &self.security_protocol {
            pairs.push(("sink.kafka.security.protocol".into(), v.clone()));
        }
        if let Some(v) = &self.sasl_mechanism {
            pairs.push(("sink.kafka.sasl.mechanism".into(), v.clone()));
        }
        if let Some(v) = &self.sasl_username {
            pairs.push(("sink.kafka.sasl.username".into(), v.clone()));
        }
        if self.sasl_password.is_some() {
            pairs.push(("sink.kafka.sasl.password".into(), "***".into()));
        }
        if let Some(v) = &self.ssl_ca_location {
            pairs.push(("sink.kafka.ssl.ca.location".into(), v.clone()));
        }
        if let Some(v) = &self.ssl_certificate_location {
            pairs.push(("sink.kafka.ssl.certificate.location".into(), v.clone()));
        }
        if let Some(v) = &self.ssl_key_location {
            pairs.push(("sink.kafka.ssl.key.location".into(), v.clone()));
        }
        if let Some(v) = &self.ssl_keystore_location {
            pairs.push(("sink.kafka.ssl.keystore.location".into(), v.clone()));
        }
        pairs
    }

    /// Apply options onto an rdkafka [`ClientConfig`] builder map (key → value).
    pub fn apply_client_config(&self, set: &mut dyn FnMut(&str, &str)) {
        let acks = self.acks.as_deref().unwrap_or("all");
        set("acks", acks);
        set("enable.idempotence", "true");
        set("metadata.request.timeout.ms", "5000");
        set("max.in.flight.requests.per.connection", "5");

        if let Some(v) = &self.compression_type {
            set("compression.type", v);
        }
        if let Some(v) = self.batch_size {
            set("batch.size", &v.to_string());
        }
        if let Some(v) = self.linger_ms {
            set("linger.ms", &v.to_string());
        }
        if let Some(v) = self.retries {
            set("retries", &v.to_string());
        }

        match self.partitioner {
            KafkaPartitioner::Default => {}
            KafkaPartitioner::RoundRobin => set("partitioner", "round_robin"),
            KafkaPartitioner::Fixed => {
                // Partition forced per-record; keep default partitioner.
            }
        }

        if self.delivery_guarantee == KafkaDeliveryGuarantee::ExactlyOnce {
            let timeout = self.transaction_timeout_ms.unwrap_or(900_000);
            set("transaction.timeout.ms", &timeout.to_string());
        } else if let Some(v) = self.transaction_timeout_ms {
            set("transaction.timeout.ms", &v.to_string());
        }

        if let Some(v) = &self.security_protocol {
            set("security.protocol", v);
        }
        if let Some(v) = &self.sasl_mechanism {
            set("sasl.mechanisms", v);
        }
        if let Some(v) = &self.sasl_username {
            set("sasl.username", v);
        }
        if let Some(v) = &self.sasl_password {
            set("sasl.password", v);
        }
        if let Some(v) = &self.ssl_ca_location {
            set("ssl.ca.location", v);
        }
        if let Some(v) = &self.ssl_certificate_location {
            set("ssl.certificate.location", v);
        }
        if let Some(v) = &self.ssl_key_location {
            set("ssl.key.location", v);
        }
        if let Some(v) = &self.ssl_key_password {
            set("ssl.key.password", v);
        }
        if let Some(v) = &self.ssl_keystore_location {
            set("ssl.keystore.location", v);
        }
        if let Some(v) = &self.ssl_keystore_password {
            set("ssl.keystore.password", v);
        }
        // truststore.password has no direct librdkafka PEM equivalent; ignored if only password set.
        let _ = &self.ssl_truststore_password;
    }
}

fn reject_unknown_kafka_option_keys(options: &HashMap<String, String>) -> Result<()> {
    let mut unknown = Vec::new();
    for k in options.keys() {
        if !k.starts_with("sink.kafka.") {
            continue;
        }
        if k == "sink.kafka.brokers" || k == "sink.kafka.topic" {
            continue;
        }
        if !KAFKA_OPTION_KEYS.contains(&k.as_str()) {
            unknown.push(k.as_str());
        }
    }
    if unknown.is_empty() {
        return Ok(());
    }
    unknown.sort_unstable();
    Err(TsdbError::Query(format!(
        "unsupported kafka options {} (supported: {})",
        unknown.join(", "),
        KAFKA_OPTION_KEYS.join(", ")
    )))
}

/// Parse Flink-style JAAS: `... username="u" password="p";`
pub fn parse_jaas_credentials(jaas: &str) -> Result<(String, String)> {
    let user = extract_jaas_quoted(jaas, "username").ok_or_else(|| {
        TsdbError::Query(
            "sink.kafka.sasl.jaas.config missing username=\"...\" (or set sink.kafka.sasl.username)"
                .into(),
        )
    })?;
    let pass = extract_jaas_quoted(jaas, "password").ok_or_else(|| {
        TsdbError::Query(
            "sink.kafka.sasl.jaas.config missing password=\"...\" (or set sink.kafka.sasl.password)"
                .into(),
        )
    })?;
    Ok((user, pass))
}

fn extract_jaas_quoted(jaas: &str, key: &str) -> Option<String> {
    let needle = format!("{key}=");
    let idx = jaas.find(&needle)?;
    let rest = &jaas[idx + needle.len()..];
    let rest = rest.trim_start();
    let quote = rest.chars().next()?;
    if quote != '"' && quote != '\'' {
        // unquoted token until whitespace or ;
        let end = rest
            .find(|c: char| c.is_whitespace() || c == ';')
            .unwrap_or(rest.len());
        return Some(rest[..end].to_string());
    }
    let body = &rest[1..];
    let end = body.find(quote)?;
    Some(body[..end].to_string())
}

fn non_empty<'a>(options: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    options
        .get(key)
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
}

fn parse_u32(raw: &str, key: &str) -> Result<u32> {
    raw.trim()
        .parse()
        .map_err(|_| TsdbError::Query(format!("invalid u32 for {key}: {raw}")))
}

fn parse_u64(raw: &str, key: &str) -> Result<u64> {
    raw.trim()
        .parse()
        .map_err(|_| TsdbError::Query(format!("invalid u64 for {key}: {raw}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn parses_flink_style_tuning_and_security() {
        let o = KafkaSinkOptions::from_ddl(&opts(&[
            ("sink.kafka.key.format", "json"),
            ("sink.kafka.key.fields", "order_id, region"),
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
                r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="secret";"#,
            ),
            ("sink.kafka.ssl.truststore.location", "/certs/ca.pem"),
            ("sink.kafka.ssl.keystore.location", "/certs/client.p12"),
            ("sink.kafka.ssl.keystore.password", "kspass"),
        ]))
        .unwrap();

        assert_eq!(o.key_fields, vec!["order_id", "region"]);
        assert_eq!(o.key_fields_prefix, "k_");
        assert_eq!(o.delivery_guarantee, KafkaDeliveryGuarantee::ExactlyOnce);
        assert_eq!(o.transaction_timeout_ms, Some(900_000));
        assert_eq!(o.compression_type.as_deref(), Some("lz4"));
        assert_eq!(o.batch_size, Some(65536));
        assert_eq!(o.linger_ms, Some(20));
        assert_eq!(o.sasl_username.as_deref(), Some("admin"));
        assert_eq!(o.sasl_password.as_deref(), Some("secret"));
        assert_eq!(o.ssl_ca_location.as_deref(), Some("/certs/ca.pem"));
        assert_eq!(
            o.ssl_keystore_location.as_deref(),
            Some("/certs/client.p12")
        );
    }

    #[test]
    fn rejects_unknown_kafka_keys() {
        let err = KafkaSinkOptions::from_ddl(&opts(&[("sink.kafka.foo", "1")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("unsupported kafka options"), "{err}");
        assert!(err.contains("sink.kafka.foo"), "{err}");
    }

    #[test]
    fn jaas_parse_supports_quotes() {
        let (u, p) = parse_jaas_credentials(
            r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="a" password="b";"#,
        )
        .unwrap();
        assert_eq!(u, "a");
        assert_eq!(p, "b");
    }

    #[test]
    fn eos_rejects_non_all_acks() {
        let err = KafkaSinkOptions::from_ddl(&opts(&[
            ("sink.kafka.delivery-guarantee", "exactly-once"),
            ("sink.kafka.acks", "1"),
        ]))
        .unwrap_err()
        .to_string();
        assert!(err.contains("acks"), "{err}");
    }

    #[test]
    fn apply_client_config_sets_librdkafka_props() {
        let o = KafkaSinkOptions::from_ddl(&opts(&[
            ("sink.kafka.compression.type", "zstd"),
            ("sink.kafka.linger.ms", "5"),
            ("sink.kafka.partitioner", "round-robin"),
            ("sink.kafka.security.protocol", "SASL_SSL"),
            ("sink.kafka.sasl.mechanism", "SCRAM-SHA-256"),
            ("sink.kafka.sasl.username", "u"),
            ("sink.kafka.sasl.password", "p"),
        ]))
        .unwrap();
        let mut got = HashMap::new();
        o.apply_client_config(&mut |k, v| {
            got.insert(k.to_string(), v.to_string());
        });
        assert_eq!(
            got.get("compression.type").map(String::as_str),
            Some("zstd")
        );
        assert_eq!(got.get("linger.ms").map(String::as_str), Some("5"));
        assert_eq!(
            got.get("partitioner").map(String::as_str),
            Some("round_robin")
        );
        assert_eq!(
            got.get("sasl.mechanisms").map(String::as_str),
            Some("SCRAM-SHA-256")
        );
        assert_eq!(got.get("sasl.username").map(String::as_str), Some("u"));
    }
}
