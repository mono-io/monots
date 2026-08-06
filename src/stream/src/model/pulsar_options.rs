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

//! Pulsar sink options (`sink.pulsar.*`).
//!
//! DDL mirrors the Flink Pulsar connector (prefixed with `sink.pulsar.`):
//! - required: `topic`, `service-url`, `admin-url`
//! - delivery: `delivery-guarantee`, `transaction-timeout`
//! - routing: `message-router`, `custom-message-router`
//! - keys: `key.format` / `key.fields` / `key.fields-prefix`
//! - auth: `auth-plugin`, `auth-params`

use std::collections::HashMap;
use std::time::Duration;

use common::{Result, TsdbError};

pub const PULSAR_OPTION_PREFIX: &str = "sink.pulsar";

/// Optional Pulsar DDL keys (excludes required topic / service-url / admin-url).
pub const PULSAR_OPTION_KEYS: &[&str] = &[
    "sink.pulsar.delivery-guarantee",
    "sink.pulsar.transaction-timeout",
    "sink.pulsar.message-router",
    "sink.pulsar.custom-message-router",
    "sink.pulsar.key.format",
    "sink.pulsar.key.fields",
    "sink.pulsar.key.fields-prefix",
    "sink.pulsar.auth-plugin",
    "sink.pulsar.auth-params",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PulsarDeliveryGuarantee {
    /// Enqueue without waiting for broker acknowledgement.
    None,
    /// Wait for broker send receipt (default).
    #[default]
    AtLeastOnce,
    /// Pulsar transactions (Flink EOS). Not available in the Rust client yet.
    ExactlyOnce,
}

impl PulsarDeliveryGuarantee {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "none" => Ok(Self::None),
            "at-least-once" | "at_least_once" | "alo" => Ok(Self::AtLeastOnce),
            "exactly-once" | "exactly_once" | "eos" => Ok(Self::ExactlyOnce),
            other => Err(TsdbError::Query(format!(
                "invalid sink.pulsar.delivery-guarantee: {other} \
                 (use none | at-least-once | exactly-once)"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::AtLeastOnce => "at-least-once",
            Self::ExactlyOnce => "exactly-once",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PulsarMessageRouter {
    #[default]
    RoundRobin,
    Single,
    KeyHash,
}

impl PulsarMessageRouter {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "round-robin" | "round_robin" => Ok(Self::RoundRobin),
            "single" => Ok(Self::Single),
            "key-hash" | "key_hash" => Ok(Self::KeyHash),
            "custom" => Err(TsdbError::Query(
                "sink.pulsar.message-router = custom is not supported \
                 (Java router classes cannot run in MonoTS; use round-robin | single | key-hash)"
                    .into(),
            )),
            other => Err(TsdbError::Query(format!(
                "invalid sink.pulsar.message-router: {other} \
                 (use round-robin | single | key-hash)"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::RoundRobin => "round-robin",
            Self::Single => "single",
            Self::KeyHash => "key-hash",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PulsarSinkOptions {
    pub topic: String,
    pub service_url: String,
    pub admin_url: String,
    pub delivery_guarantee: PulsarDeliveryGuarantee,
    /// Flink-style duration (`3 h`, `15m`) or milliseconds as a bare integer.
    pub transaction_timeout: Option<Duration>,
    pub message_router: PulsarMessageRouter,
    pub key_format: Option<String>,
    pub key_fields: Vec<String>,
    pub key_fields_prefix: String,
    pub auth_plugin: Option<String>,
    pub auth_params: Option<String>,
}

impl Default for PulsarSinkOptions {
    fn default() -> Self {
        Self {
            topic: String::new(),
            service_url: String::new(),
            admin_url: String::new(),
            delivery_guarantee: PulsarDeliveryGuarantee::AtLeastOnce,
            transaction_timeout: None,
            message_router: PulsarMessageRouter::RoundRobin,
            key_format: None,
            key_fields: Vec::new(),
            key_fields_prefix: String::new(),
            auth_plugin: None,
            auth_params: None,
        }
    }
}

impl PulsarSinkOptions {
    pub fn from_ddl(options: &HashMap<String, String>) -> Result<Self> {
        reject_unknown_pulsar_option_keys(options)?;

        let topic = required(options, "topic")?;
        let service_url = required(options, "service-url")?;
        let admin_url = required(options, "admin-url")?;

        let mut opts = Self {
            topic,
            service_url,
            admin_url,
            ..Self::default()
        };

        if let Some(v) = non_empty(options, "sink.pulsar.delivery-guarantee") {
            opts.delivery_guarantee = PulsarDeliveryGuarantee::parse(v)?;
        }
        if let Some(v) = non_empty(options, "sink.pulsar.transaction-timeout") {
            opts.transaction_timeout = Some(parse_duration(v, "sink.pulsar.transaction-timeout")?);
        }
        if let Some(v) = non_empty(options, "sink.pulsar.message-router") {
            opts.message_router = PulsarMessageRouter::parse(v)?;
        }
        if non_empty(options, "sink.pulsar.custom-message-router").is_some() {
            return Err(TsdbError::Query(
                "sink.pulsar.custom-message-router is not supported \
                 (Java router classes cannot run in MonoTS)"
                    .into(),
            ));
        }
        if let Some(v) = non_empty(options, "sink.pulsar.key.format") {
            let fmt = v.to_ascii_lowercase();
            if fmt != "json" {
                return Err(TsdbError::Query(format!(
                    "unsupported sink.pulsar.key.format: {v} (only json)"
                )));
            }
            opts.key_format = Some("json".into());
        }
        if let Some(v) = non_empty(options, "sink.pulsar.key.fields") {
            opts.key_fields = v
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
        if let Some(v) = options.get("sink.pulsar.key.fields-prefix") {
            opts.key_fields_prefix = v.clone();
        }
        if let Some(v) = non_empty(options, "sink.pulsar.auth-plugin") {
            opts.auth_plugin = Some(v.to_string());
        }
        if let Some(v) = non_empty(options, "sink.pulsar.auth-params") {
            opts.auth_params = Some(v.to_string());
        }

        opts.validate()?;
        Ok(opts)
    }

    pub fn from_properties(props: &HashMap<String, String>) -> Result<Self> {
        Self::from_ddl(props)
    }

    fn validate(&self) -> Result<()> {
        if self.topic.is_empty() {
            return Err(TsdbError::Query(
                "pulsar sink requires sink.pulsar.topic".into(),
            ));
        }
        if self.service_url.is_empty() {
            return Err(TsdbError::Query(
                "pulsar sink requires sink.pulsar.service-url".into(),
            ));
        }
        if self.admin_url.is_empty() {
            return Err(TsdbError::Query(
                "pulsar sink requires sink.pulsar.admin-url".into(),
            ));
        }
        if !self.key_fields.is_empty() {
            let fmt = self.key_format.as_deref().unwrap_or("json");
            if fmt != "json" {
                return Err(TsdbError::Query(
                    "sink.pulsar.key.fields requires sink.pulsar.key.format = json".into(),
                ));
            }
        }
        if self.message_router == PulsarMessageRouter::KeyHash && self.key_fields.is_empty() {
            return Err(TsdbError::Query(
                "sink.pulsar.message-router = key-hash requires sink.pulsar.key.fields".into(),
            ));
        }
        if self.delivery_guarantee == PulsarDeliveryGuarantee::ExactlyOnce {
            return Err(TsdbError::Query(
                "sink.pulsar.delivery-guarantee = exactly-once is not available yet: \
                 the Rust Pulsar client has no Transaction API \
                 (use at-least-once or none; Flink EOS requires transactionCoordinatorEnabled)"
                    .into(),
            ));
        }
        if let Some(plugin) = &self.auth_plugin {
            let _ = resolve_auth_kind(plugin, self.auth_params.as_deref())?;
        } else if self.auth_params.is_some() {
            return Err(TsdbError::Query(
                "sink.pulsar.auth-params requires sink.pulsar.auth-plugin".into(),
            ));
        }
        Ok(())
    }

    pub fn has_key(&self) -> bool {
        !self.key_fields.is_empty()
    }

    /// Resolved token string when Token auth is configured.
    pub fn auth_token(&self) -> Result<Option<String>> {
        match (&self.auth_plugin, &self.auth_params) {
            (None, _) => Ok(None),
            (Some(plugin), params) => match resolve_auth_kind(plugin, params.as_deref())? {
                AuthKind::Token(token) => Ok(Some(token)),
                AuthKind::Unsupported(name) => Err(TsdbError::Query(format!(
                    "unsupported sink.pulsar.auth-plugin: {name} \
                     (supported: AuthenticationToken / token)"
                ))),
            },
        }
    }

    pub fn to_properties(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("sink.pulsar.topic".into(), self.topic.clone());
        m.insert("sink.pulsar.service-url".into(), self.service_url.clone());
        m.insert("sink.pulsar.admin-url".into(), self.admin_url.clone());
        m.insert(
            "sink.pulsar.delivery-guarantee".into(),
            self.delivery_guarantee.as_str().into(),
        );
        if let Some(d) = self.transaction_timeout {
            m.insert(
                "sink.pulsar.transaction-timeout".into(),
                format_duration_ms(d),
            );
        }
        m.insert(
            "sink.pulsar.message-router".into(),
            self.message_router.as_str().into(),
        );
        if let Some(v) = &self.key_format {
            m.insert("sink.pulsar.key.format".into(), v.clone());
        }
        if !self.key_fields.is_empty() {
            m.insert("sink.pulsar.key.fields".into(), self.key_fields.join(","));
        }
        if !self.key_fields_prefix.is_empty() {
            m.insert(
                "sink.pulsar.key.fields-prefix".into(),
                self.key_fields_prefix.clone(),
            );
        }
        if let Some(v) = &self.auth_plugin {
            m.insert("sink.pulsar.auth-plugin".into(), v.clone());
        }
        if let Some(v) = &self.auth_params {
            m.insert("sink.pulsar.auth-params".into(), v.clone());
        }
        m
    }

    pub fn ddl_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = vec![
            ("sink.pulsar.topic".into(), self.topic.clone()),
            ("sink.pulsar.service-url".into(), self.service_url.clone()),
            ("sink.pulsar.admin-url".into(), self.admin_url.clone()),
            (
                "sink.pulsar.delivery-guarantee".into(),
                self.delivery_guarantee.as_str().into(),
            ),
            (
                "sink.pulsar.message-router".into(),
                self.message_router.as_str().into(),
            ),
        ];
        if let Some(d) = self.transaction_timeout {
            pairs.push((
                "sink.pulsar.transaction-timeout".into(),
                format_duration_ms(d),
            ));
        }
        if let Some(v) = &self.key_format {
            pairs.push(("sink.pulsar.key.format".into(), v.clone()));
        }
        if !self.key_fields.is_empty() {
            pairs.push(("sink.pulsar.key.fields".into(), self.key_fields.join(",")));
        }
        if !self.key_fields_prefix.is_empty() {
            pairs.push((
                "sink.pulsar.key.fields-prefix".into(),
                self.key_fields_prefix.clone(),
            ));
        }
        if let Some(v) = &self.auth_plugin {
            pairs.push(("sink.pulsar.auth-plugin".into(), v.clone()));
        }
        if self.auth_params.is_some() {
            pairs.push(("sink.pulsar.auth-params".into(), "***".into()));
        }
        pairs
    }
}

enum AuthKind {
    Token(String),
    Unsupported(String),
}

fn resolve_auth_kind(plugin: &str, params: Option<&str>) -> Result<AuthKind> {
    let lower = plugin.to_ascii_lowercase();
    let is_token = lower == "token"
        || lower.ends_with("authenticationtoken")
        || lower.contains("authenticationtoken");
    if !is_token {
        return Ok(AuthKind::Unsupported(plugin.to_string()));
    }
    let raw = params.ok_or_else(|| {
        TsdbError::Query(
            "sink.pulsar.auth-plugin AuthenticationToken requires sink.pulsar.auth-params \
             (e.g. token:<jwt> or the raw token string)"
                .into(),
        )
    })?;
    Ok(AuthKind::Token(parse_token_params(raw)))
}

fn parse_token_params(raw: &str) -> String {
    let trimmed = raw.trim();
    if let Some(rest) = trimmed.strip_prefix("token:") {
        return rest.trim().to_string();
    }
    // Flink sometimes uses `token:xxx` or JSON `{"token":"..."}`.
    if trimmed.starts_with('{') {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
            if let Some(t) = v.get("token").and_then(|x| x.as_str()) {
                return t.to_string();
            }
        }
    }
    trimmed.to_string()
}

fn reject_unknown_pulsar_option_keys(options: &HashMap<String, String>) -> Result<()> {
    let mut unknown = Vec::new();
    for k in options.keys() {
        if !k.starts_with("sink.pulsar.") {
            continue;
        }
        if matches!(
            k.as_str(),
            "sink.pulsar.topic" | "sink.pulsar.service-url" | "sink.pulsar.admin-url"
        ) {
            continue;
        }
        if !PULSAR_OPTION_KEYS.contains(&k.as_str()) {
            unknown.push(k.as_str());
        }
    }
    if unknown.is_empty() {
        return Ok(());
    }
    unknown.sort_unstable();
    Err(TsdbError::Query(format!(
        "unsupported pulsar options {} (supported: {})",
        unknown.join(", "),
        PULSAR_OPTION_KEYS.join(", ")
    )))
}

fn required(options: &HashMap<String, String>, short: &str) -> Result<String> {
    let key = format!("sink.pulsar.{short}");
    non_empty(options, &key)
        .map(|s| s.to_string())
        .ok_or_else(|| TsdbError::Query(format!("pulsar sink requires {key}")))
}

fn non_empty<'a>(options: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    options
        .get(key)
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
}

fn format_duration_ms(d: Duration) -> String {
    format!("{}ms", d.as_millis())
}

/// Parse Flink-style durations (`3 h`, `15m`, `30s`) or a bare millisecond integer.
pub fn parse_duration(raw: &str, key: &str) -> Result<Duration> {
    let s = raw.trim().to_ascii_lowercase();
    if s.is_empty() {
        return Err(TsdbError::Query(format!("empty duration for {key}")));
    }
    // Bare integer → milliseconds (Kafka-style).
    if s.chars().all(|c| c.is_ascii_digit()) {
        let ms: u64 = s
            .parse()
            .map_err(|_| TsdbError::Query(format!("invalid duration for {key}: {raw}")))?;
        return Ok(Duration::from_millis(ms));
    }

    let (num_str, unit) = split_duration(&s).ok_or_else(|| {
        TsdbError::Query(format!(
            "invalid sink.pulsar.transaction-timeout: {raw} \
             (use e.g. '3 h', '15m', '30s', or milliseconds)"
        ))
    })?;
    let num: f64 = num_str
        .parse()
        .map_err(|_| TsdbError::Query(format!("invalid duration number for {key}: {raw}")))?;
    if num < 0.0 {
        return Err(TsdbError::Query(format!(
            "duration for {key} must be non-negative"
        )));
    }
    let secs = match unit {
        "ms" | "millis" | "millisecond" | "milliseconds" => num / 1000.0,
        "s" | "sec" | "secs" | "second" | "seconds" => num,
        "m" | "min" | "mins" | "minute" | "minutes" => num * 60.0,
        "h" | "hr" | "hrs" | "hour" | "hours" => num * 3600.0,
        "d" | "day" | "days" => num * 86400.0,
        other => {
            return Err(TsdbError::Query(format!(
                "unknown duration unit `{other}` for {key}"
            )))
        }
    };
    Ok(Duration::from_secs_f64(secs))
}

fn split_duration(s: &str) -> Option<(&str, &str)> {
    let s = s.trim();
    let digit_end = s
        .char_indices()
        .find(|(_, c)| !(c.is_ascii_digit() || *c == '.'))
        .map(|(i, _)| i)
        .unwrap_or(s.len());
    if digit_end == 0 {
        return None;
    }
    let num = s[..digit_end].trim();
    let unit = s[digit_end..].trim();
    if num.is_empty() || unit.is_empty() {
        return None;
    }
    Some((num, unit))
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
    fn parses_required_and_defaults() {
        let o = PulsarSinkOptions::from_ddl(&opts(&[
            ("sink.pulsar.topic", "persistent://public/default/t"),
            ("sink.pulsar.service-url", "pulsar://localhost:6650"),
            ("sink.pulsar.admin-url", "http://localhost:8080"),
        ]))
        .unwrap();
        assert_eq!(o.delivery_guarantee, PulsarDeliveryGuarantee::AtLeastOnce);
        assert_eq!(o.message_router, PulsarMessageRouter::RoundRobin);
        assert!(!o.has_key());
    }

    #[test]
    fn parses_flink_style_options() {
        let o = PulsarSinkOptions::from_ddl(&opts(&[
            ("sink.pulsar.topic", "persistent://public/default/t"),
            ("sink.pulsar.service-url", "pulsar://localhost:6650"),
            ("sink.pulsar.admin-url", "http://localhost:8080"),
            ("sink.pulsar.delivery-guarantee", "none"),
            ("sink.pulsar.transaction-timeout", "3 h"),
            ("sink.pulsar.message-router", "key-hash"),
            ("sink.pulsar.key.format", "json"),
            ("sink.pulsar.key.fields", "order_id"),
            ("sink.pulsar.key.fields-prefix", "k_"),
            (
                "sink.pulsar.auth-plugin",
                "org.apache.pulsar.client.impl.auth.AuthenticationToken",
            ),
            ("sink.pulsar.auth-params", "token:abc.def"),
        ]))
        .unwrap();
        assert_eq!(o.delivery_guarantee, PulsarDeliveryGuarantee::None);
        assert_eq!(o.transaction_timeout, Some(Duration::from_secs(3 * 3600)));
        assert_eq!(o.message_router, PulsarMessageRouter::KeyHash);
        assert_eq!(o.auth_token().unwrap().as_deref(), Some("abc.def"));
    }

    #[test]
    fn rejects_exactly_once() {
        let err = PulsarSinkOptions::from_ddl(&opts(&[
            ("sink.pulsar.topic", "t"),
            ("sink.pulsar.service-url", "pulsar://localhost:6650"),
            ("sink.pulsar.admin-url", "http://localhost:8080"),
            ("sink.pulsar.delivery-guarantee", "exactly-once"),
        ]))
        .unwrap_err();
        assert!(err.to_string().contains("exactly-once"), "{err}");
    }

    #[test]
    fn rejects_custom_router() {
        let err = PulsarSinkOptions::from_ddl(&opts(&[
            ("sink.pulsar.topic", "t"),
            ("sink.pulsar.service-url", "pulsar://localhost:6650"),
            ("sink.pulsar.admin-url", "http://localhost:8080"),
            ("sink.pulsar.message-router", "custom"),
        ]))
        .unwrap_err();
        assert!(err.to_string().contains("custom"), "{err}");
    }

    #[test]
    fn parse_duration_variants() {
        assert_eq!(
            parse_duration("15m", "k").unwrap(),
            Duration::from_secs(900)
        );
        assert_eq!(
            parse_duration("900000", "k").unwrap(),
            Duration::from_millis(900_000)
        );
        assert_eq!(parse_duration("30 s", "k").unwrap(), Duration::from_secs(30));
    }
}
