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

//! MQTT sink options (`sink.mqtt.*`).
//!
//! DDL mirrors Flink-style MQTT connectors (prefixed with `sink.mqtt.`):
//! - required: `url` / `server-uri`, `topic`
//! - optional: `client-id`, `username`, `password`, `clean-session`
//! - protocol: `qos`, `retained`
//! - tuning: `connection-timeout`, `keep-alive-interval`, `max-inflight`

use std::collections::HashMap;
use std::time::Duration;

use common::{Result, TsdbError};

pub const MQTT_OPTION_PREFIX: &str = "sink.mqtt";

/// Optional MQTT DDL keys (excludes required url/server-uri + topic).
pub const MQTT_OPTION_KEYS: &[&str] = &[
    "sink.mqtt.client-id",
    "sink.mqtt.username",
    "sink.mqtt.password",
    "sink.mqtt.clean-session",
    "sink.mqtt.qos",
    "sink.mqtt.retained",
    "sink.mqtt.connection-timeout",
    "sink.mqtt.keep-alive-interval",
    "sink.mqtt.max-inflight",
];

/// Core required / alias keys used by foreign-sink rejection.
pub const MQTT_CORE_KEYS: &[&str] = &["sink.mqtt.url", "sink.mqtt.server-uri", "sink.mqtt.topic"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MqttQos {
    AtMostOnce = 0,
    #[default]
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl MqttQos {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim() {
            "0" => Ok(Self::AtMostOnce),
            "1" => Ok(Self::AtLeastOnce),
            "2" => Ok(Self::ExactlyOnce),
            other => Err(TsdbError::Query(format!(
                "invalid sink.mqtt.qos: {other} (use 0 | 1 | 2)"
            ))),
        }
    }

    pub fn as_u8(self) -> u8 {
        match self {
            Self::AtMostOnce => 0,
            Self::AtLeastOnce => 1,
            Self::ExactlyOnce => 2,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::AtMostOnce => "0",
            Self::AtLeastOnce => "1",
            Self::ExactlyOnce => "2",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttSinkOptions {
    /// Broker URL (`tcp://` / `mqtt://` / `ssl://` / `mqtts://` / `ws://` / `wss://`).
    pub url: String,
    pub topic: String,
    /// Empty means generate `monots-<uuid>` at connect time.
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub clean_session: bool,
    pub qos: MqttQos,
    pub retained: bool,
    pub connection_timeout: Duration,
    pub keep_alive_interval: Duration,
    pub max_inflight: u16,
}

impl Default for MqttSinkOptions {
    fn default() -> Self {
        Self {
            url: String::new(),
            topic: String::new(),
            client_id: None,
            username: None,
            password: None,
            clean_session: true,
            qos: MqttQos::AtLeastOnce,
            retained: false,
            connection_timeout: Duration::from_secs(30),
            keep_alive_interval: Duration::from_secs(60),
            max_inflight: 1000,
        }
    }
}

impl MqttSinkOptions {
    pub fn from_ddl(options: &HashMap<String, String>) -> Result<Self> {
        reject_unknown_mqtt_option_keys(options)?;

        let url = required_url(options)?;
        let topic = required(options, "topic")?;

        let mut opts = Self {
            url,
            topic,
            ..Self::default()
        };

        if let Some(v) = non_empty(options, "sink.mqtt.client-id") {
            opts.client_id = Some(v.to_string());
        }
        if let Some(v) = non_empty(options, "sink.mqtt.username") {
            opts.username = Some(v.to_string());
        }
        if let Some(v) = options.get("sink.mqtt.password") {
            // Allow empty password when username is set.
            opts.password = Some(v.clone());
        }
        if let Some(v) = non_empty(options, "sink.mqtt.clean-session") {
            opts.clean_session = parse_bool(v, "sink.mqtt.clean-session")?;
        }
        if let Some(v) = non_empty(options, "sink.mqtt.qos") {
            opts.qos = MqttQos::parse(v)?;
        }
        if let Some(v) = non_empty(options, "sink.mqtt.retained") {
            opts.retained = parse_bool(v, "sink.mqtt.retained")?;
        }
        if let Some(v) = non_empty(options, "sink.mqtt.connection-timeout") {
            opts.connection_timeout = parse_seconds(v, "sink.mqtt.connection-timeout")?;
        }
        if let Some(v) = non_empty(options, "sink.mqtt.keep-alive-interval") {
            opts.keep_alive_interval = parse_seconds(v, "sink.mqtt.keep-alive-interval")?;
        }
        if let Some(v) = non_empty(options, "sink.mqtt.max-inflight") {
            opts.max_inflight = parse_u16(v, "sink.mqtt.max-inflight")?;
            if opts.max_inflight == 0 {
                return Err(TsdbError::Query(
                    "sink.mqtt.max-inflight must be >= 1".into(),
                ));
            }
        }

        opts.validate()?;
        Ok(opts)
    }

    pub fn from_properties(props: &HashMap<String, String>) -> Result<Self> {
        Self::from_ddl(props)
    }

    fn validate(&self) -> Result<()> {
        if self.url.is_empty() {
            return Err(TsdbError::Query(
                "mqtt sink requires sink.mqtt.url or sink.mqtt.server-uri".into(),
            ));
        }
        if self.topic.is_empty() {
            return Err(TsdbError::Query(
                "mqtt sink requires sink.mqtt.topic".into(),
            ));
        }
        if self.password.is_some() && self.username.is_none() {
            return Err(TsdbError::Query(
                "sink.mqtt.password requires sink.mqtt.username".into(),
            ));
        }
        if !self.clean_session
            && self
                .client_id
                .as_ref()
                .map(|s| s.is_empty())
                .unwrap_or(true)
        {
            return Err(TsdbError::Query(
                "sink.mqtt.clean-session = false requires sink.mqtt.client-id".into(),
            ));
        }
        Ok(())
    }

    pub fn to_properties(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("sink.mqtt.url".into(), self.url.clone());
        m.insert("sink.mqtt.topic".into(), self.topic.clone());
        if let Some(v) = &self.client_id {
            m.insert("sink.mqtt.client-id".into(), v.clone());
        }
        if let Some(v) = &self.username {
            m.insert("sink.mqtt.username".into(), v.clone());
        }
        if let Some(v) = &self.password {
            m.insert("sink.mqtt.password".into(), v.clone());
        }
        m.insert(
            "sink.mqtt.clean-session".into(),
            self.clean_session.to_string(),
        );
        m.insert("sink.mqtt.qos".into(), self.qos.as_str().into());
        m.insert("sink.mqtt.retained".into(), self.retained.to_string());
        m.insert(
            "sink.mqtt.connection-timeout".into(),
            self.connection_timeout.as_secs().to_string(),
        );
        m.insert(
            "sink.mqtt.keep-alive-interval".into(),
            self.keep_alive_interval.as_secs().to_string(),
        );
        m.insert(
            "sink.mqtt.max-inflight".into(),
            self.max_inflight.to_string(),
        );
        m
    }

    pub fn ddl_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = vec![
            ("sink.mqtt.url".into(), self.url.clone()),
            ("sink.mqtt.topic".into(), self.topic.clone()),
            ("sink.mqtt.qos".into(), self.qos.as_str().into()),
            (
                "sink.mqtt.clean-session".into(),
                self.clean_session.to_string(),
            ),
            ("sink.mqtt.retained".into(), self.retained.to_string()),
            (
                "sink.mqtt.connection-timeout".into(),
                self.connection_timeout.as_secs().to_string(),
            ),
            (
                "sink.mqtt.keep-alive-interval".into(),
                self.keep_alive_interval.as_secs().to_string(),
            ),
            (
                "sink.mqtt.max-inflight".into(),
                self.max_inflight.to_string(),
            ),
        ];
        if let Some(v) = &self.client_id {
            pairs.push(("sink.mqtt.client-id".into(), v.clone()));
        }
        if let Some(v) = &self.username {
            pairs.push(("sink.mqtt.username".into(), v.clone()));
        }
        if self.password.is_some() {
            pairs.push(("sink.mqtt.password".into(), "***".into()));
        }
        pairs
    }
}

fn reject_unknown_mqtt_option_keys(options: &HashMap<String, String>) -> Result<()> {
    let mut unknown = Vec::new();
    for key in options.keys() {
        if !key.starts_with("sink.mqtt.") {
            continue;
        }
        let known = key == "sink.mqtt.url"
            || key == "sink.mqtt.server-uri"
            || key == "sink.mqtt.topic"
            || MQTT_OPTION_KEYS.contains(&key.as_str());
        if !known {
            unknown.push(key.clone());
        }
    }
    if unknown.is_empty() {
        return Ok(());
    }
    unknown.sort();
    Err(TsdbError::Query(format!(
        "unknown mqtt sink options: {} (supported under sink.mqtt.*)",
        unknown.join(", ")
    )))
}

fn required_url(options: &HashMap<String, String>) -> Result<String> {
    if let Some(v) = non_empty(options, "sink.mqtt.url") {
        return Ok(v.to_string());
    }
    if let Some(v) = non_empty(options, "sink.mqtt.server-uri") {
        return Ok(v.to_string());
    }
    Err(TsdbError::Query(
        "mqtt sink requires sink.mqtt.url or sink.mqtt.server-uri".into(),
    ))
}

fn required(options: &HashMap<String, String>, leaf: &str) -> Result<String> {
    let key = format!("sink.mqtt.{leaf}");
    non_empty(options, &key)
        .map(|s| s.to_string())
        .ok_or_else(|| TsdbError::Query(format!("mqtt sink requires {key}")))
}

fn non_empty<'a>(options: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    options.get(key).map(|s| s.trim()).filter(|s| !s.is_empty())
}

fn parse_bool(raw: &str, key: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(TsdbError::Query(format!(
            "invalid {key}: {other} (use true | false)"
        ))),
    }
}

fn parse_seconds(raw: &str, key: &str) -> Result<Duration> {
    let s = raw.trim();
    let secs: u64 = s.parse().map_err(|_| {
        TsdbError::Query(format!(
            "invalid {key}: {raw} (expected seconds as integer)"
        ))
    })?;
    if secs == 0 {
        return Err(TsdbError::Query(format!("{key} must be >= 1")));
    }
    Ok(Duration::from_secs(secs))
}

fn parse_u16(raw: &str, key: &str) -> Result<u16> {
    raw.trim()
        .parse()
        .map_err(|_| TsdbError::Query(format!("invalid {key}: {raw} (expected integer 1..=65535)")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("sink.mqtt.url".into(), "tcp://127.0.0.1:1883".into());
        m.insert("sink.mqtt.topic".into(), "sensor/temp/out".into());
        m
    }

    #[test]
    fn parses_defaults() {
        let o = MqttSinkOptions::from_ddl(&base()).expect("parse");
        assert_eq!(o.url, "tcp://127.0.0.1:1883");
        assert_eq!(o.topic, "sensor/temp/out");
        assert!(o.clean_session);
        assert_eq!(o.qos, MqttQos::AtLeastOnce);
        assert!(!o.retained);
        assert_eq!(o.connection_timeout, Duration::from_secs(30));
        assert_eq!(o.keep_alive_interval, Duration::from_secs(60));
        assert_eq!(o.max_inflight, 1000);
    }

    #[test]
    fn accepts_server_uri_alias() {
        let mut m = HashMap::new();
        m.insert(
            "sink.mqtt.server-uri".into(),
            "mqtt://broker.emqx.io:1883".into(),
        );
        m.insert("sink.mqtt.topic".into(), "t".into());
        m.insert("sink.mqtt.qos".into(), "2".into());
        m.insert("sink.mqtt.username".into(), "admin".into());
        m.insert("sink.mqtt.password".into(), "secret".into());
        let o = MqttSinkOptions::from_ddl(&m).expect("parse");
        assert_eq!(o.url, "mqtt://broker.emqx.io:1883");
        assert_eq!(o.qos, MqttQos::ExactlyOnce);
        assert_eq!(o.username.as_deref(), Some("admin"));
    }

    #[test]
    fn rejects_unknown_keys() {
        let mut m = base();
        m.insert("sink.mqtt.foo".into(), "x".into());
        assert!(MqttSinkOptions::from_ddl(&m).is_err());
    }
}
