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

//! Default Delta sink options that are wired into the S3 / object-store client.
//!
//! Omitting a key always fills the default — CREATE / SHOW CREATE materializes
//! the full set. Only knobs that affect runtime I/O belong here.

use std::collections::HashMap;

use common::{Result, TsdbError};

/// Default AWS / S3-compatible region when neither DDL nor env sets one.
pub const DEFAULT_REGION: &str = "us-east-1";
/// Max concurrent object-store connections (≈ Flink `fs.s3a.connection.maximum`).
pub const DEFAULT_CONNECTION_MAXIMUM: u32 = 500;
/// Connect timeout in milliseconds (≈ Flink `fs.s3a.connection.timeout`).
pub const DEFAULT_CONNECTION_TIMEOUT_MS: u64 = 200_000;
/// Request retry budget (≈ Flink `fs.s3a.attempts.maximum`).
pub const DEFAULT_ATTEMPTS_MAXIMUM: u32 = 20;

/// Removed / unsupported keys — reject so callers do not assume Flink semantics.
const REMOVED_DELTA_OPTION_KEYS: &[&str] = &[
    "sink.rolling-policy.file-size",
    "sink.rolling-policy.rollover-interval",
    "sink.rolling-policy.check-interval",
    "delta.autoOptimize.optimizeWrite",
    "delta.autoOptimize.autoCompact",
    "delta.logRetentionDuration",
    "delta.deletedFileRetentionDuration",
];

/// DDL / property keys owned by the Delta sink.
pub const DELTA_OPTION_KEYS: &[&str] = &[
    "sink.delta.access.key",
    "sink.delta.secret.key",
    "sink.delta.region",
    "sink.delta.path.style.access",
    "sink.delta.connection.maximum",
    "sink.delta.connection.timeout",
    "sink.delta.attempts.maximum",
];

/// Strongly-typed Delta sink knobs with industrial defaults always applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaSinkOptions {
    /// Optional explicit credentials (prefer `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`).
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    /// S3 region (`sink.delta.region` / `AWS_REGION`).
    pub region: String,
    /// Force path-style addressing (`sink.delta.path.style.access`).
    /// `None` → auto: `true` when a custom endpoint is set, else `false`.
    pub path_style_access: Option<bool>,
    pub connection_maximum: u32,
    pub connection_timeout_ms: u64,
    pub attempts_maximum: u32,
}

impl Default for DeltaSinkOptions {
    fn default() -> Self {
        Self {
            access_key: None,
            secret_key: None,
            region: DEFAULT_REGION.into(),
            path_style_access: None,
            connection_maximum: DEFAULT_CONNECTION_MAXIMUM,
            connection_timeout_ms: DEFAULT_CONNECTION_TIMEOUT_MS,
            attempts_maximum: DEFAULT_ATTEMPTS_MAXIMUM,
        }
    }
}

impl DeltaSinkOptions {
    /// Fill from DDL options; missing keys keep industrial defaults.
    pub fn from_ddl(options: &HashMap<String, String>) -> Result<Self> {
        reject_removed_keys(options)?;

        let mut opts = Self::default();

        if let Some(v) = non_empty(options, "sink.delta.access.key") {
            opts.access_key = Some(v.to_string());
        }
        if let Some(v) = non_empty(options, "sink.delta.secret.key") {
            opts.secret_key = Some(v.to_string());
        }
        if let Some(v) = non_empty(options, "sink.delta.region") {
            opts.region = v.to_string();
        }
        if let Some(v) = non_empty(options, "sink.delta.path.style.access") {
            opts.path_style_access = Some(parse_bool(v)?);
        }
        if let Some(v) = non_empty(options, "sink.delta.connection.maximum") {
            opts.connection_maximum = parse_u32(v, "sink.delta.connection.maximum")?;
        }
        if let Some(v) = non_empty(options, "sink.delta.connection.timeout") {
            opts.connection_timeout_ms = parse_duration_ms(v, "sink.delta.connection.timeout")?;
        }
        if let Some(v) = non_empty(options, "sink.delta.attempts.maximum") {
            opts.attempts_maximum = parse_u32(v, "sink.delta.attempts.maximum")?;
        }

        Ok(opts)
    }

    /// Restore from protobuf `properties` map (missing keys → defaults).
    pub fn from_properties(props: &HashMap<String, String>) -> Result<Self> {
        Self::from_ddl(props)
    }

    /// Flatten into protobuf / SHOW CREATE property pairs (always full set).
    /// Credentials are only included when explicitly set (never invent placeholders).
    pub fn to_properties(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        if let Some(k) = &self.access_key {
            m.insert("sink.delta.access.key".into(), k.clone());
        }
        if let Some(k) = &self.secret_key {
            m.insert("sink.delta.secret.key".into(), k.clone());
        }
        m.insert("sink.delta.region".into(), self.region.clone());
        if let Some(ps) = self.path_style_access {
            m.insert("sink.delta.path.style.access".into(), bool_str(ps).into());
        }
        m.insert(
            "sink.delta.connection.maximum".into(),
            self.connection_maximum.to_string(),
        );
        m.insert(
            "sink.delta.connection.timeout".into(),
            format_duration_ms(self.connection_timeout_ms),
        );
        m.insert(
            "sink.delta.attempts.maximum".into(),
            self.attempts_maximum.to_string(),
        );
        m
    }

    /// Ordered key/value pairs for `CREATE STREAM … WITH` formatting.
    pub fn ddl_pairs(&self, endpoint: Option<&str>) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        if let Some(k) = &self.access_key {
            pairs.push(("sink.delta.access.key".into(), k.clone()));
        }
        if let Some(k) = &self.secret_key {
            pairs.push(("sink.delta.secret.key".into(), k.clone()));
        }
        pairs.push(("sink.delta.region".into(), self.region.clone()));
        let path_style = self
            .path_style_access
            .unwrap_or_else(|| endpoint.map(|e| !e.is_empty()).unwrap_or(false));
        pairs.push((
            "sink.delta.path.style.access".into(),
            bool_str(path_style).into(),
        ));
        pairs.push((
            "sink.delta.connection.maximum".into(),
            self.connection_maximum.to_string(),
        ));
        pairs.push((
            "sink.delta.connection.timeout".into(),
            format_duration_ms(self.connection_timeout_ms),
        ));
        pairs.push((
            "sink.delta.attempts.maximum".into(),
            self.attempts_maximum.to_string(),
        ));
        pairs
    }

    /// Effective path-style flag given optional custom endpoint.
    pub fn effective_path_style(&self, endpoint: Option<&str>) -> bool {
        self.path_style_access
            .unwrap_or_else(|| endpoint.map(|e| !e.is_empty()).unwrap_or(false))
    }

    /// Object-store / deltalake `storage_options` for S3-compatible backends.
    ///
    /// When DDL does **not** set access/secret keys, credentials are omitted so
    /// `deltalake::aws` can use the default AWS credential provider chain
    /// (environment, shared config, instance/IRSA role, STS). That chain refreshes
    /// temporary tokens; MonoTS drops the cached `DeltaTable` on auth errors so a
    /// new store client is built.
    pub fn storage_options(&self, endpoint: Option<&str>) -> HashMap<String, String> {
        let mut opts = HashMap::new();
        // Single writer per table/stream (FunctionStream default).
        opts.insert("AWS_S3_ALLOW_UNSAFE_RENAME".into(), "true".into());
        opts.insert("AWS_REGION".into(), self.region.clone());
        opts.insert("AWS_DEFAULT_REGION".into(), self.region.clone());

        if let Some(ak) = &self.access_key {
            opts.insert("AWS_ACCESS_KEY_ID".into(), ak.clone());
        }
        if let Some(sk) = &self.secret_key {
            opts.insert("AWS_SECRET_ACCESS_KEY".into(), sk.clone());
        }

        if let Some(ep) = endpoint.filter(|s| !s.is_empty()) {
            opts.insert("AWS_ENDPOINT_URL".into(), ep.to_string());
            opts.insert("AWS_ENDPOINT".into(), ep.to_string());
            opts.insert("AWS_ALLOW_HTTP".into(), "true".into());
            opts.insert("allow_http".into(), "true".into());
        }

        let path_style = self.effective_path_style(endpoint);
        // object_store: virtual_hosted_style_request=false ⇒ path-style.
        opts.insert(
            "aws_virtual_hosted_style_request".into(),
            bool_str(!path_style).into(),
        );
        opts.insert(
            "AWS_VIRTUAL_HOSTED_STYLE_REQUEST".into(),
            bool_str(!path_style).into(),
        );

        opts.insert(
            "OBJECT_STORE_CONCURRENCY_LIMIT".into(),
            self.connection_maximum.to_string(),
        );
        let connect_secs = (self.connection_timeout_ms.saturating_add(999)) / 1000;
        opts.insert("connect_timeout".into(), format!("{connect_secs}s"));
        opts.insert("timeout".into(), format!("{connect_secs}s"));
        opts.insert("max_retries".into(), self.attempts_maximum.to_string());

        opts
    }
}

fn reject_removed_keys(options: &HashMap<String, String>) -> Result<()> {
    let present: Vec<&str> = REMOVED_DELTA_OPTION_KEYS
        .iter()
        .copied()
        .filter(|k| options.contains_key(*k))
        .collect();
    if present.is_empty() {
        return Ok(());
    }
    Err(TsdbError::Query(format!(
        "unsupported delta options {} (MonoTS does not implement Flink rolling-policy / autoOptimize / retention; remove these keys)",
        present.join(", ")
    )))
}

fn non_empty<'a>(options: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    options
        .get(key)
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
}

fn bool_str(v: bool) -> &'static str {
    if v {
        "true"
    } else {
        "false"
    }
}

fn parse_bool(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        other => Err(TsdbError::Query(format!("invalid boolean: {other}"))),
    }
}

fn parse_u32(raw: &str, key: &str) -> Result<u32> {
    raw.trim()
        .parse()
        .map_err(|_| TsdbError::Query(format!("invalid u32 for {key}: {raw}")))
}

/// Parse a SQL duration into milliseconds.
///
/// Accepts:
/// - bare integer → milliseconds (`200000`)
/// - number + unit: `ms` / `s` / `sec` / `secs` / `second` / `seconds` /
///   `m` / `min` / `mins` / `minute` / `minutes` /
///   `h` / `hr` / `hrs` / `hour` / `hours`
///
/// Examples: `200s`, `200 s`, `3 min`, `1.5h`, `200000ms`.
fn parse_duration_ms(raw: &str, key: &str) -> Result<u64> {
    let s = raw.trim();
    if s.is_empty() {
        return Err(TsdbError::Query(format!("empty duration for {key}")));
    }

    // Bare integer → milliseconds.
    if let Ok(ms) = s.parse::<u64>() {
        return Ok(ms);
    }

    let lower = s.to_ascii_lowercase();
    let (num_part, unit_part) = split_duration_parts(&lower).ok_or_else(|| {
        TsdbError::Query(format!(
            "invalid duration for {key}: {raw} (examples: 200s, 3 min, 200000ms)"
        ))
    })?;

    let value: f64 = num_part.parse().map_err(|_| {
        TsdbError::Query(format!(
            "invalid duration for {key}: {raw} (examples: 200s, 3 min, 200000ms)"
        ))
    })?;
    if !value.is_finite() || value < 0.0 {
        return Err(TsdbError::Query(format!(
            "invalid duration for {key}: {raw}"
        )));
    }

    let mult = match unit_part {
        "ms" | "msec" | "msecs" | "millisecond" | "milliseconds" => 1.0,
        "s" | "sec" | "secs" | "second" | "seconds" => 1_000.0,
        "m" | "min" | "mins" | "minute" | "minutes" => 60_000.0,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000.0,
        other => {
            return Err(TsdbError::Query(format!(
                "unsupported duration unit '{other}' for {key} (use ms|s|min|h)"
            )));
        }
    };

    let ms = value * mult;
    if ms > u64::MAX as f64 {
        return Err(TsdbError::Query(format!(
            "duration for {key} is too large: {raw}"
        )));
    }
    Ok(ms.round() as u64)
}

fn split_duration_parts(lower: &str) -> Option<(&str, &str)> {
    let bytes = lower.as_bytes();
    let mut i = 0;
    // optional leading digits / decimal
    while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
        i += 1;
    }
    if i == 0 {
        return None;
    }
    let num = &lower[..i];
    let rest = lower[i..].trim_start();
    if rest.is_empty() {
        return None;
    }
    Some((num, rest))
}

/// Prefer a compact human-readable unit for DDL / properties.
fn format_duration_ms(ms: u64) -> String {
    if ms == 0 {
        return "0ms".into();
    }
    if ms % 3_600_000 == 0 {
        return format!("{}h", ms / 3_600_000);
    }
    if ms % 60_000 == 0 {
        return format!("{} min", ms / 60_000);
    }
    if ms % 1_000 == 0 {
        return format!("{}s", ms / 1_000);
    }
    format!("{ms}ms")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_fill_when_ddl_omits_keys() {
        let opts = DeltaSinkOptions::from_ddl(&HashMap::new()).unwrap();
        assert_eq!(opts.region, DEFAULT_REGION);
        assert_eq!(opts.connection_maximum, DEFAULT_CONNECTION_MAXIMUM);
        assert_eq!(opts.connection_timeout_ms, DEFAULT_CONNECTION_TIMEOUT_MS);
        assert_eq!(opts.attempts_maximum, DEFAULT_ATTEMPTS_MAXIMUM);
    }

    #[test]
    fn path_style_auto_true_with_endpoint() {
        let opts = DeltaSinkOptions::default();
        assert!(opts.effective_path_style(Some("http://127.0.0.1:9000")));
        assert!(!opts.effective_path_style(None));
        assert!(!opts.effective_path_style(Some("")));
    }

    #[test]
    fn storage_options_include_network_defaults() {
        let opts = DeltaSinkOptions::default();
        let s = opts.storage_options(Some("http://minio:9000"));
        assert_eq!(s.get("AWS_ENDPOINT_URL").unwrap(), "http://minio:9000");
        assert_eq!(s.get("aws_virtual_hosted_style_request").unwrap(), "false");
        assert_eq!(s.get("max_retries").unwrap(), "20");
        assert_eq!(s.get("OBJECT_STORE_CONCURRENCY_LIMIT").unwrap(), "500");
        assert_eq!(s.get("connect_timeout").unwrap(), "200s");
    }

    #[test]
    fn ddl_pairs_always_materialize_full_set() {
        let opts = DeltaSinkOptions::default();
        let pairs = opts.ddl_pairs(Some("http://x"));
        let keys: Vec<_> = pairs.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"sink.delta.region"));
        assert!(keys.contains(&"sink.delta.path.style.access"));
        assert!(keys.contains(&"sink.delta.connection.maximum"));
        assert!(!keys.contains(&"sink.rolling-policy.file-size"));
        assert!(!keys.contains(&"delta.logRetentionDuration"));
        assert!(!keys.contains(&"sink.delta.access.key"));
        let timeout = pairs
            .iter()
            .find(|(k, _)| k == "sink.delta.connection.timeout")
            .unwrap()
            .1
            .as_str();
        assert_eq!(timeout, "200s");
    }

    #[test]
    fn parses_sql_duration_intervals() {
        assert_eq!(parse_duration_ms("200000", "k").unwrap(), 200_000);
        assert_eq!(parse_duration_ms("200s", "k").unwrap(), 200_000);
        assert_eq!(parse_duration_ms("200 s", "k").unwrap(), 200_000);
        assert_eq!(parse_duration_ms("3 min", "k").unwrap(), 180_000);
        assert_eq!(parse_duration_ms("3m", "k").unwrap(), 180_000);
        assert_eq!(parse_duration_ms("1h", "k").unwrap(), 3_600_000);
        assert_eq!(parse_duration_ms("1.5s", "k").unwrap(), 1_500);
        assert_eq!(parse_duration_ms("500ms", "k").unwrap(), 500);

        let mut m = HashMap::new();
        m.insert("sink.delta.connection.timeout".into(), "3 min".into());
        let opts = DeltaSinkOptions::from_ddl(&m).unwrap();
        assert_eq!(opts.connection_timeout_ms, 180_000);
        assert_eq!(
            opts.ddl_pairs(None)
                .into_iter()
                .find(|(k, _)| k == "sink.delta.connection.timeout")
                .unwrap()
                .1,
            "3 min"
        );
    }

    #[test]
    fn rejects_removed_rolling_and_table_property_keys() {
        let mut m = HashMap::new();
        m.insert("delta.autoOptimize.optimizeWrite".into(), "true".into());
        let err = DeltaSinkOptions::from_ddl(&m).unwrap_err().to_string();
        assert!(err.contains("unsupported"), "{err}");
        assert!(err.contains("delta.autoOptimize.optimizeWrite"), "{err}");
    }

    #[test]
    fn from_properties_rejects_removed_keys() {
        let mut m = HashMap::new();
        m.insert("sink.delta.region".into(), "ap-east-1".into());
        m.insert(
            "delta.logRetentionDuration".into(),
            "interval 15 days".into(),
        );
        let err = DeltaSinkOptions::from_properties(&m)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unsupported"), "{err}");
    }
}
