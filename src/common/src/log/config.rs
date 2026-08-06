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

//! Logging configuration (YAML only — no environment-variable overrides).

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable lines for development / foreground servers.
    #[default]
    Pretty,
    /// JSON lines for production log aggregation (ELK, Loki, etc.).
    Json,
}

/// File rotation policy for on-disk logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogRotation {
    Hourly,
    #[default]
    Daily,
    Never,
}

/// Per-module log level (`error` | `warn` | `info` | `debug` | `trace`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

/// Industrial logging settings (see `conf/config.yaml` → `logging`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LogConfig {
    /// Global default level (`info` includes warn + error).
    pub level: LogLevel,
    /// Per-crate / module overrides (keys are tracing targets, e.g. `monots_storage`).
    #[serde(default)]
    pub modules: BTreeMap<String, LogLevel>,
    /// Console + file line format.
    pub format: LogFormat,
    /// Relative log directory (resolved against `MONOTS_HOME` / config base).
    pub directory: String,
    /// Base name for rotated log files (suffix added by rotation policy).
    pub file_name: String,
    /// Emit logs to stdout/stderr.
    pub console: bool,
    /// Emit logs to rotating files under `directory`.
    pub file: bool,
    /// ANSI colors on console (pretty format only).
    pub ansi: bool,
    /// Include source file + line in each event.
    pub line_number: bool,
    /// Include Rust module target in each event.
    pub target: bool,
    /// On-disk rotation policy.
    pub rotation: LogRotation,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            modules: BTreeMap::new(),
            format: LogFormat::Pretty,
            directory: "logs".into(),
            file_name: "monots.log".into(),
            console: true,
            file: true,
            ansi: true,
            line_number: false,
            target: true,
            rotation: LogRotation::Daily,
        }
    }
}

impl LogConfig {
    /// Build tracing `EnvFilter` directive from YAML fields.
    pub fn filter_directive(&self) -> String {
        if self.modules.is_empty() {
            return self.level.as_str().to_string();
        }
        let mut parts = vec![self.level.as_str().to_string()];
        for (name, lvl) in &self.modules {
            parts.push(format!("{name}={}", lvl.as_str()));
        }
        parts.join(",")
    }

    /// Resolve log directory relative to deployment base (`MONOTS_HOME` or config parent).
    pub fn resolve_directory(&self, base: &Path) -> PathBuf {
        let dir = PathBuf::from(&self.directory);
        if dir.is_absolute() {
            dir
        } else {
            base.join(dir)
        }
    }

    /// Console-only config for CLI when no YAML is provided.
    pub fn cli_default() -> Self {
        Self {
            console: true,
            file: false,
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_level_is_info() {
        assert_eq!(LogConfig::default().filter_directive(), "info");
    }

    #[test]
    fn modules_compile_to_tracing_directive() {
        let mut modules = BTreeMap::new();
        modules.insert("monots_storage".into(), LogLevel::Debug);
        modules.insert("monots_query".into(), LogLevel::Warn);
        let cfg = LogConfig {
            level: LogLevel::Info,
            modules,
            ..LogConfig::default()
        };
        assert_eq!(
            cfg.filter_directive(),
            "info,monots_query=warn,monots_storage=debug"
        );
    }

    #[test]
    fn resolve_relative_directory_against_base() {
        let cfg = LogConfig {
            directory: "logs".into(),
            ..LogConfig::default()
        };
        assert_eq!(
            cfg.resolve_directory(Path::new("/app")),
            PathBuf::from("/app/logs")
        );
    }
}
