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

//! Tracing subscriber setup: console + optional rotating file, pretty or JSON.

use crate::log::config::{LogConfig, LogFormat, LogRotation};
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

static LOG_GUARD: OnceLock<LogGuardInner> = OnceLock::new();

struct LogGuardInner {
    _file_worker: Option<tracing_appender::non_blocking::WorkerGuard>,
}

/// Keeps the non-blocking file writer alive for the process lifetime.
pub struct LogGuard;

impl LogGuard {
    /// Install the global subscriber once. Subsequent calls are no-ops.
    pub fn init(config: &LogConfig, log_dir: &Path) -> Self {
        if LOG_GUARD.get().is_some() {
            return Self;
        }

        let filter = EnvFilter::try_new(config.filter_directive())
            .unwrap_or_else(|_| EnvFilter::new("info"));

        let span_events = FmtSpan::CLOSE;
        let format = config.format;
        let mut file_worker = None;

        let file_sink = if config.file {
            match std::fs::create_dir_all(log_dir) {
                Ok(()) => {
                    let rotation = match config.rotation {
                        LogRotation::Hourly => tracing_appender::rolling::Rotation::HOURLY,
                        LogRotation::Daily => tracing_appender::rolling::Rotation::DAILY,
                        LogRotation::Never => tracing_appender::rolling::Rotation::NEVER,
                    };
                    let appender = tracing_appender::rolling::RollingFileAppender::new(
                        rotation,
                        log_dir,
                        &config.file_name,
                    );
                    let (writer, guard) = tracing_appender::non_blocking(appender);
                    file_worker = Some(guard);
                    Some(Arc::new(Mutex::new(writer)) as Arc<Mutex<dyn Write + Send>>)
                }
                Err(e) => {
                    eprintln!(
                        "monots: failed to create log directory {}: {e}",
                        log_dir.display()
                    );
                    None
                }
            }
        } else {
            None
        };

        let writer = LogWriter::new(config.console, file_sink);

        match format {
            LogFormat::Pretty => Registry::default()
                .with(filter)
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_writer(writer)
                        .with_ansi(config.ansi && config.console)
                        .with_target(config.target)
                        .with_file(config.line_number)
                        .with_line_number(config.line_number)
                        .with_thread_ids(true)
                        .with_thread_names(true)
                        .with_span_events(span_events),
                )
                .init(),
            LogFormat::Json => Registry::default()
                .with(filter)
                .with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .with_writer(writer)
                        .with_target(config.target)
                        .with_file(config.line_number)
                        .with_line_number(config.line_number)
                        .with_thread_ids(true)
                        .with_thread_names(true)
                        .with_span_events(span_events),
                )
                .init(),
        }

        let _ = LOG_GUARD.set(LogGuardInner {
            _file_worker: file_worker,
        });

        tracing::info!(
            log_dir = %log_dir.display(),
            format = ?format,
            level = %config.filter_directive(),
            console = config.console,
            file = config.file,
            rotation = ?config.rotation,
            "monots logging initialized"
        );

        Self
    }

    /// Console-only fallback when no YAML config is available.
    pub fn init_default() -> Self {
        Self::init(&LogConfig::cli_default(), Path::new("."))
    }
}

#[derive(Clone)]
struct LogWriter {
    console: bool,
    file: Option<Arc<Mutex<dyn Write + Send>>>,
}

impl LogWriter {
    fn new(console: bool, file: Option<Arc<Mutex<dyn Write + Send>>>) -> Self {
        Self { console, file }
    }
}

struct LogWriterInstance {
    console: bool,
    file: Option<Arc<Mutex<dyn Write + Send>>>,
}

impl<'a> MakeWriter<'a> for LogWriter {
    type Writer = LogWriterInstance;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriterInstance {
            console: self.console,
            file: self.file.clone(),
        }
    }
}

impl Write for LogWriterInstance {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.console {
            io::stdout().write_all(buf)?;
        }
        if let Some(file) = &self.file {
            file.lock()
                .map_err(|e| io::Error::other(format!("log file lock poisoned: {e}")))?
                .write_all(buf)?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.console {
            io::stdout().flush()?;
        }
        if let Some(file) = &self.file {
            file.lock()
                .map_err(|e| io::Error::other(format!("log file lock poisoned: {e}")))?
                .flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_is_idempotent_in_process() {
        let cfg = LogConfig::cli_default();
        let _ = LogGuard::init(&cfg, Path::new("target/test-logs"));
        let _ = LogGuard::init(&cfg, Path::new("target/test-logs"));
    }
}
