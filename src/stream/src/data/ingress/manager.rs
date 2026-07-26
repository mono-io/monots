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

//! Stream Source Manager: first-time bootstrap (flush + hard-link history) vs restart recover from `pending/`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::{CaptureSource, Result, StreamCaptureMode};
use monots_storage::LsmEngine;

use super::source::StreamSource;
use crate::data::memory::StreamArrowBlock;

/// Owns stream capture layout under `{base}/cdc_streams/{stream}/{table}/`.
pub struct StreamSourceManager {
    base_dir: PathBuf,
    engine: Arc<LsmEngine>,
}

impl StreamSourceManager {
    pub fn new(base_dir: impl Into<PathBuf>, engine: Arc<LsmEngine>) -> Self {
        Self {
            base_dir: base_dir.into(),
            engine,
        }
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn stream_table_dir(&self, stream_id: &str, table: &str) -> PathBuf {
        self.base_dir
            .join("cdc_streams")
            .join(stream_id)
            .join(table)
    }

    /// Load existing durable queue, or create a new stream capture.
    ///
    /// `capture_mode` drives the WAL switch: batch-only sources never receive Arrow inserts.
    /// `arrow_block` is the stream's Arrow budget; deferred Inserts are loaded at the sink.
    ///
    /// - **First create**: create `pending/{flush,compact}` + `cursor`, register CaptureSource
    ///   (write-lock → flush MemTables → hard-link historical SSTs into `flush/` → attach live).
    /// - **Restart**: scan `pending/flush` → Flush/Bulk queue, `pending/compact` → Compact queue;
    ///   attach live only (no historical replay — filesystem is the queue).
    pub async fn load_or_create_source(
        &self,
        stream_id: &str,
        table: &str,
        capture_mode: StreamCaptureMode,
        arrow_block: Option<Arc<StreamArrowBlock>>,
    ) -> Result<Arc<StreamSource>> {
        let stream_dir = self.stream_table_dir(stream_id, table);
        let is_first_time = !stream_dir.exists();
        let capture_wal = capture_mode.includes_log();

        let source = Arc::new(StreamSource::open_with_capture_wal(
            stream_id,
            table,
            &stream_dir,
            capture_wal,
        )?);

        if let Some(block) = arrow_block {
            source.attach_arrow_block(block);
        }

        let capture: Arc<dyn CaptureSource> = Arc::clone(&source) as Arc<dyn CaptureSource>;

        if is_first_time {
            self.engine
                .register_capture_source(stream_id, table, Arc::clone(&capture))
                .await?;
            tracing::info!(
                stream = %stream_id,
                table = %table,
                capture_wal,
                ?capture_mode,
                dir = %stream_dir.display(),
                "stream first create: historical SST hard-linked into pending/"
            );
        } else {
            let recovered = source.recover_pending_queue()?;
            self.engine
                .attach_capture_source(stream_id, table, capture)
                .await?;
            tracing::info!(
                stream = %stream_id,
                table = %table,
                capture_wal,
                ?capture_mode,
                recovered,
                cursor = source.cursor_lsn().unwrap_or(0),
                "stream recover: rebuilt Flush/Bulk queue from pending/"
            );
        }

        Ok(source)
    }
}
