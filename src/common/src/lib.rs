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

//! MonoTS shared crate: CDC contracts, stream metadata, schema constants, logging.
//!
//! ```text
//! common/
//!   arrow/     — RecordBatch time sort / time-column helpers
//!   banner/    — startup ASCII logo + git hash
//!   cdc/       — LogEvent, BatchEvent, LSN, TableCaptureListener, capture progress
//!   stream/    — StreamDef, connector / capture-mode / checkpoint / status
//!   log/       — LogConfig, LogGuard, process title
//!   schema.rs  — on-disk layout + well-known column names
//!   error.rs   — TsdbError / Result
//! ```

pub mod arrow;
pub mod banner;
pub mod cdc;
pub mod error;
pub mod log;
pub mod schema;
pub mod stream;

pub use arrow::{
    ensure_sorted_by_time, is_sorted_by_time, sort_batch_by_time, time_column_index, time_value_at,
    time_values_slice,
};
pub use banner::{git_hash, print_banner, version_label, RELEASE_CHANNEL};
pub use cdc::{
    BatchEvent, BatchOrigin, CaptureBootstrapReport, CaptureFileMeta, CaptureProgress,
    CaptureSource, CaptureSourceHandle, CdcEvent, CommitDurability, FileAddEvent, LogEvent,
    LsnAllocator, LsnRange, TableCaptureListener, RETENTION_UNPINNED,
};
pub use error::{Result, TsdbError};
pub use log::{
    set_process_name, LogConfig, LogFormat, LogGuard, LogLevel, LogRotation, DEFAULT_PROCESS_NAME,
};
pub use schema::{
    WellKnownColumn, BULK_TMP_DIR, COMPACT_TMP_DIR, FLUSH_TMP_DIR, META_DIR, SEQUENCE_FILE,
    SST_FILE_SUFFIX, TIMESTAMP_COLUMN, TIME_COLUMN, WAL_SEGMENTS_DIR,
};
pub use stream::{
    ConnectorType, StreamCaptureMode, StreamCheckpoint, StreamDef, StreamPhase, StreamStatus,
    TableCaptureStatus, TableCheckpoint, TableStreamStatus,
};
