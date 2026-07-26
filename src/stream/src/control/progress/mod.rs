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

//! Stream-owned capture progress: durable LSN cursors that pin WAL retention.

mod manager;
mod registry;
mod wal;

pub use manager::{ProgressManager, SinkCommitted};
pub use registry::CaptureProgressRegistry;
pub use wal::{CommitStore, WalCommitLog, TOMBSTONE_LSN};

pub fn capture_progress_id(stream: &str, table: &str) -> String {
    format!("{stream}::log::{table}")
}

pub fn parse_capture_progress_id(progress_id: &str) -> Option<(&str, &str)> {
    let mut parts = progress_id.splitn(3, "::");
    let stream = parts.next()?;
    let kind = parts.next()?;
    let table = parts.next()?;
    if kind != "log" || stream.is_empty() || table.is_empty() {
        return None;
    }
    Some((stream, table))
}
