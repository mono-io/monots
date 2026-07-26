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

//! Write-ahead log: writer, on-disk format, backlog, notify, CDC load cache, bulk-load WAL.
//!
//! Frames carry an optional global **LSN** (logical clock for CDC / recovery trim).
//! Memtable freeze does **not** write a special WAL marker — flush is driven by memory size;
//! crash recovery replays batch frames whose LSN is not yet covered by SST metadata.

pub mod backlog;
pub mod bulk_load;
pub mod format;
pub mod load_cache;
pub mod lsn_recover;
pub mod notify;
pub mod writer;

pub use backlog::{TableBacklog, WalBacklogBudget};
pub use bulk_load::{
    ArcBulkLoadWal, BulkLoadWal, FileEventLog, BULK_LOAD_DIR_NAME, BULK_LOAD_LOG_NAME,
};
pub use format::{
    list_numbered_wal_paths, list_wal_file_ids, list_wal_memtable_ids, numbered_wal_path,
    read_wal_batches_from_sequence, segment_format_readable, wal_file_path, wal_segment_tail,
    WalFrameCursor, WalFrameEvent, WalFramedBatch, WalSegmentTail, DEFAULT_WAL_BLOCK_MAX_BYTES,
    DEFAULT_WAL_SEGMENT_MAX_BYTES, FRAME_KNOWN_FLAGS, PAYLOAD_FORMAT_ARROW_IPC,
    SEGMENT_FORMAT_VERSION, SEGMENT_KNOWN_FLAGS, SEGMENT_MAGIC, SEGMENT_MAX_READ_VERSION,
    SEGMENT_MIN_READ_VERSION, WAL_FILE_NAME, WAL_SEGMENT_EXT,
};
pub use load_cache::{
    list_wal_segment_paths, WalLoadCache, WalLoadCursor, WalLoadKey,
    DEFAULT_WAL_LOAD_CACHE_MAX_BYTES,
};
pub use lsn_recover::{
    can_drop_wal_file, can_drop_wal_for_lsn_watermark, data_bearing_memtable_wal_ids,
    data_bearing_wal_file_ids, find_wal_file_for_lsn, has_recoverable_memtable_wal,
    lsn_range_in_memtable_wal, lsn_range_in_segment, max_lsn_in_memtable_wal, max_lsn_in_segment,
    max_lsn_in_sst_metas, max_lsn_in_table_wals, next_wal_file_after, segment_has_batches,
    sst_has_lsn_watermark, walk_unflushed_partitions, WalRecoverPartition,
};
pub use notify::{WalAppendEvent, WalAppendHub};
pub use writer::{
    MemTableWal, WalDurabilityMode, WalWriter, WalWriterOptions,
    DEFAULT_WAL_GLOBAL_BACKLOG_MAX_BYTES, DEFAULT_WAL_MICRO_BATCH_MAX_BYTES,
    DEFAULT_WAL_TABLE_BACKLOG_MAX_BYTES, WAL_CHANNEL_CAPACITY, WAL_MICRO_BATCH_MAX_WAIT_US,
};
