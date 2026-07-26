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

//! Pure LSM storage engine (RocksDB / TiKV style).
//!
//! Layout:
//! - [`wal`] — write-ahead log (writer / format / backlog / notify / load cache / bulk-load WAL)
//! - [`memtable`] — in-memory table, builders, batch accumulation
//! - [`compaction`] — SST merge and Parquet I/O
//! - [`bulk_load`] — external Parquet ingest into SST
//! - [`capture`] — CDC write-path notify only (progress lives in Stream)\n//! - [`replication`] — global LSN + retention pin injected by Stream

pub mod bulk_load;
pub mod capture;
pub mod compaction;
pub mod disk_space;
pub mod engine;
pub mod lifecycle;
pub mod materialize;
pub mod memory;
pub mod memtable;
pub mod replication;
pub mod sequence;
pub mod table;
pub mod validate;
pub mod version;
pub mod wal;

// --- public API (stable paths for dependents) ---

pub use bulk_load::{
    collect_parquet_inputs, ingest_parquet_file, ingest_parquet_paths, seal_bulk_sst_identity,
    write_bulk_parquet, BulkLoadResult, ParquetInspect,
};
pub use capture::{
    hard_link_into_pending, RegisteredTableCapturer, TableCaptureHub, TableCapturer,
    DEFAULT_TABLE_CAPTURE_CAPACITY,
};
pub use common::{
    BatchEvent, BatchOrigin, CaptureFileMeta, CaptureProgress, CdcEvent, CommitDurability,
    FileAddEvent, LogEvent, LsnAllocator, LsnRange, TableCaptureListener, RETENTION_UNPINNED,
};
pub use compaction::{
    batches_are_time_ordered, bulk_tmp_dir, cleanup_bulk_tmp, cleanup_compact_tmp,
    cleanup_flush_tmp, cleanup_flush_tmp_under, cleanup_sst_staging, cleanup_sst_staging_under,
    compact_tmp_dir, dedupe_batches_newest_wins, filter_batch_by_time,
    filter_batch_skip_seen_timestamps, flush_tmp_dir, merge_sst_layers, needs_layer_dedupe,
    parse_sst_filename, pick_compaction, prepare_flush_batch, prepare_scan_batches,
    promote_sst_from_compact_tmp, promote_sst_from_flush_tmp, promote_sst_from_tmp,
    read_parquet_file, read_parquet_schema, sort_batch_by_timestamp, write_sst, BatchAligner,
    CompactionStrategy, Compactor, DedupeConfig, FileIndex, GlobalCompactor, ParquetReadOptions,
    SstFile, SstIdentity, SstMeta, SstWriteConfig, DEFAULT_COMPACTION_MAX_CONCURRENT_JOBS,
    DEFAULT_COMPACTION_MAX_MERGE_FILES, FLUSH_WINDOW_ROWS,
};
pub use disk_space::{DiskSpaceController, DiskUsage, DEFAULT_DISK_MIN_FREE_RATIO};
pub use engine::LsmEngine;
pub use lifecycle::{EngineLifecycle, EngineLifecycleGate};
pub use materialize::{
    materialize_log_event_with_cache, materialize_logical_event_with_cache,
    read_wal_batches_for_lsn_range,
};
pub use memory::{
    MemoryController, DEFAULT_GLOBAL_MEMORY_SOFT_THRESHOLD_RATIO, GLOBAL_MEMORY_LIMIT_EXCEEDED,
};
pub use memtable::{
    coalesce_batches, ActiveBuilders, BatchBuffer, ChunkSnapshot, MemTable, MemTableLayout,
    DEFAULT_MEMTABLE_BATCH_MAX_BYTES, DEFAULT_MEMTABLE_BATCH_MAX_ROWS, DEFAULT_TARGET_BATCH_SIZE,
};
pub use replication::{
    ReplicationManager, RetentionPin, TableReplication, UnpinnedRetention, WalRetention,
};
pub use sequence::TableSequence;
pub use table::{LsmTable, SstFlushOptions, TableOpenOptions, WalRecoveryMode};
pub use validate::validate_write_batch;
pub use wal::{
    can_drop_wal_file, can_drop_wal_for_lsn_watermark, data_bearing_memtable_wal_ids,
    data_bearing_wal_file_ids, find_wal_file_for_lsn, has_recoverable_memtable_wal,
    list_numbered_wal_paths, list_wal_file_ids, list_wal_memtable_ids, list_wal_segment_paths,
    lsn_range_in_memtable_wal, lsn_range_in_segment, max_lsn_in_memtable_wal, max_lsn_in_segment,
    max_lsn_in_sst_metas, max_lsn_in_table_wals, next_wal_file_after, numbered_wal_path,
    read_wal_batches_from_sequence, segment_format_readable, segment_has_batches,
    sst_has_lsn_watermark, wal_file_path, wal_segment_tail, walk_unflushed_partitions,
    ArcBulkLoadWal, BulkLoadWal, FileEventLog, MemTableWal, TableBacklog, WalAppendEvent,
    WalAppendHub, WalBacklogBudget, WalDurabilityMode, WalFrameCursor, WalFrameEvent,
    WalFramedBatch, WalLoadCache, WalLoadCursor, WalLoadKey, WalRecoverPartition, WalSegmentTail,
    WalWriter, WalWriterOptions, BULK_LOAD_DIR_NAME, BULK_LOAD_LOG_NAME,
    DEFAULT_WAL_BLOCK_MAX_BYTES, DEFAULT_WAL_GLOBAL_BACKLOG_MAX_BYTES,
    DEFAULT_WAL_LOAD_CACHE_MAX_BYTES, DEFAULT_WAL_MICRO_BATCH_MAX_BYTES,
    DEFAULT_WAL_SEGMENT_MAX_BYTES, DEFAULT_WAL_TABLE_BACKLOG_MAX_BYTES, FRAME_KNOWN_FLAGS,
    PAYLOAD_FORMAT_ARROW_IPC, SEGMENT_FORMAT_VERSION, SEGMENT_KNOWN_FLAGS, SEGMENT_MAGIC,
    SEGMENT_MAX_READ_VERSION, SEGMENT_MIN_READ_VERSION, WAL_CHANNEL_CAPACITY, WAL_FILE_NAME,
    WAL_MICRO_BATCH_MAX_WAIT_US, WAL_SEGMENT_EXT,
};

// Compatibility aliases for deep imports (e.g. monots_storage::parquet_read::…).
pub use compaction::job as compaction_job;
pub use compaction::{dedup, parquet_read, reader, sst, sst_id};
pub use memtable::{
    accumulator as batch_accumulator, builders as active_builders, chunk_buffer,
    memory as memtable_memory, table as memtable_table,
};
pub use wal::{
    backlog as wal_backlog, bulk_load as wal_bulk_load, format as wal_format,
    load_cache as wal_load_cache, notify as wal_notify, writer as wal_writer,
};
