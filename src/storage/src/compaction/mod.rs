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

//! Compaction / SST merge and Parquet I/O helpers.

pub mod dedup;
pub mod job;
pub mod parquet_read;
pub mod reader;
pub mod sst;
pub mod sst_id;

pub use dedup::{
    batches_are_time_ordered, dedupe_batches_newest_wins, filter_batch_skip_seen_timestamps,
    merge_sst_layers, needs_layer_dedupe, prepare_flush_batch, prepare_scan_batches,
    sort_batch_by_timestamp, DedupeConfig, FLUSH_WINDOW_ROWS,
};
pub use job::{
    lsn_spans_contiguous, pick_compaction, CompactionStrategy, Compactor, GlobalCompactor,
    DEFAULT_COMPACTION_MAX_CONCURRENT_JOBS, DEFAULT_COMPACTION_MAX_MERGE_FILES,
};
pub use parquet_read::{
    filter_batch_by_time, parquet_file_time_bounds, read_parquet_file, read_parquet_schema,
    ParquetReadOptions,
};
pub use reader::BatchAligner;
pub use sst::{
    bulk_tmp_dir, cleanup_bulk_tmp, cleanup_compact_tmp, cleanup_flush_tmp,
    cleanup_flush_tmp_under, cleanup_sst_staging, cleanup_sst_staging_under, compact_tmp_dir,
    flush_tmp_dir, promote_sst_from_compact_tmp, promote_sst_from_flush_tmp, promote_sst_from_tmp,
    write_sst, FileIndex, SstFile, SstMeta, SstWriteConfig,
};
pub use sst_id::{is_staging_sst_filename, parse_sst_filename, SstIdentity};
