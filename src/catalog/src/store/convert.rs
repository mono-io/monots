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

use monots_storage::sst::SstMeta;

pub(crate) fn sst_to_proto(f: SstMeta) -> proto::meta::ParquetFileMeta {
    proto::meta::ParquetFileMeta {
        file_path: f.file_path,
        min_ts: f.min_ts,
        max_ts: f.max_ts,
        row_count: f.row_count as u64,
        file_size: f.file_size,
        creation_time_ms: f.creation_time_ms,
        inner_compaction_count: f.inner_compaction_count,
        cross_compaction_count: f.cross_compaction_count,
        base_lsn: f.base_lsn,
        max_lsn: f.max_lsn,
    }
}

pub(crate) fn sst_from_proto(f: &proto::meta::ParquetFileMeta) -> SstMeta {
    SstMeta {
        file_path: f.file_path.clone(),
        min_ts: f.min_ts,
        max_ts: f.max_ts,
        row_count: f.row_count as usize,
        file_size: f.file_size,
        creation_time_ms: f.creation_time_ms,
        inner_compaction_count: f.inner_compaction_count,
        cross_compaction_count: f.cross_compaction_count,
        base_lsn: f.base_lsn,
        max_lsn: f.max_lsn,
    }
}
