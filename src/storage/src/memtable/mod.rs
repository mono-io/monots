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

//! In-memory memtable, builders, and batch accumulation.

pub mod accumulator;
pub mod builders;
pub mod chunk_buffer;
pub mod memory;
pub mod table;

pub use accumulator::{
    coalesce_batches, DEFAULT_MEMTABLE_BATCH_MAX_BYTES, DEFAULT_MEMTABLE_BATCH_MAX_ROWS,
};
pub use builders::{ActiveBuilders, BatchBuffer, DEFAULT_TARGET_BATCH_SIZE};
pub use table::{ChunkSnapshot, MemTable, MemTableLayout};
