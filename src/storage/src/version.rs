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

//! Atomic LSM version snapshot: mutable/immutable memtables + SST index updated together.

use crate::compaction::sst::{FileIndex, SstMeta};
use crate::memtable::MemTable;
use parking_lot::RwLock;
use std::sync::Arc;

/// Point-in-time view of one table's LSM levels.
#[derive(Clone)]
pub struct TableVersion {
    pub mutable: Arc<MemTable>,
    pub immutables: Vec<Arc<MemTable>>,
    pub sstables: Arc<FileIndex>,
}

impl TableVersion {
    pub fn new(mutable: Arc<MemTable>, sstables: Arc<FileIndex>) -> Self {
        Self {
            mutable,
            immutables: Vec::new(),
            sstables,
        }
    }

    pub fn snapshot(&self) -> (Arc<MemTable>, Vec<Arc<MemTable>>, Vec<SstMeta>) {
        (
            self.mutable.clone(),
            self.immutables.clone(),
            self.sstables.snapshot(),
        )
    }
}

/// Guarded version state — readers/writers share one lock for consistent views.
pub type VersionState = RwLock<TableVersion>;
