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

use common::{Result, TsdbError, SEQUENCE_FILE};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SequenceState {
    next_memtable_id: u64,
}

/// Per-table monotonic memtable id allocator (persisted).
pub struct TableSequence {
    path: PathBuf,
    state: SequenceState,
}

impl TableSequence {
    pub fn load(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join(SEQUENCE_FILE);
        let state = if path.exists() {
            let bytes = fs::read(&path)?;
            serde_json::from_slice(&bytes)
                .map_err(|e| TsdbError::Storage(format!("invalid {SEQUENCE_FILE}: {e}")))?
        } else {
            SequenceState {
                next_memtable_id: 1,
            }
        };
        Ok(Self { path, state })
    }

    pub fn next_id(&mut self) -> Result<u64> {
        let id = self.state.next_memtable_id;
        self.state.next_memtable_id = id.saturating_add(1);
        self.persist()?;
        Ok(id)
    }

    pub fn ensure_at_least(&mut self, min_next: u64) -> Result<()> {
        if self.state.next_memtable_id < min_next {
            self.state.next_memtable_id = min_next;
            self.persist()?;
        }
        Ok(())
    }

    fn persist(&self) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(&self.state)
            .map_err(|e| TsdbError::Storage(e.to_string()))?;
        fs::write(&self.path, bytes)?;
        Ok(())
    }
}
