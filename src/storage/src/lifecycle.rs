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

//! Storage engine lifecycle: boot phases before the LSM accepts user writes.
//!
//! Boot order owned by `TsdbEngine`:
//! 1. [`EngineLifecycle::Starting`] — mount tables (memory + catalog SST index only)
//! 2. recover / attach Stream capture
//! 3. [`EngineLifecycle::Recovering`] — replay sealed memtable WAL → SST
//! 4. [`EngineLifecycle::Running`] — accept writes / start stream supervisors

use common::{Result, TsdbError};
use std::sync::atomic::{AtomicU8, Ordering};

/// LSM storage engine lifecycle phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EngineLifecycle {
    /// Memory structures mounted; disk WAL not yet replayed.
    Starting = 0,
    /// Replaying sealed memtable WAL into SST.
    Recovering = 1,
    /// Ready for user writes and background work.
    Running = 2,
    /// Engine shut down / unloaded.
    Stopped = 3,
}

impl EngineLifecycle {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Starting,
            1 => Self::Recovering,
            2 => Self::Running,
            _ => Self::Stopped,
        }
    }
}

/// Atomic lifecycle gate shared by [`crate::LsmEngine`].
#[derive(Debug)]
pub struct EngineLifecycleGate {
    phase: AtomicU8,
}

impl Default for EngineLifecycleGate {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineLifecycleGate {
    pub fn new() -> Self {
        Self {
            phase: AtomicU8::new(EngineLifecycle::Starting as u8),
        }
    }

    pub fn get(&self) -> EngineLifecycle {
        EngineLifecycle::from_u8(self.phase.load(Ordering::Acquire))
    }

    pub fn is_running(&self) -> bool {
        self.get() == EngineLifecycle::Running
    }

    pub fn begin_disk_recovery(&self) -> Result<()> {
        self.transition(EngineLifecycle::Starting, EngineLifecycle::Recovering)
    }

    pub fn mark_running(&self) -> Result<()> {
        match self.get() {
            EngineLifecycle::Recovering | EngineLifecycle::Starting => {
                self.phase
                    .store(EngineLifecycle::Running as u8, Ordering::Release);
                Ok(())
            }
            EngineLifecycle::Running => Ok(()),
            EngineLifecycle::Stopped => Err(TsdbError::Storage(
                "cannot mark Running: storage engine is Stopped".into(),
            )),
        }
    }

    pub fn mark_stopped(&self) {
        self.phase
            .store(EngineLifecycle::Stopped as u8, Ordering::Release);
    }

    pub fn ensure_running(&self) -> Result<()> {
        match self.get() {
            EngineLifecycle::Running => Ok(()),
            other => Err(TsdbError::Storage(format!(
                "storage engine is not Running (phase={other:?})"
            ))),
        }
    }

    fn transition(&self, from: EngineLifecycle, to: EngineLifecycle) -> Result<()> {
        match self
            .phase
            .compare_exchange(from as u8, to as u8, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Ok(()),
            Err(actual) => Err(TsdbError::Storage(format!(
                "storage lifecycle: expected {from:?} → {to:?}, found {:?}",
                EngineLifecycle::from_u8(actual)
            ))),
        }
    }
}
