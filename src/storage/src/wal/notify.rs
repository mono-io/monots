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

//! Push notification when a WAL batch frame is durably appended (for stream realtime tail).

use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

const WAL_NOTIFY_CAPACITY: usize = 4096;

#[derive(Debug, Clone)]
pub struct WalAppendEvent {
    pub table: Arc<str>,
    pub memtable_id: u64,
    pub sequence: u64,
}

#[derive(Clone, Default)]
pub struct WalAppendHub {
    inner: Arc<DashMap<String, broadcast::Sender<WalAppendEvent>>>,
}

impl WalAppendHub {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    pub fn subscribe(&self, table: &str) -> broadcast::Receiver<WalAppendEvent> {
        self.sender(table).subscribe()
    }

    pub fn notify(&self, table: &str, memtable_id: u64, sequence: u64) {
        let _ = self.sender(table).send(WalAppendEvent {
            table: Arc::from(table),
            memtable_id,
            sequence,
        });
    }

    fn sender(&self, table: &str) -> broadcast::Sender<WalAppendEvent> {
        if let Some(existing) = self.inner.get(table) {
            return existing.clone();
        }
        let (tx, _) = broadcast::channel(WAL_NOTIFY_CAPACITY);
        self.inner.insert(table.to_string(), tx.clone());
        tx
    }
}
