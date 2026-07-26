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

//! Stream-level Source registry (one [`StreamSource`] per table under a stream).

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;

use super::source::StreamSource;
use crate::data::memory::StreamArrowBlock;

#[derive(Clone, Default)]
pub struct StreamSourceRegistry {
    /// stream → per-table sources
    inner: Arc<DashMap<String, StreamSources>>,
}

#[derive(Clone, Default)]
pub struct StreamSources {
    pub tables: HashMap<String, Arc<StreamSource>>,
    /// Shared Arrow Block for this stream (returned to pool when last Arc drops).
    pub arrow_block: Option<Arc<StreamArrowBlock>>,
}

impl StreamSourceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Ensure an empty stream entry exists (tables filled by [`Self::insert`]).
    pub fn ensure_stream(&self, stream: &str) {
        self.inner
            .entry(stream.to_string())
            .or_insert_with(StreamSources::default);
    }

    pub fn set_arrow_block(&self, stream: &str, block: Arc<StreamArrowBlock>) {
        self.inner
            .entry(stream.to_string())
            .and_modify(|e| e.arrow_block = Some(Arc::clone(&block)))
            .or_insert_with(|| StreamSources {
                tables: HashMap::new(),
                arrow_block: Some(block),
            });
    }

    pub fn insert(&self, stream: &str, table: &str, source: Arc<StreamSource>) {
        self.inner
            .entry(stream.to_string())
            .and_modify(|e| {
                e.tables.insert(table.to_string(), Arc::clone(&source));
            })
            .or_insert_with(|| {
                let mut tables = HashMap::new();
                tables.insert(table.to_string(), Arc::clone(&source));
                StreamSources {
                    tables,
                    arrow_block: None,
                }
            });
    }

    pub fn get_stream(&self, stream: &str) -> Option<StreamSources> {
        self.inner.get(stream).map(|e| e.clone())
    }

    pub fn remove_stream(&self, stream: &str) {
        // Dropping StreamSources releases table Sources then the Arrow Block → pool.
        self.inner.remove(stream);
    }

    pub fn remove_table(&self, stream: &str, table: &str) {
        if let Some(mut entry) = self.inner.get_mut(stream) {
            entry.tables.remove(table);
            if entry.tables.is_empty() {
                drop(entry);
                self.inner.remove(stream);
            }
        }
    }
}
