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

//! K-way async queue over multiple [`StreamSource`]s (min-LSN multiplexing).

use std::sync::Arc;

use tokio::sync::Notify;

use super::source::StreamSource;
use crate::model::event::DataEvent;

/// Event routed from a specific capture table.
#[derive(Debug)]
pub struct RoutedEvent {
    pub table: Arc<str>,
    pub event: DataEvent,
}

/// Virtual queue over one or more sources, ordered by global min LSN.
pub struct AsyncSourceGroup {
    sources: Vec<(Arc<str>, Arc<StreamSource>)>,
    shared_notify: Arc<Notify>,
}

impl AsyncSourceGroup {
    /// Panics if `sources` is empty (empty group would hang forever in [`Self::recv`]).
    pub fn new(sources: Vec<(String, Arc<StreamSource>)>) -> Self {
        assert!(
            !sources.is_empty(),
            "AsyncSourceGroup requires at least one source to prevent deadlocks"
        );

        let shared_notify = Arc::new(Notify::new());
        let sources = sources
            .into_iter()
            .map(|(table, source)| {
                source.attach_notify(Arc::clone(&shared_notify));
                (Arc::<str>::from(table), source)
            })
            .collect();

        shared_notify.notify_one();
        Self {
            sources,
            shared_notify,
        }
    }

    /// Cancellation-safe: mutation only happens in the sync `try_pop_min_lsn` path.
    pub async fn recv(&self) -> RoutedEvent {
        loop {
            if let Some(routed) = self.try_pop_min_lsn() {
                return routed;
            }
            self.shared_notify.notified().await;
        }
    }

    pub fn peek_head_lsn(&self) -> Option<u64> {
        self.find_best_source().map(|(_, lsn)| lsn)
    }

    pub fn peek_next(&self) -> Option<RoutedEvent> {
        let (idx, _) = self.find_best_source()?;
        let (table, source) = &self.sources[idx];
        source.peek_next().map(|event| RoutedEvent {
            table: Arc::clone(table),
            event,
        })
    }

    fn try_pop_min_lsn(&self) -> Option<RoutedEvent> {
        let (idx, _) = self.find_best_source()?;
        let (table, source) = &self.sources[idx];
        source.pop_next().map(|event| RoutedEvent {
            table: Arc::clone(table),
            event,
        })
    }

    #[inline]
    fn find_best_source(&self) -> Option<(usize, u64)> {
        self.sources
            .iter()
            .enumerate()
            .filter_map(|(i, (_, src))| src.peek_head_lsn().map(|lsn| (i, lsn)))
            .min_by_key(|&(_, lsn)| lsn)
    }

    pub fn teardown(&self) {
        for (_, source) in &self.sources {
            source.detach_notify();
        }
    }
}

impl Drop for AsyncSourceGroup {
    fn drop(&mut self) {
        self.teardown();
    }
}
