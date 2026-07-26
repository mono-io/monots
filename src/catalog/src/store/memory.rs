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

//! In-memory byte budget for metadata caches (schemas + manifests).

use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, Copy)]
pub struct MetaMemoryStats {
    pub used_bytes: usize,
    pub limit_bytes: usize,
    pub over_budget: bool,
}

/// Tracks approximate metadata RAM usage and enforces a soft cap.
pub struct MetaMemoryBudget {
    limit_bytes: usize,
    used_bytes: AtomicUsize,
}

impl MetaMemoryBudget {
    pub fn new(limit_bytes: usize) -> Self {
        Self {
            limit_bytes: limit_bytes.max(1),
            used_bytes: AtomicUsize::new(0),
        }
    }

    pub fn limit_bytes(&self) -> usize {
        self.limit_bytes
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes.load(Ordering::Relaxed)
    }

    pub fn stats(&self) -> MetaMemoryStats {
        let used = self.used_bytes();
        MetaMemoryStats {
            used_bytes: used,
            limit_bytes: self.limit_bytes,
            over_budget: used > self.limit_bytes,
        }
    }

    pub fn try_reserve(&self, bytes: usize) -> bool {
        if bytes == 0 {
            return true;
        }
        loop {
            let current = self.used_bytes.load(Ordering::Relaxed);
            if current.saturating_add(bytes) > self.limit_bytes {
                return false;
            }
            if self
                .used_bytes
                .compare_exchange_weak(
                    current,
                    current + bytes,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return true;
            }
        }
    }

    pub fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.used_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn reset(&self, bytes: usize) {
        self.used_bytes.store(bytes, Ordering::Relaxed);
    }
}

pub fn estimate_dashmap_bytes(
    schemas: &dashmap::DashMap<String, proto::meta::TableSchema>,
    manifests: &dashmap::DashMap<String, proto::meta::TableManifest>,
) -> usize {
    let mut n = 64;
    for entry in schemas.iter() {
        n += entry.key().len() + 32;
        let v = entry.value();
        n += v.table_name.len() + v.data_dir.len();
        for c in &v.columns {
            n += c.name.len() + c.data_type.len() + 16;
        }
    }
    for entry in manifests.iter() {
        n += entry.key().len() + 32;
        for f in &entry.value().files {
            n += f.file_path.len() + 64;
        }
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_reserve_release() {
        let b = MetaMemoryBudget::new(100);
        assert!(b.try_reserve(50));
        assert!(b.try_reserve(40));
        assert!(!b.try_reserve(20));
        b.release(50);
        assert!(b.try_reserve(20));
    }
}
