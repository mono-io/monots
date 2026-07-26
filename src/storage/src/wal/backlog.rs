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

use std::sync::Arc;
use tokio::sync::Semaphore;

/// A single `acquire_many` call is bounded by `u32`; batches are far smaller than this in practice.
fn permits(limit: usize, bytes: usize) -> u32 {
    bytes.min(limit).min(u32::MAX as usize) as u32
}

/// Per-table slice of the shared WAL backlog, backed by its own [`Semaphore`].
///
/// Bytes are acquired on enqueue and returned once the WAL worker persists them. Waiters sleep on
/// the semaphore (zero CPU) instead of spinning.
pub struct TableBacklog {
    sem: Semaphore,
    limit: usize,
}

impl TableBacklog {
    pub fn new(limit: usize) -> Self {
        let limit = limit.max(1).min(Semaphore::MAX_PERMITS);
        Self {
            sem: Semaphore::new(limit),
            limit,
        }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn used_bytes(&self) -> usize {
        self.limit.saturating_sub(self.sem.available_permits())
    }
}

/// Shared WAL backlog budget across all tables in one engine.
///
/// Enqueues reserve against both a global [`Semaphore`] and the caller's [`TableBacklog`]; the WAL
/// worker releases both once the batch is durable. Reservations block on the semaphores rather than
/// busy-looping, so writers waiting on a full budget consume no CPU.
pub struct WalBacklogBudget {
    global: Semaphore,
    global_limit: usize,
    table_limit: usize,
}

impl WalBacklogBudget {
    pub fn new(global_limit: usize, table_limit: usize) -> Self {
        let global_limit = global_limit.max(1).min(Semaphore::MAX_PERMITS);
        Self {
            global: Semaphore::new(global_limit),
            global_limit,
            table_limit: table_limit.max(1),
        }
    }

    pub fn global_limit(&self) -> usize {
        self.global_limit
    }

    pub fn table_limit(&self) -> usize {
        self.table_limit
    }

    pub fn global_used(&self) -> usize {
        self.global_limit
            .saturating_sub(self.global.available_permits())
    }

    /// Allocate a per-table backlog slice sized to this budget's table cap.
    pub fn new_table_backlog(self: &Arc<Self>) -> Arc<TableBacklog> {
        Arc::new(TableBacklog::new(self.table_limit))
    }

    /// Reserve `bytes` under both the per-table and global caps, sleeping until they are available.
    pub async fn reserve(&self, table: &TableBacklog, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let table_n = permits(self.table_limit, bytes);
        let global_n = permits(self.global_limit, bytes);
        // Consistent acquire order (table then global) across all callers avoids deadlock.
        if let Ok(p) = table.sem.acquire_many(table_n).await {
            p.forget();
        }
        if let Ok(p) = self.global.acquire_many(global_n).await {
            p.forget();
        }
    }

    /// Non-blocking reservation; returns false without side effects when either cap is exhausted.
    pub fn try_reserve(&self, table: &TableBacklog, bytes: usize) -> bool {
        if bytes == 0 {
            return true;
        }
        let table_n = permits(self.table_limit, bytes);
        let global_n = permits(self.global_limit, bytes);
        match table.sem.try_acquire_many(table_n) {
            Ok(table_permit) => match self.global.try_acquire_many(global_n) {
                Ok(global_permit) => {
                    table_permit.forget();
                    global_permit.forget();
                    true
                }
                // `table_permit` drops here, returning its permits to the per-table semaphore.
                Err(_) => false,
            },
            Err(_) => false,
        }
    }

    /// Return `bytes` to both caps, waking any writers blocked in [`Self::reserve`].
    pub fn release(&self, table: &TableBacklog, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.global
            .add_permits(permits(self.global_limit, bytes) as usize);
        table
            .sem
            .add_permits(permits(self.table_limit, bytes) as usize);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_cap_blocks_across_tables() {
        let budget = WalBacklogBudget::new(100, 80);
        let table_a = TableBacklog::new(80);
        let table_b = TableBacklog::new(80);
        let table_c = TableBacklog::new(80);
        assert!(budget.try_reserve(&table_a, 60));
        assert!(budget.try_reserve(&table_b, 30));
        assert!(!budget.try_reserve(&table_c, 20));
        budget.release(&table_a, 60);
        assert!(budget.try_reserve(&table_c, 20));
    }

    #[test]
    fn per_table_cap_prevents_monopoly() {
        let budget = WalBacklogBudget::new(10_000, 100);
        let hot = TableBacklog::new(100);
        let other = TableBacklog::new(100);
        assert!(budget.try_reserve(&hot, 100));
        assert!(!budget.try_reserve(&hot, 1));
        assert!(budget.try_reserve(&other, 50));
    }

    #[tokio::test]
    async fn reserve_wakes_on_release_without_spinning() {
        let budget = Arc::new(WalBacklogBudget::new(64, 64));
        let table = budget.new_table_backlog();
        budget.reserve(&table, 64).await;
        assert_eq!(budget.global_used(), 64);

        let budget2 = budget.clone();
        let table2 = table.clone();
        let waiter = tokio::spawn(async move {
            budget2.reserve(&table2, 32).await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "waiter should be parked while full");
        budget.release(&table, 64);
        waiter.await.unwrap();
        assert_eq!(budget.global_used(), 32);
    }
}
