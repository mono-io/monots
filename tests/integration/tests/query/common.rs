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

//! Shared helpers for query integration tests.
//!
//! Each `query_*.rs` file is a separate Cargo `[[test]]` binary, so this module is
//! pulled in with `#[path = "common.rs"] mod common;` rather than via `mod.rs`.
//!
//! Not every consumer uses every helper; silence unused warnings per binary.

#![allow(dead_code)]

use std::fmt::Write;

use monots_integration_tests::MonotsInstance;

/// Start a fresh MonoTS instance and return an authenticated SQL client.
pub async fn boot(name: &str) -> (MonotsInstance, sdk::Client) {
    let mut inst = MonotsInstance::new(name).unwrap();
    inst.start().await.unwrap();
    let client = inst.authenticated_client().await.unwrap();
    (inst, client)
}

/// Insert `(time, value_col)` rows with unique timestamps starting at `start_ts`.
///
/// Value at offset `i` is `value_base + i`. Chunks inserts to keep SQL statements bounded.
pub async fn insert_numeric_series(
    client: &mut sdk::Client,
    table: &str,
    value_col: &str,
    start_ts: i64,
    count: usize,
    value_base: i64,
) {
    const INSERT_CHUNK: usize = 1000;
    let mut ts = start_ts;
    let mut remaining = count;
    while remaining > 0 {
        let n = remaining.min(INSERT_CHUNK);
        let mut values = String::new();
        for i in 0..n {
            if i > 0 {
                values.push(',');
            }
            let v = value_base + (ts - start_ts) + i as i64;
            write!(&mut values, "({}, {v})", ts + i as i64).unwrap();
        }
        client
            .no_query(&format!(
                "INSERT INTO {table} (time, {value_col}) VALUES {values}"
            ))
            .await
            .unwrap();
        ts += n as i64;
        remaining -= n;
    }
}
