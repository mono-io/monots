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

//! Shared boot/assert helpers for integration tests.

use arrow::array::{
    Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::record_batch::RecordBatch;
use pretty_assertions::assert_eq;

use crate::framework::instance::MonotsInstance;

pub const TIME_COL: &str = "time";
pub const VALUE_COL: &str = "value";

/// Fresh MonoTS process + authenticated client for one test case.
pub struct TestContext {
    pub inst: MonotsInstance,
    pub client: sdk::Client,
}

impl TestContext {
    pub async fn new(test_name: &str) -> Self {
        let mut inst = MonotsInstance::new(test_name).expect("create MonotsInstance");
        inst.start().await.expect("start monots-server");
        let client = inst
            .authenticated_client()
            .await
            .expect("authenticated client");
        Self { inst, client }
    }

    /// Re-authenticate after [`MonotsInstance::restart`].
    pub async fn refresh_client(&mut self) {
        self.client = self
            .inst
            .authenticated_client()
            .await
            .expect("re-authenticate after restart");
    }

    pub fn assert_ms_timestamp_col(&self, rows: &[RecordBatch], col_name: &str, expected: &[i64]) {
        assert!(!rows.is_empty(), "result batches should not be empty");
        let col = rows[0].column_by_name(col_name).unwrap_or_else(|| {
            panic!(
                "column `{col_name}` not found; schema={:?}",
                rows[0].schema()
            )
        });
        let arr = col
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap_or_else(|| {
                panic!(
                    "column `{col_name}` is not TimestampMillisecond; type={:?}",
                    col.data_type()
                )
            });
        let actual: Vec<i64> = (0..arr.len()).map(|i| arr.value(i)).collect();
        assert_eq!(actual, expected, "timestamp(ms) mismatch in `{col_name}`");
    }

    pub fn assert_us_timestamp_col(&self, rows: &[RecordBatch], col_name: &str, expected: &[i64]) {
        assert!(!rows.is_empty(), "result batches should not be empty");
        let col = rows[0]
            .column_by_name(col_name)
            .unwrap_or_else(|| panic!("column `{col_name}` not found"));
        let arr = col
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap_or_else(|| panic!("column `{col_name}` is not TimestampMicrosecond"));
        let actual: Vec<i64> = (0..arr.len()).map(|i| arr.value(i)).collect();
        assert_eq!(actual, expected);
    }

    pub fn assert_s_timestamp_col(&self, rows: &[RecordBatch], col_name: &str, expected: &[i64]) {
        assert!(!rows.is_empty(), "result batches should not be empty");
        let col = rows[0]
            .column_by_name(col_name)
            .unwrap_or_else(|| panic!("column `{col_name}` not found"));
        let arr = col
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap_or_else(|| panic!("column `{col_name}` is not TimestampSecond"));
        let actual: Vec<i64> = (0..arr.len()).map(|i| arr.value(i)).collect();
        assert_eq!(actual, expected);
    }

    pub fn assert_ns_timestamp_col(&self, rows: &[RecordBatch], col_name: &str, expected: &[i64]) {
        assert!(!rows.is_empty(), "result batches should not be empty");
        let col = rows[0]
            .column_by_name(col_name)
            .unwrap_or_else(|| panic!("column `{col_name}` not found"));
        let arr = col
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap_or_else(|| panic!("column `{col_name}` is not TimestampNanosecond"));
        let actual: Vec<i64> = (0..arr.len()).map(|i| arr.value(i)).collect();
        assert_eq!(actual, expected);
    }
}

/// Extract `create_statement` from `SHOW CREATE TABLE` result.
pub fn show_create_statement(rows: &[RecordBatch]) -> String {
    rows[0]
        .column_by_name("create_statement")
        .expect("create_statement column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("create_statement is Utf8")
        .value(0)
        .to_string()
}

/// Assert default table scan is non-decreasing in `time` **without** shipping all rows.
///
/// Uses a single aggregate over adjacent `LAG(time)` pairs so the server streams the scan
/// and the client only receives one integer (inversion count). Prefer this over pulling the
/// full result and calling [`crate::is_sorted_by_time`] for anything beyond tiny fixtures.
pub async fn assert_time_scan_non_decreasing(client: &mut sdk::Client, table: &str) {
    // `OVER ()` follows physical input order (MonoTS SST/memtable scan is time-ordered).
    // Do **not** add `ORDER BY time` here — that would sort before LAG and hide disorder.
    let sql = format!(
        "SELECT COUNT(*) AS inversions FROM (
           SELECT {TIME_COL} AS t,
                  LAG({TIME_COL}) OVER () AS prev_t
           FROM {table}
         ) s
         WHERE prev_t IS NOT NULL AND t < prev_t"
    );
    let rows = client
        .query(&sql)
        .await
        .unwrap_or_else(|e| panic!("time-order check query failed on {table}: {e}"));
    let inversions = crate::helpers::scalar_i64_named(&rows, "inversions");
    assert_eq!(
        inversions, 0,
        "scan of `{table}` has {inversions} time inversions (expected fully sorted ascending)"
    );

    // Cheap endpoints: first scanned row must be the global MIN(time).
    let first = client
        .query(&format!("SELECT {TIME_COL} AS t FROM {table} LIMIT 1"))
        .await
        .unwrap();
    let min = client
        .query(&format!("SELECT MIN({TIME_COL}) AS t FROM {table}"))
        .await
        .unwrap();
    assert_eq!(
        crate::helpers::scalar_i64_named(&first, "t"),
        crate::helpers::scalar_i64_named(&min, "t"),
        "first scan row must equal MIN(time)"
    );
}

/// Assert an error message mentions at least one of the needles (case-insensitive).
pub fn assert_err_contains(err: impl std::fmt::Display, needles: &[&str]) {
    let msg = err.to_string().to_lowercase();
    let ok = needles.iter().any(|n| msg.contains(&n.to_lowercase()));
    assert!(
        ok,
        "expected error to contain one of {needles:?}, got: {msg}"
    );
}
