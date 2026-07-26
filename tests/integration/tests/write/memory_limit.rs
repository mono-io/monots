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

use monots_core::metadata::catalog::ColumnDef;
use monots_integration_tests::{ts_col, unique_table, MonotsInstance};

#[tokio::test]
async fn large_insert_stays_within_memory_limit() {
    let mut inst =
        MonotsInstance::with_memory_limits("memory_large_write", 512 * 1024, 8 * 1024 * 1024)
            .unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let table = unique_table("mem");
    client
        .create_table(
            &table,
            vec![
                ts_col(),
                ColumnDef {
                    name: "value".into(),
                    data_type: "Float64".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();

    let mut ts_base = 1_700_000_000_000_i64;
    let mut rows_inserted = 0_u64;
    for _batch_idx in 0..40 {
        let mut values = String::new();
        for i in 0..2000 {
            if i > 0 {
                values.push(',');
            }
            values.push_str(&format!("({}, {})", ts_base + i as i64, i));
        }
        ts_base += 2000;
        rows_inserted += client
            .no_query(&format!(
                "INSERT INTO {table} (time, value) VALUES {values}"
            ))
            .await
            .unwrap();
    }
    assert_eq!(rows_inserted, 80_000);

    let rows = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    let count = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 80_000);
}
