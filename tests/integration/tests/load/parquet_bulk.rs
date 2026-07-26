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

//! LOAD PARQUET integration tests: happy path, schema/null guards, LSN invariants.

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use monots_integration_tests::{
    assert_err_contains, list_sst_files, scalar_i64_named, total_rows, unique_table,
    write_i64_parquet, TestContext, TIME_COL,
};
use monots_storage::parse_sst_filename;
use parquet::arrow::ArrowWriter;
use pretty_assertions::assert_eq;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

fn write_parquet(path: &Path, timestamps: &[i64], values: &[i64]) {
    write_i64_parquet(path, timestamps, values);
}

/// Parquet may declare columns nullable even when catalog forbids nulls.
fn write_null_parquet(path: &Path, time_null: bool, value_null: bool) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
    ]));

    let times: Vec<Option<i64>> = if time_null {
        vec![Some(100), None, Some(300)]
    } else {
        vec![Some(100), Some(200), Some(300)]
    };
    let vals: Vec<Option<i64>> = if value_null {
        vec![Some(10), None, Some(30)]
    } else {
        vec![Some(10), Some(20), Some(30)]
    };

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(times)),
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .unwrap();
    let file = fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn write_bad_schema_parquet(path: &Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64])),
            Arc::new(StringArray::from(vec!["bad"])),
        ],
    )
    .unwrap();
    let file = fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn write_int32_time_parquet(path: &Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int32, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1000])),
            Arc::new(Int64Array::from(vec![Some(10)])),
        ],
    )
    .unwrap();
    let file = fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn write_utf8_time_parquet(path: &Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Utf8, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["2024-01-01"])),
            Arc::new(Int64Array::from(vec![Some(10)])),
        ],
    )
    .unwrap();
    let file = fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Stream a large Parquet without holding the full batch in one vec of Options.
fn write_large_parquet(path: &Path, rows: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let file = fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

    const CHUNK: usize = 10_000;
    let mut offset = 0usize;
    while offset < rows {
        let n = (rows - offset).min(CHUNK);
        let times: Vec<i64> = (0..n).map(|i| (offset + i) as i64).collect();
        let vals: Vec<i64> = (0..n).map(|i| ((offset + i) % 1000) as i64).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(times)),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        offset += n;
    }
    writer.close().unwrap();
}

/// Same row count as [`write_large_parquet`], but timestamps decrease (global reverse order).
fn write_large_parquet_reversed(path: &Path, rows: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let file = fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

    const CHUNK: usize = 10_000;
    let mut written = 0usize;
    while written < rows {
        let n = (rows - written).min(CHUNK);
        // Highest times first: time = (rows - 1 - written - i)
        let times: Vec<i64> = (0..n).map(|i| (rows - 1 - written - i) as i64).collect();
        let vals: Vec<i64> = times.iter().map(|t| t % 1000).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(times)),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        written += n;
    }
    writer.close().unwrap();
}

fn assert_sst_filename(name: &str) {
    assert!(name.ends_with(".parquet"), "unexpected SST name: {name}");
    let id = parse_sst_filename(name).unwrap_or_else(|e| {
        panic!("invalid SST filename {name}: {e}");
    });
    assert!(id.min_lsn > 0, "SST must seal non-zero LSN: {name}");
    assert!(id.max_lsn >= id.min_lsn, "invalid LSN span in {name}");
    assert_eq!(
        id.inner_compaction_count, 0,
        "fresh SST inner count in {name}"
    );
    assert_eq!(
        id.cross_compaction_count, 0,
        "fresh SST cross count in {name}"
    );
}

fn large_load_rows() -> usize {
    std::env::var("MONOTS_IT_BULK_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000)
}

#[tokio::test]
async fn bulk_load_parquet_via_sql_and_query() {
    let table = unique_table("bulk");
    let mut ctx = TestContext::new("bulk_load_sql").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("batch.parquet");
    write_parquet(&file, &[1000, 2000, 3000], &[10, 20, 30]);

    let rows = ctx
        .client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", file.display()))
        .await
        .unwrap();
    assert_eq!(rows, 3);

    let result = ctx
        .client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&result, "c"), 3);
}

#[tokio::test]
async fn bulk_load_parquet_via_grpc() {
    let table = unique_table("bulk_rpc");
    let mut ctx = TestContext::new("bulk_load_grpc").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let f1 = tmp.path().join("a.parquet");
    let f2 = tmp.path().join("b.parquet");
    write_parquet(&f1, &[10, 20], &[1, 2]);
    write_parquet(&f2, &[30, 40], &[3, 4]);

    let (rows, files) = ctx
        .client
        .bulk_load(&table, vec![tmp.path().to_string_lossy().to_string()])
        .await
        .unwrap();
    assert_eq!(files, 2);
    assert_eq!(rows, 4);

    let result = ctx
        .client
        .query(&format!("SELECT SUM(value) AS s FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&result, "s"), 10);
}

#[tokio::test]
async fn bulk_load_survives_restart() {
    let table = unique_table("bulk_rec");
    let mut ctx = TestContext::new("bulk_load_restart").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("import_restart.parquet");
    write_parquet(&file, &[5000, 6000], &[7, 8]);

    ctx.client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", file.display()))
        .await
        .unwrap();

    ctx.inst.restart().await.unwrap();
    ctx.refresh_client().await;
    let result = ctx
        .client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&result, "c"), 2);
}

#[tokio::test]
async fn bulk_load_rejects_schema_mismatch() {
    let table = unique_table("bulk_bad");
    let mut ctx = TestContext::new("bulk_load_bad_schema").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("bad_schema.parquet");
    write_bad_schema_parquet(&file);

    let err = ctx
        .client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", file.display()))
        .await
        .unwrap_err();
    assert_err_contains(&err, &["type mismatch", "column", "schema"]);
}

#[tokio::test]
async fn bulk_load_creates_sst_files_with_version_suffix() {
    let table = unique_table("bulk_sst");
    let mut ctx = TestContext::new("bulk_load_sst_name").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("ingest.parquet");
    write_parquet(&file, &[42], &[99]);

    ctx.client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", file.display()))
        .await
        .unwrap();

    let table_dir = ctx.inst.data_dir().join(&table);
    let ssts = list_sst_files(&table_dir);
    assert_eq!(ssts.len(), 1);
    assert_sst_filename(&ssts[0]);
}

#[tokio::test]
async fn bulk_load_combined_with_insert() {
    let table = unique_table("bulk_mix");
    let mut ctx = TestContext::new("bulk_load_mix_insert").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("cold.parquet");
    write_parquet(&file, &[100, 200], &[1, 2]);

    ctx.client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", file.display()))
        .await
        .unwrap();

    ctx.client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, value) VALUES (300, 3)"
        ))
        .await
        .unwrap();

    let result = ctx
        .client
        .query(&format!("SELECT value FROM {table} WHERE {TIME_COL} = 300"))
        .await
        .unwrap();
    assert_eq!(total_rows(&result), 1);
    assert_eq!(scalar_i64_named(&result, "value"), 3);

    let count = ctx
        .client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 3);
}

#[tokio::test]
async fn bulk_load_rejected_by_query_api() {
    let table = unique_table("bulk_route");
    let mut ctx = TestContext::new("bulk_load_query_route").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let err = ctx
        .client
        .query(&format!("LOAD PARQUET '/tmp/x.parquet' INTO {table}"))
        .await
        .unwrap_err();
    assert_err_contains(&err, &["NoQuery"]);
}

#[tokio::test]
async fn bulk_load_time_range_filter_works() {
    let table = unique_table("bulk_range");
    let mut ctx = TestContext::new("bulk_load_time_range").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("range.parquet");
    write_parquet(&file, &[1000, 2000, 3000, 4000], &[1, 2, 3, 4]);

    ctx.client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", file.display()))
        .await
        .unwrap();

    let result = ctx
        .client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {table} WHERE {TIME_COL} >= 2000 AND {TIME_COL} <= 3000"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&result, "c"), 2);
}

#[tokio::test]
async fn bulk_load_missing_table_fails() {
    let mut ctx = TestContext::new("bulk_load_no_table").await;

    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("orphan.parquet");
    write_parquet(&file, &[1], &[1]);

    let err = ctx
        .client
        .no_query(&format!(
            "LOAD PARQUET '{}' INTO not_exists_table_xyz",
            file.display()
        ))
        .await
        .unwrap_err();
    assert_err_contains(&err, &["not found", "NotFound"]);
}

#[tokio::test]
async fn bulk_load_twice_accumulates_rows() {
    let table = unique_table("bulk_twice");
    let mut ctx = TestContext::new("bulk_load_twice").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let f1 = tmp.path().join("t1.parquet");
    let f2 = tmp.path().join("t2.parquet");
    write_parquet(&f1, &[10, 20], &[1, 2]);
    write_parquet(&f2, &[30], &[3]);

    ctx.client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", f1.display()))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", f2.display()))
        .await
        .unwrap();

    let table_dir = ctx.inst.data_dir().join(&table);
    assert_eq!(list_sst_files(&table_dir).len(), 2);

    let count = ctx
        .client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 3);
}

#[tokio::test]
async fn bulk_load_invalid_sql_syntax_rejected() {
    let mut ctx = TestContext::new("bulk_load_bad_sql").await;

    let err = ctx
        .client
        .no_query("LOAD PARQUET /no/quotes INTO t")
        .await
        .unwrap_err();
    assert_err_contains(&err, &["quoted", "INTO"]);
}

#[tokio::test]
async fn bulk_load_dedupes_duplicate_timestamps_in_file() {
    let table = unique_table("bulk_dedup");
    let mut ctx = TestContext::new("bulk_dedup_file").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("dup.parquet");
    write_parquet(&file, &[100, 100, 200], &[1, 9, 2]);

    let rows = ctx
        .client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", file.display()))
        .await
        .unwrap();
    assert_eq!(rows, 2);

    let count = ctx
        .client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 2);

    let val = ctx
        .client
        .query(&format!("SELECT value FROM {table} WHERE {TIME_COL} = 100"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&val, "value"), 9);
}

#[tokio::test]
async fn bulk_load_out_of_order_parquet_becomes_fully_time_sorted() {
    use monots_integration_tests::{assert_time_scan_non_decreasing, col_i64, scalar_i64_named};

    let table = unique_table("bulk_ooo");
    let mut ctx = TestContext::new("bulk_load_out_of_order").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("ooo.parquet");
    // Deliberately disordered timestamps in the Parquet file.
    write_parquet(
        &file,
        &[5000, 1000, 4000, 2000, 3000],
        &[50, 10, 40, 20, 30],
    );

    let loaded = ctx
        .client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", file.display()))
        .await
        .unwrap();
    assert_eq!(loaded, 5);

    // Server-side order check: streams scan, returns one row (inversion count == 0).
    assert_time_scan_non_decreasing(&mut ctx.client, &table).await;

    // Payload mapping after reorder (point lookups — O(1) result size).
    assert_eq!(
        scalar_i64_named(
            &ctx.client
                .query(&format!(
                    "SELECT value FROM {table} WHERE {TIME_COL} = 1000"
                ))
                .await
                .unwrap(),
            "value"
        ),
        10
    );
    assert_eq!(
        scalar_i64_named(
            &ctx.client
                .query(&format!(
                    "SELECT value FROM {table} WHERE {TIME_COL} = 5000"
                ))
                .await
                .unwrap(),
            "value"
        ),
        50
    );
    let mid = ctx
        .client
        .query(&format!(
            "SELECT {TIME_COL}, value FROM {table} WHERE {TIME_COL} = 3000"
        ))
        .await
        .unwrap();
    assert_eq!(col_i64(&mid, TIME_COL, 0), 3000);
    assert_eq!(col_i64(&mid, "value", 0), 30);
}

#[tokio::test]
async fn sql_insert_out_of_order_then_flush_is_time_sorted() {
    use monots_integration_tests::{assert_time_scan_non_decreasing, scalar_i64_named};

    let table = unique_table("sql_ooo");
    let mut ctx = TestContext::new("sql_insert_out_of_order").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    ctx.client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, value) VALUES
             (300, 3), (100, 1), (200, 2)"
        ))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!("FLUSH TABLE {table}"))
        .await
        .unwrap();

    assert_time_scan_non_decreasing(&mut ctx.client, &table).await;
    assert_eq!(
        scalar_i64_named(
            &ctx.client
                .query(&format!("SELECT value FROM {table} WHERE {TIME_COL} = 100"))
                .await
                .unwrap(),
            "value"
        ),
        1
    );
    assert_eq!(
        scalar_i64_named(
            &ctx.client
                .query(&format!("SELECT value FROM {table} WHERE {TIME_COL} = 300"))
                .await
                .unwrap(),
            "value"
        ),
        3
    );
}

#[tokio::test]
async fn bulk_load_large_out_of_order_parquet_stays_time_sorted() {
    use monots_integration_tests::assert_time_scan_non_decreasing;

    // Large enough to matter for streaming checks; override with MONOTS_IT_BULK_ROWS.
    let rows = large_load_rows().min(50_000);
    let table = unique_table("bulk_ooo_large");
    let mut ctx = TestContext::new("bulk_load_ooo_large").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("ooo_large.parquet");
    // Reverse-time chunks → forces bulk-load disorder / sort path.
    write_large_parquet_reversed(&file, rows);

    let loaded = ctx
        .client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", file.display()))
        .await
        .unwrap();
    assert_eq!(loaded as usize, rows);

    let count = ctx
        .client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), rows as i64);

    // Must not pull `rows` to the client — only inversion aggregate + endpoints.
    assert_time_scan_non_decreasing(&mut ctx.client, &table).await;
}

#[tokio::test]
async fn bulk_load_rejects_null_in_not_null_value_column() {
    use monots_core::metadata::catalog::ColumnDef;
    use monots_integration_tests::ts_col;

    let table = unique_table("bulk_null_err");
    let mut ctx = TestContext::new("bulk_load_not_null").await;

    // SQL CREATE always marks non-time columns nullable=true; use SDK to enforce NOT NULL.
    ctx.client
        .create_table(
            &table,
            vec![
                ts_col(),
                ColumnDef {
                    name: "value".into(),
                    data_type: "Int64".into(),
                    nullable: false,
                },
            ],
        )
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("not_null_err.parquet");
    write_null_parquet(&file, false, true);

    let err = ctx
        .client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", file.display()))
        .await
        .expect_err("expected NOT NULL violation on value");
    assert_err_contains(&err, &["null", "nullable", "cannot be null"]);
}

#[tokio::test]
async fn bulk_load_rejects_null_in_time_column() {
    let table = unique_table("bulk_time_null");
    let mut ctx = TestContext::new("bulk_load_time_null").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("time_null.parquet");
    write_null_parquet(&file, true, false);

    let err = ctx
        .client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", file.display()))
        .await
        .unwrap_err();
    assert_err_contains(&err, &["null", "time", "cannot be null"]);
}

#[tokio::test]
async fn bulk_load_rejects_non_bigint_time_column() {
    let table = unique_table("bulk_time_err");
    let mut ctx = TestContext::new("bulk_load_time_type").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let int32_file = tmp.path().join("time_i32.parquet");
    write_int32_time_parquet(&int32_file);
    let err = ctx
        .client
        .no_query(&format!(
            "LOAD PARQUET '{}' INTO {table}",
            int32_file.display()
        ))
        .await
        .unwrap_err();
    assert_err_contains(&err, &["time", "type mismatch", "Int32", "Int64"]);

    let utf8_file = tmp.path().join("time_utf8.parquet");
    write_utf8_time_parquet(&utf8_file);
    let err = ctx
        .client
        .no_query(&format!(
            "LOAD PARQUET '{}' INTO {table}",
            utf8_file.display()
        ))
        .await
        .unwrap_err();
    assert_err_contains(&err, &["time", "type mismatch", "Utf8", "Int64"]);
}

#[tokio::test]
async fn bulk_load_consecutive_loads_maintain_lsn_ordering() {
    let table = unique_table("bulk_lsn");
    let mut ctx = TestContext::new("bulk_load_lsn").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let f1 = tmp.path().join("lsn1.parquet");
    let f2 = tmp.path().join("lsn2.parquet");
    write_parquet(&f1, &[1], &[1]);
    write_parquet(&f2, &[2], &[2]);

    ctx.client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", f1.display()))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", f2.display()))
        .await
        .unwrap();

    // Subsequent SQL insert + flush must also allocate LSN above bulk loads.
    ctx.client
        .no_query(&format!(
            "INSERT INTO {table} ({TIME_COL}, value) VALUES (3, 3)"
        ))
        .await
        .unwrap();
    ctx.client
        .no_query(&format!("FLUSH TABLE {table}"))
        .await
        .unwrap();

    let table_dir = ctx.inst.data_dir().join(&table);
    let ssts = list_sst_files(&table_dir);
    assert!(
        ssts.len() >= 3,
        "expected bulk SSTs + flush SST, got {ssts:?}"
    );

    let mut ids: Vec<_> = ssts
        .iter()
        .map(|name| {
            assert_sst_filename(name);
            parse_sst_filename(name).unwrap()
        })
        .collect();
    ids.sort_by_key(|id| id.min_lsn);

    for w in ids.windows(2) {
        assert!(
            w[1].min_lsn > w[0].max_lsn,
            "LSN must be globally monotone: {:?} then {:?}",
            w[0],
            w[1]
        );
    }
}

#[tokio::test]
async fn bulk_load_large_parquet_smoke() {
    let rows = large_load_rows();
    let table = unique_table("bulk_large");
    let mut ctx = TestContext::new("bulk_load_large").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("large.parquet");
    write_large_parquet(&file, rows);

    let loaded = ctx
        .client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", file.display()))
        .await
        .unwrap();
    assert_eq!(loaded as usize, rows);

    let count = ctx
        .client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), rows as i64);

    let sample = ctx
        .client
        .query(&format!(
            "SELECT SUM(value) AS s FROM {table} WHERE {TIME_COL} < 1000"
        ))
        .await
        .unwrap();
    // values are i % 1000 for i in 0..1000 → sum 0..999
    assert_eq!(scalar_i64_named(&sample, "s"), 999 * 1000 / 2);
}

#[tokio::test]
async fn bulk_load_rejects_corrupt_parquet_file() {
    let table = unique_table("bulk_corrupt");
    let mut ctx = TestContext::new("bulk_load_corrupt").await;

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, value BIGINT)"
        ))
        .await
        .unwrap();

    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("broken.parquet");
    fs::write(&file, b"this is not a parquet file at all").unwrap();

    let err = ctx
        .client
        .no_query(&format!("LOAD PARQUET '{}' INTO {table}", file.display()))
        .await
        .unwrap_err();
    assert_err_contains(
        &err,
        &[
            "parquet", "parse", "footer", "magic", "invalid", "corrupt", "eof",
        ],
    );
}
