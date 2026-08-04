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

//! Filesystem sink IT: sender exports Parquet to local dir; receiver LOAD and verifies.

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::Client as S3Client;
use monots_integration_tests::framework::docker::{
    require_docker_stack, MINIO_ACCESS_KEY, MINIO_BUCKET, MINIO_ENDPOINT, MINIO_SECRET_KEY,
};
use monots_integration_tests::{scalar_i64_named, scalar_str_named, unique_table, MonotsInstance};
use tempfile::TempDir;

const TOTAL_ROWS: usize = 10_000;
const FLUSH_EVERY: usize = 1_000;
const BASE_TS: i64 = 1_700_000_000_000;
const EXPECTED_FILES: usize = TOTAL_ROWS / FLUSH_EVERY;

struct ExpectedStats {
    count: i64,
    sum: i64,
    tmin: i64,
    tmax: i64,
    vmin: i64,
    vmax: i64,
}

impl ExpectedStats {
    fn new() -> Self {
        Self {
            count: TOTAL_ROWS as i64,
            sum: (TOTAL_ROWS as i64 - 1) * TOTAL_ROWS as i64 / 2,
            tmin: BASE_TS,
            tmax: BASE_TS + TOTAL_ROWS as i64 - 1,
            vmin: 0,
            vmax: TOTAL_ROWS as i64 - 1,
        }
    }
}

fn metrics_batch(start_ts: i64, start_value: i64, count: usize) -> RecordBatch {
    let mut times = Vec::with_capacity(count);
    let mut regions = Vec::with_capacity(count);
    let mut values = Vec::with_capacity(count);
    for i in 0..count {
        let idx = start_value + i as i64;
        times.push(start_ts + i as i64);
        regions.push(if idx % 2 == 0 { "east" } else { "west" });
        values.push(idx);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(times)),
            Arc::new(StringArray::from(regions)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

fn count_parquet_files(dir: &Path) -> usize {
    let Ok(entries) = fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext.eq_ignore_ascii_case("parquet"))
                .unwrap_or(false)
        })
        .count()
}

async fn assert_table_stats(
    client: &mut sdk::Client,
    table_name: &str,
    label: &str,
    expected: &ExpectedStats,
) {
    let stats = client
        .query(&format!(
            "SELECT COUNT(*) AS c, SUM(value) AS s, MIN(time) AS tmin, MAX(time) AS tmax, \
             MIN(value) AS vmin, MAX(value) AS vmax FROM {table_name}"
        ))
        .await
        .unwrap_or_else(|e| panic!("[{label}] stats query failed: {e}"));

    assert_eq!(
        scalar_i64_named(&stats, "c"),
        expected.count,
        "[{label}] row count mismatch"
    );
    assert_eq!(
        scalar_i64_named(&stats, "s"),
        expected.sum,
        "[{label}] value sum mismatch"
    );
    assert_eq!(
        scalar_i64_named(&stats, "tmin"),
        expected.tmin,
        "[{label}] min time mismatch"
    );
    assert_eq!(
        scalar_i64_named(&stats, "tmax"),
        expected.tmax,
        "[{label}] max time mismatch"
    );
    assert_eq!(
        scalar_i64_named(&stats, "vmin"),
        expected.vmin,
        "[{label}] min value mismatch"
    );
    assert_eq!(
        scalar_i64_named(&stats, "vmax"),
        expected.vmax,
        "[{label}] max value mismatch"
    );
}

async fn wait_export_ready(
    client: &mut sdk::Client,
    stream: &str,
    export_dir: &Path,
    expect_files: usize,
    timeout: Duration,
) {
    let start = Instant::now();
    loop {
        let status = client
            .query(&format!("SHOW STREAM STATUS FOR {stream}"))
            .await
            .expect("failed to query stream status");

        let phase = scalar_str_named(&status, "phase");

        if matches!(phase.as_str(), "failed" | "suspended") {
            let files_done = scalar_i64_named(&status, "batch_files_done");
            let acked_lsn = scalar_i64_named(&status, "acked_lsn");
            let files = count_parquet_files(export_dir);
            panic!(
                "stream {stream} entered terminal phase={phase}; \
                 acked_lsn={acked_lsn} batch_files_done={files_done} parquet_files={files}"
            );
        }

        // On-disk file count is the hard readiness signal.
        let files = count_parquet_files(export_dir);
        if files >= expect_files {
            return;
        }

        if start.elapsed() > timeout {
            let files_done = scalar_i64_named(&status, "batch_files_done");
            let acked_lsn = scalar_i64_named(&status, "acked_lsn");
            panic!(
                "stream {stream} export incomplete within {timeout:?}: \
                 phase={phase} acked_lsn={acked_lsn} batch_files_done={files_done} \
                 parquet_files={files} (want >= {expect_files}) dir={}",
                export_dir.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn filesystem_sink_export_then_receiver_load_matches() {
    let sender_table = unique_table("fs_src");
    let receiver_table = unique_table("fs_dst");
    let stream = unique_table("fs_stream");

    let mut sender = MonotsInstance::new("fs_sink_sender").unwrap();
    let mut receiver = MonotsInstance::new("fs_sink_receiver").unwrap();

    tokio::try_join!(sender.start(), receiver.start()).expect("failed to start instances");

    let export_root = sender.data_dir().join("fs_export");
    if export_root.exists() {
        fs::remove_dir_all(&export_root).unwrap();
    }
    fs::create_dir_all(&export_root).unwrap();
    let export_path = export_root.display().to_string();

    let mut sender_client = sender.authenticated_client().await.unwrap();
    let mut receiver_client = receiver.authenticated_client().await.unwrap();

    sender_client
        .no_query(&format!(
            "CREATE TABLE {sender_table} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await
        .unwrap();
    receiver_client
        .no_query(&format!(
            "CREATE TABLE {receiver_table} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await
        .unwrap();

    sender_client
        .no_query(&format!(
            "CREATE STREAM {stream} WITH (
              'sink.type' = 'filesystem',
              'sink.filesystem.path' = '{export_path}',
              'source.table' = '{sender_table}'
            )"
        ))
        .await
        .unwrap();

    let show = sender_client.query("SHOW STREAMS").await.unwrap();
    let stream_exists = show.iter().any(|batch| {
        batch
            .column_by_name("stream_name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .is_some_and(|arr| arr.iter().any(|opt_s| opt_s == Some(stream.as_str())))
    });
    assert!(
        stream_exists,
        "filesystem stream '{stream}' should appear in SHOW STREAMS"
    );

    let mut written = 0usize;
    while written < TOTAL_ROWS {
        let n = (TOTAL_ROWS - written).min(FLUSH_EVERY);
        let batch = metrics_batch(BASE_TS + written as i64, written as i64, n);
        let rows = sender_client
            .write_batches(&sender_table, vec![batch])
            .await
            .unwrap();
        assert_eq!(rows, n as u64, "Rows written mismatch in loop");
        sender_client
            .no_query(&format!("FLUSH TABLE {sender_table}"))
            .await
            .unwrap();
        written += n;
    }

    let table_export_dir = export_root.join(&sender_table);
    wait_export_ready(
        &mut sender_client,
        &stream,
        &table_export_dir,
        EXPECTED_FILES,
        Duration::from_secs(90),
    )
    .await;

    assert_eq!(
        count_parquet_files(&table_export_dir),
        EXPECTED_FILES,
        "exact parquet file count mismatch under {}",
        table_export_dir.display()
    );

    let loaded = receiver_client
        .no_query(&format!(
            "LOAD PARQUET '{}' INTO {receiver_table}",
            table_export_dir.display()
        ))
        .await
        .unwrap();
    assert_eq!(loaded, TOTAL_ROWS as u64, "Loaded rows mismatch");

    let expected = ExpectedStats::new();
    assert_table_stats(&mut sender_client, &sender_table, "sender", &expected).await;
    assert_table_stats(&mut receiver_client, &receiver_table, "receiver", &expected).await;

    let sample_query = |table: &str| {
        format!(
            "SELECT time, region, value FROM {table}
             WHERE time = {BASE_TS} OR time = {}
             ORDER BY time",
            BASE_TS + TOTAL_ROWS as i64 - 1
        )
    };

    let sender_sample = sender_client
        .query(&sample_query(&sender_table))
        .await
        .unwrap();
    let receiver_sample = receiver_client
        .query(&sample_query(&receiver_table))
        .await
        .unwrap();

    let sender_fmt = pretty_format_batches(&sender_sample)
        .expect("format sender sample")
        .to_string();
    let receiver_fmt = pretty_format_batches(&receiver_sample)
        .expect("format receiver sample")
        .to_string();
    assert_eq!(
        sender_fmt, receiver_fmt,
        "Sample data pretty format mismatch"
    );

    // Region split must be exact (even/odd value → east/west).
    let region_stats = receiver_client
        .query(&format!(
            "SELECT region, COUNT(*) AS c, SUM(value) AS s FROM {receiver_table} GROUP BY region ORDER BY region"
        ))
        .await
        .unwrap();
    assert_eq!(
        scalar_i64_named(&region_stats, "c"),
        TOTAL_ROWS as i64 / 2,
        "first region group count (east) should be half; batches={region_stats:?}"
    );

    let distinct = receiver_client
        .query(&format!(
            "SELECT COUNT(DISTINCT value) AS d FROM {receiver_table}"
        ))
        .await
        .unwrap();
    assert_eq!(
        scalar_i64_named(&distinct, "d"),
        TOTAL_ROWS as i64,
        "values must be unique 0..N-1"
    );
}

/// `file://` URI must behave like a local path with full closed-loop integrity.
#[tokio::test]
async fn filesystem_sink_file_uri_export_load_complete() {
    let sender_table = unique_table("fs_file_src");
    let receiver_table = unique_table("fs_file_dst");
    let stream = unique_table("fs_file_stream");

    let mut sender = MonotsInstance::new("fs_sink_file_uri").unwrap();
    let mut receiver = MonotsInstance::new("fs_sink_file_uri_recv").unwrap();
    tokio::try_join!(sender.start(), receiver.start()).unwrap();

    let export_root = sender.data_dir().join("fs_file_export");
    fs::create_dir_all(&export_root).unwrap();
    let export_uri = format!("file://{}", export_root.display());

    let mut sender_client = sender.authenticated_client().await.unwrap();
    let mut receiver_client = receiver.authenticated_client().await.unwrap();

    sender_client
        .no_query(&format!(
            "CREATE TABLE {sender_table} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await
        .unwrap();
    receiver_client
        .no_query(&format!(
            "CREATE TABLE {receiver_table} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await
        .unwrap();

    sender_client
        .no_query(&format!(
            "CREATE STREAM {stream} WITH (
              'sink.type' = 'filesystem',
              'sink.filesystem.path' = '{export_uri}',
              'source.table' = '{sender_table}',
              'cdc.mode' = 'batch'
            )"
        ))
        .await
        .unwrap();

    let mut written = 0usize;
    while written < TOTAL_ROWS {
        let n = (TOTAL_ROWS - written).min(FLUSH_EVERY);
        let batch = metrics_batch(BASE_TS + written as i64, written as i64, n);
        sender_client
            .write_batches(&sender_table, vec![batch])
            .await
            .unwrap();
        sender_client
            .no_query(&format!("FLUSH TABLE {sender_table}"))
            .await
            .unwrap();
        written += n;
    }

    let table_export_dir = export_root.join(&sender_table);
    wait_export_ready(
        &mut sender_client,
        &stream,
        &table_export_dir,
        EXPECTED_FILES,
        Duration::from_secs(90),
    )
    .await;

    let loaded = receiver_client
        .no_query(&format!(
            "LOAD PARQUET '{}' INTO {receiver_table}",
            table_export_dir.display()
        ))
        .await
        .unwrap();
    assert_eq!(loaded, TOTAL_ROWS as u64);

    let expected = ExpectedStats::new();
    assert_table_stats(
        &mut receiver_client,
        &receiver_table,
        "file_uri_recv",
        &expected,
    )
    .await;

    let distinct = receiver_client
        .query(&format!(
            "SELECT COUNT(DISTINCT value) AS d FROM {receiver_table}"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&distinct, "d"), TOTAL_ROWS as i64);
}

/// Filesystem sink to MinIO (`s3://`) — download objects, LOAD, verify full integrity.
#[tokio::test]
async fn filesystem_sink_minio_export_load_complete() {
    if let Err(e) = require_docker_stack().await {
        eprintln!("SKIP filesystem MinIO IT: {e}");
        return;
    }

    const ROWS: usize = 2_000;
    const FLUSH: usize = 1_000;

    let sender_table = unique_table("fs_s3_src");
    let receiver_table = unique_table("fs_s3_dst");
    let stream = unique_table("fs_s3_stream");
    let prefix = unique_table("fs_s3");
    let s3_uri = format!("s3://{MINIO_BUCKET}/{prefix}");

    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let creds = Credentials::new(
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY,
        None,
        None,
        "minio-hardcoded",
    );
    let shared = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .credentials_provider(creds)
        .endpoint_url(MINIO_ENDPOINT)
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&shared)
        .force_path_style(true)
        .build();
    let s3_client = S3Client::from_conf(s3_config);

    let mut sender = MonotsInstance::new("fs_sink_minio").unwrap();
    let mut receiver = MonotsInstance::new("fs_sink_minio_recv").unwrap();
    tokio::try_join!(sender.start(), receiver.start()).unwrap();

    let mut sender_client = sender.authenticated_client().await.unwrap();
    let mut receiver_client = receiver.authenticated_client().await.unwrap();

    sender_client
        .no_query(&format!(
            "CREATE TABLE {sender_table} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await
        .unwrap();
    receiver_client
        .no_query(&format!(
            "CREATE TABLE {receiver_table} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await
        .unwrap();

    sender_client
        .no_query(&format!(
            "CREATE STREAM {stream} WITH (
              'sink.type' = 'filesystem',
              'sink.filesystem.path' = '{s3_uri}',
              'sink.filesystem.endpoint' = '{MINIO_ENDPOINT}',
              'sink.filesystem.access.key' = '{MINIO_ACCESS_KEY}',
              'sink.filesystem.secret.key' = '{MINIO_SECRET_KEY}',
              'sink.filesystem.region' = 'us-east-1',
              'sink.filesystem.path.style.access' = 'true',
              'source.table' = '{sender_table}',
              'cdc.mode' = 'batch'
            )"
        ))
        .await
        .unwrap();

    let mut written = 0usize;
    while written < ROWS {
        let n = (ROWS - written).min(FLUSH);
        let batch = metrics_batch(BASE_TS + written as i64, written as i64, n);
        sender_client
            .write_batches(&sender_table, vec![batch])
            .await
            .unwrap();
        sender_client
            .no_query(&format!("FLUSH TABLE {sender_table}"))
            .await
            .unwrap();
        written += n;
    }

    // Wait until S3 has expected parquet objects under table prefix.
    let object_prefix = format!("{prefix}/{sender_table}/");
    let expect_files = ROWS / FLUSH;
    let start = Instant::now();
    let timeout = Duration::from_secs(180);
    loop {
        let status = sender_client
            .query(&format!("SHOW STREAM STATUS FOR {stream}"))
            .await
            .unwrap();
        let phase = scalar_str_named(&status, "phase");
        if matches!(phase.as_str(), "failed" | "suspended") {
            panic!("filesystem minio stream terminal phase={phase}");
        }
        let list = s3_client
            .list_objects_v2()
            .bucket(MINIO_BUCKET)
            .prefix(&object_prefix)
            .send()
            .await
            .unwrap();
        let parquet_n = list
            .contents()
            .iter()
            .filter(|o| o.key().unwrap_or_default().ends_with(".parquet"))
            .count();
        if parquet_n >= expect_files {
            break;
        }
        if start.elapsed() > timeout {
            panic!("filesystem MinIO incomplete: parquet={parquet_n}/{expect_files} phase={phase}");
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    let temp = TempDir::new().unwrap();
    let download = temp.path();
    let list = s3_client
        .list_objects_v2()
        .bucket(MINIO_BUCKET)
        .prefix(&object_prefix)
        .send()
        .await
        .unwrap();
    for object in list.contents() {
        let Some(key) = object.key() else { continue };
        if !key.ends_with(".parquet") {
            continue;
        }
        let name = key.rsplit('/').next().unwrap_or(key);
        let get = s3_client
            .get_object()
            .bucket(MINIO_BUCKET)
            .key(key)
            .send()
            .await
            .unwrap();
        let bytes = get.body.collect().await.unwrap().into_bytes();
        tokio::fs::write(download.join(name), &bytes).await.unwrap();
    }

    let loaded = receiver_client
        .no_query(&format!(
            "LOAD PARQUET '{}' INTO {receiver_table}",
            download.display()
        ))
        .await
        .unwrap();
    assert_eq!(loaded, ROWS as u64, "loaded row count");

    let stats = receiver_client
        .query(&format!(
            "SELECT COUNT(*) AS c, SUM(value) AS s, MIN(value) AS vmin, MAX(value) AS vmax, \
             COUNT(DISTINCT value) AS d FROM {receiver_table}"
        ))
        .await
        .unwrap();
    let expected_c = ROWS as i64;
    let expected_sum = (ROWS as i64 - 1) * ROWS as i64 / 2;
    assert_eq!(scalar_i64_named(&stats, "c"), expected_c);
    assert_eq!(scalar_i64_named(&stats, "s"), expected_sum);
    assert_eq!(scalar_i64_named(&stats, "vmin"), 0);
    assert_eq!(scalar_i64_named(&stats, "vmax"), expected_c - 1);
    assert_eq!(scalar_i64_named(&stats, "d"), expected_c);

    eprintln!("filesystem MinIO IT passed: {ROWS} rows verified end-to-end");
}
