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

//! Delta sink IT: local lake + MinIO (`sink.delta.endpoint`) via Docker.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::Client as S3Client;
use monots_integration_tests::framework::docker::{
    require_docker_stack, MINIO_ACCESS_KEY, MINIO_BUCKET, MINIO_ENDPOINT, MINIO_SECRET_KEY,
};
use monots_integration_tests::{scalar_i64_named, scalar_str_named, unique_table, MonotsInstance};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;

const TOTAL_ROWS: usize = 5_000;
const FLUSH_EVERY: usize = 1_000;
const BASE_TS: i64 = 1_700_000_000_000;

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

fn list_parquet_files(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.extension()
                    .and_then(|x| x.to_str())
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"))
        })
        .collect()
}

fn count_parquet_rows(dir: &Path) -> u64 {
    let mut total = 0u64;
    for path in list_parquet_files(dir) {
        let file = fs::File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build().unwrap();
        while let Some(batch) = reader.next() {
            total += batch.unwrap().num_rows() as u64;
        }
    }
    total
}

async fn wait_delta_ready(
    client: &mut sdk::Client,
    stream: &str,
    lake: &Path,
    expect_files: usize,
    expect_rows: u64,
    timeout: Duration,
) {
    let start = Instant::now();
    loop {
        let status = client
            .query(&format!("SHOW STREAM STATUS FOR {stream}"))
            .await
            .unwrap();
        let files_done = scalar_i64_named(&status, "batch_files_done");
        let phase = scalar_str_named(&status, "phase");
        let files = list_parquet_files(lake).len();
        let rows = count_parquet_rows(lake);
        let has_log = lake.join("_delta_log").is_dir();

        if has_log && files >= expect_files && rows >= expect_rows {
            return;
        }
        if matches!(phase.as_str(), "failed" | "suspended") {
            panic!(
                "stream {stream} terminal phase={phase}; files_done={files_done} \
                 parquet_files={files} rows={rows} lake={}",
                lake.display()
            );
        }
        if start.elapsed() > timeout {
            panic!(
                "delta export incomplete within {timeout:?}: phase={phase} \
                 batch_files_done={files_done} parquet_files={files} rows={rows} \
                 _delta_log={} (want files>={expect_files} rows>={expect_rows}) lake={}",
                has_log,
                lake.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn write_flush_batches(client: &mut sdk::Client, table: &str, from: usize, to: usize) {
    let mut written = from;
    while written < to {
        let n = (to - written).min(FLUSH_EVERY);
        let batch = metrics_batch(BASE_TS + written as i64, written as i64, n);
        let rows = client.write_batches(table, vec![batch]).await.unwrap();
        assert_eq!(rows, n as u64);
        client
            .no_query(&format!("FLUSH TABLE {table}"))
            .await
            .unwrap();
        written += n;
    }
}

fn assert_data_stats(stats: &[RecordBatch]) {
    let expected_c = TOTAL_ROWS as i64;
    let expected_sum = (TOTAL_ROWS as i64 - 1) * TOTAL_ROWS as i64 / 2;

    assert_eq!(
        scalar_i64_named(stats, "c"),
        expected_c,
        "Row count mismatch"
    );
    assert_eq!(
        scalar_i64_named(stats, "s"),
        expected_sum,
        "SUM(value) mismatch"
    );
    assert_eq!(scalar_i64_named(stats, "vmin"), 0, "MIN(value) mismatch");
    assert_eq!(
        scalar_i64_named(stats, "vmax"),
        expected_c - 1,
        "MAX(value) mismatch"
    );
}

#[tokio::test]
async fn delta_local_flush_1k_write_queryable() -> Result<(), Box<dyn std::error::Error>> {
    let table = unique_table("delta_src");
    let stream = unique_table("delta_stream");
    let receiver = unique_table("delta_dst");

    let mut sender = MonotsInstance::new("delta_sink_local")?;
    let mut recv = MonotsInstance::new("delta_sink_local_recv")?;
    sender.start().await?;
    recv.start().await?;

    let lake = sender.data_dir().join("delta_lake").join(&table);
    fs::create_dir_all(&lake)?;
    let lake_path = lake.display().to_string();

    let mut sender_client = sender.authenticated_client().await?;
    let mut recv_client = recv.authenticated_client().await?;

    sender_client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await?;
    recv_client
        .no_query(&format!(
            "CREATE TABLE {receiver} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await?;

    sender_client
        .no_query(&format!(
            "CREATE STREAM {stream} WITH (
              'sink.type' = 'delta',
              'sink.delta.path' = '{lake_path}',
              'source.table' = '{table}',
              'cdc.mode' = 'batch'
            )"
        ))
        .await?;

    write_flush_batches(&mut sender_client, &table, 0, TOTAL_ROWS).await;

    wait_delta_ready(
        &mut sender_client,
        &stream,
        &lake,
        TOTAL_ROWS / FLUSH_EVERY,
        TOTAL_ROWS as u64,
        Duration::from_secs(180),
    )
    .await;

    assert_eq!(count_parquet_rows(&lake), TOTAL_ROWS as u64);

    let loaded = recv_client
        .no_query(&format!(
            "LOAD PARQUET '{}' INTO {receiver}",
            lake.display()
        ))
        .await?;
    assert_eq!(loaded, TOTAL_ROWS as u64);

    let stats = recv_client
        .query(&format!(
            "SELECT COUNT(*) AS c, SUM(value) AS s, MIN(value) AS vmin, MAX(value) AS vmax, \
             COUNT(DISTINCT value) AS d FROM {receiver}"
        ))
        .await?;
    assert_data_stats(&stats);
    assert_eq!(
        scalar_i64_named(&stats, "d"),
        TOTAL_ROWS as i64,
        "distinct values must cover full range"
    );

    let regions = recv_client
        .query(&format!(
            "SELECT region, COUNT(*) AS c FROM {receiver} GROUP BY region ORDER BY region"
        ))
        .await?;
    // ORDER BY region → east first
    assert_eq!(scalar_i64_named(&regions, "c"), TOTAL_ROWS as i64 / 2);

    Ok(())
}

/// Local CREATE with only path still materializes industrial S3 client defaults in SHOW CREATE.
#[tokio::test]
async fn delta_local_show_create_emits_s3_client_defaults() -> Result<(), Box<dyn std::error::Error>>
{
    let table = unique_table("delta_cfg_src");
    let stream = unique_table("delta_cfg_stream");

    let mut inst = MonotsInstance::new("delta_sink_show_defaults")?;
    inst.start().await?;
    let mut client = inst.authenticated_client().await?;

    let lake = inst.data_dir().join("delta_lake").join(&table);
    fs::create_dir_all(&lake)?;
    let lake_path = lake.display().to_string();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await?;
    client
        .no_query(&format!(
            "CREATE STREAM {stream} WITH (
              'sink.type' = 'delta',
              'sink.delta.path' = '{lake_path}',
              'source.table' = '{table}'
            )"
        ))
        .await?;

    let detail = client.query(&format!("SHOW STREAM {stream}")).await?;
    let ddl = scalar_str_named(&detail, "create_statement");
    assert!(
        ddl.contains("'sink.delta.region' = 'us-east-1'"),
        "missing region default: {ddl}"
    );
    assert!(
        ddl.contains("'sink.delta.path.style.access' = 'false'"),
        "local path should default path-style=false: {ddl}"
    );
    assert!(
        ddl.contains("'sink.delta.connection.maximum' = '500'"),
        "missing connection.maximum: {ddl}"
    );
    assert!(
        ddl.contains("'sink.delta.connection.timeout' = '200s'"),
        "timeout should render as duration: {ddl}"
    );
    assert!(
        ddl.contains("'sink.delta.attempts.maximum' = '20'"),
        "missing attempts.maximum: {ddl}"
    );
    assert!(
        !ddl.contains("rolling-policy") && !ddl.contains("autoOptimize"),
        "removed Flink-only keys must not appear: {ddl}"
    );

    // Reject removed keys at CREATE time.
    let bad = client
        .no_query(&format!(
            "CREATE STREAM {stream}_bad WITH (
              'sink.type' = 'delta',
              'sink.delta.path' = '{lake_path}/bad',
              'source.table' = '{table}',
              'delta.autoOptimize.optimizeWrite' = 'true'
            )"
        ))
        .await;
    assert!(bad.is_err(), "removed autoOptimize key should be rejected");

    Ok(())
}

/// MinIO with full SQL-side S3 knobs (DDL credentials + duration timeout), no AWS_* env.
#[tokio::test]
async fn delta_minio_full_sql_config_write_verify() -> Result<(), Box<dyn std::error::Error>> {
    require_docker_stack().await?;

    const ROWS: usize = 2_000;
    const FLUSH: usize = 1_000;

    let table = unique_table("delta_full_src");
    let stream = unique_table("delta_full_stream");
    let prefix = unique_table("delta_full");
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

    // Intentionally omit AWS_* env — credentials come from DDL only.
    let mut inst_sender = MonotsInstance::new("delta_sink_minio_full_cfg")?;
    let mut inst_recv = MonotsInstance::new("delta_sink_minio_full_cfg_recv")?;
    inst_sender.start().await?;
    inst_recv.start().await?;

    let mut client_sender = inst_sender.authenticated_client().await?;
    let mut client_recv = inst_recv.authenticated_client().await?;

    let temp_dir = TempDir::new()?;
    let download_path = temp_dir.path();
    let receiver = unique_table("delta_full_dst");

    client_sender
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await?;
    client_recv
        .no_query(&format!(
            "CREATE TABLE {receiver} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await?;

    client_sender
        .no_query(&format!(
            "CREATE STREAM {stream} WITH (
              'sink.type' = 'delta',
              'sink.delta.path' = '{s3_uri}',
              'sink.delta.endpoint' = '{MINIO_ENDPOINT}',
              'sink.delta.access.key' = '{MINIO_ACCESS_KEY}',
              'sink.delta.secret.key' = '{MINIO_SECRET_KEY}',
              'sink.delta.region' = 'us-east-1',
              'sink.delta.path.style.access' = 'true',
              'sink.delta.connection.maximum' = '64',
              'sink.delta.connection.timeout' = '3 min',
              'sink.delta.attempts.maximum' = '10',
              'source.table' = '{table}',
              'cdc.mode' = 'batch'
            )"
        ))
        .await?;

    let detail = client_sender
        .query(&format!("SHOW STREAM {stream}"))
        .await?;
    let ddl = scalar_str_named(&detail, "create_statement");
    assert!(
        ddl.contains(&format!("'sink.delta.endpoint' = '{MINIO_ENDPOINT}'")),
        "{ddl}"
    );
    assert!(
        ddl.contains("'sink.delta.path.style.access' = 'true'"),
        "{ddl}"
    );
    assert!(
        ddl.contains("'sink.delta.connection.maximum' = '64'"),
        "{ddl}"
    );
    assert!(
        ddl.contains("'sink.delta.connection.timeout' = '3 min'"),
        "duration must round-trip: {ddl}"
    );
    assert!(
        ddl.contains("'sink.delta.attempts.maximum' = '10'"),
        "{ddl}"
    );
    assert!(
        ddl.contains(&format!("'sink.delta.access.key' = '{MINIO_ACCESS_KEY}'")),
        "DDL credentials should appear in SHOW CREATE: {ddl}"
    );

    // Smaller closed loop than the 5k MinIO IT — config wiring is the focus.
    let mut written = 0usize;
    while written < ROWS {
        let n = (ROWS - written).min(FLUSH);
        let batch = metrics_batch(BASE_TS + written as i64, written as i64, n);
        let rows = client_sender.write_batches(&table, vec![batch]).await?;
        assert_eq!(rows, n as u64);
        client_sender
            .no_query(&format!("FLUSH TABLE {table}"))
            .await?;
        written += n;
    }

    wait_stream_files(
        &mut client_sender,
        &stream,
        ROWS / FLUSH,
        Duration::from_secs(180),
    )
    .await;

    let expect_files_num = ROWS / FLUSH;
    let prefix_slash = format!("{prefix}/");
    let start = Instant::now();
    let timeout = Duration::from_secs(180);
    loop {
        let list_objects = s3_client
            .list_objects_v2()
            .bucket(MINIO_BUCKET)
            .prefix(&prefix)
            .send()
            .await?;
        let files = list_objects.contents();
        let parquet_files_count = files
            .iter()
            .filter(|o| o.key().unwrap_or_default().ends_with(".parquet"))
            .count();
        let log_files_count = files
            .iter()
            .filter(|o| {
                let k = o.key().unwrap_or_default();
                k.contains("_delta_log") && k.ends_with(".json")
            })
            .count();

        if parquet_files_count >= expect_files_num && log_files_count >= expect_files_num {
            for object in files {
                let Some(key) = object.key() else {
                    continue;
                };
                let relative_key = key
                    .strip_prefix(&prefix_slash)
                    .or_else(|| key.strip_prefix(&prefix))
                    .unwrap_or(key)
                    .trim_start_matches('/');
                if relative_key.is_empty() {
                    continue;
                }
                let local_file_path = download_path.join(relative_key);
                if let Some(parent) = local_file_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                let get = s3_client
                    .get_object()
                    .bucket(MINIO_BUCKET)
                    .key(key)
                    .send()
                    .await?;
                let bytes = get.body.collect().await?.into_bytes();
                tokio::fs::write(&local_file_path, &bytes).await?;
            }
            break;
        }
        if start.elapsed() > timeout {
            panic!(
                "MinIO full-config incomplete: parquet={parquet_files_count}/{expect_files_num} \
                 log={log_files_count}/{expect_files_num}"
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let loaded = client_recv
        .no_query(&format!(
            "LOAD PARQUET '{}' INTO {receiver}",
            download_path.display()
        ))
        .await?;
    assert_eq!(loaded, ROWS as u64);

    let stats = client_recv
        .query(&format!(
            "SELECT COUNT(*) AS c, SUM(value) AS s, MIN(value) AS vmin, MAX(value) AS vmax \
             FROM {receiver}"
        ))
        .await?;
    let expected_c = ROWS as i64;
    let expected_sum = (ROWS as i64 - 1) * ROWS as i64 / 2;
    assert_eq!(scalar_i64_named(&stats, "c"), expected_c);
    assert_eq!(scalar_i64_named(&stats, "s"), expected_sum);
    assert_eq!(scalar_i64_named(&stats, "vmin"), 0);
    assert_eq!(scalar_i64_named(&stats, "vmax"), expected_c - 1);

    eprintln!("MinIO Delta full SQL config IT passed: {ROWS} rows");
    Ok(())
}

/// MinIO path: MonoTS LOAD does not take S3 URIs yet, so download objects then LOAD locally.
/// Credentials via process env (`AWS_*`); endpoint via DDL.
#[tokio::test]
async fn delta_minio_flush_1k_write_manual_download_verify(
) -> Result<(), Box<dyn std::error::Error>> {
    require_docker_stack().await?;

    let table = unique_table("delta_s3_src");
    let stream = unique_table("delta_s3_stream");
    let prefix = unique_table("delta_s3");
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

    let mut inst_sender = MonotsInstance::new("delta_sink_minio_sender")?
        .with_env("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
        .with_env("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
        .with_env("AWS_REGION", "us-east-1")
        .with_env("AWS_ALLOW_HTTP", "true");
    let mut inst_recv = MonotsInstance::new("delta_sink_minio_recv")?;

    inst_sender.start().await?;
    inst_recv.start().await?;

    let mut client_sender = inst_sender.authenticated_client().await?;
    let mut client_recv = inst_recv.authenticated_client().await?;

    let temp_dir = TempDir::new()?;
    let download_path = temp_dir.path();
    let receiver = unique_table("delta_s3_dst");

    client_sender
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await?;
    client_recv
        .no_query(&format!(
            "CREATE TABLE {receiver} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await?;

    client_sender
        .no_query(&format!(
            "CREATE STREAM {stream} WITH (
              'sink.type' = 'delta',
              'sink.delta.path' = '{s3_uri}',
              'sink.delta.endpoint' = '{MINIO_ENDPOINT}',
              'source.table' = '{table}',
              'cdc.mode' = 'batch'
            )"
        ))
        .await?;

    write_flush_batches(&mut client_sender, &table, 0, TOTAL_ROWS).await;

    eprintln!("Waiting for stream status done");
    wait_stream_files(
        &mut client_sender,
        &stream,
        TOTAL_ROWS / FLUSH_EVERY,
        Duration::from_secs(180),
    )
    .await;

    eprintln!("Downloading objects from MinIO for closed-loop verify");
    let start = Instant::now();
    let timeout = Duration::from_secs(180);
    let expect_files_num = TOTAL_ROWS / FLUSH_EVERY;
    let prefix_slash = format!("{prefix}/");

    loop {
        let list_objects = s3_client
            .list_objects_v2()
            .bucket(MINIO_BUCKET)
            .prefix(&prefix)
            .send()
            .await?;

        let files = list_objects.contents();
        let parquet_files_count = files
            .iter()
            .filter(|o| o.key().unwrap_or_default().ends_with(".parquet"))
            .count();
        let log_files_count = files
            .iter()
            .filter(|o| {
                let k = o.key().unwrap_or_default();
                k.contains("_delta_log") && k.ends_with(".json")
            })
            .count();

        if parquet_files_count >= expect_files_num && log_files_count >= expect_files_num {
            eprintln!(
                "MinIO data complete (parquet={parquet_files_count} log={log_files_count}), downloading..."
            );

            for object in files {
                let Some(key) = object.key() else {
                    continue;
                };
                let relative_key = key
                    .strip_prefix(&prefix_slash)
                    .or_else(|| key.strip_prefix(&prefix))
                    .unwrap_or(key)
                    .trim_start_matches('/');
                if relative_key.is_empty() {
                    continue;
                }
                let local_file_path = download_path.join(relative_key);
                if let Some(parent) = local_file_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }

                let get = s3_client
                    .get_object()
                    .bucket(MINIO_BUCKET)
                    .key(key)
                    .send()
                    .await?;
                let bytes = get.body.collect().await?.into_bytes();
                tokio::fs::write(&local_file_path, &bytes).await?;
            }
            break;
        }

        if start.elapsed() > timeout {
            panic!(
                "MinIO data incomplete within {timeout:?}: found {parquet_files_count}/{expect_files_num} \
                 parquet files, {log_files_count}/{expect_files_num} log files in s3://{MINIO_BUCKET}/{prefix}"
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    eprintln!("Receiver: LOAD downloaded PARQUET into {receiver}");
    let loaded = client_recv
        .no_query(&format!(
            "LOAD PARQUET '{}' INTO {receiver}",
            download_path.display()
        ))
        .await?;
    assert_eq!(loaded, TOTAL_ROWS as u64, "LOAD rows count mismatch");

    let stats = client_recv
        .query(&format!(
            "SELECT COUNT(*) AS c, SUM(value) AS s, MIN(value) AS vmin, MAX(value) AS vmax \
             FROM {receiver}"
        ))
        .await?;
    assert_data_stats(&stats);
    eprintln!("MinIO Delta sink IT passed: {TOTAL_ROWS} rows verified via download + LOAD");

    Ok(())
}

async fn wait_stream_files(
    client: &mut sdk::Client,
    stream: &str,
    expect_files: usize,
    timeout: Duration,
) {
    let start = Instant::now();
    loop {
        let status = client
            .query(&format!("SHOW STREAM STATUS FOR {stream}"))
            .await
            .unwrap();
        let files_done = scalar_i64_named(&status, "batch_files_done");
        let phase = scalar_str_named(&status, "phase");
        if files_done >= expect_files as i64 {
            return;
        }
        if matches!(phase.as_str(), "failed" | "suspended") {
            panic!("stream {stream} terminal phase={phase}; status={status:?}");
        }
        if start.elapsed() > timeout {
            panic!(
                "stream {stream} incomplete within {timeout:?}: phase={phase} \
                 batch_files_done={files_done} want>={expect_files}"
            );
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}
