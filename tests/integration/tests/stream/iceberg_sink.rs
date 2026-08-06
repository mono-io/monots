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

//! Iceberg sink IT: Hadoop Catalog (MinIO warehouse) + REST Catalog (Docker fixture).
//!
//! Integrity: download written Parquet from MinIO → LOAD into a receiver MonoTS
//! instance → assert COUNT / SUM / MIN / MAX / DISTINCT / region split.

use std::path::Path;
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
    require_docker_stack, require_iceberg_rest, ICEBERG_REST_URI, ICEBERG_REST_WAREHOUSE_PREFIX,
    MINIO_ACCESS_KEY, MINIO_BUCKET, MINIO_ENDPOINT, MINIO_SECRET_KEY,
};
use monots_integration_tests::{scalar_i64_named, scalar_str_named, unique_table, MonotsInstance};
use tempfile::TempDir;

const TOTAL_ROWS: usize = 2_000;
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

fn assert_integrity(stats: &[RecordBatch], rows: usize) {
    let expected_c = rows as i64;
    let expected_sum = (rows as i64 - 1) * rows as i64 / 2;
    assert_eq!(
        scalar_i64_named(stats, "c"),
        expected_c,
        "COUNT(*) mismatch"
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
    assert_eq!(
        scalar_i64_named(stats, "d"),
        expected_c,
        "COUNT(DISTINCT value) must equal row count"
    );
}

async fn s3_client() -> S3Client {
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
    S3Client::from_conf(s3_config)
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
                 batch_files_done={files_done} (want >={expect_files})"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Wait until MinIO has Iceberg metadata + at least `min_parquet` data files under `prefix`.
async fn wait_iceberg_objects(
    s3: &S3Client,
    prefix: &str,
    min_parquet: usize,
    timeout: Duration,
) -> Vec<String> {
    let start = Instant::now();
    loop {
        let list = s3
            .list_objects_v2()
            .bucket(MINIO_BUCKET)
            .prefix(prefix)
            .send()
            .await
            .expect("list_objects_v2");
        let keys: Vec<String> = list
            .contents()
            .iter()
            .filter_map(|o| o.key().map(str::to_string))
            .collect();
        let parquet = keys.iter().filter(|k| k.ends_with(".parquet")).count();
        let has_hint = keys.iter().any(|k| k.ends_with("version-hint.text"));
        let has_meta = keys.iter().any(|k| k.ends_with(".metadata.json"));

        // Hadoop writes version-hint; REST exposes *.metadata.json under the table path.
        if parquet >= min_parquet && (has_hint || has_meta) {
            return keys;
        }
        if start.elapsed() > timeout {
            panic!(
                "Iceberg objects incomplete under s3://{MINIO_BUCKET}/{prefix} within {timeout:?}: \
                 parquet={parquet}/{min_parquet} hint={has_hint} meta={has_meta} keys={keys:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn download_parquets(
    s3: &S3Client,
    keys: &[String],
    _prefix: &str,
    dest: &Path,
) -> Result<usize, Box<dyn std::error::Error>> {
    // MonoTS `LOAD PARQUET <dir>` is non-recursive — flatten data files into `dest`.
    tokio::fs::create_dir_all(dest).await?;
    let mut n = 0usize;
    for key in keys {
        if !key.ends_with(".parquet") {
            continue;
        }
        let file_name = key.rsplit('/').next().unwrap_or(key);
        if file_name.is_empty() {
            continue;
        }
        let local = dest.join(file_name);
        let get = s3.get_object().bucket(MINIO_BUCKET).key(key).send().await?;
        let bytes = get.body.collect().await?.into_bytes();
        tokio::fs::write(&local, &bytes).await?;
        n += 1;
    }
    Ok(n)
}

async fn verify_via_load(
    recv: &mut sdk::Client,
    download_path: &Path,
    receiver: &str,
    rows: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    recv.no_query(&format!(
        "CREATE TABLE {receiver} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
    ))
    .await?;

    let loaded = recv
        .no_query(&format!(
            "LOAD PARQUET '{}' INTO {receiver}",
            download_path.display()
        ))
        .await?;
    assert_eq!(loaded, rows as u64, "LOAD PARQUET row count mismatch");

    let stats = recv
        .query(&format!(
            "SELECT COUNT(*) AS c, SUM(value) AS s, MIN(value) AS vmin, MAX(value) AS vmax, \
             COUNT(DISTINCT value) AS d FROM {receiver}"
        ))
        .await?;
    assert_integrity(&stats, rows);

    let regions = recv
        .query(&format!(
            "SELECT region, COUNT(*) AS c FROM {receiver} GROUP BY region ORDER BY region"
        ))
        .await?;
    assert_eq!(
        scalar_i64_named(&regions, "c"),
        rows as i64 / 2,
        "east/west region split mismatch"
    );
    Ok(())
}

/// Hadoop Catalog + MinIO warehouse: end-to-end write + integrity.
#[tokio::test]
async fn iceberg_hadoop_minio_flush_integrity() -> Result<(), Box<dyn std::error::Error>> {
    require_docker_stack().await?;

    let table = unique_table("ice_h_src");
    let stream = unique_table("ice_h_stream");
    let ns = unique_table("ice_h_ns");
    let ice_table = unique_table("ice_h_tbl");
    let prefix = unique_table("iceberg-hadoop");
    let warehouse = format!("s3://{MINIO_BUCKET}/{prefix}");
    let receiver = unique_table("ice_h_dst");

    let s3 = s3_client().await;

    let mut sender = MonotsInstance::new("iceberg_hadoop_minio")?;
    let mut recv = MonotsInstance::new("iceberg_hadoop_minio_recv")?;
    sender.start().await?;
    recv.start().await?;

    let mut client = sender.authenticated_client().await?;
    let mut recv_client = recv.authenticated_client().await?;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await?;

    client
        .no_query(&format!(
            "CREATE STREAM {stream} WITH (
              'sink.type' = 'iceberg',
              'sink.iceberg.catalog-type' = 'hadoop',
              'sink.iceberg.catalog-name' = 'it_hadoop',
              'sink.iceberg.warehouse' = '{warehouse}',
              'sink.iceberg.namespace' = '{ns}',
              'sink.iceberg.table' = '{ice_table}',
              'sink.iceberg.endpoint' = '{MINIO_ENDPOINT}',
              'sink.iceberg.access.key' = '{MINIO_ACCESS_KEY}',
              'sink.iceberg.secret.key' = '{MINIO_SECRET_KEY}',
              'sink.iceberg.region' = 'us-east-1',
              'sink.iceberg.path.style.access' = 'true',
              'source.table' = '{table}',
              'cdc.mode' = 'batch'
            )"
        ))
        .await?;

    let detail = client.query(&format!("SHOW STREAM {stream}")).await?;
    let ddl = scalar_str_named(&detail, "create_statement");
    assert!(
        ddl.contains("'sink.iceberg.catalog-type' = 'hadoop'"),
        "{ddl}"
    );
    assert!(
        ddl.contains(&format!("'sink.iceberg.endpoint' = '{MINIO_ENDPOINT}'")),
        "{ddl}"
    );

    write_flush_batches(&mut client, &table, 0, TOTAL_ROWS).await;
    wait_stream_files(
        &mut client,
        &stream,
        TOTAL_ROWS / FLUSH_EVERY,
        Duration::from_secs(180),
    )
    .await;

    // Table lives under warehouse/{ns}/{table}/…
    let table_prefix = format!("{prefix}/{ns}/{ice_table}");
    let keys = wait_iceberg_objects(
        &s3,
        &table_prefix,
        TOTAL_ROWS / FLUSH_EVERY,
        Duration::from_secs(180),
    )
    .await;

    let temp = TempDir::new()?;
    let downloaded = download_parquets(&s3, &keys, &table_prefix, temp.path()).await?;
    assert!(
        downloaded >= TOTAL_ROWS / FLUSH_EVERY,
        "expected >= {} parquet files, got {downloaded}",
        TOTAL_ROWS / FLUSH_EVERY
    );

    verify_via_load(&mut recv_client, temp.path(), &receiver, TOTAL_ROWS).await?;
    eprintln!(
        "Iceberg Hadoop+MinIO IT passed: {TOTAL_ROWS} rows, {downloaded} parquet files verified"
    );
    Ok(())
}

/// REST Catalog fixture + MinIO warehouse: end-to-end write + integrity.
#[tokio::test]
async fn iceberg_rest_minio_flush_integrity() -> Result<(), Box<dyn std::error::Error>> {
    require_iceberg_rest().await?;

    let table = unique_table("ice_r_src");
    let stream = unique_table("ice_r_stream");
    let ns = unique_table("ice_r_ns");
    let ice_table = unique_table("ice_r_tbl");
    let receiver = unique_table("ice_r_dst");
    // REST fixture warehouse is fixed; isolate by unique namespace/table under it.
    let warehouse = format!("s3://{MINIO_BUCKET}/{ICEBERG_REST_WAREHOUSE_PREFIX}");

    let s3 = s3_client().await;

    let mut sender = MonotsInstance::new("iceberg_rest_minio")?;
    let mut recv = MonotsInstance::new("iceberg_rest_minio_recv")?;
    sender.start().await?;
    recv.start().await?;

    let mut client = sender.authenticated_client().await?;
    let mut recv_client = recv.authenticated_client().await?;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await?;

    client
        .no_query(&format!(
            "CREATE STREAM {stream} WITH (
              'sink.type' = 'iceberg',
              'sink.iceberg.catalog-type' = 'rest',
              'sink.iceberg.catalog-name' = 'it_rest',
              'sink.iceberg.uri' = '{ICEBERG_REST_URI}',
              'sink.iceberg.warehouse' = '{warehouse}',
              'sink.iceberg.namespace' = '{ns}',
              'sink.iceberg.table' = '{ice_table}',
              'sink.iceberg.endpoint' = '{MINIO_ENDPOINT}',
              'sink.iceberg.access.key' = '{MINIO_ACCESS_KEY}',
              'sink.iceberg.secret.key' = '{MINIO_SECRET_KEY}',
              'sink.iceberg.region' = 'us-east-1',
              'sink.iceberg.path.style.access' = 'true',
              'source.table' = '{table}',
              'cdc.mode' = 'batch'
            )"
        ))
        .await?;

    let detail = client.query(&format!("SHOW STREAM {stream}")).await?;
    let ddl = scalar_str_named(&detail, "create_statement");
    assert!(
        ddl.contains("'sink.iceberg.catalog-type' = 'rest'"),
        "{ddl}"
    );
    assert!(
        ddl.contains(&format!("'sink.iceberg.uri' = '{ICEBERG_REST_URI}'")),
        "{ddl}"
    );

    write_flush_batches(&mut client, &table, 0, TOTAL_ROWS).await;
    wait_stream_files(
        &mut client,
        &stream,
        TOTAL_ROWS / FLUSH_EVERY,
        Duration::from_secs(240),
    )
    .await;

    // REST warehouse layout: {warehouse}/{ns}/{table}/… (JdbcCatalog default location).
    let table_prefix = format!("{ICEBERG_REST_WAREHOUSE_PREFIX}/{ns}/{ice_table}");
    let keys = wait_iceberg_objects(
        &s3,
        &table_prefix,
        TOTAL_ROWS / FLUSH_EVERY,
        Duration::from_secs(240),
    )
    .await;

    let temp = TempDir::new()?;
    let downloaded = download_parquets(&s3, &keys, &table_prefix, temp.path()).await?;
    assert!(
        downloaded >= TOTAL_ROWS / FLUSH_EVERY,
        "expected >= {} parquet files, got {downloaded}",
        TOTAL_ROWS / FLUSH_EVERY
    );

    verify_via_load(&mut recv_client, temp.path(), &receiver, TOTAL_ROWS).await?;
    eprintln!(
        "Iceberg REST+MinIO IT passed: {TOTAL_ROWS} rows, {downloaded} parquet files verified"
    );
    Ok(())
}
