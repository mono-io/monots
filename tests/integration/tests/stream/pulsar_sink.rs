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

//! Pulsar sink IT (Docker): write rows, flush, consume JSON payloads and verify integrity.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use monots_integration_tests::framework::docker::{
    require_pulsar, PULSAR_ADMIN_URL, PULSAR_SERVICE_URL,
};
use monots_integration_tests::{scalar_i64_named, scalar_str_named, unique_table, MonotsInstance};
use pulsar::consumer::InitialPosition;
use pulsar::{Consumer, ConsumerOptions, Pulsar, SubType, TokioExecutor};
use serde::Deserialize;
use tokio::time::timeout;

const TOTAL_ROWS: usize = 2_000;
const FLUSH_EVERY: usize = 1_000;
const BASE_TS: i64 = 1_700_000_000_000;

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct MetricsRow {
    time: i64,
    region: String,
    value: i64,
}

fn generate_expected_row(idx: i64) -> MetricsRow {
    MetricsRow {
        time: BASE_TS + idx,
        region: if idx % 2 == 0 {
            "east".to_string()
        } else {
            "west".to_string()
        },
        value: idx,
    }
}

fn metrics_batch(start_value: i64, count: usize) -> Result<RecordBatch, String> {
    let mut times = Vec::with_capacity(count);
    let mut regions = Vec::with_capacity(count);
    let mut values = Vec::with_capacity(count);
    for i in 0..count {
        let expected = generate_expected_row(start_value + i as i64);
        times.push(expected.time);
        regions.push(expected.region);
        values.push(expected.value);
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
    .map_err(|e| format!("build metrics batch: {e}"))
}

/// Create a non-partitioned persistent topic via Admin REST (idempotent).
async fn ensure_topic(topic_leaf: &str) -> Result<(), String> {
    let client = reqwest::Client::new();

    // Standalone can briefly report broker health before `public/default` exists.
    let ns_url = format!("{PULSAR_ADMIN_URL}/admin/v2/namespaces/public/default");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let resp = client
            .get(&ns_url)
            .send()
            .await
            .map_err(|e| format!("get namespace: {e}"))?;
        if resp.status().is_success() {
            break;
        }
        if Instant::now() > deadline {
            return Err(format!(
                "Pulsar namespace public/default not ready: HTTP {}",
                resp.status()
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Admin path: /admin/v2/persistent/{tenant}/{namespace}/{topic}
    let url = format!("{PULSAR_ADMIN_URL}/admin/v2/persistent/public/default/{topic_leaf}");
    let resp = client
        .put(&url)
        .send()
        .await
        .map_err(|e| format!("create topic HTTP: {e}"))?;
    let status = resp.status();
    // 204 Created / 409 AlreadyExists are both fine.
    if !(status.is_success() || status.as_u16() == 409) {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("create Pulsar topic failed: HTTP {status}: {body}"));
    }
    Ok(())
}

async fn make_consumer(topic: &str) -> Result<Consumer<Vec<u8>, TokioExecutor>, String> {
    let pulsar: Pulsar<_> = Pulsar::builder(PULSAR_SERVICE_URL, TokioExecutor)
        .build()
        .await
        .map_err(|e| format!("pulsar client: {e}"))?;
    pulsar
        .consumer()
        .with_topic(topic)
        .with_subscription(format!("monots-it-{topic}"))
        .with_subscription_type(SubType::Exclusive)
        .with_options(ConsumerOptions::default().with_initial_position(InitialPosition::Earliest))
        .build()
        .await
        .map_err(|e| format!("pulsar consumer: {e}"))
}

async fn consume_rows(
    consumer: &mut Consumer<Vec<u8>, TokioExecutor>,
    expect: usize,
    overall: Duration,
) -> Result<Vec<MetricsRow>, String> {
    let deadline = Instant::now() + overall;
    let mut rows = Vec::with_capacity(expect);
    while rows.len() < expect {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        // Short poll; keep looping until outer deadline (same pattern as Kafka IT).
        let poll = remaining.min(Duration::from_secs(2));
        match timeout(poll, consumer.try_next()).await {
            Ok(Ok(Some(msg))) => {
                let payload_bytes = msg.deserialize();
                let payload =
                    String::from_utf8(payload_bytes).map_err(|e| format!("utf8 payload: {e}"))?;
                let row: MetricsRow = serde_json::from_str(&payload)
                    .map_err(|e| format!("json payload: {e}; payload={payload}"))?;
                consumer.ack(&msg).await.map_err(|e| format!("ack: {e}"))?;
                rows.push(row);
            }
            Ok(Ok(None)) => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Ok(Err(e)) => return Err(format!("pulsar consume error: {e}")),
            Err(_) => {
                // idle poll
            }
        }
    }
    Ok(rows)
}

fn check_integrity(got: &[MetricsRow], expect: usize) -> Result<(), String> {
    if got.len() != expect {
        return Err(format!(
            "row count mismatch: got {} want {expect}",
            got.len()
        ));
    }
    let mut values: Vec<i64> = got.iter().map(|r| r.value).collect();
    values.sort_unstable();
    let expected_values: Vec<i64> = (0..expect as i64).collect();
    if values != expected_values {
        return Err(format!(
            "value set mismatch: got {} unique/sorted values, want 0..{}",
            values.len(),
            expect.saturating_sub(1)
        ));
    }

    for row in got {
        let expected = generate_expected_row(row.value);
        if row != &expected {
            return Err(format!(
                "row content mismatch for value={}: got={row:?} want={expected:?}",
                row.value
            ));
        }
    }

    let east = got.iter().filter(|r| r.region == "east").count();
    let west = got.iter().filter(|r| r.region == "west").count();
    if east + west != expect {
        return Err(format!("region sum {east}+{west} != {expect}"));
    }
    if east != (expect + 1) / 2 || west != expect / 2 {
        return Err(format!(
            "region split east={east} west={west} expect={expect}"
        ));
    }
    Ok(())
}

async fn wait_stream_not_failed(client: &mut sdk::Client, stream: &str) -> Result<(), String> {
    let status = client
        .query(&format!("SHOW STREAM STATUS FOR {stream}"))
        .await
        .map_err(|e| format!("SHOW STREAM STATUS: {e}"))?;
    let phase = scalar_str_named(&status, "phase");
    if matches!(phase.as_str(), "failed" | "suspended") {
        return Err(format!(
            "stream {stream} terminal phase={phase}; status={status:?}"
        ));
    }
    Ok(())
}

#[tokio::test]
async fn pulsar_sink_flush_integrity() -> Result<(), String> {
    require_pulsar()
        .await
        .map_err(|e| format!("Docker Pulsar stack required: {e}"))?;

    let table = unique_table("pulsar_src");
    let stream = unique_table("pulsar_stream");
    let topic_leaf = unique_table("pulsar_topic");
    let topic = format!("persistent://public/default/{topic_leaf}");

    ensure_topic(&topic_leaf).await?;

    let mut inst = MonotsInstance::new("pulsar_sink_it")?;
    inst.start().await?;
    let mut client = inst.authenticated_client().await?;

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await
        .map_err(|e| format!("CREATE TABLE: {e}"))?;

    client
        .no_query(&format!(
            "CREATE STREAM {stream} WITH (
              'sink.type' = 'pulsar',
              'sink.pulsar.topic' = '{topic}',
              'sink.pulsar.service-url' = '{PULSAR_SERVICE_URL}',
              'sink.pulsar.admin-url' = '{PULSAR_ADMIN_URL}',
              'sink.pulsar.delivery-guarantee' = 'at-least-once',
              'source.table' = '{table}',
              'sink.format' = 'json',
              'cdc.mode' = 'batch'
            )"
        ))
        .await
        .map_err(|e| format!("CREATE STREAM: {e}"))?;

    let mut consumer = make_consumer(&topic).await?;

    let mut written = 0usize;
    while written < TOTAL_ROWS {
        let n = (TOTAL_ROWS - written).min(FLUSH_EVERY);
        let batch = metrics_batch(written as i64, n)?;
        let rows = client
            .write_batches(&table, vec![batch])
            .await
            .map_err(|e| format!("write_batches: {e}"))?;
        if rows != n as u64 {
            return Err(format!("write_batches row count: got {rows} want {n}"));
        }
        client
            .no_query(&format!("FLUSH TABLE {table}"))
            .await
            .map_err(|e| format!("FLUSH TABLE: {e}"))?;
        written += n;
        wait_stream_not_failed(&mut client, &stream).await?;
    }

    // Wait until stream has committed both flush files before draining the topic.
    let flush_deadline = Instant::now() + Duration::from_secs(90);
    loop {
        wait_stream_not_failed(&mut client, &stream).await?;
        let status = client
            .query(&format!("SHOW STREAM STATUS FOR {stream}"))
            .await
            .map_err(|e| format!("SHOW STREAM STATUS: {e}"))?;
        let files_done = scalar_i64_named(&status, "batch_files_done");
        if files_done >= (TOTAL_ROWS / FLUSH_EVERY) as i64 {
            break;
        }
        if Instant::now() > flush_deadline {
            return Err(format!(
                "stream did not finish flushes in time; status={status:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let got = consume_rows(&mut consumer, TOTAL_ROWS, Duration::from_secs(120)).await?;
    check_integrity(&got, TOTAL_ROWS)?;

    let status = client
        .query(&format!("SHOW STREAM STATUS FOR {stream}"))
        .await
        .map_err(|e| format!("SHOW STREAM STATUS: {e}"))?;
    let files_done = scalar_i64_named(&status, "batch_files_done");
    if files_done < (TOTAL_ROWS / FLUSH_EVERY) as i64 {
        return Err(format!("batch_files_done={files_done}"));
    }
    Ok(())
}
