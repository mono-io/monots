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

fn metrics_batch(start_value: i64, count: usize) -> RecordBatch {
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
    .unwrap()
}

/// Create a non-partitioned persistent topic via Admin REST (idempotent).
async fn ensure_topic(topic_leaf: &str) {
    // Admin path: /admin/v2/persistent/{tenant}/{namespace}/{topic}
    let url = format!("{PULSAR_ADMIN_URL}/admin/v2/persistent/public/default/{topic_leaf}");
    let client = reqwest::Client::new();
    let resp = client.put(&url).send().await.expect("create topic HTTP");
    let status = resp.status();
    // 204 Created / 409 AlreadyExists are both fine.
    if !(status.is_success() || status.as_u16() == 409) {
        let body = resp.text().await.unwrap_or_default();
        panic!("create Pulsar topic failed: HTTP {status}: {body}");
    }
}

async fn make_consumer(topic: &str) -> Consumer<Vec<u8>, TokioExecutor> {
    let pulsar: Pulsar<_> = Pulsar::builder(PULSAR_SERVICE_URL, TokioExecutor)
        .build()
        .await
        .expect("pulsar client");
    pulsar
        .consumer()
        .with_topic(topic)
        .with_subscription(format!("monots-it-{topic}"))
        .with_subscription_type(SubType::Exclusive)
        .with_options(ConsumerOptions::default())
        .build()
        .await
        .expect("pulsar consumer")
}

async fn consume_and_verify(
    consumer: &mut Consumer<Vec<u8>, TokioExecutor>,
    expect: usize,
    start_idx: i64,
    overall: Duration,
) -> usize {
    let deadline = Instant::now() + overall;
    let mut got = 0usize;
    while got < expect {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let msg = match timeout(remaining, consumer.try_next()).await {
            Ok(Ok(Some(msg))) => msg,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("pulsar consume error: {e}"),
            Err(_) => break,
        };
        let payload_bytes = msg.deserialize();
        let payload = String::from_utf8(payload_bytes).expect("utf8 payload");
        let row: MetricsRow = serde_json::from_str(&payload).expect("json payload");
        let expected = generate_expected_row(start_idx + got as i64);
        assert_eq!(row, expected, "payload mismatch at offset {got}");
        consumer.ack(&msg).await.expect("ack");
        got += 1;
    }
    got
}

async fn wait_stream_not_failed(client: &mut sdk::Client, stream: &str) {
    let status = client
        .query(&format!("SHOW STREAM STATUS FOR {stream}"))
        .await
        .unwrap();
    let phase = scalar_str_named(&status, "phase");
    if matches!(phase.as_str(), "failed" | "suspended") {
        panic!("stream {stream} terminal phase={phase}; status={status:?}");
    }
}

#[tokio::test]
async fn pulsar_sink_flush_integrity() {
    require_pulsar()
        .await
        .expect("Docker Pulsar stack required for pulsar sink IT");

    let table = unique_table("pulsar_src");
    let stream = unique_table("pulsar_stream");
    let topic_leaf = unique_table("pulsar_topic");
    let topic = format!("persistent://public/default/{topic_leaf}");

    ensure_topic(&topic_leaf).await;

    let mut inst = MonotsInstance::new("pulsar_sink_it").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!(
            "CREATE TABLE {table} (time BIGINT NOT NULL, region VARCHAR, value BIGINT)"
        ))
        .await
        .unwrap();

    client
        .no_query(&format!(
            "CREATE STREAM {stream} WITH (
              'sink.type' = 'pulsar',
              'sink.pulsar.topic' = '{topic}',
              'sink.pulsar.service-url' = '{PULSAR_SERVICE_URL}',
              'sink.pulsar.admin-url' = '{PULSAR_ADMIN_URL}',
              'sink.pulsar.delivery-guarantee' = 'at-least-once',
              'sink.pulsar.message-router' = 'round-robin',
              'source.table' = '{table}',
              'sink.format' = 'json',
              'cdc.mode' = 'batch'
            )"
        ))
        .await
        .unwrap();

    let mut consumer = make_consumer(&topic).await;

    let mut written = 0usize;
    while written < TOTAL_ROWS {
        let n = (TOTAL_ROWS - written).min(FLUSH_EVERY);
        let batch = metrics_batch(written as i64, n);
        let rows = client.write_batches(&table, vec![batch]).await.unwrap();
        assert_eq!(rows, n as u64);
        client
            .no_query(&format!("FLUSH TABLE {table}"))
            .await
            .unwrap();
        written += n;
        wait_stream_not_failed(&mut client, &stream).await;
    }

    let got = consume_and_verify(&mut consumer, TOTAL_ROWS, 0, Duration::from_secs(120)).await;
    assert_eq!(got, TOTAL_ROWS, "Did not consume expected Pulsar messages");

    let status = client
        .query(&format!("SHOW STREAM STATUS FOR {stream}"))
        .await
        .unwrap();
    let files_done = scalar_i64_named(&status, "batch_files_done");
    assert!(
        files_done >= (TOTAL_ROWS / FLUSH_EVERY) as i64,
        "batch_files_done={files_done}"
    );
}
