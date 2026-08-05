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

//! Kafka sink IT (Docker): write 10k rows, flush every 1k, consume and verify JSON payloads.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use monots_integration_tests::framework::docker::{require_docker_stack, KAFKA_BOOTSTRAP};
use monots_integration_tests::{scalar_i64_named, scalar_str_named, unique_table, MonotsInstance};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use serde::Deserialize;
use tokio::time::timeout;

const TOTAL_ROWS: usize = 10_000;
const FLUSH_EVERY: usize = 1_000;
const BASE_TS: i64 = 1_700_000_000_000;

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct MetricsRow {
    time: i64,
    region: String,
    value: i64,
}

/// Shared row logic for writers and Kafka payload checks (`idx` is the global value).
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

async fn ensure_topic(topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BOOTSTRAP)
        .create()
        .expect("create kafka admin client");
    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(15)));
    let results = admin
        .create_topics(std::slice::from_ref(&new_topic), &opts)
        .await
        .expect("CreateTopics RPC failed");
    for result in results {
        match result {
            Ok(name) => {
                eprintln!("kafka topic ready: {name}");
            }
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => panic!("failed to create kafka topic {name}: {code:?}"),
        }
    }
}

fn make_consumer(topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BOOTSTRAP)
        .set("group.id", format!("monots-it-{topic}"))
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        // Do not rely on consumer-side auto-create; topic is created via AdminClient.
        .set("allow.auto.create.topics", "false")
        .create()
        .expect("create kafka consumer");
    consumer.subscribe(&[topic]).expect("subscribe kafka topic");
    consumer
}

fn is_retryable_kafka_error(err: &KafkaError) -> bool {
    matches!(
        err.rdkafka_error_code(),
        Some(
            RDKafkaErrorCode::UnknownTopicOrPartition
                | RDKafkaErrorCode::LeaderNotAvailable
                | RDKafkaErrorCode::NotLeaderForPartition
                | RDKafkaErrorCode::NetworkException
        )
    )
}

/// Consume `want` JSON rows and verify each payload against `generate_expected_row`.
///
/// Rows are buffered then sorted by `value` so multi-partition delivery order does not
/// falsely fail content checks.
async fn consume_and_verify(
    consumer: &StreamConsumer,
    want: usize,
    base_value: i64,
    timeout_all: Duration,
) -> usize {
    let start = Instant::now();
    let mut got = Vec::with_capacity(want);
    while got.len() < want {
        if start.elapsed() > timeout_all {
            break;
        }
        match timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(msg)) => {
                if let Some(payload) = msg.payload() {
                    let json_str =
                        std::str::from_utf8(payload).expect("Kafka payload is not valid UTF-8");
                    let actual_row: MetricsRow =
                        serde_json::from_str(json_str).unwrap_or_else(|e| {
                            panic!("Failed to parse JSON: {e}. Payload: {json_str}")
                        });
                    got.push(actual_row);
                }
            }
            Ok(Err(e)) if is_retryable_kafka_error(&e) => {
                // Topic/metadata can lag briefly after CreateTopics or first produce.
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Ok(Err(e)) => panic!("kafka consumer error: {e}"),
            Err(_) => {
                // idle poll; keep waiting until outer timeout
            }
        }
    }

    got.sort_by_key(|r| r.value);
    for (i, actual_row) in got.iter().enumerate() {
        let current_idx = base_value + i as i64;
        let expected_row = generate_expected_row(current_idx);
        assert_eq!(
            actual_row, &expected_row,
            "Data mismatch at local i={i}, global idx={current_idx}.\nExpected: {expected_row:?}\nActual: {actual_row:?}"
        );
    }
    got.len()
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
async fn kafka_sink_flush_1k_write_10k_consumable() {
    require_docker_stack()
        .await
        .expect("Docker Kafka/MinIO stack required for kafka sink IT");

    let table = unique_table("kafka_src");
    let stream = unique_table("kafka_stream");
    let topic = unique_table("kafka_topic");

    // Create topic before consumer subscribe / producer write.
    ensure_topic(&topic).await;

    let mut inst = MonotsInstance::new("kafka_sink_it").unwrap();
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
              'sink.type' = 'kafka',
              'sink.kafka.brokers' = '{KAFKA_BOOTSTRAP}',
              'sink.kafka.topic' = '{topic}',
              'source.table' = '{table}',
              'sink.format' = 'json',
              'cdc.mode' = 'batch'
            )"
        ))
        .await
        .unwrap();

    let consumer = make_consumer(&topic);

    // Phase 1: first 1000 rows — content-verify before continuing.
    {
        let batch = metrics_batch(0, FLUSH_EVERY);
        let rows = client.write_batches(&table, vec![batch]).await.unwrap();
        assert_eq!(rows, FLUSH_EVERY as u64);
        client
            .no_query(&format!("FLUSH TABLE {table}"))
            .await
            .unwrap();
        wait_stream_not_failed(&mut client, &stream).await;

        let got = consume_and_verify(&consumer, FLUSH_EVERY, 0, Duration::from_secs(90)).await;
        assert_eq!(
            got, FLUSH_EVERY,
            "Did not consume expected count in Phase 1"
        );
    }

    // Phase 2: write remaining 9k (flush every 1k).
    let mut written = FLUSH_EVERY;
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
    }

    wait_stream_not_failed(&mut client, &stream).await;

    // Phase 3: content-verify remaining 9k.
    let remaining = TOTAL_ROWS - FLUSH_EVERY;
    let got_rest = consume_and_verify(
        &consumer,
        remaining,
        FLUSH_EVERY as i64,
        Duration::from_secs(120),
    )
    .await;
    assert_eq!(
        got_rest, remaining,
        "Did not consume expected count in Phase 2"
    );

    let status = client
        .query(&format!("SHOW STREAM STATUS FOR {stream}"))
        .await
        .unwrap();
    let files_done = scalar_i64_named(&status, "batch_files_done");
    assert!(
        files_done >= (TOTAL_ROWS / FLUSH_EVERY) as i64,
        "batch_files_done={files_done}, want >= {}",
        TOTAL_ROWS / FLUSH_EVERY
    );
}

/// Keyed JSON + producer tuning: verify every key/value pair and full-set integrity
/// (count, uniqueness, region split, key↔value consistency).
#[tokio::test]
async fn kafka_sink_keyed_tuning_complete() {
    require_docker_stack()
        .await
        .expect("Docker Kafka/MinIO stack required for kafka keyed IT");

    const ROWS: usize = 4_000;
    const FLUSH: usize = 1_000;

    let table = unique_table("kafka_key_src");
    let stream = unique_table("kafka_key_stream");
    let topic = unique_table("kafka_key_topic");
    ensure_topic(&topic).await;

    let mut inst = MonotsInstance::new("kafka_sink_keyed").unwrap();
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
              'sink.type' = 'kafka',
              'sink.kafka.brokers' = '{KAFKA_BOOTSTRAP}',
              'sink.kafka.topic' = '{topic}',
              'sink.format' = 'json',
              'sink.kafka.key.format' = 'json',
              'sink.kafka.key.fields' = 'value',
              'sink.kafka.key.fields-prefix' = 'k_',
              'sink.kafka.partitioner' = 'default',
              'sink.kafka.delivery-guarantee' = 'at-least-once',
              'sink.kafka.compression.type' = 'lz4',
              'sink.kafka.batch.size' = '65536',
              'sink.kafka.linger.ms' = '5',
              'sink.kafka.acks' = 'all',
              'sink.kafka.retries' = '5',
              'source.table' = '{table}',
              'cdc.mode' = 'batch'
            )"
        ))
        .await
        .unwrap();

    let show = client
        .query(&format!("SHOW STREAM {stream}"))
        .await
        .unwrap();
    let ddl = scalar_str_named(&show, "create_statement");
    assert!(ddl.contains("key.fields"), "{ddl}");
    assert!(ddl.contains("compression.type"), "{ddl}");
    assert!(ddl.contains("lz4"), "{ddl}");

    let consumer = make_consumer(&topic);

    let mut written = 0usize;
    while written < ROWS {
        let n = (ROWS - written).min(FLUSH);
        let batch = metrics_batch(written as i64, n);
        client.write_batches(&table, vec![batch]).await.unwrap();
        client
            .no_query(&format!("FLUSH TABLE {table}"))
            .await
            .unwrap();
        written += n;
    }
    wait_stream_not_failed(&mut client, &stream).await;

    #[derive(Deserialize, Debug)]
    struct KeyPayload {
        k_value: i64,
    }

    let start = Instant::now();
    let mut rows = Vec::with_capacity(ROWS);
    while rows.len() < ROWS {
        if start.elapsed() > Duration::from_secs(120) {
            break;
        }
        match timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(msg)) => {
                let payload = msg.payload().expect("value payload");
                let key = msg.key().expect("key payload required for keyed sink");
                let value: MetricsRow = serde_json::from_slice(payload).unwrap();
                let key_obj: KeyPayload = serde_json::from_slice(key).unwrap_or_else(|e| {
                    panic!("bad key json: {e}; key={}", String::from_utf8_lossy(key))
                });
                assert_eq!(
                    key_obj.k_value, value.value,
                    "key.k_value must equal value.value"
                );
                let expected = generate_expected_row(value.value);
                assert_eq!(value, expected);
                rows.push(value);
            }
            Ok(Err(e)) if is_retryable_kafka_error(&e) => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Ok(Err(e)) => panic!("kafka consumer error: {e}"),
            Err(_) => {}
        }
    }

    assert_eq!(rows.len(), ROWS, "must consume all keyed rows");
    rows.sort_by_key(|r| r.value);
    // Uniqueness + contiguous 0..N-1
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.value, i as i64);
        assert_eq!(*row, generate_expected_row(i as i64));
    }
    let east = rows.iter().filter(|r| r.region == "east").count();
    let west = rows.iter().filter(|r| r.region == "west").count();
    assert_eq!(east, ROWS / 2);
    assert_eq!(west, ROWS / 2);
    let sum: i64 = rows.iter().map(|r| r.value).sum();
    assert_eq!(sum, (ROWS as i64 - 1) * ROWS as i64 / 2);

    eprintln!("kafka keyed IT passed: {ROWS} rows with key/value integrity");
}
