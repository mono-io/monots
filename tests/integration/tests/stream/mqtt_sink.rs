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

//! MQTT sink IT (Docker Mosquitto): write rows, flush, subscribe and verify JSON integrity.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use monots_integration_tests::framework::docker::{require_mqtt, MQTT_BROKER_URL};
use monots_integration_tests::{scalar_i64_named, scalar_str_named, unique_table, MonotsInstance};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use serde::Deserialize;
use tokio::sync::mpsc;

const TOTAL_ROWS: usize = 500;
const FLUSH_EVERY: usize = 250;
const BASE_TS: i64 = 1_700_000_000_000;

#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
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

/// Start a subscriber that forwards JSON payloads to a channel.
async fn start_subscriber(
    topic: &str,
) -> Result<(mpsc::Receiver<MetricsRow>, tokio::task::JoinHandle<()>), String> {
    let client_id = format!("monots-it-sub-{}", uuid::Uuid::new_v4());
    // Strip tcp:// for rumqttc host/port constructor.
    let url = MQTT_BROKER_URL.trim_start_matches("tcp://");
    let (host, port) = match url.rsplit_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse::<u16>()
                .map_err(|e| format!("bad MQTT port: {e}"))?,
        ),
        None => return Err(format!("bad MQTT_BROKER_URL: {MQTT_BROKER_URL}")),
    };

    let mut opts = MqttOptions::new(client_id, host, port);
    opts.set_keep_alive(Duration::from_secs(30));
    opts.set_clean_session(true);

    let (client, mut eventloop) = AsyncClient::new(opts, 64);
    client
        .subscribe(topic, QoS::AtLeastOnce)
        .await
        .map_err(|e| format!("mqtt subscribe: {e}"))?;

    let (tx, rx) = mpsc::channel(TOTAL_ROWS * 2);
    let handle = tokio::spawn(async move {
        // Drain ConnAck / SubAck, then collect publishes.
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    let Ok(payload) = String::from_utf8(p.payload.to_vec()) else {
                        continue;
                    };
                    let Ok(row) = serde_json::from_str::<MetricsRow>(&payload) else {
                        continue;
                    };
                    if tx.send(row).await.is_err() {
                        break;
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    // Brief settle so SUBSCRIBE is processed before the sink publishes.
    tokio::time::sleep(Duration::from_millis(300)).await;
    Ok((rx, handle))
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
            "value set mismatch: got {} sorted values, want 0..{}",
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
async fn mqtt_sink_flush_integrity() -> Result<(), String> {
    require_mqtt()
        .await
        .map_err(|e| format!("Docker Mosquitto required: {e}"))?;

    let table = unique_table("mqtt_src");
    let stream = unique_table("mqtt_stream");
    let topic = unique_table("mqtt/topic");

    let (mut rx, sub_handle) = start_subscriber(&topic).await?;

    let mut inst = MonotsInstance::new("mqtt_sink_it")?;
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
              'sink.type' = 'mqtt',
              'sink.mqtt.url' = '{MQTT_BROKER_URL}',
              'sink.mqtt.topic' = '{topic}',
              'sink.mqtt.qos' = '1',
              'sink.mqtt.clean-session' = 'true',
              'source.table' = '{table}',
              'sink.format' = 'json',
              'cdc.mode' = 'batch'
            )"
        ))
        .await
        .map_err(|e| format!("CREATE STREAM: {e}"))?;

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

    let mut got = Vec::with_capacity(TOTAL_ROWS);
    let collect_deadline = Instant::now() + Duration::from_secs(60);
    while got.len() < TOTAL_ROWS {
        let remaining = collect_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining.min(Duration::from_secs(2)), rx.recv()).await {
            Ok(Some(row)) => got.push(row),
            Ok(None) => break,
            Err(_) => {}
        }
    }

    sub_handle.abort();
    check_integrity(&got, TOTAL_ROWS)?;
    Ok(())
}
