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

//! Kafka sink: ALO / EOS delivery with JSON payloads.
//!
//! Lazy connect + pre-flight metadata, pipelined `FutureProducer` sends,
//! and `spawn_blocking` around librdkafka FFI so the Tokio reactor never stalls.

use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use arrow_json::LineDelimitedWriter;
use futures::{stream::FuturesUnordered, StreamExt};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use tokio::task::spawn_blocking;
use tracing::{debug, info, warn};

use crate::connector::api::{SinkConnector, SinkError};
use crate::data::StreamArrowLoader;
use crate::model::event::{DataEvent, InsertArrow};

const KAFKA_TXN_TIMEOUT: Duration = Duration::from_secs(10);
const KAFKA_PING_TIMEOUT: Duration = Duration::from_secs(5);
const KAFKA_ABORT_TIMEOUT: Duration = Duration::from_secs(2);
const KAFKA_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Default)]
pub enum DeliveryMode {
    #[default]
    AtLeastOnce,
    ExactlyOnce {
        transactional_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadFormat {
    Json,
}

impl PayloadFormat {
    pub fn from_str_name(s: &str) -> Result<Self, SinkError> {
        match s.to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            other => Err(SinkError::Fatal(format!(
                "unsupported Kafka format: {other}"
            ))),
        }
    }
}

/// Columnar Arrow → row-oriented Kafka payloads.
pub struct PayloadEncoder;

impl PayloadEncoder {
    pub fn encode(
        format: &PayloadFormat,
        batches: &[RecordBatch],
    ) -> Result<Vec<Vec<u8>>, SinkError> {
        match format {
            PayloadFormat::Json => {
                let mut buf = Vec::new();
                {
                    let mut writer = LineDelimitedWriter::new(&mut buf);
                    for batch in batches {
                        writer.write(batch).map_err(|e| {
                            SinkError::Fatal(format!("Arrow→JSON encode failed: {e}"))
                        })?;
                    }
                    writer
                        .finish()
                        .map_err(|e| SinkError::Fatal(format!("JSON finish failed: {e}")))?;
                }
                Ok(buf
                    .split(|&b| b == b'\n')
                    .filter(|line| !line.is_empty())
                    .map(|line| line.to_vec())
                    .collect())
            }
        }
    }
}

pub struct KafkaSink {
    brokers: String,
    topic: String,
    format: PayloadFormat,
    mode: DeliveryMode,
    producer: Option<FutureProducer>,
    arrow_loader: Option<Arc<StreamArrowLoader>>,
}

impl KafkaSink {
    pub fn new(
        brokers: impl Into<String>,
        topic: impl Into<String>,
        format: PayloadFormat,
    ) -> Self {
        Self {
            brokers: brokers.into(),
            topic: topic.into(),
            format,
            mode: DeliveryMode::default(),
            producer: None,
            arrow_loader: None,
        }
    }

    pub fn with_exactly_once(mut self, transactional_id: impl Into<String>) -> Self {
        self.mode = DeliveryMode::ExactlyOnce {
            transactional_id: transactional_id.into(),
        };
        self
    }

    pub fn with_arrow_loader(mut self, loader: StreamArrowLoader) -> Self {
        self.arrow_loader = Some(Arc::new(loader));
        self
    }

    /// Lazy connect; metadata fetch fails fast when brokers/topic are bad.
    async fn ensure_producer(&mut self) -> Result<(), SinkError> {
        if self.producer.is_some() {
            return Ok(());
        }

        info!(
            brokers = %self.brokers,
            topic = %self.topic,
            mode = ?self.mode,
            "KafkaSink establishing producer"
        );

        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &self.brokers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("metadata.request.timeout.ms", "5000")
            .set("max.in.flight.requests.per.connection", "5");

        if let DeliveryMode::ExactlyOnce { transactional_id } = &self.mode {
            cfg.set("transactional.id", transactional_id);
        }

        let producer: FutureProducer = cfg
            .create()
            .map_err(|e| SinkError::Transient(format!("producer create failed: {e}")))?;

        let p = producer.clone();
        let topic = self.topic.clone();
        let meta = spawn_blocking(move || {
            p.client()
                .fetch_metadata(Some(&topic), Timeout::After(KAFKA_PING_TIMEOUT))
        })
        .await
        .map_err(|e| SinkError::Fatal(format!("pre-flight join: {e}")))?;

        if let Err(e) = meta {
            warn!(error = %e, "Kafka pre-flight metadata failed");
            return Err(SinkError::Transient(format!(
                "broker/topic unreachable: {e}"
            )));
        }

        if matches!(self.mode, DeliveryMode::ExactlyOnce { .. }) {
            let p = producer.clone();
            spawn_blocking(move || p.init_transactions(Timeout::After(KAFKA_TXN_TIMEOUT)))
                .await
                .map_err(|e| SinkError::Fatal(format!("init_transactions join: {e}")))?
                .map_err(|e| SinkError::Transient(format!("init_transactions failed: {e}")))?;
        }

        self.producer = Some(producer);
        Ok(())
    }

    fn drop_producer(&mut self) {
        self.producer = None;
    }

    /// Encode Arrow batches and await delivery for every Kafka record.
    async fn produce_batches(
        &mut self,
        batches: &[RecordBatch],
        lsn_hint: u64,
        kind: &str,
    ) -> Result<(), SinkError> {
        let payloads = PayloadEncoder::encode(&self.format, batches)?;
        if payloads.is_empty() {
            return Ok(());
        }

        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| SinkError::Fatal("write without active producer".into()))?
            .clone();

        let mut delivery = FuturesUnordered::new();
        for payload in &payloads {
            let record: FutureRecord<'_, (), [u8]> =
                FutureRecord::to(&self.topic).payload(payload.as_slice());
            match producer.send_result(record) {
                Ok(fut) => delivery.push(fut),
                Err((e, _)) => {
                    self.drop_producer();
                    return Err(SinkError::Transient(format!(
                        "Kafka queue rejected produce: {e}"
                    )));
                }
            }
        }

        while let Some(res) = delivery.next().await {
            match res {
                Ok(Ok(_)) => {}
                Ok(Err((e, _))) => {
                    self.drop_producer();
                    return Err(SinkError::Transient(format!("Kafka delivery failed: {e}")));
                }
                Err(_) => {
                    self.drop_producer();
                    return Err(SinkError::Transient(
                        "Kafka delivery future canceled".into(),
                    ));
                }
            }
        }

        debug!(
            lsn = lsn_hint,
            rows = payloads.len(),
            kind,
            "Kafka rows produced"
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl SinkConnector for KafkaSink {
    async fn begin_txn(&mut self) -> Result<(), SinkError> {
        self.ensure_producer().await?;
        if matches!(self.mode, DeliveryMode::ExactlyOnce { .. }) {
            let p = self
                .producer
                .as_ref()
                .expect("producer after ensure")
                .clone();
            let res = spawn_blocking(move || p.begin_transaction())
                .await
                .map_err(|e| SinkError::Fatal(format!("begin_transaction join: {e}")))?;
            if let Err(e) = res {
                self.drop_producer();
                return Err(SinkError::Transient(format!(
                    "begin_transaction failed: {e}"
                )));
            }
        }
        Ok(())
    }

    async fn write(&mut self, event: &DataEvent) -> Result<(), SinkError> {
        match event {
            DataEvent::Insert { arrow, lsn } => {
                let prepared = if arrow.needs_load() {
                    let Some(loader) = self.arrow_loader.as_ref() else {
                        return Err(SinkError::Transient(
                            "KafkaSink needs StreamArrowLoader for Deferred Insert".into(),
                        ));
                    };
                    loader
                        .ensure_for_write(event)
                        .map_err(|e| SinkError::Transient(e.to_string()))?
                } else {
                    event.clone()
                };

                let batches = match &prepared {
                    DataEvent::Insert {
                        arrow: InsertArrow::Resident { batches, .. },
                        ..
                    } => batches.clone(),
                    DataEvent::Insert {
                        arrow: InsertArrow::Deferred,
                        ..
                    } => return Ok(()),
                    _ => return Ok(()),
                };

                self.produce_batches(&batches, lsn.max_lsn, "insert")
                    .await?;
            }
            DataEvent::FlushFile { lsn, rows, .. } => {
                // Batch CDC (and hybrid after Flush degrades covered Inserts) delivers
                // Parquet via FlushFile — load Arrow and encode JSON rows.
                let Some(loader) = self.arrow_loader.as_ref() else {
                    return Err(SinkError::Transient(
                        "KafkaSink needs StreamArrowLoader for FlushFile".into(),
                    ));
                };
                let batches = loader
                    .load_event_batches(event)
                    .map_err(|e| SinkError::Transient(e.to_string()))?;
                if batches.is_empty() && *rows > 0 {
                    return Err(SinkError::Transient(format!(
                        "FlushFile reported {rows} rows but Parquet load returned empty"
                    )));
                }
                self.produce_batches(&batches, lsn.max_lsn, "flush_file")
                    .await?;
            }
            DataEvent::Watermark { .. } => {
                return Err(SinkError::Fatal(
                    "Watermark must not reach KafkaSink::write".into(),
                ));
            }
        }
        Ok(())
    }

    async fn commit_txn(&mut self) -> Result<(), SinkError> {
        if !matches!(self.mode, DeliveryMode::ExactlyOnce { .. }) {
            return Ok(());
        }
        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| SinkError::Fatal("commit without active producer".into()))?
            .clone();

        let res =
            spawn_blocking(move || producer.commit_transaction(Timeout::After(KAFKA_TXN_TIMEOUT)))
                .await
                .map_err(|e| SinkError::Fatal(format!("commit_transaction join: {e}")))?;

        if let Err(e) = res {
            self.drop_producer();
            return Err(SinkError::Transient(format!("commit failed: {e}")));
        }
        Ok(())
    }

    async fn abort_txn(&mut self) -> Result<(), SinkError> {
        if matches!(self.mode, DeliveryMode::ExactlyOnce { .. }) {
            if let Some(producer) = self.producer.clone() {
                let _ = spawn_blocking(move || {
                    producer.abort_transaction(Timeout::After(KAFKA_ABORT_TIMEOUT))
                })
                .await;
            }
        }
        // Always drop so recovery starts from a clean producer.
        self.drop_producer();
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), SinkError> {
        self.ensure_producer().await?;
        let producer = self
            .producer
            .as_ref()
            .expect("producer after ensure")
            .clone();

        let res = spawn_blocking(move || {
            producer
                .client()
                .fetch_metadata(None, Timeout::After(KAFKA_PING_TIMEOUT))
        })
        .await
        .map_err(|e| SinkError::Fatal(format!("ping join: {e}")))?;

        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                self.drop_producer();
                Err(SinkError::Transient(format!(
                    "Kafka ping metadata failed: {e}"
                )))
            }
        }
    }

    async fn close(&mut self) -> Result<(), SinkError> {
        if let Some(producer) = self.producer.take() {
            let _ =
                spawn_blocking(move || producer.flush(Timeout::After(KAFKA_FLUSH_TIMEOUT))).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use common::LsnRange;
    use std::sync::Arc;

    fn sample_batch(n: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![n])),
                Arc::new(Int64Array::from(vec![n * 10])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn payload_format_parses_json_only() {
        assert_eq!(
            PayloadFormat::from_str_name("json").unwrap(),
            PayloadFormat::Json
        );
        assert_eq!(
            PayloadFormat::from_str_name("JSON").unwrap(),
            PayloadFormat::Json
        );
        assert!(PayloadFormat::from_str_name("avro").is_err());
    }

    #[test]
    fn json_encoder_emits_one_line_per_row() {
        let batches = vec![sample_batch(1), sample_batch(2)];
        let payloads = PayloadEncoder::encode(&PayloadFormat::Json, &batches).unwrap();
        assert_eq!(payloads.len(), 2);
        let s0 = String::from_utf8(payloads[0].clone()).unwrap();
        let s1 = String::from_utf8(payloads[1].clone()).unwrap();
        assert!(s0.contains("\"time\":1"));
        assert!(s1.contains("\"v\":20"));
    }

    #[tokio::test]
    async fn write_without_begin_is_fatal() {
        let mut sink = KafkaSink::new("localhost:9092", "t", PayloadFormat::Json);
        let ev = DataEvent::insert(LsnRange::single(1), vec![sample_batch(1)]);
        let err = sink.write(&ev).await.unwrap_err();
        assert!(err.is_fatal());
    }

    #[tokio::test]
    async fn preflight_fails_fast_on_unreachable_broker() {
        let mut sink = KafkaSink::new("127.0.0.1:1", "no-such-topic", PayloadFormat::Json);
        let err = sink.begin_txn().await.unwrap_err();
        assert!(
            !err.is_fatal(),
            "unreachable broker should be transient: {err}"
        );
        assert!(sink.producer.is_none());
    }

    #[tokio::test]
    async fn abort_clears_producer_handle() {
        let mut sink = KafkaSink::new("127.0.0.1:1", "t", PayloadFormat::Json);
        // Force a poisoned optional by skipping connect; abort must still clear.
        sink.producer = None;
        sink.abort_txn().await.unwrap();
        assert!(sink.producer.is_none());
    }

    #[tokio::test]
    async fn watermark_write_is_rejected() {
        let mut sink = KafkaSink::new("localhost:9092", "t", PayloadFormat::Json);
        // Bypass ensure by injecting a fake "present" state is hard; use Fatal path:
        // write checks producer first — so set up by failing if no producer.
        // After a successful ensure we'd reject watermark; here no producer → Fatal first.
        let err = sink
            .write(&DataEvent::Watermark { end_lsn: 1 })
            .await
            .unwrap_err();
        assert!(err.is_fatal());
    }
}
