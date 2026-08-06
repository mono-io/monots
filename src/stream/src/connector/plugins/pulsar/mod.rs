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

//! Pulsar sink: JSON payloads with Flink-style delivery / routing / key options.
//!
//! Delivery:
//! - `none` — enqueue without awaiting broker receipt
//! - `at-least-once` — await send receipt (default)
//! - `exactly-once` — rejected at DDL (Rust client has no Transaction API)
//!
//! Message routing maps to pulsar-rs [`RoutingPolicy`]. `admin-url` is used for
//! HTTP health preflight; produce traffic uses `service-url`.

use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_json::LineDelimitedWriter;
use futures::{stream::FuturesUnordered, StreamExt};
use pulsar::producer::{self, Producer, ProducerOptions};
use pulsar::routing_policy::{CustomRoutingPolicy, RoutingPolicy};
use pulsar::{Authentication, Error as PulsarError, Pulsar, SerializeMessage, TokioExecutor};
use tracing::{debug, info, warn};

use crate::connector::api::{SinkConnector, SinkError};
use crate::data::StreamArrowLoader;
use crate::model::event::{DataEvent, InsertArrow};
use crate::model::{PulsarDeliveryGuarantee, PulsarMessageRouter, PulsarSinkOptions};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadFormat {
    Json,
}

impl PayloadFormat {
    pub fn from_str_name(s: &str) -> Result<Self, SinkError> {
        match s.to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            other => Err(SinkError::Fatal(format!(
                "unsupported Pulsar format: {other}"
            ))),
        }
    }
}

struct PulsarRecord {
    key: Option<String>,
    value: Vec<u8>,
}

impl SerializeMessage for PulsarRecord {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        Ok(producer::Message {
            payload: input.value,
            partition_key: input.key,
            ..Default::default()
        })
    }
}

struct KeyHashRouter;

impl CustomRoutingPolicy for KeyHashRouter {
    fn route(&self, message: &producer::Message, num_producers: usize) -> usize {
        if num_producers == 0 {
            return 0;
        }
        match &message.partition_key {
            Some(key) => RoutingPolicy::compute_partition_index_for_key(key, num_producers),
            None => 0,
        }
    }
}

fn encode_batch_json_lines(batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SinkError> {
    let mut buf = Vec::new();
    {
        let mut writer = LineDelimitedWriter::new(&mut buf);
        writer
            .write(batch)
            .map_err(|e| SinkError::Fatal(format!("Arrow→JSON encode failed: {e}")))?;
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

fn project_key_batch(
    batch: &RecordBatch,
    fields: &[String],
    prefix: &str,
) -> Result<RecordBatch, SinkError> {
    let mut cols = Vec::with_capacity(fields.len());
    let mut schema_fields = Vec::with_capacity(fields.len());
    for name in fields {
        let idx = batch.schema().index_of(name).map_err(|_| {
            SinkError::Fatal(format!(
                "sink.pulsar.key.fields column `{name}` not found in batch schema {:?}",
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>()
            ))
        })?;
        let array = batch.column(idx).clone();
        let out_name = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}{name}")
        };
        schema_fields.push(Field::new(
            out_name,
            array.data_type().clone(),
            array.is_nullable(),
        ));
        cols.push(array);
    }
    let schema = Arc::new(Schema::new(schema_fields));
    RecordBatch::try_new(schema, cols)
        .map_err(|e| SinkError::Fatal(format!("project Pulsar key batch failed: {e}")))
}

fn encode_records(
    format: &PayloadFormat,
    batches: &[RecordBatch],
    options: &PulsarSinkOptions,
) -> Result<Vec<PulsarRecord>, SinkError> {
    match format {
        PayloadFormat::Json => {
            let mut records = Vec::new();
            for batch in batches {
                let values = encode_batch_json_lines(batch)?;
                let keys = if options.has_key() {
                    let projected =
                        project_key_batch(batch, &options.key_fields, &options.key_fields_prefix)?;
                    let lines = encode_batch_json_lines(&projected)?;
                    lines
                        .into_iter()
                        .map(|b| Some(String::from_utf8_lossy(&b).into_owned()))
                        .collect::<Vec<_>>()
                } else {
                    vec![None; values.len()]
                };
                if keys.len() != values.len() {
                    return Err(SinkError::Fatal(format!(
                        "Pulsar key/value row count mismatch: {} keys vs {} values",
                        keys.len(),
                        values.len()
                    )));
                }
                for (key, value) in keys.into_iter().zip(values) {
                    records.push(PulsarRecord { key, value });
                }
            }
            Ok(records)
        }
    }
}

fn routing_policy(router: PulsarMessageRouter) -> RoutingPolicy {
    match router {
        PulsarMessageRouter::RoundRobin => RoutingPolicy::RoundRobin,
        PulsarMessageRouter::Single => RoutingPolicy::Single,
        PulsarMessageRouter::KeyHash => RoutingPolicy::Custom(Arc::new(KeyHashRouter)),
    }
}

pub struct PulsarSink {
    format: PayloadFormat,
    options: PulsarSinkOptions,
    client: Option<Pulsar<TokioExecutor>>,
    producer: Option<Producer<TokioExecutor>>,
    arrow_loader: Option<Arc<StreamArrowLoader>>,
}

impl PulsarSink {
    pub fn new(format: PayloadFormat, options: PulsarSinkOptions) -> Self {
        Self {
            format,
            options,
            client: None,
            producer: None,
            arrow_loader: None,
        }
    }

    pub fn options(&self) -> &PulsarSinkOptions {
        &self.options
    }

    pub fn with_arrow_loader(mut self, loader: StreamArrowLoader) -> Self {
        self.arrow_loader = Some(Arc::new(loader));
        self
    }

    async fn ensure_producer(&mut self) -> Result<(), SinkError> {
        if self.producer.is_some() {
            return Ok(());
        }

        info!(
            service_url = %self.options.service_url,
            admin_url = %self.options.admin_url,
            topic = %self.options.topic,
            delivery = %self.options.delivery_guarantee.as_str(),
            router = %self.options.message_router.as_str(),
            "PulsarSink establishing producer"
        );

        // Best-effort Admin HTTP probe (Flink uses admin-url for metadata).
        probe_admin(&self.options.admin_url).await;

        let mut builder = Pulsar::builder(&self.options.service_url, TokioExecutor);
        if let Some(token) = self
            .options
            .auth_token()
            .map_err(|e| SinkError::Fatal(e.to_string()))?
        {
            builder = builder.with_auth(Authentication {
                name: "token".into(),
                data: token.into_bytes(),
            });
        }

        let client = builder.build().await.map_err(|e| {
            SinkError::Transient(format!("Pulsar client connect failed: {e}"))
        })?;

        // Topic lookup fails fast when the broker/topic is unreachable.
        client
            .lookup_topic(self.options.topic.clone())
            .await
            .map_err(|e| SinkError::Transient(format!("Pulsar topic lookup failed: {e}")))?;

        let producer = client
            .producer()
            .with_topic(self.options.topic.clone())
            .with_name(format!("monots-{}", uuid::Uuid::new_v4()))
            .with_options(ProducerOptions {
                routing_policy: Some(routing_policy(self.options.message_router)),
                ..Default::default()
            })
            .build()
            .await
            .map_err(|e| SinkError::Transient(format!("Pulsar producer create failed: {e}")))?;

        self.client = Some(client);
        self.producer = Some(producer);
        Ok(())
    }

    fn drop_handles(&mut self) {
        self.producer = None;
        self.client = None;
    }

    async fn produce_batches(
        &mut self,
        batches: &[RecordBatch],
        lsn_hint: u64,
        kind: &str,
    ) -> Result<(), SinkError> {
        let records = encode_records(&self.format, batches, &self.options)?;
        if records.is_empty() {
            return Ok(());
        }

        let wait_receipt = !matches!(
            self.options.delivery_guarantee,
            PulsarDeliveryGuarantee::None
        );

        // Scope the producer borrow so we can drop handles on failure afterwards.
        let delivery_result = {
            let producer = match self.producer.as_mut() {
                Some(p) => p,
                None => {
                    return Err(SinkError::Fatal(
                        "write without active Pulsar producer".into(),
                    ))
                }
            };
            #[allow(unused_mut)] // FuturesUnordered::push needs &mut; rustc may not see it in async.
            let mut futs = Vec::new();
            let mut enqueue_err: Option<SinkError> = None;
            for rec in records {
                match producer.send_non_blocking(rec).await {
                    Ok(fut) => futs.push(fut),
                    Err(e) => {
                        enqueue_err = Some(SinkError::Transient(format!(
                            "Pulsar enqueue failed: {e}"
                        )));
                        break;
                    }
                }
            }
            match enqueue_err {
                Some(e) => Err(e),
                None => {
                    let mut pending = FuturesUnordered::new();
                    for fut in futs {
                        pending.push(fut);
                    }
                    Ok((pending, wait_receipt))
                }
            }
        };

        let (mut delivery, wait_receipt) = match delivery_result {
            Ok(d) => d,
            Err(e) => {
                self.drop_handles();
                return Err(e);
            }
        };

        if wait_receipt {
            while let Some(res) = delivery.next().await {
                if let Err(e) = res {
                    self.drop_handles();
                    return Err(SinkError::Transient(format!(
                        "Pulsar delivery failed: {e}"
                    )));
                }
            }
        } else {
            // Fire-and-forget: drop pending receipt futures without awaiting.
            drop(delivery);
        }

        debug!(
            lsn = lsn_hint,
            keyed = self.options.has_key(),
            wait_receipt,
            kind,
            "Pulsar rows produced"
        );
        Ok(())
    }
}

async fn probe_admin(admin_url: &str) {
    let base = admin_url.trim_end_matches('/');
    let url = format!("{base}/admin/v2/brokers/health");
    match reqwest::get(&url).await {
        Ok(resp) if resp.status().is_success() => {
            debug!(%url, "Pulsar admin health ok");
        }
        Ok(resp) => {
            warn!(%url, status = %resp.status(), "Pulsar admin health unexpected status");
        }
        Err(e) => {
            warn!(%url, error = %e, "Pulsar admin health probe failed (continuing with service-url)");
        }
    }
}

#[async_trait::async_trait]
impl SinkConnector for PulsarSink {
    async fn begin_txn(&mut self) -> Result<(), SinkError> {
        self.ensure_producer().await
    }

    async fn write(&mut self, event: &DataEvent) -> Result<(), SinkError> {
        match event {
            DataEvent::Insert { arrow, lsn } => {
                let prepared = if arrow.needs_load() {
                    let Some(loader) = self.arrow_loader.as_ref() else {
                        return Err(SinkError::Transient(
                            "PulsarSink needs StreamArrowLoader for Deferred Insert".into(),
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
                let Some(loader) = self.arrow_loader.as_ref() else {
                    return Err(SinkError::Transient(
                        "PulsarSink needs StreamArrowLoader for FlushFile".into(),
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
                    "Watermark must not reach PulsarSink::write".into(),
                ));
            }
        }
        Ok(())
    }

    async fn commit_txn(&mut self) -> Result<(), SinkError> {
        Ok(())
    }

    async fn abort_txn(&mut self) -> Result<(), SinkError> {
        self.drop_handles();
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), SinkError> {
        self.ensure_producer().await?;
        let res = {
            let producer = self
                .producer
                .as_ref()
                .ok_or_else(|| SinkError::Fatal("ping without producer".into()))?;
            producer.check_connection().await
        };
        match res {
            Ok(()) => Ok(()),
            Err(e) => {
                self.drop_handles();
                Err(SinkError::Transient(format!("Pulsar ping failed: {e}")))
            }
        }
    }

    async fn close(&mut self) -> Result<(), SinkError> {
        if let Some(mut producer) = self.producer.take() {
            let _ = producer.close().await;
        }
        self.client = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use common::LsnRange;
    use std::sync::Arc;

    fn sample_batch(order_id: i64, region: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![order_id])),
                Arc::new(StringArray::from(vec![Some(region)])),
                Arc::new(Int64Array::from(vec![order_id * 10])),
            ],
        )
        .unwrap()
    }

    fn minimal_options() -> PulsarSinkOptions {
        PulsarSinkOptions {
            topic: "persistent://public/default/t".into(),
            service_url: "pulsar://127.0.0.1:1".into(),
            admin_url: "http://127.0.0.1:1".into(),
            ..Default::default()
        }
    }

    #[test]
    fn payload_format_parses_json_only() {
        assert_eq!(
            PayloadFormat::from_str_name("json").unwrap(),
            PayloadFormat::Json
        );
        assert!(PayloadFormat::from_str_name("avro").is_err());
    }

    #[test]
    fn json_encoder_emits_keyed_records() {
        let mut options = minimal_options();
        options.key_format = Some("json".into());
        options.key_fields = vec!["order_id".into()];
        options.key_fields_prefix = "k_".into();
        let records = encode_records(&PayloadFormat::Json, &[sample_batch(7, "east")], &options)
            .unwrap();
        assert_eq!(records.len(), 1);
        let key = records[0].key.as_ref().unwrap();
        assert!(key.contains("\"k_order_id\":7"), "{key}");
        let val = String::from_utf8(records[0].value.clone()).unwrap();
        assert!(val.contains("\"order_id\":7"));
    }

    #[tokio::test]
    async fn write_without_begin_is_fatal() {
        let mut sink = PulsarSink::new(PayloadFormat::Json, minimal_options());
        let ev = DataEvent::insert(LsnRange::single(1), vec![sample_batch(1, "e")]);
        let err = sink.write(&ev).await.unwrap_err();
        assert!(err.is_fatal());
    }

    #[tokio::test]
    async fn preflight_fails_fast_on_unreachable_broker() {
        let mut sink = PulsarSink::new(PayloadFormat::Json, minimal_options());
        let err = sink.begin_txn().await.unwrap_err();
        assert!(!err.is_fatal(), "unreachable broker should be transient: {err}");
        assert!(sink.producer.is_none());
    }

    #[tokio::test]
    async fn watermark_write_is_rejected() {
        let mut sink = PulsarSink::new(PayloadFormat::Json, minimal_options());
        let err = sink
            .write(&DataEvent::Watermark { end_lsn: 1 })
            .await
            .unwrap_err();
        assert!(err.is_fatal());
    }
}
