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

//! MQTT sink: JSON payloads with Flink-style `sink.mqtt.*` options (rumqttc).

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use arrow_json::LineDelimitedWriter;
use rumqttc::{
    AsyncClient, ConnectReturnCode, Event, Incoming, MqttOptions, NetworkOptions, QoS, Transport,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::connector::api::{SinkConnector, SinkError};
use crate::data::StreamArrowLoader;
use crate::model::event::{DataEvent, InsertArrow};
use crate::model::{MqttQos, MqttSinkOptions};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadFormat {
    Json,
}

impl PayloadFormat {
    pub fn from_str_name(s: &str) -> Result<Self, SinkError> {
        match s.to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            other => Err(SinkError::Fatal(format!(
                "unsupported MQTT format: {other}"
            ))),
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

fn encode_payloads(
    format: &PayloadFormat,
    batches: &[RecordBatch],
) -> Result<Vec<Vec<u8>>, SinkError> {
    match format {
        PayloadFormat::Json => {
            let mut out = Vec::new();
            for batch in batches {
                out.extend(encode_batch_json_lines(batch)?);
            }
            Ok(out)
        }
    }
}

fn map_qos(q: MqttQos) -> QoS {
    match q {
        MqttQos::AtMostOnce => QoS::AtMostOnce,
        MqttQos::AtLeastOnce => QoS::AtLeastOnce,
        MqttQos::ExactlyOnce => QoS::ExactlyOnce,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BrokerTransport {
    Tcp,
    Tls,
    Ws,
    Wss,
}

/// Parse Flink-style broker URL into host / port / transport.
///
/// For `ws://` / `wss://`, rumqttc expects the full URL (including path) as
/// `broker_addr`; callers should pass the original URL string into
/// [`MqttOptions::new`].
fn parse_broker_url(raw: &str) -> Result<(String, u16, BrokerTransport), SinkError> {
    let s = raw.trim();
    if s.is_empty() {
        return Err(SinkError::Fatal("empty MQTT broker url".into()));
    }

    let (scheme, rest) = if let Some(idx) = s.find("://") {
        (&s[..idx], &s[idx + 3..])
    } else {
        ("tcp", s)
    };

    let transport = match scheme.to_ascii_lowercase().as_str() {
        "tcp" | "mqtt" => BrokerTransport::Tcp,
        "ssl" | "mqtts" | "tls" => BrokerTransport::Tls,
        "ws" => BrokerTransport::Ws,
        "wss" => BrokerTransport::Wss,
        other => {
            return Err(SinkError::Fatal(format!(
                "unsupported MQTT url scheme `{other}` \
                 (use tcp:// | mqtt:// | ssl:// | mqtts:// | ws:// | wss://)"
            )));
        }
    };

    // Authority only (strip path / query); WebSocket path is kept on the original URL.
    let hostport = rest.split('/').next().unwrap_or(rest);
    let hostport = hostport.split('?').next().unwrap_or(hostport);
    if hostport.is_empty() {
        return Err(SinkError::Fatal(format!("MQTT url missing host: {raw}")));
    }

    let default_port = match transport {
        BrokerTransport::Tcp => 1883u16,
        BrokerTransport::Tls => 8883u16,
        BrokerTransport::Ws => 80u16,
        BrokerTransport::Wss => 443u16,
    };

    let (host, port) = if let Some((h, p)) = hostport.rsplit_once(':') {
        // IPv6 in brackets: [2001:db8::1]:1883
        let host = if h.starts_with('[') && h.ends_with(']') {
            h[1..h.len() - 1].to_string()
        } else if h.contains(':') && !h.starts_with('[') {
            // Bare IPv6 without port.
            return Ok((hostport.to_string(), default_port, transport));
        } else {
            h.to_string()
        };
        let port: u16 = p
            .parse()
            .map_err(|_| SinkError::Fatal(format!("invalid MQTT port in url: {raw}")))?;
        (host, port)
    } else {
        (hostport.to_string(), default_port)
    };

    if host.is_empty() {
        return Err(SinkError::Fatal(format!("MQTT url missing host: {raw}")));
    }
    Ok((host, port, transport))
}

pub struct MqttSink {
    format: PayloadFormat,
    options: MqttSinkOptions,
    client: Option<AsyncClient>,
    eventloop_task: Option<JoinHandle<()>>,
    /// Last fatal error observed by the background event loop.
    loop_fail: Option<watch::Receiver<Option<String>>>,
    arrow_loader: Option<Arc<StreamArrowLoader>>,
}

impl MqttSink {
    pub fn new(format: PayloadFormat, options: MqttSinkOptions) -> Self {
        Self {
            format,
            options,
            client: None,
            eventloop_task: None,
            loop_fail: None,
            arrow_loader: None,
        }
    }

    pub fn options(&self) -> &MqttSinkOptions {
        &self.options
    }

    pub fn with_arrow_loader(mut self, loader: StreamArrowLoader) -> Self {
        self.arrow_loader = Some(Arc::new(loader));
        self
    }

    fn check_loop_health(&self) -> Result<(), SinkError> {
        if let Some(rx) = &self.loop_fail {
            if let Some(err) = rx.borrow().as_ref() {
                return Err(SinkError::Transient(format!(
                    "MQTT event loop failed: {err}"
                )));
            }
        }
        Ok(())
    }

    async fn ensure_client(&mut self) -> Result<(), SinkError> {
        if self.client.is_some() {
            self.check_loop_health()?;
            return Ok(());
        }

        let (host, port, transport) = parse_broker_url(&self.options.url)?;
        let client_id = self
            .options
            .client_id
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("monots-{}", uuid::Uuid::new_v4()));

        info!(
            url = %self.options.url,
            host = %host,
            port,
            topic = %self.options.topic,
            qos = self.options.qos.as_u8(),
            client_id = %client_id,
            "MqttSink establishing client"
        );

        // rumqttc WebSocket uses the full URL as broker_addr (path matters, e.g. /mqtt).
        let broker_addr = match transport {
            BrokerTransport::Ws | BrokerTransport::Wss => self.options.url.trim().to_string(),
            BrokerTransport::Tcp | BrokerTransport::Tls => host,
        };
        let mut mqtt_opts = MqttOptions::new(client_id, broker_addr, port);
        mqtt_opts.set_keep_alive(self.options.keep_alive_interval);
        mqtt_opts.set_clean_session(self.options.clean_session);
        mqtt_opts.set_inflight(self.options.max_inflight);
        let channel_cap = (self.options.max_inflight as usize).max(16);
        mqtt_opts.set_request_channel_capacity(channel_cap);

        match transport {
            BrokerTransport::Tcp => {
                mqtt_opts.set_transport(Transport::Tcp);
            }
            BrokerTransport::Tls => {
                mqtt_opts.set_transport(Transport::tls_with_default_config());
            }
            BrokerTransport::Ws => {
                mqtt_opts.set_transport(Transport::Ws);
            }
            BrokerTransport::Wss => {
                mqtt_opts.set_transport(Transport::wss_with_default_config());
            }
        }

        if let Some(user) = &self.options.username {
            let pass = self.options.password.clone().unwrap_or_default();
            mqtt_opts.set_credentials(user.clone(), pass);
        }

        let (client, mut eventloop) = AsyncClient::new(mqtt_opts, channel_cap);
        let mut net = NetworkOptions::new();
        net.set_connection_timeout(self.options.connection_timeout.as_secs().max(1));
        eventloop.set_network_options(net);

        // Wait for ConnAck before handing the event loop to a background task.
        let deadline = Instant::now() + self.options.connection_timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(SinkError::Transient(format!(
                    "MQTT connect timed out after {}s ({})",
                    self.options.connection_timeout.as_secs(),
                    self.options.url
                )));
            }
            match tokio::time::timeout(remaining, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Incoming::ConnAck(ack)))) => {
                    if ack.code != ConnectReturnCode::Success {
                        return Err(SinkError::Transient(format!(
                            "MQTT CONNACK rejected: {:?}",
                            ack.code
                        )));
                    }
                    break;
                }
                Ok(Ok(other)) => {
                    debug!(?other, "MQTT event before ConnAck");
                }
                Ok(Err(e)) => {
                    return Err(SinkError::Transient(format!("MQTT connect failed: {e}")));
                }
                Err(_) => {
                    return Err(SinkError::Transient(format!(
                        "MQTT connect timed out after {}s ({})",
                        self.options.connection_timeout.as_secs(),
                        self.options.url
                    )));
                }
            }
        }

        let (fail_tx, fail_rx) = watch::channel(None);
        let task = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(_) => {}
                    Err(e) => {
                        warn!(error = %e, "MQTT event loop stopped");
                        let _ = fail_tx.send(Some(e.to_string()));
                        break;
                    }
                }
            }
        });

        self.client = Some(client);
        self.eventloop_task = Some(task);
        self.loop_fail = Some(fail_rx);
        Ok(())
    }

    async fn drop_handles(&mut self) {
        if let Some(client) = self.client.take() {
            let _ = client.disconnect().await;
        }
        if let Some(task) = self.eventloop_task.take() {
            task.abort();
            let _ = task.await;
        }
        self.loop_fail = None;
    }

    async fn produce_batches(
        &mut self,
        batches: &[RecordBatch],
        lsn_hint: u64,
        kind: &str,
    ) -> Result<(), SinkError> {
        let payloads = encode_payloads(&self.format, batches)?;
        if payloads.is_empty() {
            return Ok(());
        }

        self.check_loop_health()?;
        let qos = map_qos(self.options.qos);
        let retained = self.options.retained;
        let topic = self.options.topic.clone();

        let client = match self.client.as_ref() {
            Some(c) => c,
            None => {
                return Err(SinkError::Fatal("write without active MQTT client".into()));
            }
        };

        for payload in payloads {
            self.check_loop_health()?;
            if let Err(e) = client.publish(&topic, qos, retained, payload).await {
                self.drop_handles().await;
                return Err(SinkError::Transient(format!("MQTT publish failed: {e}")));
            }
        }

        // Give the event loop a brief window to flush QoS>0 handshakes.
        if !matches!(self.options.qos, MqttQos::AtMostOnce) {
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.check_loop_health()?;
        }

        debug!(lsn = lsn_hint, kind, "MQTT rows published");
        Ok(())
    }
}

#[async_trait::async_trait]
impl SinkConnector for MqttSink {
    async fn begin_txn(&mut self) -> Result<(), SinkError> {
        self.ensure_client().await
    }

    async fn write(&mut self, event: &DataEvent) -> Result<(), SinkError> {
        match event {
            DataEvent::Insert { arrow, lsn } => {
                let prepared = if arrow.needs_load() {
                    let Some(loader) = self.arrow_loader.as_ref() else {
                        return Err(SinkError::Transient(
                            "MqttSink needs StreamArrowLoader for Deferred Insert".into(),
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
                        "MqttSink needs StreamArrowLoader for FlushFile".into(),
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
                    "Watermark must not reach MqttSink::write".into(),
                ));
            }
        }
        Ok(())
    }

    async fn commit_txn(&mut self) -> Result<(), SinkError> {
        self.check_loop_health()
    }

    async fn abort_txn(&mut self) -> Result<(), SinkError> {
        self.drop_handles().await;
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), SinkError> {
        self.ensure_client().await?;
        self.check_loop_health()
    }

    async fn close(&mut self) -> Result<(), SinkError> {
        self.drop_handles().await;
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

    fn sample_batch() -> Result<RecordBatch, String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("east")])),
                Arc::new(Int64Array::from(vec![7])),
            ],
        )
        .map_err(|e| e.to_string())
    }

    fn minimal_options() -> MqttSinkOptions {
        MqttSinkOptions {
            url: "tcp://127.0.0.1:1".into(),
            topic: "t".into(),
            connection_timeout: Duration::from_secs(1),
            ..Default::default()
        }
    }

    #[test]
    fn parses_broker_urls() -> Result<(), String> {
        let (h, p, t) = parse_broker_url("tcp://broker.emqx.io:1883").map_err(|e| e.to_string())?;
        assert_eq!(h, "broker.emqx.io");
        assert_eq!(p, 1883);
        assert_eq!(t, BrokerTransport::Tcp);

        let (h, p, t) = parse_broker_url("ssl://secure.example:8883").map_err(|e| e.to_string())?;
        assert_eq!(h, "secure.example");
        assert_eq!(p, 8883);
        assert_eq!(t, BrokerTransport::Tls);

        let (h, p, _) = parse_broker_url("mqtt://only-host").map_err(|e| e.to_string())?;
        assert_eq!(h, "only-host");
        assert_eq!(p, 1883);

        let (h, p, t) =
            parse_broker_url("ws://broker.example:8083/mqtt").map_err(|e| e.to_string())?;
        assert_eq!(h, "broker.example");
        assert_eq!(p, 8083);
        assert_eq!(t, BrokerTransport::Ws);

        let (h, p, t) = parse_broker_url("wss://secure.example/mqtt").map_err(|e| e.to_string())?;
        assert_eq!(h, "secure.example");
        assert_eq!(p, 443);
        assert_eq!(t, BrokerTransport::Wss);
        Ok(())
    }

    #[test]
    fn payload_format_json_only() {
        assert!(matches!(
            PayloadFormat::from_str_name("json"),
            Ok(PayloadFormat::Json)
        ));
        assert!(PayloadFormat::from_str_name("avro").is_err());
    }

    #[tokio::test]
    async fn write_without_begin_is_fatal() -> Result<(), String> {
        let mut sink = MqttSink::new(PayloadFormat::Json, minimal_options());
        let ev = DataEvent::insert(LsnRange::single(1), vec![sample_batch()?]);
        let err = sink
            .write(&ev)
            .await
            .err()
            .ok_or_else(|| "expected write error".to_string())?;
        assert!(err.is_fatal());
        Ok(())
    }

    #[tokio::test]
    async fn preflight_fails_fast_on_unreachable_broker() -> Result<(), String> {
        let mut sink = MqttSink::new(PayloadFormat::Json, minimal_options());
        let err = sink
            .begin_txn()
            .await
            .err()
            .ok_or_else(|| "expected begin_txn error".to_string())?;
        assert!(
            !err.is_fatal(),
            "unreachable broker should be transient: {err}"
        );
        assert!(sink.client.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn watermark_write_is_rejected() -> Result<(), String> {
        let mut sink = MqttSink::new(PayloadFormat::Json, minimal_options());
        let err = sink
            .write(&DataEvent::Watermark { end_lsn: 1 })
            .await
            .err()
            .ok_or_else(|| "expected watermark error".to_string())?;
        assert!(err.is_fatal());
        Ok(())
    }
}
