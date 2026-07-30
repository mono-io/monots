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

use monots_integration_tests::helpers::scalar_str_named;
use monots_integration_tests::MonotsInstance;

const METRICS_DDL: &str = "CREATE TABLE metrics (time TIMESTAMP NOT NULL, value DOUBLE)";

const CREATE_DELTA: &str = "CREATE STREAM metrics_out WITH (
  'sink.type' = 'delta',
  'sink.delta.path' = '/tmp/monots-it/metrics_out',
  'source.table' = 'metrics'
)";

const CREATE_DELTA_LAKE: &str = "CREATE STREAM lake_out WITH (
  'sink.type' = 'delta',
  'sink.delta.path' = '/tmp/lake/metrics',
  'source.table' = 'metrics'
)";

#[tokio::test]
async fn create_show_drop_stream_lifecycle() {
    let mut inst = MonotsInstance::new("stream_lifecycle").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(METRICS_DDL).await.unwrap();
    client.no_query(CREATE_DELTA).await.unwrap();

    let show = client.query("SHOW STREAMS").await.unwrap();
    assert_eq!(scalar_str_named(&show, "stream_name"), "metrics_out");
    assert_eq!(scalar_str_named(&show, "connector_type"), "delta");
    assert_eq!(scalar_str_named(&show, "cdc_mode"), "batch");

    let detail = client.query("SHOW STREAM metrics_out").await.unwrap();
    assert_eq!(scalar_str_named(&detail, "stream_name"), "metrics_out");
    let plan = scalar_str_named(&detail, "stream_plan");
    assert!(plan.contains("sync_batch_parquet"));
    assert!(plan.contains("activate"));
    let ddl = scalar_str_named(&detail, "create_statement");
    assert!(
        ddl.contains("'cdc.mode' = 'batch'"),
        "CREATE statement should round-trip cdc.mode: {ddl}"
    );

    let status = client
        .query("SHOW STREAM STATUS FOR metrics_out")
        .await
        .unwrap();
    let phase = scalar_str_named(&status, "phase");
    assert!(
        matches!(
            phase.as_str(),
            "syncingbatch" | "active" | "completed" | "preparinglog" | "syncinglog"
        ),
        "CREATE STREAM should start stream worker immediately, got phase={phase}"
    );
    assert_eq!(scalar_str_named(&status, "cdc_mode"), "batch");

    client.no_query("DROP STREAM metrics_out").await.unwrap();
    let empty = client.query("SHOW STREAMS").await.unwrap();
    assert_eq!(empty[0].num_rows(), 0);
}

#[tokio::test]
async fn delta_sink_uses_batch_capture() {
    let mut inst = MonotsInstance::new("stream_batch_default").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(METRICS_DDL).await.unwrap();
    client.no_query(CREATE_DELTA).await.unwrap();

    let show = client.query("SHOW STREAMS").await.unwrap();
    assert_eq!(scalar_str_named(&show, "cdc_mode"), "batch");

    let detail = client.query("SHOW STREAM metrics_out").await.unwrap();
    let plan = scalar_str_named(&detail, "stream_plan");
    assert!(plan.contains("sync_batch_parquet"));
    assert!(!plan.contains("tail_log_wal"));
}

#[tokio::test]
async fn unsupported_connector_type_rejected() {
    let mut inst = MonotsInstance::new("stream_bad_connector").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(METRICS_DDL).await.unwrap();
    let err = client
        .no_query(
            "CREATE STREAM metrics_out WITH (
              'sink.type' = 'json',
              'sink.delta.path' = '/tmp/x',
              'source.table' = 'metrics'
            )",
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unsupported sink.type"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn cdc_mode_log_rejected() {
    let mut inst = MonotsInstance::new("stream_cdc_mode_log_rejected").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(METRICS_DDL).await.unwrap();
    let err = client
        .no_query(
            "CREATE STREAM bad WITH (
              'sink.type' = 'delta',
              'sink.delta.path' = '/tmp/lake',
              'source.table' = 'metrics',
              'cdc.mode' = 'log'
            )",
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("not supported"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn cdc_mode_hybrid_accepted() {
    let mut inst = MonotsInstance::new("stream_cdc_mode_hybrid").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(METRICS_DDL).await.unwrap();
    client
        .no_query(
            "CREATE STREAM hybrid_out WITH (
              'sink.type' = 'delta',
              'sink.delta.path' = '/tmp/lake/hybrid',
              'source.table' = 'metrics',
              'cdc.mode' = 'hybrid'
            )",
        )
        .await
        .unwrap();

    let show = client.query("SHOW STREAMS").await.unwrap();
    assert_eq!(scalar_str_named(&show, "cdc_mode"), "hybrid");
}

#[tokio::test]
async fn kafka_sink_defaults_to_hybrid() {
    let mut inst = MonotsInstance::new("stream_kafka_default").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(METRICS_DDL).await.unwrap();
    client
        .no_query(
            "CREATE STREAM kafka_out WITH (
              'sink.type' = 'kafka',
              'sink.kafka.brokers' = 'localhost:9092',
              'sink.kafka.topic' = 'metrics',
              'source.table' = 'metrics'
            )",
        )
        .await
        .unwrap();

    let show = client.query("SHOW STREAMS").await.unwrap();
    assert_eq!(scalar_str_named(&show, "cdc_mode"), "hybrid");
}

#[tokio::test]
async fn rejects_time_filter_options() {
    let mut inst = MonotsInstance::new("stream_no_time_filter").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(METRICS_DDL).await.unwrap();
    let err = client
        .no_query(
            "CREATE STREAM bounded WITH (
              'sink.type' = 'delta',
              'sink.delta.path' = '/tmp/monots-it/bounded',
              'source.table' = 'metrics',
              'cdc.from_timestamp' = '0',
              'cdc.to_timestamp' = '9999999999999'
            )",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not supported"));
}

#[tokio::test]
async fn auto_end_option() {
    let mut inst = MonotsInstance::new("stream_auto_end").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(METRICS_DDL).await.unwrap();
    client
        .no_query(
            "CREATE STREAM once WITH (
              'sink.type' = 'delta',
              'sink.delta.path' = '/tmp/monots-it/once',
              'source.table' = 'metrics',
              'cdc.auto_end' = 'true'
            )",
        )
        .await
        .unwrap();

    let status = client.query("SHOW STREAM STATUS FOR once").await.unwrap();
    assert_eq!(scalar_str_named(&status, "auto_end"), "true");
}

#[tokio::test]
async fn create_stream_requires_existing_table() {
    let mut inst = MonotsInstance::new("stream_missing_table").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let err = client.no_query(CREATE_DELTA_LAKE).await.unwrap_err();
    assert!(
        err.to_string().contains("not found") || err.to_string().contains("Table"),
        "unexpected: {err}"
    );
}

#[tokio::test]
async fn create_stream_routes_to_no_query_show_to_query() {
    let mut inst = MonotsInstance::new("stream_route").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.no_query(METRICS_DDL).await.unwrap();
    client.no_query(CREATE_DELTA).await.unwrap();
    let _ = client.query("SHOW STREAMS").await.unwrap();
}

#[tokio::test]
async fn stream_ddl_invalid_syntax_still_rejected() {
    let mut inst = MonotsInstance::new("stream_bad_syntax").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    let err = client.no_query("CREATE STREAM bad").await.unwrap_err();
    assert!(!err.to_string().is_empty());
}
