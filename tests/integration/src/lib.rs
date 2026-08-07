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

//! MonoTS integration test framework (FunctionStream-style).

pub mod all_types_arrow;
pub mod framework;
pub mod helpers;
pub mod parquet_util;
pub mod test_context;

pub use all_types_arrow::{
    enum_value_at, full_types_batch, full_types_columns, full_types_ddl, full_types_schema,
};
pub use framework::docker::{
    require_docker_stack, require_iceberg_rest, require_mqtt, require_pulsar, ICEBERG_REST_URI,
    ICEBERG_REST_WAREHOUSE_PREFIX, KAFKA_BOOTSTRAP, MINIO_ACCESS_KEY, MINIO_BUCKET, MINIO_ENDPOINT,
    MINIO_SECRET_KEY, MQTT_BROKER_URL, PULSAR_ADMIN_URL, PULSAR_SERVICE_URL,
};
pub use framework::instance::{ts_col, unique_table, MonotsInstance};
pub use helpers::{
    col_f64, col_i64, col_is_null, col_str, scalar_bool_named, scalar_f32_named, scalar_f64_named,
    scalar_i64, scalar_i64_named, scalar_str_named, table_names_from_show, total_rows,
};
pub use parquet_util::{corrupt_file_mid, list_sst_files, write_i64_parquet};
pub use sdk::{is_sorted_by_time, sort_batch_by_time};
pub use test_context::{
    assert_err_contains, assert_time_scan_non_decreasing, show_create_statement, TestContext,
    TIME_COL, VALUE_COL,
};
