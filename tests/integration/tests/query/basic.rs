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

//! Query IT smoke: single-table filters, projection, ORDER/LIMIT, basic aggs.

use monots_integration_tests::{
    scalar_f64_named, scalar_i64, scalar_i64_named, total_rows, unique_table, MonotsInstance,
};

async fn setup_metrics_table(inst: &MonotsInstance, table: &str) {
    let mut client = inst.authenticated_client().await.unwrap();
    client
        .no_query(&format!(
            "CREATE TABLE {table} (
                time BIGINT NOT NULL,
                region VARCHAR,
                value DOUBLE
            )"
        ))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, region, value) VALUES
            (1000, 'east', 10.0),
            (2000, 'east', 20.0),
            (3000, 'west', 30.0),
            (4000, 'west', 40.0),
            (5000, 'north', 50.0)"
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn count_sum_avg_min_max_aggregations() {
    let table = unique_table("agg");
    let mut inst = MonotsInstance::new("query_aggregations").unwrap();
    inst.start().await.unwrap();
    setup_metrics_table(&inst, &table).await;

    let mut client = inst.authenticated_client().await.unwrap();

    let count = client
        .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&count, "c"), 5);

    let stats = client
        .query(&format!(
            "SELECT SUM(value) AS s, AVG(value) AS a, MIN(value) AS mn, MAX(value) AS mx FROM {table}"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_f64_named(&stats, "s"), 150.0);
    assert_eq!(scalar_f64_named(&stats, "a"), 30.0);
    assert_eq!(scalar_f64_named(&stats, "mn"), 10.0);
    assert_eq!(scalar_f64_named(&stats, "mx"), 50.0);
}

#[tokio::test]
async fn group_by_with_having() {
    let table = unique_table("grp");
    let mut inst = MonotsInstance::new("query_group_by").unwrap();
    inst.start().await.unwrap();
    setup_metrics_table(&inst, &table).await;

    let mut client = inst.authenticated_client().await.unwrap();

    let rows = client
        .query(&format!(
            "SELECT region, SUM(value) AS total FROM {table}
             GROUP BY region
             HAVING SUM(value) >= 50
             ORDER BY total DESC"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);

    let regions: Vec<String> = (0..rows[0].num_rows())
        .map(|i| {
            rows[0]
                .column_by_name("region")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(i)
                .to_string()
        })
        .collect();
    assert!(regions.contains(&"west".to_string()));
    assert!(regions.contains(&"north".to_string()));
}

#[tokio::test]
async fn order_by_desc_limit_offset() {
    let table = unique_table("ord");
    let mut inst = MonotsInstance::new("query_order_limit").unwrap();
    inst.start().await.unwrap();
    setup_metrics_table(&inst, &table).await;

    let mut client = inst.authenticated_client().await.unwrap();

    let rows = client
        .query(&format!(
            "SELECT time, value FROM {table}
             ORDER BY time DESC
             LIMIT 2 OFFSET 1"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    let ts = rows[0]
        .column_by_name("time")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(ts.value(0), 4000);
    assert_eq!(ts.value(1), 3000);
}

#[tokio::test]
async fn where_and_or_in() {
    let table = unique_table("filt");
    let mut inst = MonotsInstance::new("query_where").unwrap();
    inst.start().await.unwrap();
    setup_metrics_table(&inst, &table).await;

    let mut client = inst.authenticated_client().await.unwrap();

    let rows = client
        .query(&format!(
            "SELECT value FROM {table}
             WHERE (region = 'east' AND value >= 15) OR region = 'north'"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);

    let rows = client
        .query(&format!(
            "SELECT value FROM {table} WHERE region IN ('east', 'north') ORDER BY value"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 3);
}

#[tokio::test]
async fn distinct_and_column_projection() {
    let table = unique_table("proj");
    let mut inst = MonotsInstance::new("query_distinct").unwrap();
    inst.start().await.unwrap();
    setup_metrics_table(&inst, &table).await;

    let mut client = inst.authenticated_client().await.unwrap();

    let rows = client
        .query(&format!(
            "SELECT DISTINCT region FROM {table} ORDER BY region"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 3);

    let rows = client
        .query(&format!(
            "SELECT region, COUNT(*) AS c FROM {table} GROUP BY region ORDER BY region"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 3);
    assert_eq!(scalar_i64(&rows, 1), 2);
}

#[tokio::test]
async fn time_range_and_between() {
    let table = unique_table("time");
    let mut inst = MonotsInstance::new("query_time_range").unwrap();
    inst.start().await.unwrap();
    setup_metrics_table(&inst, &table).await;

    let mut client = inst.authenticated_client().await.unwrap();

    let rows = client
        .query(&format!(
            "SELECT COUNT(*) AS c FROM {table}
             WHERE time BETWEEN 2000 AND 4000"
        ))
        .await
        .unwrap();
    assert_eq!(scalar_i64_named(&rows, "c"), 3);
}
