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

use arrow::array::{Float64Array, Int64Array};
use monots_core::metadata::catalog::ColumnDef;
use monots_integration_tests::{ts_col, unique_table, MonotsInstance};

fn total_rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn create_insert_select() {
    let table = unique_table("t");
    let mut inst = MonotsInstance::new("sql_create_insert_select").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .create_table(
            &table,
            vec![
                ts_col(),
                ColumnDef {
                    name: "value".into(),
                    data_type: "Float64".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();

    let inserted = client
        .no_query(&format!(
            "INSERT INTO {table} (time, value) VALUES (1718000000000, 1.5), (1718000060000, 2.5)"
        ))
        .await
        .unwrap();
    assert_eq!(inserted, 2);

    let rows = client
        .query(&format!("SELECT * FROM {table} ORDER BY time"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    let values = rows[0].column_by_name("value").unwrap();
    let arr = values.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(arr.value(0), 1.5);
    assert_eq!(arr.value(1), 2.5);
}

#[tokio::test]
async fn time_range_filter() {
    let table = unique_table("t");
    let mut inst = MonotsInstance::new("sql_time_range").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .create_table(
            &table,
            vec![
                ts_col(),
                ColumnDef {
                    name: "v".into(),
                    data_type: "Int64".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();

    client
        .no_query(&format!(
            "INSERT INTO {table} (time, v) VALUES (1000, 1), (2000, 2), (3000, 3)"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!(
            "SELECT v FROM {table} WHERE time >= 2000 AND time <= 2500"
        ))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    let v = rows[0].column_by_name("v").unwrap();
    let arr = v.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(arr.value(0), 2);
}

#[tokio::test]
async fn multi_table_isolated() {
    let t1 = unique_table("a");
    let t2 = unique_table("b");
    let mut inst = MonotsInstance::new("sql_multi_table").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .create_table(
            &t1,
            vec![
                ts_col(),
                ColumnDef {
                    name: "x".into(),
                    data_type: "Int64".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();
    client
        .create_table(
            &t2,
            vec![
                ts_col(),
                ColumnDef {
                    name: "y".into(),
                    data_type: "Float64".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();

    client
        .no_query(&format!("INSERT INTO {t1} (time, x) VALUES (1, 10)"))
        .await
        .unwrap();
    client
        .no_query(&format!("INSERT INTO {t2} (time, y) VALUES (2, 3.3)"))
        .await
        .unwrap();

    assert_eq!(
        total_rows(&client.query(&format!("SELECT * FROM {t1}")).await.unwrap()),
        1
    );
    assert_eq!(
        total_rows(&client.query(&format!("SELECT * FROM {t2}")).await.unwrap()),
        1
    );
}

#[tokio::test]
async fn add_column_and_null_pad() {
    let table = unique_table("t");
    let mut inst = MonotsInstance::new("sql_add_column").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .create_table(
            &table,
            vec![
                ts_col(),
                ColumnDef {
                    name: "a".into(),
                    data_type: "Int64".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();
    client
        .no_query(&format!("INSERT INTO {table} (time, a) VALUES (100, 7)"))
        .await
        .unwrap();
    client
        .add_column(
            &table,
            ColumnDef {
                name: "b".into(),
                data_type: "Utf8".into(),
                nullable: true,
            },
        )
        .await
        .unwrap();
    client
        .no_query(&format!("INSERT INTO {table} (time, a) VALUES (200, 8)"))
        .await
        .unwrap();

    let rows = client
        .query(&format!("SELECT * FROM {table} ORDER BY time"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
    let b = rows[0].column_by_name("b").unwrap();
    assert_eq!(b.null_count(), 2);
}

#[tokio::test]
async fn bulk_insert_via_sql() {
    let table = unique_table("t");
    let mut inst = MonotsInstance::new("sql_bulk_insert").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .create_table(
            &table,
            vec![
                ts_col(),
                ColumnDef {
                    name: "value".into(),
                    data_type: "Float64".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();

    let rows = client
        .no_query(&format!(
            "INSERT INTO {table} (time, value) VALUES (1718000000000, 1.1), (1718000060000, 2.2)"
        ))
        .await
        .unwrap();
    assert_eq!(rows, 2);

    let batches = client
        .query(&format!("SELECT * FROM {table}"))
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 2);
}

#[tokio::test]
async fn drop_table_removes_metadata_and_data() {
    let table = unique_table("t");
    let mut inst = MonotsInstance::new("sql_drop_table").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .create_table(
            &table,
            vec![
                ts_col(),
                ColumnDef {
                    name: "value".into(),
                    data_type: "Float64".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, value) VALUES (1, 1.0)"
        ))
        .await
        .unwrap();

    client
        .no_query(&format!("DROP TABLE {table}"))
        .await
        .unwrap();

    let err = client
        .query(&format!("SELECT * FROM {table}"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not found") || err.to_string().contains("Table"));
}

#[tokio::test]
async fn drop_table_if_exists_is_idempotent() {
    let table = unique_table("t");
    let mut inst = MonotsInstance::new("sql_drop_if_exists").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .no_query(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .unwrap();
    client.create_table(&table, vec![ts_col()]).await.unwrap();
    client
        .no_query(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .unwrap();
    client
        .no_query(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .unwrap();
}

#[tokio::test]
async fn add_column_then_query_old_and_new_rows() {
    let table = unique_table("t");
    let mut inst = MonotsInstance::new("sql_add_col_query").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .create_table(
            &table,
            vec![
                ts_col(),
                ColumnDef {
                    name: "a".into(),
                    data_type: "Int64".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();
    client
        .no_query(&format!("INSERT INTO {table} (time, a) VALUES (100, 7)"))
        .await
        .unwrap();
    client
        .no_query(&format!("ALTER TABLE {table} ADD COLUMN b VARCHAR"))
        .await
        .unwrap();
    client
        .no_query(&format!(
            "INSERT INTO {table} (time, a, b) VALUES (200, 8, 'x')"
        ))
        .await
        .unwrap();

    let rows = client
        .query(&format!("SELECT time, a, b FROM {table} ORDER BY time"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 2);
}

#[tokio::test]
async fn show_tables_lists_catalog_metadata() {
    let table = unique_table("t");
    let mut inst = MonotsInstance::new("sql_show_tables").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .create_table(
            &table,
            vec![
                ts_col(),
                ColumnDef {
                    name: "value".into(),
                    data_type: "Float64".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();

    let rows = client.query("SHOW TABLES").await.unwrap();
    assert_eq!(total_rows(&rows), 1);
    let names = rows[0]
        .column_by_name("table_name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(names.value(0), table);
    let cols = rows[0]
        .column_by_name("column_count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(cols.value(0), 2);
}

#[tokio::test]
async fn show_create_table_returns_ddl_and_metadata() {
    let table = unique_table("t");
    let mut inst = MonotsInstance::new("sql_show_create").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client
        .create_table(
            &table,
            vec![
                ts_col(),
                ColumnDef {
                    name: "value".into(),
                    data_type: "Float64".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();

    let rows = client
        .query(&format!("SHOW CREATE TABLE {table}"))
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
    let ddl = rows[0]
        .column_by_name("create_statement")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert!(ddl.value(0).contains(&format!("CREATE TABLE {table}")));
    assert!(ddl.value(0).contains("time BIGINT NOT NULL"));
    assert!(ddl.value(0).contains("value DOUBLE"));

    let err = client
        .query("SHOW CREATE TABLE not_exists")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not found") || err.to_string().contains("Table"));
}

#[tokio::test]
async fn query_and_no_query_routes_are_enforced() {
    let table = unique_table("t");
    let mut inst = MonotsInstance::new("sql_route_guard").unwrap();
    inst.start().await.unwrap();
    let mut client = inst.authenticated_client().await.unwrap();

    client.create_table(&table, vec![ts_col()]).await.unwrap();

    let err = client
        .query(&format!("INSERT INTO {table} (time) VALUES (1)"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("NoQuery"));

    let err = client
        .no_query(&format!("SELECT * FROM {table}"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Query"));
}
