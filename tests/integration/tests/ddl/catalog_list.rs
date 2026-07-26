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

use monots_integration_tests::{
    table_names_from_show, total_rows, unique_table, TestContext, TIME_COL,
};
use pretty_assertions::assert_eq;

#[tokio::test]
async fn create_many_tables_via_sql_and_list_catalog() {
    let mut ctx = TestContext::new("catalog_many_tables").await;

    let mut tables: Vec<String> = (0..10).map(|i| unique_table(&format!("tbl_{i}"))).collect();

    for (i, table) in tables.iter().enumerate() {
        let col_type = match i % 4 {
            0 => "INT",
            1 => "DOUBLE",
            2 => "VARCHAR",
            _ => "BOOLEAN",
        };
        ctx.client
            .no_query(&format!(
                "CREATE TABLE {table} ({TIME_COL} BIGINT NOT NULL, payload {col_type})"
            ))
            .await
            .unwrap();
        ctx.client
            .no_query(&format!(
                "INSERT INTO {table} ({TIME_COL}, payload) VALUES ({}, {})",
                1000 + i as i64,
                match i % 4 {
                    0 => format!("{}", i),
                    1 => format!("{}.5", i),
                    2 => format!("'row{i}'"),
                    _ => "true".to_string(),
                }
            ))
            .await
            .unwrap();
    }

    let show = ctx.client.query("SHOW TABLES").await.unwrap();
    let mut names: Vec<String> = table_names_from_show(&show).into_iter().collect();
    names.sort();
    tables.sort();
    assert_eq!(names, tables);

    for table in &tables {
        let rows = ctx
            .client
            .query(&format!("SELECT COUNT(*) AS c FROM {table}"))
            .await
            .unwrap();
        assert_eq!(total_rows(&rows), 1);
    }
}

#[tokio::test]
async fn mixed_ddl_via_sql_and_sdk() {
    use monots_core::metadata::catalog::ColumnDef;
    use monots_integration_tests::ts_col;

    let sdk_table = unique_table("sdk");
    let sql_table = unique_table("sql");
    let mut ctx = TestContext::new("catalog_mixed_ddl").await;

    ctx.client
        .create_table(
            &sdk_table,
            vec![
                ts_col(),
                ColumnDef {
                    name: "n".into(),
                    data_type: "Int32".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();

    ctx.client
        .no_query(&format!(
            "CREATE TABLE {sql_table} ({TIME_COL} BIGINT NOT NULL, s VARCHAR)"
        ))
        .await
        .unwrap();

    let show = ctx.client.query("SHOW TABLES").await.unwrap();
    let mut names: Vec<String> = table_names_from_show(&show).into_iter().collect();
    names.sort();
    let mut expected = vec![sdk_table, sql_table];
    expected.sort();
    assert_eq!(names, expected);
}
