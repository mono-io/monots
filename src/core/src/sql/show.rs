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

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::{Result, TsdbError};

use crate::sql::types::internal_type_to_sql;
use monots_catalog::catalog::{CatalogManager, TableMeta};

const SHOW_CREATE_TABLE_PREFIX: &str = "SHOW CREATE TABLE";

pub fn is_show_tables(sql: &str) -> bool {
    let upper = sql.trim().trim_end_matches(';').to_uppercase();
    upper.starts_with("SHOW TABLES")
}

/// Parse `SHOW CREATE TABLE name`; returns `None` if SQL is not this statement.
pub fn parse_show_create_table(sql: &str) -> Result<Option<String>> {
    let trimmed = sql.trim().trim_end_matches(';');
    let upper = trimmed.to_uppercase();
    if !upper.starts_with(SHOW_CREATE_TABLE_PREFIX) {
        return Ok(None);
    }
    let rest = trimmed[SHOW_CREATE_TABLE_PREFIX.len()..].trim();
    if rest.is_empty() {
        return Err(TsdbError::Query(
            "SHOW CREATE TABLE requires a table name".into(),
        ));
    }
    let name = rest.trim_matches('`').trim();
    let name = name
        .rsplit('.')
        .next()
        .ok_or_else(|| TsdbError::Query("invalid table name in SHOW CREATE TABLE".into()))?
        .to_string();
    Ok(Some(name))
}

pub fn format_create_table_ddl(meta: &TableMeta) -> String {
    let cols: Vec<String> = meta
        .columns
        .iter()
        .map(|c| {
            let null = if c.nullable { "" } else { " NOT NULL" };
            format!(
                "  {} {}{}",
                c.name,
                internal_type_to_sql(&c.data_type),
                null
            )
        })
        .collect();
    format!(
        "CREATE TABLE {} (\n{}\n)",
        meta.table_name,
        cols.join(",\n")
    )
}

/// Show one table's metadata and equivalent CREATE TABLE DDL.
pub fn create_table_batch(catalog: &CatalogManager, table_name: &str) -> Result<RecordBatch> {
    let meta = catalog
        .get_table(table_name)
        .ok_or_else(|| TsdbError::TableNotFound(table_name.to_string()))?;

    let create_statement = format_create_table_ddl(&meta);
    let parquet_files = meta.runtime.parquet_files.len() as i64;
    let total_rows: i64 = meta
        .runtime
        .parquet_files
        .iter()
        .map(|f| f.row_count as i64)
        .sum();

    let schema = Arc::new(Schema::new(vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("create_statement", DataType::Utf8, false),
        Field::new("column_count", DataType::Int64, false),
        Field::new("parquet_files", DataType::Int64, false),
        Field::new("total_rows", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![meta.table_name.as_str()])),
            Arc::new(StringArray::from(vec![create_statement])),
            Arc::new(Int64Array::from(vec![meta.columns.len() as i64])),
            Arc::new(Int64Array::from(vec![parquet_files])),
            Arc::new(Int64Array::from(vec![total_rows])),
        ],
    )
    .map_err(|e| TsdbError::Query(e.to_string()))
}

/// List tables from MonoTS metadata catalog (not DataFusion information_schema).
pub fn tables_batch(catalog: &CatalogManager) -> Result<RecordBatch> {
    let mut names = catalog.list_tables();
    names.sort();

    let mut table_names = Vec::with_capacity(names.len());
    let mut column_counts = Vec::with_capacity(names.len());
    let mut file_counts = Vec::with_capacity(names.len());

    for name in names {
        let meta = catalog
            .get_table(&name)
            .ok_or_else(|| TsdbError::TableNotFound(name.clone()))?;
        table_names.push(name);
        column_counts.push(meta.columns.len() as i64);
        file_counts.push(meta.runtime.parquet_files.len() as i64);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_count", DataType::Int64, false),
        Field::new("parquet_files", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(table_names)),
            Arc::new(Int64Array::from(column_counts)),
            Arc::new(Int64Array::from(file_counts)),
        ],
    )
    .map_err(|e| TsdbError::Query(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use monots_catalog::catalog::ColumnDef;
    use uuid::Uuid;

    #[test]
    fn detects_show_create_table() {
        assert_eq!(
            parse_show_create_table("SHOW CREATE TABLE metrics")
                .unwrap()
                .as_deref(),
            Some("metrics")
        );
        assert_eq!(
            parse_show_create_table("show create table `metrics`;")
                .unwrap()
                .as_deref(),
            Some("metrics")
        );
        assert!(parse_show_create_table("SHOW TABLES").unwrap().is_none());
    }

    #[test]
    fn format_create_table_ddl_output() {
        let meta = TableMeta {
            table_name: "metrics".into(),
            columns: vec![
                ColumnDef {
                    name: "time".into(),
                    data_type: "Int64".into(),
                    nullable: false,
                },
                ColumnDef {
                    name: "value".into(),
                    data_type: "Float64".into(),
                    nullable: true,
                },
            ],
            data_dir: "/data/metrics".into(),
            runtime: Default::default(),
        };
        let ddl = format_create_table_ddl(&meta);
        assert!(ddl.contains("CREATE TABLE metrics"));
        assert!(ddl.contains("time BIGINT NOT NULL"));
        assert!(ddl.contains("value DOUBLE"));
    }

    #[tokio::test]
    async fn show_create_table_batch() {
        let dir = std::env::temp_dir().join(format!("monots_show_create_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let catalog = CatalogManager::new(&dir, 16 * 1024 * 1024).unwrap();
        catalog
            .create_table(
                "metrics",
                vec![
                    ColumnDef {
                        name: "time".into(),
                        data_type: "Int64".into(),
                        nullable: false,
                    },
                    ColumnDef {
                        name: "value".into(),
                        data_type: "Float64".into(),
                        nullable: true,
                    },
                ],
                &dir,
            )
            .await
            .unwrap();
        let batch = create_table_batch(&catalog, "metrics").unwrap();
        assert_eq!(batch.num_rows(), 1);
        let ddl = batch
            .column_by_name("create_statement")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(ddl.value(0).contains("CREATE TABLE metrics"));
        let err = create_table_batch(&catalog, "missing").unwrap_err();
        assert!(matches!(err, TsdbError::TableNotFound(_)));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn detects_show_tables() {
        assert!(is_show_tables("SHOW TABLES"));
        assert!(is_show_tables("show tables;"));
        assert!(!is_show_tables("SELECT * FROM t"));
        assert!(!is_show_tables("SHOW STREAMS"));
    }

    #[test]
    fn empty_catalog_returns_empty_batch() {
        let dir = std::env::temp_dir().join(format!("monots_show_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let catalog = CatalogManager::new(&dir, 16 * 1024 * 1024).unwrap();
        let batch = tables_batch(&catalog).unwrap();
        assert_eq!(batch.num_rows(), 0);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn lists_registered_tables() {
        let dir = std::env::temp_dir().join(format!("monots_show2_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let catalog = CatalogManager::new(&dir, 16 * 1024 * 1024).unwrap();
        catalog
            .create_table(
                "metrics",
                vec![
                    ColumnDef {
                        name: "time".into(),
                        data_type: "Int64".into(),
                        nullable: false,
                    },
                    ColumnDef {
                        name: "value".into(),
                        data_type: "Float64".into(),
                        nullable: true,
                    },
                ],
                &dir,
            )
            .await
            .unwrap();
        let batch = tables_batch(&catalog).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "metrics");
        let _ = std::fs::remove_dir_all(dir);
    }
}
