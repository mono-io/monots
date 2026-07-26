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

//! Classify incoming SQL into read (`Query`) vs mutating (`NoQuery`) routes.

use common::{Result, TsdbError};

use crate::sql::bulk_load;
use crate::sql::flush;
use crate::sql::stream_ddl;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoQueryKind {
    Insert,
    CreateTable,
    AddColumn,
    DropTable,
    StreamDdl,
    BulkLoad,
    FlushTable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlRoute {
    NoQuery(NoQueryKind),
    Query,
}

pub fn route_sql(sql: &str) -> Result<SqlRoute> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(TsdbError::Query("empty SQL".into()));
    }
    if stream_ddl::is_stream_mutating(trimmed) {
        return Ok(SqlRoute::NoQuery(NoQueryKind::StreamDdl));
    }
    if bulk_load::is_bulk_load_sql(trimmed) {
        return Ok(SqlRoute::NoQuery(NoQueryKind::BulkLoad));
    }
    if flush::is_flush_sql(trimmed) {
        return Ok(SqlRoute::NoQuery(NoQueryKind::FlushTable));
    }

    let upper = trimmed.to_uppercase();
    if upper.starts_with("INSERT") {
        return Ok(SqlRoute::NoQuery(NoQueryKind::Insert));
    }
    if upper.starts_with("CREATE TABLE") {
        return Ok(SqlRoute::NoQuery(NoQueryKind::CreateTable));
    }
    if upper.starts_with("ALTER TABLE") && upper.contains("ADD COLUMN") {
        return Ok(SqlRoute::NoQuery(NoQueryKind::AddColumn));
    }
    if upper.starts_with("DROP TABLE") {
        return Ok(SqlRoute::NoQuery(NoQueryKind::DropTable));
    }

    Ok(SqlRoute::Query)
}

pub fn ensure_no_query(route: SqlRoute) -> Result<NoQueryKind> {
    match route {
        SqlRoute::NoQuery(kind) => Ok(kind),
        SqlRoute::Query => Err(TsdbError::Query(
            "statement is a Query; use the Query API (SELECT)".into(),
        )),
    }
}

pub fn ensure_query(route: SqlRoute) -> Result<()> {
    match route {
        SqlRoute::Query => Ok(()),
        SqlRoute::NoQuery(kind) => Err(TsdbError::Query(format!(
            "statement is NoQuery ({kind:?}); use the NoQuery API (INSERT/DDL)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_create_stream_as_no_query() {
        assert_eq!(
            route_sql("CREATE STREAM s WITH ('sink.type'='delta')").unwrap(),
            SqlRoute::NoQuery(NoQueryKind::StreamDdl)
        );
    }

    #[test]
    fn routes_show_streams_as_query() {
        assert_eq!(route_sql("SHOW STREAMS").unwrap(), SqlRoute::Query);
        assert_eq!(
            route_sql("SHOW STREAM STATUS FOR metrics_out").unwrap(),
            SqlRoute::Query
        );
    }

    #[test]
    fn routes_flush_as_no_query() {
        assert_eq!(
            route_sql("FLUSH TABLE metrics").unwrap(),
            SqlRoute::NoQuery(NoQueryKind::FlushTable)
        );
        assert_eq!(
            route_sql("FLUSH TABLES").unwrap(),
            SqlRoute::NoQuery(NoQueryKind::FlushTable)
        );
    }

    #[test]
    fn routes_bulk_load_as_no_query() {
        assert_eq!(
            route_sql("LOAD PARQUET '/tmp/a.parquet' INTO metrics").unwrap(),
            SqlRoute::NoQuery(NoQueryKind::BulkLoad)
        );
    }

    #[test]
    fn routes_select_as_query() {
        assert_eq!(route_sql("SELECT * FROM metrics").unwrap(), SqlRoute::Query);
    }

    #[test]
    fn routes_show_create_table_as_query() {
        assert_eq!(
            route_sql("SHOW CREATE TABLE metrics").unwrap(),
            SqlRoute::Query
        );
    }

    #[test]
    fn routes_show_tables_as_query() {
        assert_eq!(route_sql("SHOW TABLES").unwrap(), SqlRoute::Query);
    }
}
