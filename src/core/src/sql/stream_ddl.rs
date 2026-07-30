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

//! Stream DDL SQL helpers (routing + parse + dispatch to stream runtime).
//!
//! Parser lives in [`crate::sql::parser`] alongside other SQL modules.

use arrow::record_batch::RecordBatch;
use common::{Result, TsdbError};
use monots_stream::{
    create_stream, drop_stream, show_stream, show_stream_status, show_streams, StreamDdlContext,
    StreamMutatingOutcome,
};

pub use crate::sql::parser::{
    classify, ensure_stream_ddl_only, parse_one, parse_sql, sql_options_to_map, CreateStreamStmt,
    DropStreamStmt, MonotsDialect, MonotsStatement, ShowStreamStatusStmt, ShowStreamStmt,
    StreamDdlKind,
};

pub fn is_stream_ddl(sql: &str) -> bool {
    is_stream_mutating(sql) || is_stream_show(sql)
}

pub fn is_stream_mutating(sql: &str) -> bool {
    let upper = sql.trim().trim_end_matches(';').to_uppercase();
    upper.starts_with("CREATE STREAM") || upper.starts_with("DROP STREAM")
}

pub fn is_stream_show(sql: &str) -> bool {
    let upper = sql.trim().trim_end_matches(';').to_uppercase();
    upper.starts_with("SHOW STREAM")
}

/// Parse-only validation helper (used by tests / routing).
pub fn parse_stream_ddl(sql: &str) -> Result<()> {
    parse_one(sql).map_err(|e| TsdbError::Query(format!("SQL parse error: {e}")))?;
    Ok(())
}

/// Parse CREATE/DROP STREAM SQL and execute via the stream runtime.
pub async fn execute_mutating(ctx: &StreamDdlContext, sql: &str) -> Result<StreamMutatingOutcome> {
    let stmt = parse_one(sql).map_err(|e| TsdbError::Query(format!("SQL parse error: {e}")))?;
    match classify(&stmt) {
        StreamDdlKind::Create => {
            let MonotsStatement::CreateStream(create) = stmt else {
                return Err(TsdbError::Query("internal: expected CreateStream".into()));
            };
            create_stream(ctx, create.name, create.if_not_exists, create.options).await
        }
        StreamDdlKind::Drop => {
            let MonotsStatement::DropStream(drop) = stmt else {
                return Err(TsdbError::Query("internal: expected DropStream".into()));
            };
            drop_stream(ctx, drop.name).await
        }
        other => Err(TsdbError::Query(format!(
            "{other:?} is a query statement; use the Query API"
        ))),
    }
}

/// Parse SHOW STREAM* SQL and execute via the stream runtime.
pub fn execute_show(ctx: &StreamDdlContext, sql: &str) -> Result<Vec<RecordBatch>> {
    let stmt = parse_one(sql).map_err(|e| TsdbError::Query(format!("SQL parse error: {e}")))?;
    match classify(&stmt) {
        StreamDdlKind::ShowAll => Ok(vec![show_streams(ctx)?]),
        StreamDdlKind::ShowOne => {
            let MonotsStatement::ShowStream(show) = stmt else {
                return Err(TsdbError::Query("internal: expected ShowStream".into()));
            };
            Ok(vec![show_stream(ctx, &show.name)?])
        }
        StreamDdlKind::ShowStatus => {
            let MonotsStatement::ShowStreamStatus(show) = stmt else {
                return Err(TsdbError::Query(
                    "internal: expected ShowStreamStatus".into(),
                ));
            };
            Ok(vec![show_stream_status(ctx, &show.stream_id)?])
        }
        other => Err(TsdbError::Query(format!(
            "{other:?} is not a SHOW stream statement"
        ))),
    }
}
