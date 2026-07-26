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

//! `LOAD PARQUET 'path' INTO table` bulk ingest.

use common::{Result, TsdbError};

const LOAD_PREFIX: &str = "LOAD PARQUET";

pub fn is_bulk_load_sql(sql: &str) -> bool {
    sql.trim().to_uppercase().starts_with(LOAD_PREFIX)
}

/// Parse `LOAD PARQUET 'file_or_dir' INTO [TABLE] name`.
pub fn parse_bulk_load(sql: &str) -> Result<(String, String)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();
    if !upper.starts_with(LOAD_PREFIX) {
        return Err(TsdbError::Query("expected LOAD PARQUET".into()));
    }

    let rest = trimmed[LOAD_PREFIX.len()..].trim();
    let (path, after_path) = parse_quoted_literal(rest).ok_or_else(|| {
        TsdbError::Query("LOAD PARQUET requires a quoted file or directory path".into())
    })?;
    let after_path = after_path.trim();
    let after_into = after_path
        .strip_prefix("INTO TABLE")
        .or_else(|| after_path.strip_prefix("INTO"))
        .ok_or_else(|| TsdbError::Query("LOAD PARQUET requires INTO table_name".into()))?
        .trim();

    let table_name = after_into
        .split_whitespace()
        .next()
        .ok_or_else(|| TsdbError::Query("missing table name after INTO".into()))?
        .trim_matches('"')
        .trim_matches('`')
        .to_string();

    if table_name.is_empty() {
        return Err(TsdbError::Query("empty table name".into()));
    }

    Ok((path, table_name))
}

fn parse_quoted_literal(input: &str) -> Option<(String, &str)> {
    let input = input.trim_start();
    if input.is_empty() {
        return None;
    }
    let quote = input.as_bytes()[0];
    if quote != b'\'' && quote != b'"' {
        return None;
    }
    let bytes = input.as_bytes();
    let mut i = 1usize;
    let mut out = String::new();
    while i < bytes.len() {
        let b = bytes[i];
        if b == quote {
            return Some((out, &input[i + 1..]));
        }
        if b == b'\\' && i + 1 < bytes.len() {
            i += 1;
            out.push(bytes[i] as char);
        } else {
            out.push(b as char);
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_bulk_load_prefix() {
        assert!(is_bulk_load_sql("LOAD PARQUET '/x' INTO t"));
        assert!(!is_bulk_load_sql("SELECT * FROM t"));
    }

    #[test]
    fn rejects_missing_into_clause() {
        let err = parse_bulk_load("LOAD PARQUET '/data/a.parquet'").unwrap_err();
        assert!(err.to_string().contains("INTO"));
    }

    #[test]
    fn rejects_unquoted_path() {
        let err = parse_bulk_load("LOAD PARQUET /data/a.parquet INTO t").unwrap_err();
        assert!(err.to_string().contains("quoted"));
    }

    #[test]
    fn parses_load_parquet_into_table() {
        let (path, table) =
            parse_bulk_load("LOAD PARQUET '/data/metrics/part-000.parquet' INTO metrics").unwrap();
        assert_eq!(path, "/data/metrics/part-000.parquet");
        assert_eq!(table, "metrics");
    }

    #[test]
    fn parses_load_parquet_into_table_keyword() {
        let (path, table) =
            parse_bulk_load("LOAD PARQUET \"/data/batch/\" INTO TABLE sensor_readings").unwrap();
        assert_eq!(path, "/data/batch/");
        assert_eq!(table, "sensor_readings");
    }
}
