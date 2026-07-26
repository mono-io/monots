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

//! `FLUSH TABLE name` / `FLUSH TABLES` — manually flush memtables to SST.

use common::{Result, TsdbError};

pub fn is_flush_sql(sql: &str) -> bool {
    let upper = sql.trim().trim_end_matches(';').trim().to_uppercase();
    upper.starts_with("FLUSH TABLE") || upper.starts_with("FLUSH TABLES")
}

/// `None` = all tables; `Some(name)` = single table.
pub fn parse_flush(sql: &str) -> Result<Option<String>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    if upper == "FLUSH TABLES" {
        return Ok(None);
    }
    if !upper.starts_with("FLUSH TABLE") {
        return Err(TsdbError::Query(
            "expected FLUSH TABLE or FLUSH TABLES".into(),
        ));
    }

    let rest = trimmed["FLUSH TABLE".len()..].trim();
    if rest.is_empty() {
        return Err(TsdbError::Query("FLUSH TABLE requires a table name".into()));
    }

    let name = rest
        .split_whitespace()
        .next()
        .ok_or_else(|| TsdbError::Query("missing table name after FLUSH TABLE".into()))?
        .trim_matches('"')
        .trim_matches('`')
        .to_string();

    if name.is_empty() {
        return Err(TsdbError::Query("empty table name".into()));
    }
    Ok(Some(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_flush_prefix() {
        assert!(is_flush_sql("FLUSH TABLE metrics"));
        assert!(is_flush_sql("FLUSH TABLES;"));
        assert!(!is_flush_sql("SELECT 1"));
    }

    #[test]
    fn parses_single_table() {
        assert_eq!(
            parse_flush("FLUSH TABLE metrics").unwrap(),
            Some("metrics".into())
        );
        assert_eq!(
            parse_flush("FLUSH TABLE `my_table`;").unwrap(),
            Some("my_table".into())
        );
    }

    #[test]
    fn parses_all_tables() {
        assert_eq!(parse_flush("FLUSH TABLES").unwrap(), None);
    }

    #[test]
    fn rejects_missing_table_name() {
        assert!(parse_flush("FLUSH TABLE").is_err());
    }
}
