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

//! WITH-clause option helpers (FunctionStream-style `sql_options_to_map`).

use datafusion::sql::sqlparser::ast::{Expr, SqlOption, Value};
use std::collections::HashMap;

/// Convert `WITH ('k' = 'v', ...)` [`SqlOption`] list to a flat property map.
pub fn sql_options_to_map(options: &[SqlOption]) -> HashMap<String, String> {
    options
        .iter()
        .filter_map(|opt| match opt {
            SqlOption::KeyValue { key, value } => Some((key.value.clone(), expr_to_string(value))),
            _ => None,
        })
        .collect()
}

fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Value(v) => value_to_string(v),
        Expr::Identifier(ident) => ident.value.clone(),
        other => other.to_string().trim_matches('\'').to_string(),
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
        Value::Number(n, _) => n.clone(),
        Value::Boolean(b) => b.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}
