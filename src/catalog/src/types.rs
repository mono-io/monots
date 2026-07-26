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

use crate::column_type::ColumnType;
use common::Result;
use datafusion::sql::sqlparser::ast::DataType as SqlDataType;

pub use crate::column_type::{enum_variants_from_field, ScalarKind};

/// Map SQL DDL type to internal canonical type name (supports nested types).
pub fn sql_type_name(dt: &SqlDataType) -> Result<String> {
    Ok(ColumnType::from_sql(dt)?.to_canonical())
}

/// Map internal canonical type name to Arrow `DataType`.
pub fn arrow_type(type_name: &str) -> Result<arrow::datatypes::DataType> {
    Ok(ColumnType::parse(type_name)?.to_arrow())
}

pub fn supported_types_help() -> &'static str {
    "Every table requires a `time` column (BIGINT or TIMESTAMP with optional s/ms/us/ns precision). \
     Scalars: Int8-Int64, UInt8-UInt64, Float32/64, Boolean, Utf8/VARCHAR/TEXT, LARGETEXT, \
     DECIMAL(p,s)/NUMERIC, DATE, TIMESTAMP (optional precision/timezone, e.g. TIMESTAMP(6)), \
     Binary/BLOB, LARGEBLOB, ENUM. \
     Nested: List<T>, Struct<name:T,...> (also ARRAY<>, T[], STRUCT<>)"
}

/// Map internal column type name back to SQL DDL keyword.
pub fn internal_type_to_sql(type_name: &str) -> String {
    ColumnType::parse(type_name)
        .map(|t| t.to_sql())
        .unwrap_or_else(|_| type_name.to_string())
}

/// Validate and normalize a persisted type string.
pub fn normalize_type_name(type_name: &str) -> Result<String> {
    Ok(ColumnType::parse(type_name)?.to_canonical())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use datafusion::sql::sqlparser::ast::{ArrayElemTypeDef, DataType as SqlDataType};

    #[test]
    fn maps_unsigned_ddl_types() {
        assert_eq!(
            sql_type_name(&SqlDataType::UnsignedTinyInt(None)).unwrap(),
            "UInt8"
        );
        assert_eq!(
            sql_type_name(&SqlDataType::UnsignedBigInt(None)).unwrap(),
            "UInt64"
        );
    }

    #[test]
    fn maps_blob_and_array_sql_types() {
        assert_eq!(sql_type_name(&SqlDataType::Blob(None)).unwrap(), "Binary");
        assert_eq!(
            sql_type_name(&SqlDataType::Array(ArrayElemTypeDef::AngleBracket(
                Box::new(SqlDataType::Int(None))
            )))
            .unwrap(),
            "List<Int32>"
        );
    }

    #[test]
    fn roundtrips_nested_internal_types() {
        let name = "Struct<payload:Binary,tags:List<Utf8>>";
        let dt = arrow_type(name).unwrap();
        assert!(matches!(dt, DataType::Struct(_)));
        assert_eq!(
            internal_type_to_sql(name),
            "STRUCT<payload BLOB, tags ARRAY<VARCHAR>>"
        );
    }

    #[test]
    fn parses_nested_list_type() {
        let dt = arrow_type("List<List<Int32>>").unwrap();
        assert!(matches!(dt, DataType::List(_)));
    }
}
