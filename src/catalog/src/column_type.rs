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

use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use common::{Result, TsdbError};
use datafusion::sql::sqlparser::ast::{
    ArrayElemTypeDef, DataType as SqlDataType, ExactNumberInfo, StructField, TimezoneInfo,
};
use std::fmt;
use std::sync::Arc;

/// Max precision representable by Arrow `Decimal128`.
const DECIMAL128_MAX_PRECISION: u8 = 38;
/// Precision used for a bare `DECIMAL` / `NUMERIC` (no `(p,s)`): wide with a money-friendly scale.
const DECIMAL_DEFAULT_PRECISION: u8 = 38;
const DECIMAL_DEFAULT_SCALE: i8 = 10;

/// Strongly typed column metadata (persisted as canonical string in proto/catalog).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Scalar(ScalarKind),
    /// Fixed-point `DECIMAL(precision, scale)` backed by Arrow `Decimal128`.
    Decimal {
        precision: u8,
        scale: i8,
    },
    /// `TIMESTAMP` with explicit precision / timezone (bare `TIMESTAMP` stays [`ScalarKind::Timestamp`]).
    Timestamp {
        unit: TimeUnit,
        tz: Option<String>,
    },
    List(Box<ColumnType>),
    Struct(Vec<(String, ColumnType)>),
    Enum(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScalarKind {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Boolean,
    Utf8,
    LargeUtf8,
    Timestamp,
    Date32,
    Binary,
    LargeBinary,
}

impl ColumnType {
    pub fn from_sql(dt: &SqlDataType) -> Result<Self> {
        match dt {
            SqlDataType::BigInt(_) => Ok(Self::Scalar(ScalarKind::Int64)),
            SqlDataType::Int(_) | SqlDataType::Integer(_) => Ok(Self::Scalar(ScalarKind::Int32)),
            SqlDataType::SmallInt(_) => Ok(Self::Scalar(ScalarKind::Int16)),
            SqlDataType::TinyInt(_) => Ok(Self::Scalar(ScalarKind::Int8)),
            SqlDataType::UnsignedBigInt(_) | SqlDataType::UnsignedInt8(_) => {
                Ok(Self::Scalar(ScalarKind::UInt64))
            }
            SqlDataType::UnsignedInt(_)
            | SqlDataType::UnsignedInt4(_)
            | SqlDataType::UnsignedInteger(_) => Ok(Self::Scalar(ScalarKind::UInt32)),
            SqlDataType::UnsignedSmallInt(_) | SqlDataType::UnsignedInt2(_) => {
                Ok(Self::Scalar(ScalarKind::UInt16))
            }
            SqlDataType::UnsignedTinyInt(_) => Ok(Self::Scalar(ScalarKind::UInt8)),
            SqlDataType::UnsignedMediumInt(_) => Ok(Self::Scalar(ScalarKind::UInt32)),
            SqlDataType::Double => Ok(Self::Scalar(ScalarKind::Float64)),
            SqlDataType::Float(_) | SqlDataType::Real => Ok(Self::Scalar(ScalarKind::Float32)),
            SqlDataType::Boolean => Ok(Self::Scalar(ScalarKind::Boolean)),
            SqlDataType::Text | SqlDataType::Varchar(_) | SqlDataType::Char(_) => {
                Ok(Self::Scalar(ScalarKind::Utf8))
            }
            SqlDataType::Decimal(info) | SqlDataType::Numeric(info) | SqlDataType::Dec(info) => {
                let (precision, scale) = decimal_precision_scale(info)?;
                Ok(Self::Decimal { precision, scale })
            }
            SqlDataType::Date => Ok(Self::Scalar(ScalarKind::Date32)),
            SqlDataType::Timestamp(precision, tz_info) => {
                let unit = timestamp_unit_from_precision(*precision);
                let tz = timestamp_tz(*tz_info);
                // Bare `TIMESTAMP` (millisecond, no tz) keeps the legacy scalar representation for
                // backward-compatible catalog strings; anything explicit becomes a parameterized type.
                if unit == TimeUnit::Millisecond && tz.is_none() {
                    Ok(Self::Scalar(ScalarKind::Timestamp))
                } else {
                    Ok(Self::Timestamp { unit, tz })
                }
            }
            SqlDataType::Blob(_)
            | SqlDataType::Binary(_)
            | SqlDataType::Varbinary(_)
            | SqlDataType::Bytes(_)
            | SqlDataType::Bytea => Ok(Self::Scalar(ScalarKind::Binary)),
            SqlDataType::Custom(name, _) => {
                let ident = name
                    .0
                    .last()
                    .map(|p| p.value.to_ascii_uppercase())
                    .unwrap_or_default();
                match ident.as_str() {
                    "LARGETEXT" | "LONGTEXT" | "LARGEUTF8" | "LARGESTRING" => {
                        Ok(Self::Scalar(ScalarKind::LargeUtf8))
                    }
                    "LARGEBLOB" | "LONGBLOB" | "LARGEBINARY" => {
                        Ok(Self::Scalar(ScalarKind::LargeBinary))
                    }
                    other => Err(TsdbError::Schema(format!("unsupported SQL type: {other}"))),
                }
            }
            SqlDataType::Enum(variants) => {
                if variants.is_empty() {
                    return Err(TsdbError::Schema(
                        "ENUM requires at least one variant".into(),
                    ));
                }
                Ok(Self::Enum(variants.clone()))
            }
            SqlDataType::Array(elem) => {
                let inner = match elem {
                    ArrayElemTypeDef::None => {
                        return Err(TsdbError::Schema(
                            "ARRAY requires an element type, e.g. ARRAY<INT> or INT[]".into(),
                        ));
                    }
                    ArrayElemTypeDef::AngleBracket(t)
                    | ArrayElemTypeDef::SquareBracket(t, _)
                    | ArrayElemTypeDef::Parenthesis(t) => Self::from_sql(t)?,
                };
                Ok(Self::List(Box::new(inner)))
            }
            SqlDataType::Struct(fields, _) | SqlDataType::Tuple(fields) => {
                Ok(Self::Struct(parse_struct_fields(fields)?))
            }
            other => Err(TsdbError::Schema(format!(
                "unsupported SQL type: {other:?}"
            ))),
        }
    }

    pub fn parse(type_name: &str) -> Result<Self> {
        let (ty, rest) = parse_type_name(type_name.trim())?;
        if !rest.is_empty() {
            return Err(TsdbError::Schema(format!(
                "invalid type suffix in {type_name:?}: {rest:?}"
            )));
        }
        Ok(ty)
    }

    pub fn to_canonical(&self) -> String {
        match self {
            Self::Scalar(kind) => kind.canonical_name().to_string(),
            Self::Decimal { precision, scale } => format!("Decimal({precision},{scale})"),
            Self::Timestamp { unit, tz } => match tz {
                Some(tz) => format!("Timestamp({},{tz})", time_unit_name(*unit)),
                None => format!("Timestamp({})", time_unit_name(*unit)),
            },
            Self::List(inner) => format!("List<{}>", inner.to_canonical()),
            Self::Struct(fields) => {
                let body = fields
                    .iter()
                    .map(|(n, t)| format!("{n}:{}", t.to_canonical()))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("Struct<{body}>")
            }
            Self::Enum(variants) => format!("Enum<{}>", variants.join(",")),
        }
    }

    pub fn to_arrow(&self) -> DataType {
        match self {
            Self::Scalar(kind) => kind.to_arrow(),
            Self::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale),
            Self::Timestamp { unit, tz } => {
                DataType::Timestamp(*unit, tz.as_ref().map(|s| Arc::from(s.as_str())))
            }
            Self::List(inner) => {
                DataType::List(Arc::new(Field::new("item", inner.to_arrow(), true)))
            }
            Self::Struct(fields) => {
                let arrow_fields: Fields = fields
                    .iter()
                    .map(|(name, ty)| Field::new(name, ty.to_arrow(), true))
                    .collect();
                DataType::Struct(arrow_fields)
            }
            Self::Enum(_variants) => {
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8))
            }
        }
    }

    pub fn to_field(&self, name: &str, nullable: bool) -> Field {
        let mut field = Field::new(name, self.to_arrow(), nullable);
        if let Self::Enum(variants) = self {
            field = field.with_metadata(std::collections::HashMap::from([(
                "enum_variants".to_string(),
                variants.join(","),
            )]));
        }
        field
    }

    pub fn to_sql(&self) -> String {
        match self {
            Self::Scalar(kind) => kind.sql_name().to_string(),
            Self::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
            Self::Timestamp { .. } => "TIMESTAMP".to_string(),
            Self::List(inner) => format!("ARRAY<{}>", inner.to_sql()),
            Self::Struct(fields) => {
                let parts = fields
                    .iter()
                    .map(|(n, t)| format!("{n} {}", t.to_sql()))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("STRUCT<{parts}>")
            }
            Self::Enum(variants) => format!("ENUM('{}')", variants.join("','")),
        }
    }

    pub fn enum_variants(&self) -> Option<&[String]> {
        match self {
            Self::Enum(v) => Some(v),
            _ => None,
        }
    }
}

impl ScalarKind {
    fn canonical_name(self) -> &'static str {
        match self {
            Self::Int8 => "Int8",
            Self::Int16 => "Int16",
            Self::Int32 => "Int32",
            Self::Int64 => "Int64",
            Self::UInt8 => "UInt8",
            Self::UInt16 => "UInt16",
            Self::UInt32 => "UInt32",
            Self::UInt64 => "UInt64",
            Self::Float32 => "Float32",
            Self::Float64 => "Float64",
            Self::Boolean => "Boolean",
            Self::Utf8 => "Utf8",
            Self::LargeUtf8 => "LargeUtf8",
            Self::Timestamp => "Timestamp",
            Self::Date32 => "Date32",
            Self::Binary => "Binary",
            Self::LargeBinary => "LargeBinary",
        }
    }

    fn from_canonical(name: &str) -> Result<Self> {
        match name.to_uppercase().as_str() {
            "INT8" | "TINYINT" => Ok(Self::Int8),
            "INT16" | "SMALLINT" => Ok(Self::Int16),
            "INT32" | "INT" | "INTEGER" => Ok(Self::Int32),
            "INT64" | "BIGINT" => Ok(Self::Int64),
            "UINT8" => Ok(Self::UInt8),
            "UINT16" => Ok(Self::UInt16),
            "UINT32" => Ok(Self::UInt32),
            "UINT64" => Ok(Self::UInt64),
            "FLOAT32" | "FLOAT" | "REAL" => Ok(Self::Float32),
            "FLOAT64" | "DOUBLE" => Ok(Self::Float64),
            "BOOLEAN" | "BOOL" => Ok(Self::Boolean),
            "UTF8" | "STRING" | "VARCHAR" | "TEXT" => Ok(Self::Utf8),
            "LARGEUTF8" | "LARGETEXT" | "LONGTEXT" | "LARGESTRING" => Ok(Self::LargeUtf8),
            "TIMESTAMP" => Ok(Self::Timestamp),
            "DATE" | "DATE32" => Ok(Self::Date32),
            "BINARY" | "BLOB" | "VARBINARY" => Ok(Self::Binary),
            "LARGEBINARY" | "LARGEBLOB" | "LONGBLOB" => Ok(Self::LargeBinary),
            other => Err(TsdbError::Schema(format!(
                "unsupported scalar type: {other}"
            ))),
        }
    }

    fn to_arrow(self) -> DataType {
        match self {
            Self::Int8 => DataType::Int8,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::UInt8 => DataType::UInt8,
            Self::UInt16 => DataType::UInt16,
            Self::UInt32 => DataType::UInt32,
            Self::UInt64 => DataType::UInt64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Boolean => DataType::Boolean,
            Self::Utf8 => DataType::Utf8,
            Self::LargeUtf8 => DataType::LargeUtf8,
            Self::Timestamp => DataType::Timestamp(TimeUnit::Millisecond, None),
            Self::Date32 => DataType::Date32,
            Self::Binary => DataType::Binary,
            Self::LargeBinary => DataType::LargeBinary,
        }
    }

    fn sql_name(self) -> &'static str {
        match self {
            Self::Int8 => "TINYINT",
            Self::Int16 => "SMALLINT",
            Self::Int32 => "INT",
            Self::Int64 => "BIGINT",
            Self::UInt8 => "TINYINT UNSIGNED",
            Self::UInt16 => "SMALLINT UNSIGNED",
            Self::UInt32 => "INT UNSIGNED",
            Self::UInt64 => "BIGINT UNSIGNED",
            Self::Float32 => "FLOAT",
            Self::Float64 => "DOUBLE",
            Self::Boolean => "BOOLEAN",
            Self::Utf8 => "VARCHAR",
            Self::LargeUtf8 => "LARGETEXT",
            Self::Timestamp => "TIMESTAMP",
            Self::Date32 => "DATE",
            Self::Binary => "BLOB",
            Self::LargeBinary => "LARGEBLOB",
        }
    }
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_canonical())
    }
}

fn parse_struct_fields(fields: &[StructField]) -> Result<Vec<(String, ColumnType)>> {
    fields
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let name = field
                .field_name
                .as_ref()
                .map(|ident| ident.value.clone())
                .unwrap_or_else(|| format!("f{idx}"));
            Ok((name, ColumnType::from_sql(&field.field_type)?))
        })
        .collect()
}

fn parse_type_name(input: &str) -> Result<(ColumnType, &str)> {
    let input = input.trim();
    if input.starts_with("List<") {
        let (body, consumed) = extract_generic_body(input, "List<".len())?;
        let (inner, tail) = parse_type_name(body)?;
        if !tail.is_empty() {
            return Err(TsdbError::Schema(format!(
                "invalid list element type: {body}"
            )));
        }
        return Ok((ColumnType::List(Box::new(inner)), &input[consumed..]));
    }
    if input.starts_with("Struct<") {
        let (body, consumed) = extract_generic_body(input, "Struct<".len())?;
        let fields = parse_named_type_fields(body)?;
        return Ok((ColumnType::Struct(fields), &input[consumed..]));
    }
    if input.starts_with("Enum<") {
        let (body, consumed) = extract_generic_body(input, "Enum<".len())?;
        if body.is_empty() {
            return Err(TsdbError::Schema("Enum requires variants".into()));
        }
        let variants: Vec<String> = body.split(',').map(|s| s.trim().to_string()).collect();
        return Ok((ColumnType::Enum(variants), &input[consumed..]));
    }
    if input.starts_with("Decimal(") {
        let (body, consumed) = extract_paren_body(input, "Decimal(".len())?;
        let mut parts = body.split(',');
        let precision: u8 = parts
            .next()
            .and_then(|s| s.trim().parse().ok())
            .ok_or_else(|| TsdbError::Schema(format!("invalid decimal precision in {input:?}")))?;
        let scale: i8 = parts
            .next()
            .and_then(|s| s.trim().parse().ok())
            .ok_or_else(|| TsdbError::Schema(format!("invalid decimal scale in {input:?}")))?;
        let precision = clamp_decimal_precision(precision)?;
        return Ok((ColumnType::Decimal { precision, scale }, &input[consumed..]));
    }
    if input.starts_with("Timestamp(") {
        let (body, consumed) = extract_paren_body(input, "Timestamp(".len())?;
        let mut parts = body.split(',');
        let unit = time_unit_from_name(parts.next().unwrap_or("").trim())?;
        let tz = parts
            .next()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        return Ok((ColumnType::Timestamp { unit, tz }, &input[consumed..]));
    }
    let name = input
        .find(|c: char| c == '<' || c == ',')
        .map(|idx| &input[..idx])
        .unwrap_or(input)
        .trim();
    let kind = ScalarKind::from_canonical(name)?;
    Ok((ColumnType::Scalar(kind), &input[name.len()..]))
}

fn parse_named_type_fields(body: &str) -> Result<Vec<(String, ColumnType)>> {
    let mut fields = Vec::new();
    for part in split_top_level_commas(body) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (name, type_text) = part
            .split_once(':')
            .ok_or_else(|| TsdbError::Schema(format!("invalid struct field: {part}")))?;
        let (ty, tail) = parse_type_name(type_text.trim())?;
        if !tail.trim().is_empty() {
            return Err(TsdbError::Schema(format!(
                "invalid struct field type: {type_text}"
            )));
        }
        fields.push((name.trim().to_string(), ty));
    }
    if fields.is_empty() {
        return Err(TsdbError::Schema(
            "struct must contain at least one field".into(),
        ));
    }
    Ok(fields)
}

/// Canonical name for an Arrow [`TimeUnit`] (used in `Timestamp(<unit>[,<tz>])`).
fn time_unit_name(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "Second",
        TimeUnit::Millisecond => "Millisecond",
        TimeUnit::Microsecond => "Microsecond",
        TimeUnit::Nanosecond => "Nanosecond",
    }
}

fn time_unit_from_name(name: &str) -> Result<TimeUnit> {
    match name.to_ascii_lowercase().as_str() {
        "second" | "s" | "sec" => Ok(TimeUnit::Second),
        "millisecond" | "ms" | "milli" => Ok(TimeUnit::Millisecond),
        "microsecond" | "us" | "micro" => Ok(TimeUnit::Microsecond),
        "nanosecond" | "ns" | "nano" => Ok(TimeUnit::Nanosecond),
        other => Err(TsdbError::Schema(format!(
            "invalid timestamp unit: {other}"
        ))),
    }
}

/// Map SQL fractional-second precision (digits) to an Arrow [`TimeUnit`]; `None` → millisecond.
fn timestamp_unit_from_precision(precision: Option<u64>) -> TimeUnit {
    match precision {
        None => TimeUnit::Millisecond,
        Some(0) => TimeUnit::Second,
        Some(1..=3) => TimeUnit::Millisecond,
        Some(4..=6) => TimeUnit::Microsecond,
        Some(_) => TimeUnit::Nanosecond,
    }
}

fn timestamp_tz(tz_info: TimezoneInfo) -> Option<String> {
    match tz_info {
        TimezoneInfo::WithTimeZone | TimezoneInfo::Tz => Some("UTC".to_string()),
        TimezoneInfo::None | TimezoneInfo::WithoutTimeZone => None,
    }
}

fn clamp_decimal_precision(precision: u8) -> Result<u8> {
    if precision == 0 || precision > DECIMAL128_MAX_PRECISION {
        return Err(TsdbError::Schema(format!(
            "DECIMAL precision must be 1..={DECIMAL128_MAX_PRECISION}, got {precision}"
        )));
    }
    Ok(precision)
}

fn decimal_precision_scale(info: &ExactNumberInfo) -> Result<(u8, i8)> {
    match info {
        ExactNumberInfo::None => Ok((DECIMAL_DEFAULT_PRECISION, DECIMAL_DEFAULT_SCALE)),
        ExactNumberInfo::Precision(p) => Ok((clamp_decimal_precision(*p as u8)?, 0)),
        ExactNumberInfo::PrecisionAndScale(p, s) => {
            Ok((clamp_decimal_precision(*p as u8)?, *s as i8))
        }
    }
}

/// Extract the body between `(` and the matching `)`; returns `(body, consumed_len)`.
fn extract_paren_body(input: &str, prefix_len: usize) -> Result<(&str, usize)> {
    let bytes = input.as_bytes();
    let mut i = prefix_len;
    while i < bytes.len() {
        if bytes[i] == b')' {
            return Ok((&input[prefix_len..i], i + 1));
        }
        i += 1;
    }
    Err(TsdbError::Schema(format!(
        "unterminated type parameters: {input}"
    )))
}

fn extract_generic_body(input: &str, prefix_len: usize) -> Result<(&str, usize)> {
    let bytes = input.as_bytes();
    if prefix_len > bytes.len() {
        return Err(TsdbError::Schema(format!(
            "unterminated generic type: {input}"
        )));
    }
    let mut depth = 1usize;
    let mut i = prefix_len;
    while i < bytes.len() {
        match bytes[i] {
            b'<' => depth += 1,
            b'>' => {
                depth -= 1;
                if depth == 0 {
                    return Ok((&input[prefix_len..i], i + 1));
                }
            }
            _ => {}
        }
        i += 1;
    }
    Err(TsdbError::Schema(format!(
        "unterminated generic type: {input}"
    )))
}

fn split_top_level_commas(input: &str) -> Vec<&str> {
    let bytes = input.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'<' => depth += 1,
            b'>' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                parts.push(&input[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    parts.push(&input[start..]);
    parts
}

pub fn enum_variants_from_field(field: &Field) -> Option<Vec<String>> {
    field
        .metadata()
        .get("enum_variants")
        .map(|s| s.split(',').map(|v| v.to_string()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::ast::DataType as SqlDataType;

    #[test]
    fn enum_type_roundtrip() {
        let ty =
            ColumnType::from_sql(&SqlDataType::Enum(vec!["open".into(), "closed".into()])).unwrap();
        assert_eq!(ty.to_canonical(), "Enum<open,closed>");
        assert!(matches!(ty.to_arrow(), DataType::Dictionary(_, _)));
    }

    #[test]
    fn nested_struct_list_roundtrip() {
        let name = "Struct<payload:Binary,tags:List<Utf8>>";
        let ty = ColumnType::parse(name).unwrap();
        assert_eq!(ty.to_canonical(), name);
        assert_eq!(ty.to_sql(), "STRUCT<payload BLOB, tags ARRAY<VARCHAR>>");
    }

    fn roundtrip_canonical(ty: &ColumnType) {
        let canonical = ty.to_canonical();
        let parsed = ColumnType::parse(&canonical).unwrap();
        assert_eq!(&parsed, ty, "canonical roundtrip failed for {canonical}");
    }

    #[test]
    fn decimal_from_sql_and_roundtrip() {
        use datafusion::sql::sqlparser::ast::ExactNumberInfo;
        let ty = ColumnType::from_sql(&SqlDataType::Decimal(ExactNumberInfo::PrecisionAndScale(
            10, 2,
        )))
        .unwrap();
        assert_eq!(
            ty,
            ColumnType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(ty.to_canonical(), "Decimal(10,2)");
        assert_eq!(ty.to_sql(), "DECIMAL(10,2)");
        assert_eq!(ty.to_arrow(), DataType::Decimal128(10, 2));
        roundtrip_canonical(&ty);

        // Bare DECIMAL uses the wide money-friendly default.
        let bare = ColumnType::from_sql(&SqlDataType::Numeric(ExactNumberInfo::None)).unwrap();
        assert_eq!(
            bare,
            ColumnType::Decimal {
                precision: 38,
                scale: 10
            }
        );
        roundtrip_canonical(&bare);
    }

    #[test]
    fn date_from_sql_and_roundtrip() {
        let ty = ColumnType::from_sql(&SqlDataType::Date).unwrap();
        assert_eq!(ty, ColumnType::Scalar(ScalarKind::Date32));
        assert_eq!(ty.to_canonical(), "Date32");
        assert_eq!(ty.to_arrow(), DataType::Date32);
        assert_eq!(ty.to_sql(), "DATE");
        roundtrip_canonical(&ty);
        // Accept the SQL keyword `DATE` as a persisted alias too.
        assert_eq!(
            ColumnType::parse("DATE").unwrap(),
            ColumnType::Scalar(ScalarKind::Date32)
        );
    }

    #[test]
    fn timestamp_precision_and_tz_roundtrip() {
        use arrow::datatypes::TimeUnit;
        use datafusion::sql::sqlparser::ast::TimezoneInfo;

        // Bare TIMESTAMP stays the legacy millisecond scalar for backward compatibility.
        let bare = ColumnType::from_sql(&SqlDataType::Timestamp(None, TimezoneInfo::None)).unwrap();
        assert_eq!(bare, ColumnType::Scalar(ScalarKind::Timestamp));
        assert_eq!(bare.to_canonical(), "Timestamp");

        // Microsecond precision → parameterized type.
        let micros =
            ColumnType::from_sql(&SqlDataType::Timestamp(Some(6), TimezoneInfo::None)).unwrap();
        assert_eq!(
            micros,
            ColumnType::Timestamp {
                unit: TimeUnit::Microsecond,
                tz: None
            }
        );
        assert_eq!(micros.to_canonical(), "Timestamp(Microsecond)");
        assert_eq!(
            micros.to_arrow(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        roundtrip_canonical(&micros);

        // With time zone.
        let tz = ColumnType::from_sql(&SqlDataType::Timestamp(Some(9), TimezoneInfo::WithTimeZone))
            .unwrap();
        assert_eq!(
            tz,
            ColumnType::Timestamp {
                unit: TimeUnit::Nanosecond,
                tz: Some("UTC".to_string())
            }
        );
        assert_eq!(tz.to_canonical(), "Timestamp(Nanosecond,UTC)");
        assert_eq!(
            tz.to_arrow(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        roundtrip_canonical(&tz);
    }

    #[test]
    fn large_utf8_and_binary_from_custom_names() {
        use datafusion::sql::sqlparser::ast::ObjectName;

        let large_text = ColumnType::from_sql(&SqlDataType::Custom(
            ObjectName(vec!["LARGETEXT".into()]),
            vec![],
        ))
        .unwrap();
        assert_eq!(large_text, ColumnType::Scalar(ScalarKind::LargeUtf8));
        assert_eq!(large_text.to_arrow(), DataType::LargeUtf8);
        roundtrip_canonical(&large_text);

        let large_blob = ColumnType::from_sql(&SqlDataType::Custom(
            ObjectName(vec!["LONGBLOB".into()]),
            vec![],
        ))
        .unwrap();
        assert_eq!(large_blob, ColumnType::Scalar(ScalarKind::LargeBinary));
        assert_eq!(large_blob.to_arrow(), DataType::LargeBinary);
        roundtrip_canonical(&large_blob);
    }

    #[test]
    fn list_of_decimal_roundtrip() {
        let ty = ColumnType::parse("List<Decimal(20,4)>").unwrap();
        assert_eq!(
            ty,
            ColumnType::List(Box::new(ColumnType::Decimal {
                precision: 20,
                scale: 4
            }))
        );
        roundtrip_canonical(&ty);
    }
}
