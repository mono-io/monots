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

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, ListArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Fields, Int8Type, SchemaRef, TimeUnit};
use common::TIMESTAMP_COLUMN;
use common::{Result, TsdbError};
use datafusion::sql::sqlparser::ast::{Expr, Insert, SetExpr, Statement, Value, Values};
use monots_catalog::column_type::enum_variants_from_field;
use monots_storage::reader::BatchAligner;
use serde_json::Value as JsonValue;
use std::sync::Arc;

pub fn build_insert_batch(insert: &Insert, schema: SchemaRef) -> Result<RecordBatch> {
    let column_names: Vec<String> = if insert.columns.is_empty() {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    } else {
        insert.columns.iter().map(|c| c.value.clone()).collect()
    };

    if !column_names.iter().any(|c| c == TIMESTAMP_COLUMN) {
        return Err(TsdbError::Schema(
            "INSERT must include the `time` column".into(),
        ));
    }

    let values = extract_values(insert)?;
    if values.is_empty() {
        return Err(TsdbError::Query("INSERT has no values".into()));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(column_names.len());

    for (col_idx, col_name) in column_names.iter().enumerate() {
        let field = schema
            .field_with_name(col_name)
            .map_err(|_| TsdbError::Schema(format!("unknown column: {col_name}")))?;
        let dt = field.data_type();

        let mut parsed = Vec::with_capacity(values.len());
        for row in &values {
            let expr = row
                .get(col_idx)
                .ok_or_else(|| TsdbError::Query("column count mismatch in VALUES".into()))?;
            parsed.push(parse_expr(expr, field, dt)?);
        }
        columns.push(build_array(field, &parsed)?);
    }

    let partial_schema = Arc::new(arrow::datatypes::Schema::new(
        column_names
            .iter()
            .map(|name| {
                schema
                    .field_with_name(name)
                    .map(|f| f.clone())
                    .map_err(|_| TsdbError::Schema(format!("unknown column: {name}")))
            })
            .collect::<Result<Vec<_>>>()?,
    ));

    let partial_batch = RecordBatch::try_new(partial_schema, columns)
        .map_err(|e| TsdbError::Query(format!("failed to build insert batch: {e}")))?;

    BatchAligner::align(partial_batch, schema)
}

fn extract_values(insert: &Insert) -> Result<Vec<Vec<Expr>>> {
    let Some(source) = &insert.source else {
        return Err(TsdbError::Query("INSERT requires VALUES clause".into()));
    };
    match source.body.as_ref() {
        SetExpr::Values(Values { rows, .. }) => Ok(rows.clone()),
        _ => Err(TsdbError::Query(
            "only INSERT ... VALUES is supported".into(),
        )),
    }
}

#[derive(Debug, Clone)]
enum CellValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Decimal128(i128),
    Str(String),
    Bytes(Vec<u8>),
    Json(JsonValue),
    List(Vec<CellValue>),
    StructRow(Vec<CellValue>),
}

fn parse_expr(expr: &Expr, field: &Field, target: &DataType) -> Result<CellValue> {
    match expr {
        Expr::Nested(inner) => parse_expr(inner, field, target),
        Expr::Array(array) if matches!(target, DataType::List(_)) => {
            let elem_type = match target {
                DataType::List(f) => f.data_type(),
                _ => unreachable!(),
            };
            let dummy = Field::new("item", elem_type.clone(), true);
            let items = array
                .elem
                .iter()
                .map(|e| parse_expr(e, &dummy, elem_type))
                .collect::<Result<Vec<_>>>()?;
            Ok(CellValue::List(items))
        }
        Expr::Tuple(values) if matches!(target, DataType::Struct(_)) => {
            let DataType::Struct(fields) = target else {
                unreachable!()
            };
            if values.len() != fields.len() {
                return Err(TsdbError::Query(format!(
                    "struct literal field count mismatch: expected {}, got {}",
                    fields.len(),
                    values.len()
                )));
            }
            let row = values
                .iter()
                .zip(fields.iter())
                .map(|(expr, field)| parse_expr(expr, field, field.data_type()))
                .collect::<Result<Vec<_>>>()?;
            Ok(CellValue::StructRow(row))
        }
        Expr::Value(value) => parse_value(value, field, target),
        Expr::UnaryOp { op, expr } => {
            use datafusion::sql::sqlparser::ast::UnaryOperator;
            let inner = parse_expr(expr, field, target)?;
            match (op, inner) {
                (UnaryOperator::Minus, CellValue::Int64(v)) => Ok(CellValue::Int64(-v)),
                (UnaryOperator::Minus, CellValue::Float64(v)) => Ok(CellValue::Float64(-v)),
                (UnaryOperator::Minus, CellValue::Decimal128(v)) => Ok(CellValue::Decimal128(-v)),
                _ => Err(TsdbError::Query(
                    "unsupported unary expression in VALUES".into(),
                )),
            }
        }
        other => Err(TsdbError::Query(format!(
            "unsupported expression in VALUES: {other:?}"
        ))),
    }
}

fn parse_value(value: &Value, field: &Field, target: &DataType) -> Result<CellValue> {
    match value {
        Value::Null => Ok(CellValue::Null),
        Value::Boolean(b) => Ok(CellValue::Bool(*b)),
        Value::Number(n, _) => {
            if let DataType::Decimal128(_, scale) = target {
                Ok(CellValue::Decimal128(parse_decimal_i128(n, *scale)?))
            } else if matches!(target, DataType::Float32 | DataType::Float64) {
                Ok(CellValue::Float64(n.parse().map_err(|_| {
                    TsdbError::Query(format!("invalid number: {n}"))
                })?))
            } else {
                Ok(CellValue::Int64(n.parse().map_err(|_| {
                    TsdbError::Query(format!("invalid integer: {n}"))
                })?))
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            if let DataType::Dictionary(_, _) = target {
                validate_enum_variant(field, s)?;
                return Ok(CellValue::Str(s.clone()));
            }
            if matches!(target, DataType::Date32) {
                return Ok(CellValue::Int64(parse_date_to_days(s)?));
            }
            if matches!(target, DataType::List(_) | DataType::Struct(_)) {
                let json = serde_json::from_str(s).map_err(|e| {
                    TsdbError::Query(format!(
                        "expected JSON literal for {target:?} column, parse error: {e}"
                    ))
                })?;
                return Ok(CellValue::Json(json));
            }
            Ok(CellValue::Str(s.clone()))
        }
        Value::HexStringLiteral(hex) => Ok(CellValue::Bytes(decode_hex(hex)?)),
        other => Err(TsdbError::Query(format!(
            "unsupported value literal in VALUES: {other:?}"
        ))),
    }
}

fn validate_enum_variant(field: &Field, value: &str) -> Result<()> {
    let variants = enum_variants_from_field(field).ok_or_else(|| {
        TsdbError::Schema(format!(
            "missing enum metadata on column `{}`",
            field.name()
        ))
    })?;
    if !variants.iter().any(|v| v == value) {
        return Err(TsdbError::Query(format!(
            "invalid enum value `{value}` for column `{}`, allowed: {}",
            field.name(),
            variants.join(", ")
        )));
    }
    Ok(())
}

fn decode_hex(hex: &str) -> Result<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return Err(TsdbError::Query(format!("invalid hex literal: {hex}")));
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|_| TsdbError::Query(format!("invalid hex literal: {hex}")))
        })
        .collect()
}

fn build_array(field: &Field, cells: &[CellValue]) -> Result<ArrayRef> {
    match field.data_type() {
        DataType::Dictionary(_, _) => build_enum_array(field, cells),
        DataType::Int8 => build_int8_array(cells),
        DataType::Int16 => build_int16_array(cells),
        DataType::Int32 => build_int32_array(cells),
        DataType::Int64 => build_int64_array(cells),
        DataType::UInt8 => build_uint8_array(cells),
        DataType::UInt16 => build_uint16_array(cells),
        DataType::UInt32 => build_uint32_array(cells),
        DataType::UInt64 => build_uint64_array(cells),
        DataType::Float32 => build_float32_array(cells),
        DataType::Float64 => build_float64_array(cells),
        DataType::Boolean => build_bool_array(cells),
        DataType::Utf8 => build_utf8_array(cells),
        DataType::LargeUtf8 => build_large_utf8_array(cells),
        DataType::Decimal128(precision, scale) => build_decimal128_array(cells, *precision, *scale),
        DataType::Date32 => build_date32_array(cells),
        DataType::Timestamp(unit, tz) => build_timestamp_array(cells, *unit, tz.clone()),
        DataType::Binary => build_binary_array(cells),
        DataType::LargeBinary => build_large_binary_array(cells),
        DataType::List(inner) => build_list_array(inner.data_type(), cells),
        DataType::Struct(fields) => build_struct_array(fields, cells),
        other => Err(TsdbError::Schema(format!(
            "unsupported insert type: {other:?}"
        ))),
    }
}

macro_rules! prim_array {
    ($cells:expr, $ty:ty, $constructor:expr, $extract:ident) => {{
        let values: Vec<Option<$ty>> = $cells
            .iter()
            .map(|c| match c {
                CellValue::Null => Ok(None),
                CellValue::$extract(v) => Ok(Some(*v as $ty)),
                CellValue::Float64(v) => Ok(Some(*v as $ty)),
                other => Err(TsdbError::Query(format!(
                    "type mismatch: expected numeric, got {other:?}"
                ))),
            })
            .collect::<Result<_>>()?;
        Ok(Arc::new($constructor(values)) as ArrayRef)
    }};
}

fn build_int8_array(cells: &[CellValue]) -> Result<ArrayRef> {
    prim_array!(cells, i8, Int8Array::from, Int64)
}
fn build_int16_array(cells: &[CellValue]) -> Result<ArrayRef> {
    prim_array!(cells, i16, Int16Array::from, Int64)
}
fn build_int32_array(cells: &[CellValue]) -> Result<ArrayRef> {
    prim_array!(cells, i32, Int32Array::from, Int64)
}
fn build_int64_array(cells: &[CellValue]) -> Result<ArrayRef> {
    prim_array!(cells, i64, Int64Array::from, Int64)
}
fn build_uint8_array(cells: &[CellValue]) -> Result<ArrayRef> {
    prim_array!(cells, u8, UInt8Array::from, Int64)
}
fn build_uint16_array(cells: &[CellValue]) -> Result<ArrayRef> {
    prim_array!(cells, u16, UInt16Array::from, Int64)
}
fn build_uint32_array(cells: &[CellValue]) -> Result<ArrayRef> {
    prim_array!(cells, u32, UInt32Array::from, Int64)
}
fn build_uint64_array(cells: &[CellValue]) -> Result<ArrayRef> {
    prim_array!(cells, u64, UInt64Array::from, Int64)
}

fn build_float32_array(cells: &[CellValue]) -> Result<ArrayRef> {
    let values: Vec<Option<f32>> = cells
        .iter()
        .map(|c| match c {
            CellValue::Null => Ok(None),
            CellValue::Float64(v) => Ok(Some(*v as f32)),
            CellValue::Int64(v) => Ok(Some(*v as f32)),
            other => Err(TsdbError::Query(format!(
                "type mismatch: expected float, got {other:?}"
            ))),
        })
        .collect::<Result<_>>()?;
    Ok(Arc::new(Float32Array::from(values)))
}

fn build_float64_array(cells: &[CellValue]) -> Result<ArrayRef> {
    let values: Vec<Option<f64>> = cells
        .iter()
        .map(|c| match c {
            CellValue::Null => Ok(None),
            CellValue::Float64(v) => Ok(Some(*v)),
            CellValue::Int64(v) => Ok(Some(*v as f64)),
            other => Err(TsdbError::Query(format!(
                "type mismatch: expected float, got {other:?}"
            ))),
        })
        .collect::<Result<_>>()?;
    Ok(Arc::new(Float64Array::from(values)))
}

fn build_bool_array(cells: &[CellValue]) -> Result<ArrayRef> {
    let values: Vec<Option<bool>> = cells
        .iter()
        .map(|c| match c {
            CellValue::Null => Ok(None),
            CellValue::Bool(v) => Ok(Some(*v)),
            other => Err(TsdbError::Query(format!(
                "type mismatch: expected bool, got {other:?}"
            ))),
        })
        .collect::<Result<_>>()?;
    Ok(Arc::new(BooleanArray::from(values)))
}

fn build_utf8_array(cells: &[CellValue]) -> Result<ArrayRef> {
    let values: Vec<Option<String>> = cells
        .iter()
        .map(|c| match c {
            CellValue::Null => Ok(None),
            CellValue::Str(v) => Ok(Some(v.clone())),
            other => Err(TsdbError::Query(format!(
                "type mismatch: expected string, got {other:?}"
            ))),
        })
        .collect::<Result<_>>()?;
    Ok(Arc::new(StringArray::from(values)))
}

fn build_large_utf8_array(cells: &[CellValue]) -> Result<ArrayRef> {
    let values: Vec<Option<String>> = cells
        .iter()
        .map(|c| match c {
            CellValue::Null => Ok(None),
            CellValue::Str(v) => Ok(Some(v.clone())),
            other => Err(TsdbError::Query(format!(
                "type mismatch: expected string, got {other:?}"
            ))),
        })
        .collect::<Result<_>>()?;
    Ok(Arc::new(LargeStringArray::from(values)))
}

fn build_decimal128_array(cells: &[CellValue], precision: u8, scale: i8) -> Result<ArrayRef> {
    let values: Vec<Option<i128>> = cells
        .iter()
        .map(|c| match c {
            CellValue::Null => Ok(None),
            CellValue::Decimal128(v) => Ok(Some(*v)),
            CellValue::Int64(v) => Ok(Some(*v as i128)),
            other => Err(TsdbError::Query(format!(
                "type mismatch: expected decimal, got {other:?}"
            ))),
        })
        .collect::<Result<_>>()?;
    let array = Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| TsdbError::Query(format!("invalid decimal value: {e}")))?;
    Ok(Arc::new(array) as ArrayRef)
}

fn build_date32_array(cells: &[CellValue]) -> Result<ArrayRef> {
    let values: Vec<Option<i32>> = cells
        .iter()
        .map(|c| match c {
            CellValue::Null => Ok(None),
            CellValue::Int64(v) => Ok(Some(*v as i32)),
            other => Err(TsdbError::Query(format!(
                "type mismatch: expected date (integer days or 'YYYY-MM-DD'), got {other:?}"
            ))),
        })
        .collect::<Result<_>>()?;
    Ok(Arc::new(Date32Array::from(values)) as ArrayRef)
}

fn build_timestamp_array(
    cells: &[CellValue],
    unit: TimeUnit,
    tz: Option<Arc<str>>,
) -> Result<ArrayRef> {
    let values: Vec<Option<i64>> = cells
        .iter()
        .map(|c| match c {
            CellValue::Null => Ok(None),
            CellValue::Int64(v) => Ok(Some(*v)),
            other => Err(TsdbError::Query(format!(
                "type mismatch: expected timestamp, got {other:?}"
            ))),
        })
        .collect::<Result<_>>()?;
    let array: ArrayRef = match unit {
        TimeUnit::Second => Arc::new(TimestampSecondArray::from(values).with_timezone_opt(tz)),
        TimeUnit::Millisecond => {
            Arc::new(TimestampMillisecondArray::from(values).with_timezone_opt(tz))
        }
        TimeUnit::Microsecond => {
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(tz))
        }
        TimeUnit::Nanosecond => {
            Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(tz))
        }
    };
    Ok(array)
}

fn build_binary_array(cells: &[CellValue]) -> Result<ArrayRef> {
    let values: Vec<Option<Vec<u8>>> = cells
        .iter()
        .map(|c| match c {
            CellValue::Null => Ok(None),
            CellValue::Bytes(v) => Ok(Some(v.clone())),
            other => Err(TsdbError::Query(format!(
                "type mismatch: expected binary (use X'...' hex literal), got {other:?}"
            ))),
        })
        .collect::<Result<_>>()?;
    Ok(Arc::new(BinaryArray::from_iter(values)) as ArrayRef)
}

fn build_large_binary_array(cells: &[CellValue]) -> Result<ArrayRef> {
    let values: Vec<Option<Vec<u8>>> = cells
        .iter()
        .map(|c| match c {
            CellValue::Null => Ok(None),
            CellValue::Bytes(v) => Ok(Some(v.clone())),
            other => Err(TsdbError::Query(format!(
                "type mismatch: expected binary (use X'...' hex literal), got {other:?}"
            ))),
        })
        .collect::<Result<_>>()?;
    Ok(Arc::new(LargeBinaryArray::from_iter(values)) as ArrayRef)
}

/// Parse a decimal literal (e.g. `123.45`) into the raw `i128` for the given `scale`.
/// Extra fractional digits are truncated; missing ones are zero-padded.
fn parse_decimal_i128(token: &str, scale: i8) -> Result<i128> {
    if scale < 0 {
        return Err(TsdbError::Query(
            "negative-scale DECIMAL literals are not supported".into(),
        ));
    }
    let scale = scale as usize;
    let negative = token.starts_with('-');
    let unsigned = token.trim_start_matches(['+', '-']);
    let (int_part, frac_part) = match unsigned.split_once('.') {
        Some((a, b)) => (a, b),
        None => (unsigned, ""),
    };
    if int_part.chars().any(|c| !c.is_ascii_digit())
        || frac_part.chars().any(|c| !c.is_ascii_digit())
    {
        return Err(TsdbError::Query(format!(
            "invalid decimal literal: {token}"
        )));
    }
    let mut frac = frac_part.to_string();
    if frac.len() > scale {
        frac.truncate(scale);
    } else {
        while frac.len() < scale {
            frac.push('0');
        }
    }
    let digits = format!("{int_part}{frac}");
    let digits = digits.trim_start_matches('0');
    let magnitude: i128 = if digits.is_empty() {
        0
    } else {
        digits
            .parse()
            .map_err(|_| TsdbError::Query(format!("decimal literal out of range: {token}")))?
    };
    Ok(if negative { -magnitude } else { magnitude })
}

/// Parse a `YYYY-MM-DD` date literal into days since the Unix epoch (Arrow `Date32`).
fn parse_date_to_days(s: &str) -> Result<i64> {
    let parts: Vec<&str> = s.trim().split('-').collect();
    if parts.len() != 3 {
        return Err(TsdbError::Query(format!(
            "invalid date literal (expected YYYY-MM-DD): {s}"
        )));
    }
    let y: i64 = parts[0]
        .parse()
        .map_err(|_| TsdbError::Query(format!("invalid date year: {s}")))?;
    let m: i64 = parts[1]
        .parse()
        .map_err(|_| TsdbError::Query(format!("invalid date month: {s}")))?;
    let d: i64 = parts[2]
        .parse()
        .map_err(|_| TsdbError::Query(format!("invalid date day: {s}")))?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(TsdbError::Query(format!("date out of range: {s}")));
    }
    Ok(days_from_civil(y, m, d))
}

/// Howard Hinnant's `days_from_civil`: proleptic Gregorian date → days since 1970-01-01.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn build_list_array(elem_type: &DataType, cells: &[CellValue]) -> Result<ArrayRef> {
    let mut offsets = vec![0_i32];
    let mut child_cells = Vec::new();
    let mut nulls = Vec::with_capacity(cells.len());

    for cell in cells {
        match cell {
            CellValue::Null => {
                nulls.push(false);
                offsets.push(offsets.last().copied().unwrap_or(0));
            }
            CellValue::List(items) => {
                nulls.push(true);
                child_cells.extend(items.clone());
                offsets.push(child_cells.len() as i32);
            }
            CellValue::Json(JsonValue::Array(items)) => {
                nulls.push(true);
                for item in items {
                    child_cells.push(json_to_cell(item, elem_type)?);
                }
                offsets.push(child_cells.len() as i32);
            }
            other => {
                return Err(TsdbError::Query(format!(
                    "expected ARRAY[...] or JSON array for list column, got {other:?}"
                )));
            }
        }
    }

    let values = build_array(&Field::new("item", elem_type.clone(), true), &child_cells)?;
    let list_field = Arc::new(Field::new("item", elem_type.clone(), true));
    let list_array = ListArray::try_new(
        list_field,
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        values,
        Some(NullBuffer::from(nulls)),
    )
    .map_err(|e| TsdbError::Query(format!("failed to build list array: {e}")))?;
    Ok(Arc::new(list_array) as ArrayRef)
}

fn build_struct_array(fields: &Fields, cells: &[CellValue]) -> Result<ArrayRef> {
    let num_rows = cells.len();
    let mut per_field: Vec<Vec<CellValue>> = vec![Vec::with_capacity(num_rows); fields.len()];

    for cell in cells {
        match cell {
            CellValue::Null => {
                for col in &mut per_field {
                    col.push(CellValue::Null);
                }
            }
            CellValue::StructRow(row) => {
                if row.len() != fields.len() {
                    return Err(TsdbError::Query(
                        "struct literal field count mismatch".into(),
                    ));
                }
                for (idx, value) in row.iter().enumerate() {
                    per_field[idx].push(value.clone());
                }
            }
            CellValue::Json(JsonValue::Object(map)) => {
                for (idx, field) in fields.iter().enumerate() {
                    per_field[idx].push(json_field_to_cell(
                        map.get(field.name()),
                        field.data_type(),
                    )?);
                }
            }
            other => {
                return Err(TsdbError::Query(format!(
                    "expected struct literal `(..)` or JSON object, got {other:?}"
                )));
            }
        }
    }

    let child_columns = fields
        .iter()
        .zip(per_field.iter())
        .map(|(field, col_cells)| build_array(field, col_cells))
        .collect::<Result<Vec<_>>>()?;

    let nulls: Vec<bool> = cells
        .iter()
        .map(|cell| !matches!(cell, CellValue::Null))
        .collect();
    let struct_array = arrow::array::StructArray::new(
        fields.clone(),
        child_columns,
        Some(NullBuffer::from(nulls)),
    );
    if struct_array.len() != num_rows {
        return Err(TsdbError::Query(format!(
            "struct row count mismatch: expected {num_rows}, got {}",
            struct_array.len()
        )));
    }
    Ok(Arc::new(struct_array) as ArrayRef)
}

fn build_enum_array(field: &Field, cells: &[CellValue]) -> Result<ArrayRef> {
    let variants = enum_variants_from_field(field).ok_or_else(|| {
        TsdbError::Schema(format!(
            "missing enum metadata on column `{}`",
            field.name()
        ))
    })?;
    let dict_values = Arc::new(StringArray::from(variants.clone()));
    let keys: Vec<Option<i8>> = cells
        .iter()
        .map(|c| match c {
            CellValue::Null => Ok(None),
            CellValue::Str(v) => variants
                .iter()
                .position(|variant| variant == v)
                .map(|idx| idx as i8)
                .ok_or_else(|| {
                    TsdbError::Query(format!(
                        "invalid enum value `{v}` for column `{}`",
                        field.name()
                    ))
                })
                .map(Some),
            other => Err(TsdbError::Query(format!(
                "type mismatch: expected enum string, got {other:?}"
            ))),
        })
        .collect::<Result<_>>()?;
    let array = DictionaryArray::<Int8Type>::try_new(Int8Array::from(keys), dict_values)
        .map_err(|e| TsdbError::Query(format!("failed to build enum array: {e}")))?;
    Ok(Arc::new(array) as ArrayRef)
}

fn json_to_cell(value: &JsonValue, target: &DataType) -> Result<CellValue> {
    if value.is_null() {
        return Ok(CellValue::Null);
    }
    match target {
        DataType::Boolean => match value {
            JsonValue::Bool(b) => Ok(CellValue::Bool(*b)),
            _ => Err(TsdbError::Query("expected boolean JSON value".into())),
        },
        DataType::Utf8 => match value {
            JsonValue::String(s) => Ok(CellValue::Str(s.clone())),
            _ => Err(TsdbError::Query("expected string JSON value".into())),
        },
        DataType::Float32 | DataType::Float64 => match value {
            JsonValue::Number(n) => Ok(CellValue::Float64(n.as_f64().unwrap_or(0.0))),
            _ => Err(TsdbError::Query("expected numeric JSON value".into())),
        },
        DataType::Binary | DataType::LargeBinary => match value {
            JsonValue::String(s) => Ok(CellValue::Bytes(s.as_bytes().to_vec())),
            _ => Err(TsdbError::Query(
                "expected string JSON value for binary".into(),
            )),
        },
        DataType::List(_) | DataType::Struct(_) => Ok(CellValue::Json(value.clone())),
        _ => match value {
            JsonValue::Number(n) => {
                Ok(CellValue::Int64(n.as_i64().ok_or_else(|| {
                    TsdbError::Query("expected integer JSON value".into())
                })?))
            }
            _ => Err(TsdbError::Query(format!(
                "unsupported JSON value for {target:?}"
            ))),
        },
    }
}

fn json_field_to_cell(value: Option<&JsonValue>, target: &DataType) -> Result<CellValue> {
    match value {
        None | Some(JsonValue::Null) => Ok(CellValue::Null),
        Some(value) => json_to_cell(value, target),
    }
}

pub fn insert_from_statement(stmt: &Statement) -> Option<&Insert> {
    match stmt {
        Statement::Insert(insert) => Some(insert),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::sql::parser::Statement as DfStatement;
    use datafusion::sql::sqlparser::ast::Statement;
    use std::sync::Arc;

    fn parse_insert(sql: &str) -> Insert {
        let stmt = datafusion::sql::parser::DFParser::parse_sql(sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        match stmt {
            DfStatement::Statement(boxed) => match *boxed {
                Statement::Insert(insert) => insert,
                other => panic!("expected insert, got {other:?}"),
            },
            other => panic!("expected statement, got {other:?}"),
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "meta",
                DataType::Struct(
                    vec![
                        Field::new("name", DataType::Utf8, true),
                        Field::new("score", DataType::Int32, true),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new("payload", DataType::Binary, true),
        ]))
    }

    #[test]
    fn parses_array_and_struct_literals_from_sqlparser() {
        let insert = parse_insert(
            "INSERT INTO t (time, tags, meta, payload) VALUES \
             (1000, ARRAY['a','b'], ('alice', 90), X'FF')",
        );
        let batch = build_insert_batch(&insert, test_schema()).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn parse_decimal_literal_scales_and_pads() {
        assert_eq!(parse_decimal_i128("123.45", 2).unwrap(), 12345);
        assert_eq!(parse_decimal_i128("1", 2).unwrap(), 100);
        assert_eq!(parse_decimal_i128("1.5", 2).unwrap(), 150);
        assert_eq!(parse_decimal_i128("1.239", 2).unwrap(), 123); // truncates extra digit
        assert_eq!(parse_decimal_i128("0", 4).unwrap(), 0);
        assert!(parse_decimal_i128("abc", 2).is_err());
    }

    #[test]
    fn days_from_civil_matches_known_dates() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1970, 1, 2), 1);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
        assert_eq!(days_from_civil(2000, 1, 1), 10957);
        assert_eq!(parse_date_to_days("2021-06-01").unwrap(), 18779);
    }

    #[test]
    fn builds_decimal_date_timestamp_and_large_columns() {
        use arrow::array::{
            Date32Array, Decimal128Array, LargeBinaryArray, LargeStringArray,
            TimestampMicrosecondArray,
        };
        use arrow::datatypes::TimeUnit;

        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("price", DataType::Decimal128(10, 2), true),
            Field::new("day", DataType::Date32, true),
            Field::new(
                "event_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("note", DataType::LargeUtf8, true),
            Field::new("blob", DataType::LargeBinary, true),
        ]));

        let insert = parse_insert(
            "INSERT INTO t (time, price, day, event_at, note, blob) VALUES \
             (1000, 12.34, '2021-06-01', 1717236000000000, 'hello', X'DEADBEEF'), \
             (2000, -5, 100, 500, 'world', X'01')",
        );
        let batch = build_insert_batch(&insert, schema).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let price = batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(price.value(0), 1234);
        assert_eq!(price.value(1), -500);
        assert_eq!(price.precision(), 10);
        assert_eq!(price.scale(), 2);

        let day = batch
            .column_by_name("day")
            .unwrap()
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(day.value(0), 18779);
        assert_eq!(day.value(1), 100);

        let event_at = batch
            .column_by_name("event_at")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(event_at.value(0), 1717236000000000);

        let note = batch
            .column_by_name("note")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(note.value(0), "hello");

        let blob = batch
            .column_by_name("blob")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(blob.value(0), &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn rejects_invalid_enum_variant() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new(
                "status",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            )
            .with_metadata(std::collections::HashMap::from([(
                "enum_variants".to_string(),
                "open,closed".to_string(),
            )])),
        ]));
        let insert = parse_insert("INSERT INTO t (time, status) VALUES (1000, 'invalid')");
        let err = build_insert_batch(&insert, schema).unwrap_err();
        assert!(err.to_string().contains("invalid enum"));
    }
}
