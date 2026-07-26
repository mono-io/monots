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

use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use common::TIME_COLUMN;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;
use datafusion_physical_expr::expressions::{binary, lit};
use datafusion_physical_expr::PhysicalExpr;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct TimeRange {
    pub min_ts: Option<i64>,
    pub max_ts: Option<i64>,
}

impl TimeRange {
    pub fn overlaps(&self, file_min: i64, file_max: i64) -> bool {
        if let Some(min) = self.min_ts {
            if file_max < min {
                return false;
            }
        }
        if let Some(max) = self.max_ts {
            if file_min > max {
                return false;
            }
        }
        true
    }
}

pub fn extract_time_range(filters: &[Expr]) -> TimeRange {
    let mut range = TimeRange::default();
    for expr in filters {
        extract_from_expr(expr, &mut range);
    }
    range
}

fn extract_from_expr(expr: &Expr, range: &mut TimeRange) {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if is_timestamp_column(left) {
                apply_ts_op(range, *op, right);
            } else if is_timestamp_column(right) {
                apply_ts_op_reversed(range, *op, left);
            }
        }
        Expr::Between(expr_between) => {
            if is_timestamp_column(&expr_between.expr) {
                if let Some(low) = scalar_to_i64(&expr_between.low) {
                    range.min_ts = Some(range.min_ts.map_or(low, |m| m.max(low)));
                }
                if let Some(high) = scalar_to_i64(&expr_between.high) {
                    range.max_ts = Some(range.max_ts.map_or(high, |m| m.min(high)));
                }
            }
        }
        Expr::Alias(alias) => extract_from_expr(&alias.expr, range),
        _ => {}
    }
}

fn is_timestamp_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(c) if c.name == TIME_COLUMN)
}

/// True when `expr` is a filter over the time column that [`extract_time_range`] can (partially)
/// turn into min/max bounds. Used to advertise inexact pushdown to the DataFusion optimizer.
pub fn is_time_filter(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
            is_timestamp_column(left) || is_timestamp_column(right)
        }
        Expr::Between(b) => is_timestamp_column(&b.expr),
        Expr::Alias(a) => is_time_filter(&a.expr),
        _ => false,
    }
}

fn apply_ts_op(range: &mut TimeRange, op: Operator, right: &Expr) {
    if let Some(val) = scalar_to_i64(right) {
        match op {
            Operator::Gt | Operator::GtEq => {
                range.min_ts = Some(range.min_ts.map_or(val, |m| m.max(val)));
            }
            Operator::Lt | Operator::LtEq => {
                range.max_ts = Some(range.max_ts.map_or(val, |m| m.min(val)));
            }
            Operator::Eq => {
                range.min_ts = Some(val);
                range.max_ts = Some(val);
            }
            _ => {}
        }
    }
}

fn apply_ts_op_reversed(range: &mut TimeRange, op: Operator, left: &Expr) {
    if let Some(val) = scalar_to_i64(left) {
        match op {
            Operator::Lt | Operator::LtEq => {
                range.min_ts = Some(range.min_ts.map_or(val, |m| m.max(val)));
            }
            Operator::Gt | Operator::GtEq => {
                range.max_ts = Some(range.max_ts.map_or(val, |m| m.min(val)));
            }
            Operator::Eq => {
                range.min_ts = Some(val);
                range.max_ts = Some(val);
            }
            _ => {}
        }
    }
}

fn scalar_to_i64(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int64(v)) => *v,
        Expr::Literal(ScalarValue::TimestampSecond(v, _)) => *v,
        Expr::Literal(ScalarValue::TimestampMillisecond(v, _)) => *v,
        Expr::Literal(ScalarValue::TimestampMicrosecond(v, _)) => *v,
        Expr::Literal(ScalarValue::TimestampNanosecond(v, _)) => *v,
        _ => None,
    }
}

fn time_bound_literal(schema: &SchemaRef, value: i64) -> Option<ScalarValue> {
    let idx = schema.index_of(TIME_COLUMN).ok()?;
    match schema.field(idx).data_type() {
        DataType::Int64 => Some(ScalarValue::Int64(Some(value))),
        DataType::Timestamp(TimeUnit::Second, tz) => {
            Some(ScalarValue::TimestampSecond(Some(value), tz.clone()))
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            Some(ScalarValue::TimestampMillisecond(Some(value), tz.clone()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            Some(ScalarValue::TimestampMicrosecond(Some(value), tz.clone()))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            Some(ScalarValue::TimestampNanosecond(Some(value), tz.clone()))
        }
        _ => None,
    }
}

/// Build a physical predicate for Parquet row-group pruning from extracted time bounds.
pub fn time_range_to_physical_predicate(
    schema: &SchemaRef,
    time_range: &TimeRange,
) -> Option<Arc<dyn PhysicalExpr>> {
    let _ = schema.index_of(TIME_COLUMN).ok()?;
    let col = datafusion_physical_expr::expressions::col(TIME_COLUMN, schema).ok()?;
    let mut parts: Vec<Arc<dyn PhysicalExpr>> = Vec::new();

    if let Some(min) = time_range.min_ts {
        let lit_val = time_bound_literal(schema, min)?;
        parts.push(binary(col.clone(), Operator::GtEq, lit(lit_val), schema.as_ref()).ok()?);
    }
    if let Some(max) = time_range.max_ts {
        let lit_val = time_bound_literal(schema, max)?;
        parts.push(binary(col, Operator::LtEq, lit(lit_val), schema.as_ref()).ok()?);
    }

    match parts.len() {
        0 => None,
        1 => Some(parts.into_iter().next().unwrap()),
        2 => Some(
            binary(
                parts[0].clone(),
                Operator::And,
                parts[1].clone(),
                schema.as_ref(),
            )
            .ok()?,
        ),
        _ => unreachable!(),
    }
}
