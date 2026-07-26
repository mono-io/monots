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

//! Fixed RAM estimation for memtable chunk blocks.
//! Variable-length payloads (Utf8/Binary/List values) must be measured at runtime.

use arrow::datatypes::DataType;

/// Rust-side overhead per allocated chunk (Vec / Arc metadata).
pub const CHUNK_CONTAINER_OVERHEAD: usize = 64;

/// Blocks needed to hold `rows` at `array_size` capacity.
#[inline(always)]
pub const fn row_blocks(rows: usize, array_size: usize) -> usize {
    if array_size == 0 {
        return 0;
    }
    rows.div_ceil(array_size)
}

/// New blocks allocated when appending `append_rows` onto `current_rows`.
#[inline(always)]
pub const fn new_row_blocks(current_rows: usize, append_rows: usize, array_size: usize) -> usize {
    row_blocks(current_rows + append_rows, array_size)
        .saturating_sub(row_blocks(current_rows, array_size))
}

/// Fixed (static) RAM for one newly allocated column block.
///
/// For Utf8/Binary/List this is offset/index storage only — value buffers need
/// runtime tracking (e.g. `Array::get_array_memory_size`).
pub fn chunk_column_fixed_ram_cost(
    data_type: &DataType,
    nullable: bool,
    array_size: usize,
) -> usize {
    let fixed_values_cost = match data_type {
        DataType::Boolean => bitmap_bytes(array_size),
        DataType::Int8 | DataType::UInt8 => array_size,
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => array_size * 2,
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_) => array_size * 4,
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => array_size * 8,
        DataType::Decimal128(_, _) => array_size * 16,
        DataType::Decimal256(_, _) => array_size * 32,
        // offsets only (payload tracked at runtime)
        DataType::Utf8 | DataType::Binary | DataType::List(_) => array_size * 4,
        DataType::LargeUtf8 | DataType::LargeBinary | DataType::LargeList(_) => array_size * 8,
        DataType::Utf8View | DataType::BinaryView => array_size * 16,
        DataType::Dictionary(key_type, _) => {
            return chunk_column_fixed_ram_cost(key_type.as_ref(), nullable, array_size);
        }
        other => {
            tracing::warn!(
                ?other,
                "unrecognized DataType in fixed RAM estimation; fallback width=8"
            );
            array_size * 8
        }
    };

    let null_bitmap_cost = if nullable {
        bitmap_bytes(array_size)
    } else {
        0
    };

    fixed_values_cost + null_bitmap_cost + CHUNK_CONTAINER_OVERHEAD
}

/// Fixed RAM for `new_blocks` across all schema columns.
pub fn schema_new_blocks_fixed_ram_cost(
    schema: &arrow::datatypes::SchemaRef,
    new_blocks: usize,
    array_size: usize,
) -> usize {
    if new_blocks == 0 {
        return 0;
    }
    schema
        .fields()
        .iter()
        .map(|field| {
            new_blocks
                * chunk_column_fixed_ram_cost(field.data_type(), field.is_nullable(), array_size)
        })
        .sum()
}

/// Backward-compatible alias for [`chunk_column_fixed_ram_cost`].
pub fn chunk_column_ram_cost(data_type: &DataType, nullable: bool, array_size: usize) -> usize {
    chunk_column_fixed_ram_cost(data_type, nullable, array_size)
}

/// Backward-compatible alias for [`schema_new_blocks_fixed_ram_cost`].
pub fn schema_new_blocks_ram_cost(
    schema: &arrow::datatypes::SchemaRef,
    new_blocks: usize,
    array_size: usize,
) -> usize {
    schema_new_blocks_fixed_ram_cost(schema, new_blocks, array_size)
}

#[inline(always)]
const fn bitmap_bytes(rows: usize) -> usize {
    rows.div_ceil(8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_row_blocks_boundary() {
        assert_eq!(row_blocks(0, 64), 0);
        assert_eq!(row_blocks(1, 64), 1);
        assert_eq!(row_blocks(64, 64), 1);
        assert_eq!(row_blocks(65, 64), 2);
    }

    #[test]
    fn test_new_row_blocks() {
        assert_eq!(new_row_blocks(63, 1, 64), 0);
        assert_eq!(new_row_blocks(64, 1, 64), 1);
        assert_eq!(new_row_blocks(0, 128, 64), 2);
    }

    #[test]
    fn utf8_fixed_cost_is_offsets_only() {
        let utf8 = chunk_column_fixed_ram_cost(&DataType::Utf8, false, 64);
        let int64 = chunk_column_fixed_ram_cost(&DataType::Int64, false, 64);
        // Utf8 fixed ≈ offsets (4*64) + overhead; must not pretend to include value bytes.
        assert_eq!(utf8, 64 * 4 + CHUNK_CONTAINER_OVERHEAD);
        assert_eq!(int64, 64 * 8 + CHUNK_CONTAINER_OVERHEAD);
    }

    #[test]
    fn schema_block_cost_scales_with_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]));
        let one = schema_new_blocks_fixed_ram_cost(&schema, 1, 64);
        assert!(one > chunk_column_fixed_ram_cost(&DataType::Int64, false, 64));
        assert_eq!(schema_new_blocks_fixed_ram_cost(&schema, 2, 64), one * 2);
        assert_eq!(schema_new_blocks_fixed_ram_cost(&schema, 0, 64), 0);
    }
}
