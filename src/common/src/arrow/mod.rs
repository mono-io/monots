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

//! Arrow helpers shared by storage flush / scan / CDC materialize.

mod batch_sort;
mod time_column;

pub use batch_sort::{ensure_sorted_by_time, is_sorted_by_time, sort_batch_by_time};
pub use time_column::{time_column_index, time_value_at, time_values_slice};
