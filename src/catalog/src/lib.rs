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

//! MonoTS catalog — table schema & durable metadata (pg_class-style).

pub mod catalog;
pub mod column_type;
pub mod store;
pub mod types;

pub use catalog::{
    validate_and_normalize_columns, CatalogManager, ColumnDef, TableMeta, TableRuntimeMeta,
};
pub use column_type::{enum_variants_from_field, ColumnType, ScalarKind};
pub use common::TIMESTAMP_COLUMN;
pub use store::{MetaMemoryStats, MetaStore, PersistMode, METADATA_STORE_VERSION};
pub use types::{
    arrow_type, internal_type_to_sql, normalize_type_name, sql_type_name, supported_types_help,
};
