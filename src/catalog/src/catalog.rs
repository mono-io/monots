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

use arrow::datatypes::{Schema, SchemaRef};
use common::{Result, TsdbError, TIMESTAMP_COLUMN};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use super::store::{MetaStore, PersistMode};
use crate::column_type::ColumnType;
use crate::types::normalize_type_name;
use monots_storage::sst::SstMeta;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

impl ColumnDef {
    pub fn normalize(&self) -> Result<Self> {
        Ok(Self {
            name: self.name.clone(),
            data_type: normalize_type_name(&self.data_type)?,
            nullable: self.nullable,
        })
    }
}

impl From<&proto::meta::ColumnDef> for ColumnDef {
    fn from(c: &proto::meta::ColumnDef) -> Self {
        Self {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableRuntimeMeta {
    pub parquet_files: Vec<SstMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
    pub data_dir: String,
    #[serde(default)]
    pub runtime: TableRuntimeMeta,
}

/// Metadata gateway for the query/storage engines: an O(1) arrow-schema cache backed by the
/// durable [`MetaStore`]. DDL and manifest commits are async and offload their WAL `fsync` to the
/// blocking pool so tokio worker threads are never parked on disk latency.
pub struct CatalogManager {
    store: Arc<MetaStore>,
    /// O(1) arrow schema resolution for the DataFusion query layer.
    arrow_schemas: DashMap<String, SchemaRef>,
}

impl CatalogManager {
    pub fn new(data_dir: &Path, metadata_memory_limit_bytes: usize) -> Result<Self> {
        let store = MetaStore::open(data_dir, metadata_memory_limit_bytes)?;
        let catalog = Self {
            store: store.clone(),
            arrow_schemas: DashMap::new(),
        };
        catalog.rebuild_schema_cache()?;
        Ok(catalog)
    }

    pub fn meta_store(&self) -> Arc<MetaStore> {
        self.store.clone()
    }

    pub fn persist_mode(&self) -> PersistMode {
        self.store.persist_mode()
    }

    fn rebuild_schema_cache(&self) -> Result<()> {
        self.arrow_schemas.clear();
        for name in self.store.list_tables() {
            if let Some(meta) = self.store.get_table_meta(&name) {
                self.arrow_schemas
                    .insert(name, Self::build_arrow_schema(&meta.columns)?);
            }
        }
        Ok(())
    }

    /// Create a table: I/O (dir creation + WAL write) runs off the async worker thread.
    ///
    /// Concurrent creates for the same name are serialized by
    /// [`MetaStore::create_schema`]'s atomic entry insert — exactly one wins.
    pub async fn create_table(
        &self,
        table_name: &str,
        columns: Vec<ColumnDef>,
        base_data_dir: &Path,
    ) -> Result<()> {
        // Fast path (still racy alone; atomic create_schema below is the authority).
        if self.store.get_table_meta(table_name).is_some() {
            return Err(TsdbError::Schema(format!(
                "table {table_name} already exists"
            )));
        }

        let columns = validate_and_normalize_columns(&columns)?;

        let data_dir = base_data_dir.join(table_name);
        tokio::fs::create_dir_all(&data_dir).await?;

        let schema = Self::build_arrow_schema(&columns)?;
        self.store
            .create_schema_async(proto::meta::TableSchema {
                table_name: table_name.to_string(),
                columns: columns
                    .iter()
                    .map(|c| proto::meta::ColumnDef {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                    })
                    .collect(),
                data_dir: data_dir.to_string_lossy().to_string(),
            })
            .await?;
        self.arrow_schemas.insert(table_name.to_string(), schema);
        Ok(())
    }

    /// Schema evolution: appended columns must be nullable (historic rows have no value).
    pub async fn add_column(&self, table_name: &str, new_col: ColumnDef) -> Result<()> {
        let mut meta = self
            .store
            .get_table_meta(table_name)
            .ok_or_else(|| TsdbError::TableNotFound(table_name.to_string()))?;

        let mut new_col = new_col.normalize()?;
        if meta.columns.iter().any(|c| c.name == new_col.name) {
            return Err(TsdbError::Schema(format!(
                "column {} already exists",
                new_col.name
            )));
        }

        new_col.nullable = true;
        meta.columns.push(new_col);
        let new_schema = Self::build_arrow_schema(&meta.columns)?;

        self.store
            .put_schema_async(proto::meta::TableSchema {
                table_name: meta.table_name.clone(),
                columns: meta
                    .columns
                    .iter()
                    .map(|c| proto::meta::ColumnDef {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                    })
                    .collect(),
                data_dir: meta.data_dir.clone(),
            })
            .await?;
        self.arrow_schemas
            .insert(table_name.to_string(), new_schema);
        Ok(())
    }

    /// Persist the physical file layout (manifest) for a table, off the async worker thread.
    pub async fn update_runtime(&self, table_name: &str, files: Vec<SstMeta>) -> Result<()> {
        self.store
            .set_manifest_async(table_name.to_string(), files)
            .await
    }

    pub async fn drop_table(&self, table_name: &str) -> Result<()> {
        self.store.drop_table_async(table_name.to_string()).await?;
        self.arrow_schemas.remove(table_name);
        Ok(())
    }

    pub fn get_schema(&self, table_name: &str) -> Option<SchemaRef> {
        self.arrow_schemas
            .get(table_name)
            .map(|entry| entry.value().clone())
    }

    pub fn get_table(&self, table_name: &str) -> Option<Arc<TableMeta>> {
        self.store.get_table_meta(table_name).map(Arc::new)
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.store.list_tables()
    }

    pub fn build_arrow_schema(columns: &[ColumnDef]) -> Result<SchemaRef> {
        let mut fields = Vec::with_capacity(columns.len());
        for col in columns {
            let ty = ColumnType::parse(&col.data_type)?;
            fields.push(ty.to_field(&col.name, col.nullable));
        }
        Ok(Arc::new(Schema::new(fields)))
    }
}

/// Validate DDL columns and normalize type names / timestamp constraints.
pub fn validate_and_normalize_columns(columns: &[ColumnDef]) -> Result<Vec<ColumnDef>> {
    if columns.is_empty() {
        return Err(TsdbError::Schema(
            "table must have at least one column".into(),
        ));
    }

    let mut seen = HashSet::new();
    let mut normalized = Vec::with_capacity(columns.len());
    for col in columns {
        if col.name.trim().is_empty() {
            return Err(TsdbError::Schema("column name cannot be empty".into()));
        }
        if !seen.insert(col.name.clone()) {
            return Err(TsdbError::Schema(format!(
                "duplicate column name: {}",
                col.name
            )));
        }
        normalized.push(col.normalize()?);
    }

    if !normalized.iter().any(|c| c.name == TIMESTAMP_COLUMN) {
        return Err(TsdbError::Schema(format!(
            "table must include a time column named `{TIMESTAMP_COLUMN}`"
        )));
    }

    let ts = normalized
        .iter()
        .find(|c| c.name == TIMESTAMP_COLUMN)
        .expect("time column checked above");
    let ts_type = ColumnType::parse(&ts.data_type)?;
    match ts_type {
        ColumnType::Scalar(crate::column_type::ScalarKind::Int64)
        | ColumnType::Scalar(crate::column_type::ScalarKind::Timestamp)
        | ColumnType::Timestamp { .. } => {}
        _ => {
            return Err(TsdbError::Schema(
                "time column must be BIGINT or TIMESTAMP (s/ms/us/ns)".into(),
            ));
        }
    }

    for col in &mut normalized {
        if col.name == TIMESTAMP_COLUMN {
            col.nullable = false;
        }
    }

    Ok(normalized)
}
