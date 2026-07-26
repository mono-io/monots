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

use crate::filter::{extract_time_range, time_range_to_physical_predicate};
use crate::lsm_merge::{build_lsm_scan_plan, LsmLayer};
use crate::mem_exec::MemTableScanExec;
use crate::schema_align_exec::SchemaAlignExec;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use chrono::TimeZone;
use common::TIME_COLUMN;
use datafusion::catalog::Session;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_physical_expr::PhysicalExpr;
use monots_storage::parquet_read::read_parquet_schema;
use monots_storage::table::LsmTable;
use monots_storage::SstMeta;
use std::any::Any;
use std::sync::Arc;

/// Bridges [`LsmTable`] to DataFusion (pipelined scan, no eager IO).
pub struct LsmTableProvider {
    pub name: String,
    pub schema: SchemaRef,
    pub table: Arc<LsmTable>,
}

impl std::fmt::Debug for LsmTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LsmTableProvider")
            .field("name", &self.name)
            .finish()
    }
}

#[async_trait]
impl TableProvider for LsmTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Advertise time-column filters as inexact pushdown so the optimizer forwards them to
    /// [`Self::scan`] (enabling file/row-group pruning) while still keeping a `FilterExec` above
    /// for exact results — min/max-based file pruning only coarsely narrows the scan.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| {
                if crate::filter::is_time_filter(expr) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let (mutable, immutables, sst_files) = self.table.get_snapshots();
        let time_range = extract_time_range(filters);

        let mut layers: Vec<LsmLayer> = Vec::new();
        let mut layer_priority = 0usize;

        // Newest → oldest; layer_priority 0 = newest.
        if mutable.size_bytes() > 0 {
            layers.push(LsmLayer {
                plan: Arc::new(MemTableScanExec::new(
                    mutable,
                    self.schema.clone(),
                    time_range.clone(),
                )),
                layer_priority,
            });
            layer_priority += 1;
        }

        for mem in immutables.iter().rev() {
            if mem.size_bytes() > 0 {
                layers.push(LsmLayer {
                    plan: Arc::new(MemTableScanExec::new(
                        mem.clone(),
                        self.schema.clone(),
                        time_range.clone(),
                    )),
                    layer_priority,
                });
                layer_priority += 1;
            }
        }

        let mut ssts = sst_files;
        ssts.sort_by_key(|f| (f.base_lsn, f.max_lsn));
        for meta in ssts.into_iter().rev() {
            if !time_range.overlaps(meta.min_ts, meta.max_ts) {
                continue;
            }
            if let Some(plan) = parquet_layer_plan(&meta, self.schema.clone(), &time_range)? {
                layers.push(LsmLayer {
                    plan,
                    layer_priority,
                });
                layer_priority += 1;
            }
        }

        if layers.is_empty() {
            // Empty MemoryExec has 0 partitions and fails AggregateExec distribution checks.
            let empty = EmptyExec::new(self.schema.clone());
            return apply_column_projection(Arc::new(empty), &self.schema, projection);
        }

        let ts_idx = self
            .schema
            .index_of(TIME_COLUMN)
            .map_err(|_| datafusion::error::DataFusionError::Plan("missing time column".into()))?;

        let plan = if layers.len() == 1 {
            build_lsm_scan_plan(layers, self.schema.clone())?
        } else {
            Arc::new(crate::lsm_merge::LsmPriorityMergeExec::new(
                layers,
                self.schema.clone(),
                ts_idx,
            ))
        };
        apply_column_projection(plan, &self.schema, projection)
    }
}

fn apply_column_projection(
    input: Arc<dyn ExecutionPlan>,
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let Some(indices) = projection else {
        return Ok(input);
    };
    if indices.len() == schema.fields().len()
        && indices.iter().enumerate().all(|(i, &col)| i == col)
    {
        return Ok(input);
    }
    let expr: Vec<(Arc<dyn PhysicalExpr>, String)> = indices
        .iter()
        .map(|&idx| {
            let field = schema.field(idx);
            (
                Arc::new(Column::new(field.name(), idx)) as Arc<dyn PhysicalExpr>,
                field.name().clone(),
            )
        })
        .collect();
    Ok(Arc::new(ProjectionExec::try_new(expr, input)?))
}

fn parquet_layer_plan(
    meta: &SstMeta,
    schema: SchemaRef,
    time_range: &crate::filter::TimeRange,
) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
    let path = std::path::Path::new(&meta.file_path);
    let location = object_store::path::Path::from_filesystem_path(path).map_err(|e| {
        datafusion::error::DataFusionError::External(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            e.to_string(),
        )))
    })?;
    let fs_meta = std::fs::metadata(path)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let partitioned_file = PartitionedFile {
        object_meta: object_store::ObjectMeta {
            location,
            last_modified: fs_meta
                .modified()
                .map(chrono::DateTime::<chrono::Utc>::from)
                .unwrap_or_else(|_| chrono::Utc.timestamp_nanos(0)),
            size: fs_meta.len() as usize,
            e_tag: None,
            version: None,
        },
        partition_values: vec![],
        range: None,
        statistics: None,
        extensions: None,
    };

    let file_schema = read_parquet_schema(path)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

    let config = FileScanConfig::new(ObjectStoreUrl::local_filesystem(), file_schema)
        .with_file(partitioned_file);

    let mut builder = ParquetExec::builder(config);
    if let Some(predicate) = time_range_to_physical_predicate(&schema, time_range) {
        builder = builder.with_predicate(predicate);
    }
    let mut parquet_options = datafusion::config::TableParquetOptions::default();
    parquet_options.global.pushdown_filters = true;
    let parquet = builder.with_table_parquet_options(parquet_options).build();
    Ok(Some(Arc::new(SchemaAlignExec::new(
        Arc::new(parquet),
        schema,
    ))))
}
