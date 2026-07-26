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

//! LSM priority-aware K-way merge: [`LsmPriorityMergeExec`] + per-layer coalesce.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common::{time_value_at, TIME_COLUMN};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
};
use datafusion_common::{DataFusionError, Result as DfResult};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::{ExecutionMode, SendableRecordBatchStream as ExecStream};
use futures::{ready, Stream, StreamExt};

use crate::lsm_stream::LsmPriorityMergeStream;

/// One LSM layer (SST ParquetExec or memtable scan).
#[derive(Debug, Clone)]
pub struct LsmLayer {
    pub plan: Arc<dyn ExecutionPlan>,
    /// 0 = newest (mutable), increasing for older layers.
    pub layer_priority: usize,
}

/// Build a pipelined scan over multiple LSM layers (newest → oldest).
pub fn build_lsm_scan_plan(
    layers: Vec<LsmLayer>,
    schema: SchemaRef,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    if layers.is_empty() {
        return Err(DataFusionError::Internal(
            "build_lsm_scan_plan requires at least one layer".into(),
        ));
    }
    if layers.len() == 1 {
        return Ok(Arc::new(CoalescePartitionsExec::new(
            layers.into_iter().next().unwrap().plan,
        )));
    }

    let ts_idx = schema
        .index_of(TIME_COLUMN)
        .map_err(|_| DataFusionError::Plan("missing time column".into()))?;

    Ok(Arc::new(LsmPriorityMergeExec::new(layers, schema, ts_idx)))
}

/// K-way merge with layer-priority dedupe (heap size = layer count).
#[derive(Debug, Clone)]
pub struct LsmPriorityMergeExec {
    layers: Vec<LsmLayer>,
    schema: SchemaRef,
    ts_column_idx: usize,
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
}

impl LsmPriorityMergeExec {
    pub fn new(layers: Vec<LsmLayer>, schema: SchemaRef, ts_column_idx: usize) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self {
            layers,
            schema,
            ts_column_idx,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }
}

impl DisplayAs for LsmPriorityMergeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LsmPriorityMergeExec: {} layers", self.layers.len())
    }
}

impl ExecutionPlan for LsmPriorityMergeExec {
    fn name(&self) -> &str {
        "LsmPriorityMergeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.layers.iter().map(|l| &l.plan).collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != self.layers.len() {
            return Err(DataFusionError::Internal(format!(
                "LsmPriorityMergeExec expected {} children, got {}",
                self.layers.len(),
                children.len()
            )));
        }
        let layers = self
            .layers
            .iter()
            .zip(children)
            .map(|(layer, plan)| LsmLayer {
                plan,
                layer_priority: layer.layer_priority,
            })
            .collect();
        Ok(Arc::new(Self::new(
            layers,
            self.schema.clone(),
            self.ts_column_idx,
        )))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DfResult<ExecStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "LsmPriorityMergeExec only supports partition 0, got {partition}"
            )));
        }

        let mut streams = Vec::with_capacity(self.layers.len());
        for layer in &self.layers {
            let coalesced = Arc::new(CoalescePartitionsExec::new(layer.plan.clone()));
            let stream = datafusion_physical_plan::execute_stream(coalesced, context.clone())?;
            streams.push((stream, layer.layer_priority));
        }

        Ok(Box::pin(LsmPriorityMergeStream::new(
            streams,
            self.schema.clone(),
            self.ts_column_idx,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Coalesce all input partitions into a single output partition (full layer scan).
#[derive(Clone)]
struct CoalescePartitionsExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl CoalescePartitionsExec {
    fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let schema = input.schema();
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self {
            input,
            schema,
            cache,
        }
    }
}

impl DisplayAs for CoalescePartitionsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CoalescePartitionsExec")
    }
}

impl std::fmt::Debug for CoalescePartitionsExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoalescePartitionsExec").finish()
    }
}

impl ExecutionPlan for CoalescePartitionsExec {
    fn name(&self) -> &str {
        "CoalescePartitionsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "CoalescePartitionsExec expected 1 child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(children.into_iter().next().unwrap())))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DfResult<ExecStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "CoalescePartitionsExec only supports partition 0, got {partition}"
            )));
        }
        let stream = datafusion_physical_plan::execute_stream(self.input.clone(), context)?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

/// Physical plan wrapper for backward-compatible tests.
#[derive(Clone)]
pub struct LsmLayeredScanExec {
    inner: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
}

impl LsmLayeredScanExec {
    pub fn new(layers: Vec<LsmLayer>, schema: SchemaRef) -> Self {
        let inner = build_lsm_scan_plan(layers, schema.clone()).unwrap_or_else(|_| {
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                schema.clone(),
            )) as Arc<dyn ExecutionPlan>
        });
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self {
            inner,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }
}

impl DisplayAs for LsmLayeredScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LsmLayeredScanExec")
    }
}

impl std::fmt::Debug for LsmLayeredScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LsmLayeredScanExec")
            .field("schema", &self.schema)
            .finish()
    }
}

impl ExecutionPlan for LsmLayeredScanExec {
    fn name(&self) -> &str {
        "LsmLayeredScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "LsmLayeredScanExec expected 1 child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self {
            inner: children.into_iter().next().unwrap(),
            schema: self.schema.clone(),
            metrics: self.metrics.clone(),
            cache: self.cache.clone(),
        }))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DfResult<ExecStream> {
        self.inner.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// O(1) adjacent dedupe on a globally sorted timestamp stream (first row per timestamp wins).
#[derive(Clone)]
pub struct LsmDedupeExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    ts_column_idx: usize,
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
}

impl LsmDedupeExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef, ts_column_idx: usize) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self {
            input,
            schema,
            ts_column_idx,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }
}

impl DisplayAs for LsmDedupeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LsmDedupeExec")
    }
}

impl std::fmt::Debug for LsmDedupeExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LsmDedupeExec").finish()
    }
}

impl ExecutionPlan for LsmDedupeExec {
    fn name(&self) -> &str {
        "LsmDedupeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "LsmDedupeExec expected 1 child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self {
            input: children.into_iter().next().unwrap(),
            schema: self.schema.clone(),
            ts_column_idx: self.ts_column_idx,
            metrics: self.metrics.clone(),
            cache: self.cache.clone(),
        }))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DfResult<ExecStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "LsmDedupeExec only supports partition 0, got {partition}"
            )));
        }
        let stream = datafusion_physical_plan::execute_stream(self.input.clone(), context)?;
        Ok(Box::pin(LsmDedupeStream {
            input: stream,
            schema: self.schema.clone(),
            ts_column_idx: self.ts_column_idx,
            last_seen_ts: None,
            _baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct LsmDedupeStream {
    input: ExecStream,
    schema: SchemaRef,
    ts_column_idx: usize,
    last_seen_ts: Option<i64>,
    _baseline_metrics: BaselineMetrics,
}

impl LsmDedupeStream {
    fn dedupe_batch(&mut self, batch: RecordBatch) -> DfResult<Option<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let ts_col = batch.column(self.ts_column_idx);
        let mut keep_mask = Vec::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            let current_ts =
                time_value_at(ts_col, i).map_err(|e| DataFusionError::External(Box::new(e)))?;
            if Some(current_ts) == self.last_seen_ts {
                keep_mask.push(false);
            } else {
                keep_mask.push(true);
                self.last_seen_ts = Some(current_ts);
            }
        }

        if !keep_mask.iter().any(|&k| k) {
            return Ok(None);
        }

        let mask_array = BooleanArray::from(keep_mask);
        let filtered = filter_record_batch(&batch, &mask_array)?;
        Ok(Some(filtered))
    }
}

impl Stream for LsmDedupeStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let batch = match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(b)) => b,
                other => return Poll::Ready(other),
            };

            match self.dedupe_batch(batch) {
                Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                Ok(None) => continue,
                Err(e) => return Poll::Ready(Some(Err(e))),
            }
        }
    }
}

impl RecordBatchStream for LsmDedupeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
