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

//! Read-time schema alignment: pad missing columns with NULL for evolved tables.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result as DfResult};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::ExecutionMode;
use datafusion_physical_plan::ExecutionPlanProperties;
use futures::StreamExt;
use monots_storage::reader::BatchAligner;

/// Align each batch from `input` to the catalog (target) schema.
#[derive(Clone)]
pub struct SchemaAlignExec {
    input: Arc<dyn ExecutionPlan>,
    target_schema: SchemaRef,
    cache: PlanProperties,
}

impl SchemaAlignExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, target_schema: SchemaRef) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(target_schema.clone()),
            input.output_partitioning().clone(),
            ExecutionMode::Bounded,
        );
        Self {
            input,
            target_schema,
            cache,
        }
    }
}

impl std::fmt::Debug for SchemaAlignExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaAlignExec").finish()
    }
}

impl DisplayAs for SchemaAlignExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SchemaAlignExec")
    }
}

impl ExecutionPlan for SchemaAlignExec {
    fn name(&self) -> &str {
        "SchemaAlignExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.target_schema.clone()
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
                "SchemaAlignExec expected 1 child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(
            children.into_iter().next().unwrap(),
            self.target_schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let target_schema = self.target_schema.clone();
        let stream = input.map(move |batch| {
            batch.and_then(|batch| {
                BatchAligner::align(batch, target_schema.clone())
                    .map_err(|e| DataFusionError::External(Box::new(e)))
            })
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.target_schema.clone(),
            stream,
        )))
    }
}
