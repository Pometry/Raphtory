use std::{any::Any, fmt, sync::Arc};

use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::common::DFSchemaRef;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{ExecutionMode, ExecutionPlanProperties};
use datafusion::{
    error::DataFusionError,
    execution::TaskContext,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    },
};

use raphtory::{arrow::graph_impl::ArrowGraph, core::Direction};

use super::operator::HopPlan;

#[derive(Debug)]
pub struct HopExec {
    graph: ArrowGraph,
    dir: Direction,
    input_col: usize,
    input: Arc<dyn ExecutionPlan>,
    layers: Vec<String>,
    right_schema: DFSchemaRef,
    props: PlanProperties,
}

impl HopExec {
    pub fn new(
        hop: &HopPlan,
        // logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
    ) -> Self {
        let graph = hop.graph();
        let dir = hop.dir;
        let input = physical_inputs[0].clone();

        let input_col = input
            .schema()
            .index_of(&hop.left_col)
            .expect("input_col not found");

        let out_schema: Schema = hop.out_schema.as_ref().into();
        let input_partitioning = input.output_partitioning().clone();

        let eq_properties = EquivalenceProperties::new(Arc::new(out_schema));
        let props = PlanProperties::new(eq_properties, input_partitioning, ExecutionMode::Bounded);
        Self {
            graph,
            dir,
            input_col,
            input,
            right_schema: hop.right_schema.clone(),
            layers: hop.right_layers.clone(),
            props,
        }
    }
}

impl DisplayAs for HopExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HopExec")
    }
}

#[async_trait]
impl ExecutionPlan for HopExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(HopExec {
            graph: self.graph.clone(),
            dir: self.dir,
            input_col: self.input_col,
            input: children[0].clone(),
            layers: self.layers.clone(),
            right_schema: self.right_schema.clone(),
            props: self.props.clone(),
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        todo!()
    }
}
