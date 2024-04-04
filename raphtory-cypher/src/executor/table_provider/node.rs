use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::{array::ArrayBuilder, datatypes::Int64Type};
use arrow_array::make_array;
use arrow_schema::{DataType, Schema};
use async_trait::async_trait;
use arrow2::array::Arrow2Arrow;
use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    common::Statistics,
    config::ConfigOptions,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::{context::SessionState, SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::MetricsSet, stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType,
        ExecutionPlan, Partitioning,
    },
};
use futures::Stream;
use raphtory::{
    arrow::{chunked_array::array_ops::Chunked, graph_impl::ArrowGraph, properties::Properties},
    core::entities::VID, db::graph::node,
};

use crate::executor::ExecError;

pub struct NodeTableProvider {
    graph: ArrowGraph,
    schema: SchemaRef,
    num_partitions: usize,
}

impl NodeTableProvider {
    pub fn new(graph: ArrowGraph) -> Result<Self, ExecError> {
        let properties = graph.node_properties().ok_or_else(|| {
            ExecError::MissingNodeProperties("Failed to find node properties".to_string())
        })?;

        let num_partitions = properties.temporal_props.timestamps().values().num_chunks();

        let schema = lift_arrow_schema(properties)?;

        Ok(Self {
            graph,
            schema,
            num_partitions,
        })
    }
}

fn lift_arrow_schema(properties: &Properties<VID>) -> Result<SchemaRef, ExecError> {
    let mut fields = vec![];

    fields.push(arrow2::datatypes::Field::new(
        "id",
        arrow2::datatypes::DataType::UInt64,
        false,
    ));

    fields.push(arrow2::datatypes::Field::new(
        "time",
        arrow2::datatypes::DataType::Int64,
        false,
    ));

    let dt_temporal = properties.temporal_props.prop_dtypes();

    fields.extend_from_slice(dt_temporal);

    let dt_const = properties.const_props.prop_dtypes();

    fields.extend_from_slice(dt_const);

    let dt: DataType = arrow2::datatypes::DataType::Struct(fields).into();

    if let DataType::Struct(fields) = dt {
        Ok(Arc::new(Schema::new(fields)))
    } else {
        unreachable!("we make the struct type above, so this should never happen")
    }
}

#[async_trait]
impl TableProvider for NodeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let schema = projection
            .as_ref()
            .map(|proj| Arc::new(self.schema().project(proj).expect("failed projection")))
            .unwrap_or_else(|| self.schema().clone());

        Ok(Arc::new(NodeScanExecPlan {
            graph: self.graph.clone(),
            schema,
            num_partitions: self.num_partitions,
        }))
    }
}

async fn produce_record_batch(
    graph: ArrowGraph,
    schema: SchemaRef,
    chunk_id: usize,
) -> Result<RecordBatch, DataFusionError> {
    let arrow_data = arrow2::array::to_data(graph.global_ordering().as_ref());
    let nodes = make_array(arrow_data);

    let properties = graph.node_properties().ok_or_else(|| {
        DataFusionError::Execution("Failed to find node properties".to_string())
    })?;

    let chunked_lists_ts = properties.temporal_props.timestamps();
    
    let offsets = chunked_lists_ts.offsets();
    let values = chunked_lists_ts.values();
    let chunk_size = values.chunk_size();

    let time_values = values.chunk(chunk_id);
    let start_offset = chunk_id * chunk_size;
    let end_offset = (chunk_id + 1) * chunk_size;

    let (start, end, local_offsets) = offsets.make_local_offsets(start_offset, end_offset);

    if start == end {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    match nodes.data_type() {
        DataType::Int64 => {
            let nodes = nodes.as_any().downcast_ref::<arrow_array::PrimitiveArray<Int64Type>>().expect("Failed to downcast nodes to Int64");
        }
        _ => {
            panic!("Nodes should be of type Int64, UInt64, or String");
        }
    }
    let mut ids = Vec::with_capacity(time_values.len());

    todo!()
}

struct NodeScanExecPlan {
    graph: ArrowGraph,
    schema: SchemaRef,
    num_partitions: usize,
}

impl NodeScanExecPlan {
    fn stream_record_batches(
        &self,
        chunk_id: usize,
    ) -> impl Stream<Item = Result<RecordBatch, DataFusionError>> {
        futures::stream::once(produce_record_batch(
            self.graph.clone(),
            self.schema.clone(),
            chunk_id,
        ))
    }
}

impl std::fmt::Debug for NodeScanExecPlan {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "NodeScanExecPlan[projection={:?}]",
            self.schema.fields().iter().map(|f| f.name())
        )
    }
}

impl DisplayAs for NodeScanExecPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "NodeScanExecPlan[projection={:?}]",
            self.schema.fields().iter().map(|f| f.name())
        )
    }
}

#[async_trait]
impl ExecutionPlan for NodeScanExecPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.num_partitions)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        Ok(None)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let stream = self.stream_record_batches(partition);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn statistics(&self) -> Result<Statistics, DataFusionError> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}
