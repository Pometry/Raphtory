use crate::{
    arrow2::{self, array::to_data, datatypes::ArrowDataType},
    executor::ExecError,
};
use arrow::datatypes::UInt64Type;
use arrow_array::{make_array, Array, PrimitiveArray};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Schema};
use async_trait::async_trait;
use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    common::Statistics,
    config::ConfigOptions,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::{context::SessionState, SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_plan::{
        metrics::MetricsSet, stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType,
        ExecutionPlan, PlanProperties,
    },
};
use futures::Stream;
use pometry_storage::properties::ConstProps;
use raphtory::{
    core::entities::VID,
    disk_graph::{prelude::*, DiskGraphStorage},
};
use std::{any::Any, fmt::Formatter, sync::Arc};

use super::plan_properties;

// FIXME: review this file, some of the assuptions and mapping between partitions and chunk sizes are not correct
pub struct NodeTableProvider {
    graph: DiskGraphStorage,
    schema: SchemaRef,
    num_partitions: usize,
    chunk_size: usize,
}

impl NodeTableProvider {
    pub fn new(g: DiskGraphStorage) -> Result<Self, ExecError> {
        let graph = g.as_ref();
        let (num_partitions, chunk_size) = graph
            .node_properties()
            .const_props
            .as_ref()
            .map(|properties| {
                let num_partitions = properties.props().num_chunks();
                let chunk_size = properties.props().chunk_size();
                (num_partitions, chunk_size)
            })
            .unwrap_or_else(|| {
                let chunk_size = graph.global_ordering().len().min(1_000_000);
                (graph.global_ordering().len() / chunk_size, chunk_size)
            });

        let name_dt = graph.global_ordering().data_type();
        let schema = lift_arrow_schema(
            name_dt.clone(),
            graph.node_properties().const_props.as_ref(),
        )?;

        Ok(Self {
            graph: g,
            schema,
            num_partitions,
            chunk_size,
        })
    }
}

pub fn lift_arrow_schema(
    gid_dt: ArrowDataType,
    properties: Option<&ConstProps<VID>>,
) -> Result<SchemaRef, ExecError> {
    let mut fields = vec![];

    fields.push(arrow2::datatypes::Field::new(
        "id",
        ArrowDataType::UInt64,
        false,
    ));

    fields.push(arrow2::datatypes::Field::new("gid", gid_dt, false));
    if let Some(properties) = properties {
        fields.extend_from_slice(properties.prop_dtypes());
    }

    let dt: DataType = ArrowDataType::Struct(fields).into();

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
            .map(|proj| self.schema().project(proj).map(Arc::new))
            .unwrap_or_else(|| Ok(self.schema().clone()))?;

        let plan_properties = plan_properties(schema.clone(), self.num_partitions);

        Ok(Arc::new(NodeScanExecPlan {
            graph: self.graph.clone(),
            schema,
            num_partitions: self.num_partitions,
            chunk_size: self.chunk_size,
            props: plan_properties,
            projection: projection.map(|proj| Arc::from(proj.as_slice())),
        }))
    }
}

async fn produce_record_batch(
    g: DiskGraphStorage,
    schema: SchemaRef,
    chunk_id: usize,
    chunk_size: usize,
    projection: Option<Arc<[usize]>>,
) -> Result<RecordBatch, DataFusionError> {
    let graph = g.as_ref();
    let properties = graph
        .node_properties()
        .const_props
        .as_ref()
        .ok_or_else(|| DataFusionError::Execution("Failed to find node properties".to_string()))?;

    let const_props = properties.props();

    let chunk = const_props.chunk(chunk_id);

    let start = chunk_id * chunk_size;
    let end = (chunk_id + 1) * chunk_size;

    let n = chunk.values()[0].len();
    let iter = (start as u64..end as u64).take(n);
    let id = Arc::new(PrimitiveArray::<UInt64Type>::new(
        ScalarBuffer::from_iter(iter),
        None,
    ));

    let length = (end - start).min(graph.global_ordering().len());
    let arr_gid = graph.global_ordering().sliced(start, length);
    let gid_data = to_data(arr_gid.as_ref());
    let gid = make_array(gid_data);

    let mut columns: Vec<Arc<dyn Array>> = vec![id, gid];

    columns.extend(chunk.values().iter().map(|col| {
        let arrow_data = arrow2::array::to_data(col.as_ref());
        make_array(arrow_data)
    }));

    if let Some(projection) = projection {
        // FIXME: this is not an actual projection we could avoid doing some work before we get here
        let columns = projection
            .iter()
            .map(|&i| columns[i].clone())
            .collect::<Vec<_>>();

        RecordBatch::try_new(schema.clone(), columns)
            .map_err(|arrow_err| DataFusionError::ArrowError(arrow_err, None))
    } else {
        RecordBatch::try_new(schema.clone(), columns)
            .map_err(|arrow_err| DataFusionError::ArrowError(arrow_err, None))
    }
}

struct NodeScanExecPlan {
    graph: DiskGraphStorage,
    schema: SchemaRef,
    num_partitions: usize,
    chunk_size: usize,
    props: PlanProperties,
    projection: Option<Arc<[usize]>>,
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
            self.chunk_size,
            self.projection.clone(),
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
    fn name(&self) -> &str {
        "NodeScanExecPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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
