use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::datatypes::{DataType, UInt64Type};
use arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::Field;
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::RecordBatch,
        datatypes::{Schema, SchemaRef},
    },
    common::{DFSchema, Statistics},
    config::ConfigOptions,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::{
        context::{ExecutionProps, SessionState},
        SendableRecordBatchStream, TaskContext,
    },
    logical_expr::{col, expr, Expr, LogicalPlanBuilder},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::MetricsSet, stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType,
        ExecutionPlan, Partitioning,
    },
    physical_planner::create_physical_sort_expr,
};
use futures::Stream;
use raphtory::arrow::{chunked_array::array_ops::{ArrayOps, BaseArrayOps, Chunked}, graph_impl::ArrowGraph};

use super::{arrow2_to_arrow, ExecError};

pub struct EdgeListTableProvider {
    layer_id: usize,
    bind_name: String,
    layer_name: String,
    graph: ArrowGraph,
    nested_schema: SchemaRef,
    num_partitions: usize,
    sorted_by: Vec<PhysicalSortExpr>,
}

impl EdgeListTableProvider {
    pub fn new(layer_name: &str, bind_name: &str, graph: ArrowGraph) -> Result<Self, ExecError> {
        let layer_id = graph
            .find_layer_id(layer_name)
            .ok_or_else(|| ExecError::LayerNotFound(layer_name.to_string()))?;

        let schema = lift_nested_arrow_schema(&graph, layer_id, bind_name)?;

        let num_partitions = graph.layer(layer_id).edges_storage().t_props_num_chunks();

        //src
        let expr = Expr::Sort(expr::Sort::new(Box::new(col("src")), true, true));
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
        let sort_by_src = create_physical_sort_expr(&expr, &df_schema, &ExecutionProps::new())?;

        //dst
        let expr = Expr::Sort(expr::Sort::new(Box::new(col("dst")), true, true));
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
        let sort_by_dst = create_physical_sort_expr(&expr, &df_schema, &ExecutionProps::new())?;

        Ok(Self {
            layer_id,
            layer_name: layer_name.to_string(),
            bind_name: bind_name.to_string(),
            graph,
            nested_schema: schema,
            num_partitions,
            sorted_by: vec![sort_by_src, sort_by_dst],
        })
    }
}

fn lift_nested_arrow_schema(
    graph: &ArrowGraph,
    layer_id: usize,
    bind_name: &str,
) -> Result<Arc<Schema>, ExecError> {
    let arrow2_fields = graph.layer(layer_id).edges_data_type();
    let a2_dt = arrow2::datatypes::DataType::Struct(arrow2_fields.clone());
    let a_dt: DataType = a2_dt.into();
    let schema = match a_dt {
        DataType::Struct(fields) => {
            let node_and_edge_fields = Schema::new(vec![
                Field::new(src_col(bind_name), DataType::UInt64, false),
                Field::new(dst_col(bind_name), DataType::UInt64, false),
            ]);
            // all the fields need to be made into lists
            let fields = fields
                .iter()
                .map(|f| Arc::new(Field::new(f.name(), DataType::List(f.clone()), false)))
                .collect::<Vec<_>>();

            let props_fields = Schema::new(fields);
            let schema = Schema::try_merge([node_and_edge_fields, props_fields])?;

            SchemaRef::new(schema)
        }
        _ => unreachable!(),
    };
    Ok(schema)
}

fn src_col(layer_name: &str) -> String {
    format!("src_{}", layer_name)
}

fn dst_col(layer_name: &str) -> String {
    format!("dst_{}", layer_name)
}

#[async_trait]
impl TableProvider for EdgeListTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.nested_schema.clone()
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
        Ok(Arc::new(EdgeListExecPlan {
            layer_id: self.layer_id,
            layer_name: self.layer_name.clone(),
            graph: self.graph.clone(),
            schema: self.nested_schema.clone(),
            num_partitions: self.num_partitions,
            sorted_by: self.sorted_by.clone(),
            projection: projection.cloned(),
        }))
    }
}

struct EdgeListExecPlan {
    layer_id: usize,
    layer_name: String,
    graph: ArrowGraph,
    schema: SchemaRef,
    num_partitions: usize,
    sorted_by: Vec<PhysicalSortExpr>,
    projection: Option<Vec<usize>>,
}

impl EdgeListExecPlan {
    fn stream_record_batches(
        &self,
        chunk_id: usize,
    ) -> impl Stream<Item = Result<RecordBatch, DataFusionError>> {
        let layer = self.graph.layer(self.layer_id);
        let edges = layer.edges_storage();
        let chunk_size = edges.time().values().chunk_size();

        let chunked_lists_ts = edges.time();
        let offsets = chunked_lists_ts.offsets();
        let values = chunked_lists_ts.values();

        let time_values = values.chunk(chunk_id);
        let start_offset = chunk_id * chunk_size;
        let end_offset = (chunk_id + 1) * chunk_size;

        let (start, end, local_offsets) = offsets.make_local_offsets(start_offset, end_offset);

        let offsets: OffsetBuffer<i64> = OffsetBuffer::new(local_offsets.into());

        let srcs = edges.srcs().sliced(start..end);
        let dsts = edges.dsts().sliced(start..end);

        // take every chunk here and surface the primitive arrays
        // convert from arrow2 to arrow-rs then to polars
        let (srcs, dsts): (Vec<_>, Vec<_>) = srcs
            .iter_chunks()
            .zip(dsts.iter_chunks())
            .map(|(srcs, dsts)| {
                let srcs = arrow2_to_arrow::<u64, UInt64Type>(srcs);
                let dsts = arrow2_to_arrow::<u64, UInt64Type>(dsts);
                (srcs, dsts)
            })
            .unzip();

        futures::stream::empty()
    }
}

impl std::fmt::Debug for EdgeListExecPlan {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "EdgeListExecPlan[layer={:?}]", self.layer_name)
    }
}

impl DisplayAs for EdgeListExecPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "EdgeListExecPlan[layer={:?}]", self.layer_name)
    }
}

#[async_trait]
impl ExecutionPlan for EdgeListExecPlan {
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
        Some(&self.sorted_by)
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
