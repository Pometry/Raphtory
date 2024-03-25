use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::datatypes::*;
use arrow_array::{Array, PrimitiveArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
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
    logical_expr::{col, expr, Expr},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::MetricsSet, stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType,
        ExecutionPlan, Partitioning,
    },
    physical_planner::create_physical_sort_expr,
};
use futures::Stream;
use raphtory::arrow::{
    chunked_array::array_ops::{ArrayOps, BaseArrayOps, Chunked},
    graph_fragment::TempColGraphFragment,
    graph_impl::ArrowGraph,
};

use super::{arrow2_to_arrow, arrow2_to_arrow_buf, utf8_arrow2_to_arrow, ExecError};

pub struct EdgeListTableProvider {
    layer_id: usize,
    layer_name: String,
    graph: ArrowGraph,
    nested_schema: SchemaRef,
    num_partitions: usize,
    sorted_by: Vec<PhysicalSortExpr>,
}

impl EdgeListTableProvider {
    pub fn new(layer_name: &str, graph: ArrowGraph) -> Result<Self, ExecError> {
        let layer_id = graph
            .find_layer_id(layer_name)
            .ok_or_else(|| ExecError::LayerNotFound(layer_name.to_string()))?;

        let schema = lift_nested_arrow_schema(&graph, layer_id)?;

        let num_partitions = graph.layer(layer_id).edges_storage().t_props_num_chunks();

        //src
        let expr = Expr::Sort(expr::Sort::new(Box::new(col("src")), true, true));
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
        let sort_by_src = create_physical_sort_expr(&expr, &df_schema, &ExecutionProps::new())?;

        //dst
        let expr = Expr::Sort(expr::Sort::new(Box::new(col("dst")), true, true));
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
        let sort_by_dst = create_physical_sort_expr(&expr, &df_schema, &ExecutionProps::new())?;

        //time
        let expr = Expr::Sort(expr::Sort::new(Box::new(col("time")), true, true));
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
        let sort_by_time = create_physical_sort_expr(&expr, &df_schema, &ExecutionProps::new())?;

        Ok(Self {
            layer_id,
            layer_name: layer_name.to_string(),
            graph,
            nested_schema: schema,
            num_partitions,
            sorted_by: vec![sort_by_src, sort_by_dst, sort_by_time],
        })
    }
}

fn lift_nested_arrow_schema(graph: &ArrowGraph, layer_id: usize) -> Result<Arc<Schema>, ExecError> {
    let arrow2_fields = graph.layer(layer_id).edges_data_type();
    let a2_dt = arrow2::datatypes::DataType::Struct(arrow2_fields.clone());
    let a_dt: DataType = a2_dt.into();
    let schema = match a_dt {
        DataType::Struct(fields) => {
            let node_and_edge_fields = Schema::new(vec![
                Field::new("src", DataType::UInt64, false),
                Field::new("dst", DataType::UInt64, false),
                Field::new("time", DataType::Int64, false),
            ]);

            let props_fields = Schema::new(&fields[1..]);
            let schema = Schema::try_merge([node_and_edge_fields, props_fields])?;

            SchemaRef::new(schema)
        }
        _ => unreachable!(),
    };
    Ok(schema)
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

async fn produce_record_batch(
    graph: ArrowGraph,
    schema: SchemaRef,
    layer_id: usize,
    chunk_id: usize,
) -> Result<RecordBatch, DataFusionError> {
    let layer = graph.layer(layer_id);
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

    let mut srcs_builder = Vec::with_capacity(srcs.len());
    let mut dsts_builder = Vec::with_capacity(dsts.len());

    // take every chunk here and surface the primitive arrays
    // convert from arrow2 to arrow-rs then to polars
    for (i, (src, dst)) in srcs
        .iter_chunks()
        .zip(dsts.iter_chunks())
        .flat_map(|(srcs, dsts)| srcs.iter().zip(dsts.iter()))
        .enumerate()
    {
        let length = (offsets[i + 1] - offsets[i]) as usize;
        for _ in 0..length {
            srcs_builder.push(*src);
            dsts_builder.push(*dst);
        }
    }

    let srcs: Arc<dyn Array> = Arc::new(PrimitiveArray::<UInt64Type>::new(
        ScalarBuffer::from(srcs_builder),
        None,
    ));
    let dsts: Arc<dyn Array> = Arc::new(PrimitiveArray::<UInt64Type>::new(
        ScalarBuffer::from(dsts_builder),
        None,
    ));
    let time: Arc<dyn Array> = Arc::new(arrow2_to_arrow_buf::<Int64Type>(time_values));

    let mut columns = vec![srcs, dsts, time];

    let temp_properties = &edges.data_type()[1..];
    for (col_id, field) in temp_properties.into_iter().enumerate() {
        // we always skip the time column
        let col_id = col_id + 1; // we skip the time column

        println!("Processing field: {:?}", field);
        let arr = property_to_arrow_column(layer, col_id, chunk_id, field);
        columns.push(arr);
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|arrow_err| DataFusionError::ArrowError(arrow_err, None))
}

fn property_to_arrow_column(
    layer: &TempColGraphFragment,
    col_id: usize,
    chunk_id: usize,
    field: &arrow2::datatypes::Field,
) -> Arc<dyn Array> {
    let edges = layer.edges_storage();
    let data_type = field.data_type();

    match data_type {
        arrow2::datatypes::DataType::Int8 => {
            let col = edges.t_prop_col_at_chunk::<i8>(col_id, chunk_id);
            let prop = arrow2_to_arrow::<Int8Type>(col);
            Arc::new(prop)
        }
        arrow2::datatypes::DataType::Int16 => {
            let col = edges.t_prop_col_at_chunk::<i16>(col_id, chunk_id);
            let prop = arrow2_to_arrow::<Int16Type>(col);
            Arc::new(prop)
        }
        arrow2::datatypes::DataType::Int32 => {
            let col = edges.t_prop_col_at_chunk::<i32>(col_id, chunk_id);
            let prop = arrow2_to_arrow::<Int32Type>(col);
            Arc::new(prop)
        }
        arrow2::datatypes::DataType::Int64 => {
            let col = edges.t_prop_col_at_chunk::<i64>(col_id, chunk_id);
            let prop = arrow2_to_arrow::<Int64Type>(col);
            Arc::new(prop)
        }
        arrow2::datatypes::DataType::UInt8 => {
            let col = edges.t_prop_col_at_chunk::<u8>(col_id, chunk_id);
            let prop = arrow2_to_arrow::<UInt8Type>(col);
            Arc::new(prop)
        }
        arrow2::datatypes::DataType::UInt16 => {
            let col = edges.t_prop_col_at_chunk::<u16>(col_id, chunk_id);
            let prop = arrow2_to_arrow::<UInt16Type>(col);
            Arc::new(prop)
        }
        arrow2::datatypes::DataType::UInt32 => {
            let col = edges.t_prop_col_at_chunk::<u32>(col_id, chunk_id);
            let prop = arrow2_to_arrow::<UInt32Type>(col);
            Arc::new(prop)
        }
        arrow2::datatypes::DataType::UInt64 => {
            let col = edges.t_prop_col_at_chunk::<u64>(col_id, chunk_id);
            let prop = arrow2_to_arrow::<UInt64Type>(col);
            Arc::new(prop)
        }
        arrow2::datatypes::DataType::Float32 => {
            let col = edges.t_prop_col_at_chunk::<f32>(col_id, chunk_id);
            let prop = arrow2_to_arrow::<Float32Type>(col);
            Arc::new(prop)
        }
        arrow2::datatypes::DataType::Float64 => {
            let col = edges.t_prop_col_at_chunk::<f64>(col_id, chunk_id);
            let prop = arrow2_to_arrow::<Float64Type>(col);
            Arc::new(prop)
        }
        arrow2::datatypes::DataType::Utf8 => {
            let col = edges.utf8_t_prop_col_at_chunk::<i32>(col_id, chunk_id);
            let prop = utf8_arrow2_to_arrow(col);
            Arc::new(prop)
        }
        arrow2::datatypes::DataType::LargeUtf8 => {
            let col = edges.utf8_t_prop_col_at_chunk::<i64>(col_id, chunk_id);
            let prop = utf8_arrow2_to_arrow(col);
            Arc::new(prop)
        }
        _ => todo!(),
    }
}

impl EdgeListExecPlan {
    fn stream_record_batches(
        &self,
        chunk_id: usize,
    ) -> impl Stream<Item = Result<RecordBatch, DataFusionError>> {
        futures::stream::once(produce_record_batch(
            self.graph.clone(),
            self.schema.clone(),
            self.layer_id,
            chunk_id,
        ))
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

#[cfg(test)]
mod test {
    use super::*;
    use arrow::util::pretty::print_batches;
    use datafusion::execution::context::SessionContext;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_edge_list_table_provider() {
        let ctx = SessionContext::new();
        let graph_dir = tempdir().unwrap();
        let edges = vec![
            (0, 1, 1, 3.),
            (0, 1, 2, 4.),
            (1, 2, 2, 4.),
            (1, 2, 3, 4.),
            (2, 3, 5, 5.),
            (3, 4, 1, 6.),
            (3, 4, 3, 6.),
            (3, 4, 7, 6.),
            (4, 5, 9, 7.),
        ];
        let graph = ArrowGraph::make_simple_graph(graph_dir, &edges, 10, 10);
        ctx.register_table(
            "graph",
            Arc::new(EdgeListTableProvider::new("_default", graph).unwrap()),
        )
        .unwrap();

        let df = ctx.sql("SELECT * FROM graph").await.unwrap();
        let data = df.collect().await.unwrap();

        print_batches(&data).unwrap();
    }
}
