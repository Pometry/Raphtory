use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::datatypes::*;
use arrow_array::{make_array, Array, PrimitiveArray};
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
        ExecutionPlan, PlanProperties,
    },
    physical_planner::create_physical_sort_expr,
};
use futures::Stream;
use pometry_storage::prelude::*;
use raphtory::disk_graph::DiskGraphStorage;

use crate::executor::{arrow2_to_arrow_buf, ExecError};

use super::plan_properties;

// use super::plan_properties;

pub struct EdgeListTableProvider {
    layer_id: usize,
    layer_name: String,
    graph: DiskGraphStorage,
    nested_schema: SchemaRef,
    num_partitions: usize,
    row_count: usize,
    sorted_by: Vec<PhysicalSortExpr>,
}

impl EdgeListTableProvider {
    pub fn new(layer_name: &str, g: DiskGraphStorage) -> Result<Self, ExecError> {
        let graph = g.as_ref();
        let layer_id = graph
            .find_layer_id(layer_name)
            .ok_or_else(|| ExecError::LayerNotFound(layer_name.to_string()))?;

        let schema = lift_nested_arrow_schema(&g, layer_id)?;

        let num_partitions = std::thread::available_parallelism()?.get();

        let row_count = graph
            .as_ref()
            .layer(layer_id)
            .edges_storage()
            .time()
            .values()
            .len();

        //src
        let expr = Expr::Sort(expr::Sort::new(Box::new(col("src")), true, true));
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
        let sort_by_src = create_physical_sort_expr(&expr, &df_schema, &ExecutionProps::new())?;

        //dst
        let expr = Expr::Sort(expr::Sort::new(Box::new(col("dst")), true, true));
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
        let sort_by_dst = create_physical_sort_expr(&expr, &df_schema, &ExecutionProps::new())?;

        //time
        let time_field_name = graph
            .layer(layer_id)
            .edges_data_type()
            .first()
            .map(|f| f.name.clone())
            .unwrap();

        let expr = Expr::Sort(expr::Sort::new(Box::new(col(time_field_name)), true, true));
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
        let sort_by_time = create_physical_sort_expr(&expr, &df_schema, &ExecutionProps::new())?;

        Ok(Self {
            layer_id,
            layer_name: layer_name.to_string(),
            graph: g,
            nested_schema: schema,
            num_partitions,
            row_count,
            sorted_by: vec![sort_by_src, sort_by_dst, sort_by_time],
        })
    }
}

fn lift_nested_arrow_schema(
    graph: &DiskGraphStorage,
    layer_id: usize,
) -> Result<Arc<Schema>, ExecError> {
    let arrow2_fields = graph.as_ref().layer(layer_id).edges_data_type();
    let a2_dt = crate::arrow2::datatypes::ArrowDataType::Struct(arrow2_fields.to_vec());
    let a_dt: DataType = a2_dt.into();
    let schema = match a_dt {
        DataType::Struct(fields) => {
            let node_ids_and_edge_fields = Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("layer_id", DataType::UInt64, false),
                Field::new("src", DataType::UInt64, false),
                Field::new("dst", DataType::UInt64, false),
            ]);

            let props_fields = Schema::new(&fields[..]);
            let schema = Schema::try_merge([node_ids_and_edge_fields, props_fields])?;

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
        let schema = projection
            .map(|proj| self.schema().project(proj).map(Arc::new))
            .unwrap_or_else(|| Ok( self.schema().clone() ))?;

        let plan_properties = plan_properties(schema.clone(), self.num_partitions);
        Ok(Arc::new(EdgeListExecPlan {
            layer_id: self.layer_id,
            layer_name: self.layer_name.clone(),
            graph: self.graph.clone(),
            schema,
            num_partitions: self.num_partitions,
            row_count: self.row_count,
            sorted_by: self.sorted_by.clone(),
            props: plan_properties,
            projection: projection.map(|proj| Arc::from(proj.as_slice())),
        }))
    }
}

struct EdgeListExecPlan {
    layer_id: usize,
    layer_name: String,
    graph: DiskGraphStorage,
    schema: SchemaRef,
    num_partitions: usize,
    row_count: usize,
    sorted_by: Vec<PhysicalSortExpr>,
    props: PlanProperties,
    projection: Option<Arc<[usize]>>,
}

fn produce_record_batch(
    graph: DiskGraphStorage,
    schema: SchemaRef,
    layer_id: usize,
    start_offset: usize,
    end_offset: usize,
    projection: Option<Arc<[usize]>>,
) -> Box<dyn Iterator<Item = Result<RecordBatch, DataFusionError>> + Send> {
    if start_offset >= end_offset {
        return Box::new(std::iter::empty());
    }

    let layer = graph.as_ref().layer(layer_id);
    let edges = layer.edges_storage();

    let chunked_lists_ts = edges.time();
    let offsets = chunked_lists_ts.offsets();
    // FIXME: potentially implement into_iter_chunks() for chunked arrays to avoid having to collect these chunks, if it turns out to be a problem
    let time_values_chunks = chunked_lists_ts
        .values()
        .slice(start_offset..end_offset)
        .iter_chunks()
        .map(|c| c.clone())
        .collect::<Vec<_>>();
    let temporal_props_chunks = edges
        .temporal_props()
        .values()
        .sliced(start_offset..end_offset)
        .iter_chunks()
        .map(|c| c.clone())
        .collect::<Vec<_>>();

    let (start, end, local_offsets) = offsets.make_local_offsets(start_offset, end_offset);

    if start == end {
        return Box::new(std::iter::empty());
    }

    let offsets: OffsetBuffer<i64> = OffsetBuffer::new(local_offsets.into());

    let srcs = edges.srcs().sliced(start..end);
    let dsts = edges.dsts().sliced(start..end);

    let ids_builder: Vec<u64> = (start_offset as u64..end_offset as u64).collect();
    let mut srcs_builder = Vec::with_capacity(time_values_chunks.len());
    let mut dsts_builder = Vec::with_capacity(time_values_chunks.len());
    let mut layer_id_builder = Vec::with_capacity(time_values_chunks.len());

    // take every chunk here and surface the primitive arrays
    // convert from arrow2 to disk_graph-rs then to polars
    for (i, ((src, dst), layer_id)) in srcs
        .iter_chunks()
        .zip(dsts.iter_chunks())
        .flat_map(|(srcs, dsts)| srcs.iter().zip(dsts.iter()))
        .zip(std::iter::repeat(layer_id as u64))
        .enumerate()
    {
        let length = (offsets[i + 1] - offsets[i]) as usize;
        for _ in 0..length {
            srcs_builder.push(*src);
            dsts_builder.push(*dst);
            layer_id_builder.push(layer_id);
        }
    }

    let layer_ids: Arc<dyn Array> = Arc::new(PrimitiveArray::<UInt64Type>::new(
        ScalarBuffer::from(layer_id_builder),
        None,
    ));

    let srcs: Arc<dyn Array> = Arc::new(PrimitiveArray::<UInt64Type>::new(
        ScalarBuffer::from(srcs_builder),
        None,
    ));
    let dsts: Arc<dyn Array> = Arc::new(PrimitiveArray::<UInt64Type>::new(
        ScalarBuffer::from(dsts_builder),
        None,
    ));

    let row_num: Arc<dyn Array> = Arc::new(PrimitiveArray::<UInt64Type>::new(
        ScalarBuffer::from(ids_builder),
        None,
    ));

    let column_ids = edges
        .data_type()
        .iter()
        .enumerate()
        .skip(1) // first one is supposed to be time
        .map(|(col_id, _)| col_id)
        .collect::<Vec<_>>();

    let iter = time_values_chunks
        .into_iter()
        .zip(temporal_props_chunks)
        .scan(0, move |from, (time_values, time_props)| {
            let len = time_values.len();
            let e_ids = row_num.slice(*from, len);
            let layer_ids = layer_ids.slice(*from, len);
            let srcs = srcs.slice(*from, len);
            let dsts = dsts.slice(*from, len);
            let time: Arc<dyn Array> = Arc::new(arrow2_to_arrow_buf::<Int64Type>(&time_values));
            let mut columns = vec![e_ids, layer_ids, srcs, dsts, time];

            for col_id in &column_ids {
                let arr = property_to_arrow_column(&time_props, *col_id);
                columns.push(arr);
            }

            *from += len;
            Some(columns)
        })
        .map(move |columns| {
            if let Some(projection) = &projection {
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
        });

    Box::new(iter)
}

fn property_to_arrow_column(
    temporal_props: &crate::arrow2::array::StructArray,
    col_id: usize,
) -> Arc<dyn Array> {
    let arr = temporal_props.values()[col_id].as_ref();
    let arrow_data = crate::arrow2::array::to_data(arr);

    (make_array(arrow_data)) as _
}

impl EdgeListExecPlan {
    fn stream_record_batches(
        &self,
        start_offset: usize,
        end_offset: usize,
    ) -> impl Stream<Item = Result<RecordBatch, DataFusionError>> {
        futures::stream::iter(produce_record_batch(
            self.graph.clone(),
            self.schema.clone(),
            self.layer_id,
            start_offset,
            end_offset,
            self.projection.clone(),
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
    fn name(&self) -> &str {
        "EdgeListExecPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }

    // fn output_partitioning(&self) -> datafusion::physical_expr::Partitioning {
    //     datafusion::physical_expr::Partitioning::UnknownPartitioning(self.num_partitions)
    // }
    // fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
    //     Some(&self.sorted_by)
    // }

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
        target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        let plan_properties = plan_properties(self.schema.clone(), target_partitions);
        Ok(Some(Arc::new(EdgeListExecPlan {
            layer_id: self.layer_id,
            layer_name: self.layer_name.clone(),
            graph: self.graph.clone(),
            schema: self.schema.clone(),
            num_partitions: target_partitions,
            row_count: self.row_count,
            sorted_by: self.sorted_by.clone(),
            props: plan_properties,
            projection: self.projection.clone(),
        })))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let start_offset = partition * self.row_count / self.num_partitions;
        let end_offset = (partition + 1) * self.row_count / self.num_partitions;
        let stream = self.stream_record_batches(start_offset, end_offset.min(self.row_count));
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
        let graph = DiskGraphStorage::make_simple_graph(graph_dir, &edges, 10, 10);
        ctx.register_table(
            "graph",
            Arc::new(EdgeListTableProvider::new("_default", graph).unwrap()),
        )
        .unwrap();

        let state = ctx.state();
        let dialect = state.config_options().sql_parser.dialect.as_str();
        let plan = state.sql_to_statement(
            "with a AS (select * from graph), b as (select * from graph2) select a.*,b.* from a,b where a.dst=b.src",
            dialect,
        );
        println!("AST {:?}", plan);

        let df = ctx
            .sql("WITH e AS (SELECT * FROM graph) SELECT e.* FROM e")
            .await
            .unwrap();
        let data = df.collect().await.unwrap();

        print_batches(&data).unwrap();
    }
}
