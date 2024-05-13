use std::{any::Any, fmt::Formatter, sync::Arc};
use std::ops::Deref;

use arrow::datatypes::*;
use arrow_array::{Array, PrimitiveArray};
use arrow_array::builder::{ArrayBuilder, make_builder};
use arrow_buffer::ScalarBuffer;
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
        DisplayAs, DisplayFormatType, ExecutionPlan, metrics::MetricsSet,
        stream::RecordBatchStreamAdapter,
    },
    physical_planner::create_physical_sort_expr,
};
use futures::Stream;
use itertools::{EitherOrBoth, Itertools, unfold};

use raphtory::db::{api::view::StaticGraphViewOps, graph::graph::Graph};
use raphtory::db::api::mutation::internal::InternalAdditionOps;
use raphtory::db::graph::views::layer_graph::LayeredGraph;
use raphtory::prelude::*;

use crate::executor::ExecError;

pub struct EdgeListTableProvider {
    layer_id: usize,
    layer_name: String,
    graph: LayeredGraph<Graph>,
    nested_schema: SchemaRef,
    num_partitions: usize,
    sorted_by: Vec<PhysicalSortExpr>,
}

impl EdgeListTableProvider {
    pub fn new(layer_name: &str, graph: Graph) -> Result<Self, ExecError> {
        let layer_id = graph.resolve_layer(Some(layer_name));
        let graph = graph.layers(layer_name)?;

        let schema = lift_nested_arrow_schema(&graph)?;

        let num_partitions = std::thread::available_parallelism()?.get();

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

fn lift_nested_arrow_schema<G: StaticGraphViewOps>(graph: &G) -> Result<Arc<Schema>, ExecError> {
    let graph_fields = graph.edge_meta().temporal_prop_meta().d_types();

    let mut fields = vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("layer_id", DataType::UInt64, false),
        Field::new("src", DataType::UInt64, false),
        Field::new("dst", DataType::UInt64, false),
    ];

    // for field in graph_fields {}

    Ok(SchemaRef::new(Schema::new(fields)))
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
            .as_ref()
            .map(|proj| Arc::new(self.schema().project(proj).expect("failed projection")))
            .unwrap_or_else(|| self.schema().clone());

        // let plan_properties = plan_properties(schema.clone(), self.num_partitions);
        Ok(Arc::new(EdgeListExecPlan {
            layer_id: self.layer_id,
            layer_name: self.layer_name.clone(),
            graph: self.graph.clone(),
            schema,
            num_partitions: self.num_partitions,
            sorted_by: self.sorted_by.clone(),
            // props: plan_properties,
            projection: projection.map(|proj| Arc::from(proj.as_slice())),
        }))
    }
}

struct EdgeListExecPlan {
    layer_id: usize,
    layer_name: String,
    graph: LayeredGraph<Graph>,
    schema: SchemaRef,
    num_partitions: usize,
    sorted_by: Vec<PhysicalSortExpr>,
    // props: PlanProperties,
    projection: Option<Arc<[usize]>>,
}

fn load_prop_iterator(data_type: &DataType, props: impl IntoIterator<Item=Option<(i64, Prop)>>, builder: &mut Box<dyn ArrayBuilder>, row_fn: impl FnMut()) {}

fn produce_record_batch<G: StaticGraphViewOps>(
    graph: G,
    schema: SchemaRef,
    layer_id: usize,
    projection: Option<Arc<[usize]>>,
) -> Box<dyn Iterator<Item=Result<RecordBatch, DataFusionError>> + Send> {
    let chunk_size = 100;
    let edges = graph.edges();
    let mut iter = edges.into_iter();

    let iter = unfold((iter, 0usize), |(edges, edge_count)| {
        let builders = schema.fields().into_iter().map(|field| {
            (field.data_type().clone(), make_builder(field.data_type(), chunk_size))
        }).collect::<Vec<_>>();

        let mut srcs_builder = Vec::with_capacity(chunk_size);
        let mut dsts_builder = Vec::with_capacity(chunk_size);
        let mut edge_ids_builder = Vec::with_capacity(chunk_size);
        let mut ts_builder = Vec::with_capacity(chunk_size);

        for edge in edges.into_iter() {
            if *edge_count >= chunk_size {
                break;
            }
            let e_ref = edge.edge;
            let src = e_ref.src();
            let dst = e_ref.dst();
            let properties = edge.properties().temporal();

            let all_ts = edge.history();
            ts_builder.extend_from_slice(&all_ts);

            for (_, prop_cols) in properties.into_iter() {
                let id = prop_cols.id();
                let (data_type, builder) = &mut builders[id];
                let iter = all_ts.iter().merge_join_by(prop_cols.iter(), |&t1, (t2, _)| t1.cmp(t2)).map(|merge_result| {
                    match merge_result {
                        EitherOrBoth::Both(_, prop) => Some(prop),
                        EitherOrBoth::Left(_) => None,
                        EitherOrBoth::Right(_) => unreachable!("merge_join_by should not produce Right only, the left side should contain all timestamps")
                    }
                });
                load_prop_iterator(data_type, iter, builder, || {
                    srcs_builder.push(src.0 as u64);
                    dsts_builder.push(dst.0 as u64);
                    edge_ids_builder.push(e_ref.pid().0 as u64);
                });
            }
            *edge_count += 1;
        }
        
        if srcs_builder.is_empty() {
            return None;
        }

        let layer_id_builder = vec![layer_id as u64; ts_builder.len()];

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

        let e_ids = Arc::new(PrimitiveArray::<UInt64Type>::new(
            ScalarBuffer::from(edge_ids_builder),
            None,
        ));

        let time = Arc::new(PrimitiveArray::<Int64Type>::new(
            ScalarBuffer::from(ts_builder),
            None,
        ));

        let mut columns = vec![e_ids, layer_ids, srcs, dsts, time];

        if let Some(projection) = &projection {
            // FIXME: this is not an actual projection we could avoid doing some work before we get here
            let columns = projection
                .iter()
                .map(|&i| columns[i].clone())
                .collect::<Vec<_>>();

            Some(
                RecordBatch::try_new(schema.clone(), columns)
                    .map_err(|arrow_err| DataFusionError::ArrowError(arrow_err, None))
            )
        } else {
            Some(RecordBatch::try_new(schema.clone(), columns)
                .map_err(|arrow_err| DataFusionError::ArrowError(arrow_err, None)))
        }
    });

    Box::new(iter)
}

impl EdgeListExecPlan {
    fn stream_record_batches(
        &self,
    ) -> impl Stream<Item=Result<RecordBatch, DataFusionError>> {
        futures::stream::iter(produce_record_batch(
            self.graph.clone(),
            self.schema.clone(),
            self.layer_id,
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn output_partitioning(&self) -> datafusion::physical_expr::Partitioning {
        datafusion::physical_expr::Partitioning::UnknownPartitioning(self.num_partitions)
    }
    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        Some(&self.sorted_by)
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        Ok(Some(Arc::new(EdgeListExecPlan {
            layer_id: self.layer_id,
            layer_name: self.layer_name.clone(),
            graph: self.graph.clone(),
            schema: self.schema.clone(),
            num_partitions: target_partitions,
            sorted_by: self.sorted_by.clone(),
            projection: self.projection.clone(),
        })))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let stream = self.stream_record_batches();
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
    use arrow::util::pretty::print_batches;
    use datafusion::execution::context::SessionContext;
    use tempfile::tempdir;

    use super::*;

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
        let graph = Graph::new();

        for (time, src, dst, weight) in edges {
            graph.add_edge(time, src, dst, [("weight", Prop::F64(weight))], None).unwrap();
        }

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
