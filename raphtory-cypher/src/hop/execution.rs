use std::{
    any::Any,
    collections::HashSet,
    fmt,
    ops::Range,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::arrow2::{offset::Offset, types::NativeType};
// use arrow::compute::take_record_batch;
use arrow_array::{
    builder::{
        make_builder, ArrayBuilder, Float32Builder, Float64Builder, GenericStringBuilder,
        Int32Builder, Int64Builder, LargeStringBuilder, PrimitiveBuilder, StringBuilder,
        UInt32Builder, UInt64Builder,
    },
    Array, ArrayRef, ArrowPrimitiveType, Int64Array, OffsetSizeTrait, RecordBatch, UInt64Array,
};
use arrow_schema::{DataType, Schema, SchemaRef};
use async_trait::async_trait;
// use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::{
    common::DFSchemaRef,
    error::DataFusionError,
    execution::{RecordBatchStream, TaskContext},
    physical_plan::{
        DisplayAs,
        DisplayFormatType,
        Distribution,
        ExecutionPlan, //ExecutionPlanProperties,
        // PlanProperties,
        SendableRecordBatchStream,
    },
};
// use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_expr::Partitioning;
use futures::{Stream, StreamExt};

use pometry_storage::graph_fragment::TempColGraphFragment;
use raphtory::{
    arrow::{
        graph_impl::DiskGraph,
        prelude::{ArrayOps, BaseArrayOps, PrimitiveCol},
    },
    core::{entities::VID, Direction},
};

use crate::take_record_batch;

use super::operator::HopPlan;

#[derive(Debug)]
pub struct HopExec {
    graph: DiskGraph,
    dir: Direction,
    input_col: usize,
    input: Arc<dyn ExecutionPlan>,
    layers: Vec<String>,
    right_schema: DFSchemaRef,

    output_schema: SchemaRef,

    right_proj: Option<Vec<usize>>,
}

// we assume to be chaining the hops so we need to find the last input column
fn find_last_input_col(hop: &HopPlan, input: &Arc<dyn ExecutionPlan>) -> usize {
    let mut input_col = None;

    for (id, field) in input.schema().fields().iter().enumerate() {
        if field.name() == &hop.left_col {
            input_col = Some(id);
        }
    }
    let input_col = input_col.expect("failed to find the input column in the input schema");
    input_col
}

impl HopExec {
    pub fn new(hop: &HopPlan, physical_inputs: &[Arc<dyn ExecutionPlan>]) -> Self {
        let graph = hop.graph();
        let dir = hop.dir;
        let input = physical_inputs[0].clone();

        let input_col = find_last_input_col(hop, &input);

        let out_schema: Schema = hop.out_schema.as_ref().into();

        Self {
            graph,
            dir,
            input_col,
            input,
            right_schema: hop.right_schema.clone(),
            layers: hop.right_layers.clone(),

            output_schema: Arc::new(out_schema),

            right_proj: hop.right_proj.clone(),
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

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }
    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
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
            output_schema: self.output_schema.clone(),
            right_proj: self.right_proj.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let batch_size = context.session_config().batch_size();
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(HopStream::new(
            input,
            self.graph.clone(),
            self.dir,
            self.input_col,
            batch_size,
            self.layers.clone(),
            self.right_schema.clone(),
            self.schema(),
            self.right_proj.clone(),
        )))
    }
}
pub(crate) struct HopStream {
    input: SendableRecordBatchStream,
    graph: DiskGraph,
    dir: Direction,
    input_col: usize,
    batch_size: usize,
    layers: Vec<String>,
    right_schema: DFSchemaRef,
    output_schema: SchemaRef,
    context: HopStreamContext,
    right_proj: Option<Vec<usize>>,
}

impl HopStream {
    fn new(
        input: SendableRecordBatchStream,
        graph: DiskGraph,
        dir: Direction,
        input_col: usize,
        batch_size: usize,
        layers: Vec<String>,
        right_schema: DFSchemaRef,
        output_schema: SchemaRef,
        right_proj: Option<Vec<usize>>,
    ) -> Self {
        Self {
            input,
            graph,
            dir,
            input_col,
            batch_size,
            layers,
            right_schema,
            output_schema,
            context: HopStreamContext {
                rb: None,
                row: 0,
                edge: 0,
                property: 0,
                layer: 0,
                last_node: None,
            },
            right_proj,
        }
    }
}

struct HopStreamContext {
    rb: Option<RecordBatch>,
    row: usize,
    edge: usize,
    property: usize,
    layer: usize,
    last_node: Option<VID>,
}

fn load_into_primitive_builder_2<T: ArrowPrimitiveType>(
    layer: &TempColGraphFragment,
    b: &mut PrimitiveBuilder<T>,
    p_id: usize,
    indices: &[Range<usize>],
) -> Option<()>
where
    T::Native: NativeType,
{
    let col = layer
        .edges_storage()
        .temporal_props()
        .values()
        .primitive_col::<T::Native>(p_id)?;
    for r in indices {
        for i in r.clone() {
            // FIXME: this is not great, every get will do a dynamic cast
            let value = col.get(i);
            b.append_option(value);
        }
    }
    Some(())
}

fn load_into_utf8_builder_2<I: OffsetSizeTrait + Offset>(
    layer: &TempColGraphFragment,
    b: &mut GenericStringBuilder<I>,
    p_id: usize,
    indices: &[Range<usize>],
) -> Option<()> {
    let array = layer.edges_storage().temporal_props().utf8_col::<I>(p_id)?;
    let col = array.values();
    for r in indices {
        for i in r.clone() {
            let value = col.get(i);
            b.append_option(value);
        }
    }
    Some(())
}

impl Stream for HopStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let window_size: usize = self.batch_size;

        let input_col = self.input_col;
        let graph = self.graph.clone();
        let layers = self.layers.clone();
        let output_schema = self.output_schema.clone();
        let right_schema = self.right_schema.clone();
        let right_proj = self.right_proj.clone();

        if let HopStreamContext {
            rb: Some(record_batch),
            row,
            edge,
            property,
            layer,
            last_node,
        } = &mut self.context
        {
            let next_record = produce_next_record(
                record_batch,
                row,
                edge,
                property,
                layer,
                window_size,
                last_node,
                input_col,
                &graph,
                layers,
                output_schema,
                right_schema,
                right_proj.clone(),
            );
            if next_record.is_some() {
                Poll::Ready(next_record)
            } else {
                self.context.rb.take();
                self.context.row = 0;
                self.context.layer = 0;
                return self.poll_next(cx);
            }
        } else {
            let record_batch = match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(record_batch))) => record_batch,
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            };
            self.context.rb = Some(record_batch);
            return self.poll_next(cx);
        }
    }
}

// FIXME: this will fail when prev_node (dst) is the same but the src node changes
// because we're not resetting the edge and time positions
fn produce_next_record(
    rb: &RecordBatch,
    row_pos: &mut usize,
    edge_pos: &mut usize,
    time_pos: &mut usize,
    layer_pos: &mut usize,
    max_record_rows: usize,

    prev_node: &mut Option<VID>,

    input_col: usize,
    graph: &DiskGraph,
    layers: Vec<String>,
    output_schema: SchemaRef,
    right_schema: DFSchemaRef,
    right_proj: Option<Vec<usize>>,
) -> Option<Result<RecordBatch, DataFusionError>> {
    // if let Some(VID(0)) = prev_node {
    //     println!(
    //         "produce_next_record row: {row_pos} edge: {edge_pos} layer: {layer_pos} time: {time_pos} max_rows: {max_record_rows}, prev_node: {prev_node:?}",
    //     );
    // }
    // print_batches(&[rb.clone()]);

    let rb = rb.slice(*row_pos, rb.num_rows() - *row_pos);
    if rb.num_rows() == 0 {
        return None;
    }
    let hop_col = rb
        .column(input_col)
        .as_any()
        .downcast_ref::<UInt64Array>()?
        .values();

    let mut take_indices = Vec::with_capacity(max_record_rows);
    let mut edge_timestamps = Vec::with_capacity(max_record_rows);
    let mut dst_indices = Vec::with_capacity(max_record_rows);

    let mut edge_ids = Vec::with_capacity(max_record_rows);
    let mut layer_ids = Vec::with_capacity(max_record_rows);

    let graph = graph.as_ref();

    let layers = graph
        .layer_names()
        .into_iter()
        .enumerate()
        .filter(|(_, name)| layers.contains(&name.to_lowercase()))
        .map(|(id, _)| graph.layer(id))
        .enumerate()
        .collect::<Vec<_>>();

    let property_names: HashSet<String> = layers
        .iter()
        .flat_map(|(_, layer)| {
            layer
                .edges_data_type()
                .into_iter()
                .skip(1) // skip the timestamp
                .map(|f| f.name.to_string())
        })
        .collect();

    let mut builders = right_schema
        .fields()
        .iter()
        .filter(|f| property_names.contains(f.name()))
        .map(|f| {
            let builder = make_builder(f.data_type(), hop_col.len());
            let prop_ids = layers
                .iter()
                .map(|(_, layer)| layer.edge_property_id(f.name()))
                .collect::<Vec<_>>();
            (builder, f, prop_ids)
        })
        .collect::<Vec<_>>();

    let mut prop_ranges: Vec<Vec<Range<usize>>> = vec![];
    for _ in 0..layers.len() {
        prop_ranges.push(vec![]);
    }

    // when we switch to a new record batch, we need to reset the edge and time positions if the node id changes
    let first = hop_col.first().map(|n| VID(*n as usize));
    if first != prev_node.map(|vid| vid) {
        // println!("Resetting edge and time positions");
        *edge_pos = 0;
        *time_pos = 0;
    }
    // println!(
    //     "2 produce_next_record row: {} edge: {} layer: {} time: {} max_rows: {}",
    //     row_pos, edge_pos, layer_pos, time_pos, max_record_rows
    // );

    let min_layer_pos = *layer_pos;
    'top: for (l, layer) in &layers[*layer_pos..] {
        for (col_id, v_id) in hop_col.into_iter().map(|n| VID(*n as usize)).enumerate() {
            for (edge, u_id) in layer
                .out_edges_from(v_id, *edge_pos)
                .map(|(e_id, u_id)| (layer.edge(e_id), u_id))
            {
                let slice = edge.timestamp_slice();
                let time_slice = slice.slice(*time_pos..);
                let start = time_slice.range().start;
                let mut end = start;
                for t in time_slice {
                    take_indices.push(col_id as u64);
                    edge_timestamps.push(t);
                    dst_indices.push(u_id.0 as u64);
                    edge_ids.push(end as u64);
                    layer_ids.push(edge.layer_id() as u64);

                    *time_pos += 1;
                    end += 1;

                    if take_indices.len() >= max_record_rows {
                        prop_ranges[*l].push(start..end);
                        prev_node.replace(v_id);
                        break 'top;
                    }
                }
                prop_ranges[*l].push(start..end);
                *time_pos = 0;
                *edge_pos += 1;
            }
            *edge_pos = 0;
            *row_pos += 1;
            prev_node.replace(v_id);
        }

        *layer_pos += 1;
    }

    if take_indices.is_empty() {
        return None;
    }

    // deal with properties
    for (p_builder, p_field, prop_ids) in builders.iter_mut() {
        for (l, layer) in &layers[min_layer_pos..] {
            if let Some(p_id) = prop_ids[*l] {
                match p_field.data_type() {
                    DataType::UInt64 => {
                        let builder = p_builder.as_any_mut().downcast_mut::<UInt64Builder>()?;
                        load_into_primitive_builder_2(layer, builder, p_id, &prop_ranges[*l])?;
                    }
                    DataType::UInt32 => {
                        let builder = p_builder.as_any_mut().downcast_mut::<UInt32Builder>()?;
                        load_into_primitive_builder_2(layer, builder, p_id, &prop_ranges[*l])?;
                    }
                    DataType::Int64 => {
                        let builder = p_builder.as_any_mut().downcast_mut::<Int64Builder>()?;
                        load_into_primitive_builder_2(layer, builder, p_id, &prop_ranges[*l])?;
                    }
                    DataType::Int32 => {
                        let builder = p_builder.as_any_mut().downcast_mut::<Int32Builder>()?;
                        load_into_primitive_builder_2(layer, builder, p_id, &prop_ranges[*l])?;
                    }
                    DataType::Float32 => {
                        let builder = p_builder.as_any_mut().downcast_mut::<Float32Builder>()?;
                        load_into_primitive_builder_2(layer, builder, p_id, &prop_ranges[*l])?;
                    }
                    DataType::Float64 => {
                        let builder = p_builder.as_any_mut().downcast_mut::<Float64Builder>()?;
                        load_into_primitive_builder_2(layer, builder, p_id, &prop_ranges[*l])?;
                    }
                    DataType::Utf8 => {
                        let builder = p_builder.as_any_mut().downcast_mut::<StringBuilder>()?;
                        load_into_utf8_builder_2(layer, builder, p_id, &prop_ranges[*l])?;
                    }
                    DataType::LargeUtf8 => {
                        let builder = p_builder
                            .as_any_mut()
                            .downcast_mut::<LargeStringBuilder>()
                            .unwrap();
                        load_into_utf8_builder_2(layer, builder, p_id, &prop_ranges[*l])?;
                    }
                    _ => {}
                }
            }
        }
    }

    let take_indices = UInt64Array::from(take_indices);
    let left_rb = take_record_batch(&rb, &take_indices).expect("take failed");

    let src_ids = left_rb.column(input_col).clone();

    let edge_timestamps = Arc::new(Int64Array::from(edge_timestamps));
    let dst_ids = Arc::new(UInt64Array::from(dst_indices));
    let edge_ids = Arc::new(UInt64Array::from(edge_ids));
    let layer_ids = Arc::new(UInt64Array::from(layer_ids));

    let mut columns: Vec<ArrayRef> = left_rb.columns().into();

    let mut right_columns: Vec<ArrayRef> = vec![];
    right_columns.push(edge_ids);
    right_columns.push(layer_ids);
    right_columns.push(src_ids);
    right_columns.push(dst_ids);
    right_columns.push(edge_timestamps);

    for (builder, _, _) in builders.iter_mut() {
        right_columns.push(builder.finish());
    }

    let right_columns = if let Some(right_proj) = right_proj {
        right_proj
            .iter()
            .map(|i| right_columns[*i].clone())
            .collect()
    } else {
        right_columns
    };

    columns.extend(right_columns);

    Some(RecordBatch::try_new(output_schema, columns).map_err(Into::into))
}

impl RecordBatchStream for HopStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

#[cfg(test)]
mod test {
    use arrow::{compute::concat_batches, util::pretty::print_batches};
    use arrow_array::{
        types::{Float64Type, Int64Type, UInt64Type},
        Float64Array, PrimitiveArray,
    };
    use arrow_schema::{ArrowError, Field};
    use datafusion::{
        common::{DFSchema, ToDFSchema},
        physical_plan::stream::RecordBatchStreamAdapter,
    };
    use futures::stream;
    use pretty_assertions::assert_eq;
    use proptest::{prelude::*, proptest};
    use tempfile::tempdir;

    use raphtory::{graphgen::random_attachment::random_attachment, prelude::*};

    use crate::run_cypher;

    use super::*;

    lazy_static::lazy_static! {
    static ref EDGES: Vec<(u64, u64, i64, f64)> = vec![
            (0, 1, 0, 3.),
            (1, 2, 1, 4.),
            (2, 3, 2, 5.),
            (2, 4, 3, 6.),
            (2, 5, 4, 7.),
            (3, 4, 5, 8.),
            (5, 4, 7, 8.),
        ];
    }
    #[tokio::test]
    async fn stream_one_hop_from_0_bs1() {
        check_rb_hop(1, 2, 2, &[0..1], 0..1).await;
    }
    #[tokio::test]
    async fn stream_one_hop_from_0_bs4() {
        check_rb_hop(4, 2, 2, &[0..1], 0..1).await;
    }

    #[tokio::test]
    async fn stream_one_hop_from_12_bs1() {
        check_rb_hop(1, 2, 2, &[1..2], 1..4).await;
    }

    #[tokio::test]
    async fn stream_one_hop_from_02_bs1() {
        check_rb_hop(1, 2, 2, &[0..2], 0..4).await;
    }

    #[tokio::test]
    async fn stream_one_hop_from_13_bs1() {
        check_rb_hop(1, 2, 2, &[1..3], 1..5).await;
    }

    #[tokio::test]
    async fn stream_one_hop_from_07_bs1() {
        check_rb_hop(1, 2, 2, &[0..7], 0..6).await;
    }

    #[tokio::test]
    async fn stream_one_hop_from_07_bs1_split_input() {
        check_rb_hop(1, 2, 2, &[0..2, 2..7], 0..6).await;
    }

    proptest! {
        #[test]
        fn stream_one_hop_from_07_bs1_split_input_proptest(
            chunk_size in 1usize..23,
            t_props_chunk_size in 1usize..11,
            edges in (1..5usize).prop_map(|num_nodes| graph_gen_edges(num_nodes))
        ) {
            tokio::runtime::Runtime::new().unwrap().block_on(
                check_random_hop(chunk_size, t_props_chunk_size, &edges)
            );
        }
    }

    fn graph_gen_edges(num_nodes: usize) -> Vec<(u64, u64, i64, f64)> {
        let graph = Graph::new();
        random_attachment(&graph, num_nodes, 10, None);
        let mut edges = vec![];
        for edge in graph.edges().into_iter() {
            for e in edge.explode() {
                let e_ref = e.edge;
                edges.push((
                    e_ref.src().0 as u64,
                    e_ref.dst().0 as u64,
                    e_ref.time_t().unwrap(),
                    0.,
                ));
            }
        }
        edges.sort_by_key(|(src, dst, t, _)| (*src, *dst, *t));
        edges
    }

    async fn check_random_hop(
        chunk_size: usize,
        t_props_chunk_size: usize,
        edges: &[(u64, u64, i64, f64)],
    ) {
        let graph_dir = tempdir().unwrap();
        let graph = DiskGraph::make_simple_graph(graph_dir, edges, chunk_size, t_props_chunk_size);

        let query = "MATCH (a)-[e1]->(b)-[e2]->(c) RETURN count(*)";
        let df = run_cypher(query, &graph, false).await.unwrap();

        let rbs = df.collect().await.unwrap();
        assert!(!rbs.is_empty());
        let col_hop = &rbs[0].columns()[0];

        let df = run_cypher(query, &graph, true).await.unwrap();

        let rbs = df.collect().await.unwrap();
        assert!(!rbs.is_empty());
        let col_join = &rbs[0].columns()[0];

        assert_eq!(col_hop, col_join);
    }

    async fn check_rb_hop(
        batch_size: usize,
        chunk_size: usize,
        t_props_chunk_size: usize,
        input_range: &[Range<usize>],
        output_range: Range<usize>,
    ) {
        let graph_dir = tempdir().unwrap();
        let graph = DiskGraph::make_simple_graph(graph_dir, &EDGES, chunk_size, t_props_chunk_size);

        let schema = make_input_schema();
        let table_schema: DFSchema = schema.clone().to_dfschema().unwrap();
        let schema = Arc::new(schema);

        let output_schema = make_out_schema();
        let input_range: Vec<Range<usize>> = input_range.into();

        let input = RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(
                make_rb(schema).into_iter().flat_map(move |rb| {
                    input_range
                        .clone()
                        .into_iter()
                        .map(move |input_range| Ok(rb.slice(input_range.start, input_range.len())))
                }), // hop from the the first node only
            ),
        );

        let stream = make_hop_stream(
            batch_size,
            graph,
            table_schema,
            output_schema.clone(),
            input,
        );

        let actual = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("Failed to collect output record batches")
            .into_iter()
            .reduce(|rb1, rb2| concat_batches(&output_schema, &[rb1, rb2]).expect("concat failed"))
            .unwrap();

        let _ = print_batches(&[actual.clone()]);

        let expected = make_output_rb(output_schema)
            .unwrap()
            .slice(output_range.start, output_range.len());

        assert_eq!(actual, expected);
    }

    fn make_output_rb(output_schema: SchemaRef) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            output_schema.clone(),
            vec![
                arr::<UInt64Type>(vec![0u64, 1, 1, 1, 2, 4]), // edge_id
                arr::<UInt64Type>(vec![0u64, 0, 0, 0, 0, 0]), // layer_id
                arr::<UInt64Type>(vec![0u64, 1, 1, 1, 2, 2]), // src
                arr::<UInt64Type>(vec![1u64, 2, 2, 2, 3, 5]), // dst
                arr::<Int64Type>(vec![0i64, 1, 1, 1, 2, 4]),  // ts
                arr::<Float64Type>(vec![3., 4., 4., 4., 5., 7.]), // weight
                arr::<UInt64Type>(vec![1u64, 2, 3, 4, 5, 6]), // edge_id
                arr::<UInt64Type>(vec![0u64, 0, 0, 0, 0, 0]), // layer_id
                arr::<UInt64Type>(vec![1u64, 2, 2, 2, 3, 5]), // src
                arr::<UInt64Type>(vec![2u64, 3, 4, 5, 4, 4]), //dst
                arr::<Int64Type>(vec![1i64, 2, 3, 4, 5, 7]),  // ts
                arr::<Float64Type>(vec![4., 5., 6., 7., 8., 8.]), //weight
            ],
        )
    }

    fn arr<T: ArrowPrimitiveType>(v: Vec<T::Native>) -> Arc<PrimitiveArray<T>> {
        Arc::new(PrimitiveArray::from_iter_values(v))
    }

    fn make_input_schema() -> Schema {
        Schema::new(vec![
            Field::new("edge_id", DataType::UInt64, false),
            Field::new("layer_id", DataType::UInt64, false),
            Field::new("src", DataType::UInt64, false),
            Field::new("dst", DataType::UInt64, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("weight", DataType::Float64, true),
        ])
    }

    fn make_out_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("edge_id", DataType::UInt64, false),
            Field::new("layer_id", DataType::UInt64, false),
            Field::new("src", DataType::UInt64, false),
            Field::new("dst", DataType::UInt64, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("weight", DataType::Float64, true),
            Field::new("edge_id", DataType::UInt64, false),
            Field::new("layer_id", DataType::UInt64, false),
            Field::new("src", DataType::UInt64, false),
            Field::new("dst", DataType::UInt64, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("weight", DataType::Float64, true),
        ]))
    }

    fn make_hop_stream(
        batch_size: usize,
        graph: DiskGraph,
        table_schema: DFSchema,
        output_schema: Arc<Schema>,
        input: RecordBatchStreamAdapter<
            impl stream::Stream<Item = Result<RecordBatch, DataFusionError>> + Send + 'static,
        >,
    ) -> HopStream {
        HopStream::new(
            Box::pin(input),
            graph,
            Direction::OUT,
            3,
            batch_size,
            vec!["_default".to_string()],
            table_schema.into(),
            output_schema,
            None,
        )
    }

    fn make_rb(schema: Arc<Schema>) -> Result<RecordBatch, DataFusionError> {
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(vec![0, 1, 2, 3, 4, 5, 6])), // edge_id
            Arc::new(UInt64Array::from(vec![0, 0, 0, 0, 0, 0, 0])), // layer_id
            Arc::new(UInt64Array::from(vec![0, 1, 2, 2, 2, 3, 5])), // src
            Arc::new(UInt64Array::from(vec![1, 2, 3, 4, 5, 4, 4])), // dst
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5, 6])),  // timestamp
            Arc::new(Float64Array::from(vec![3., 4., 5., 6., 7., 8., 9.])), // weight
        ];
        RecordBatch::try_new(schema.clone(), cols).map_err(Into::into)
    }
}
