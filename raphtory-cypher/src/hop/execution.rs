use arrow::compute::take_record_batch;
use arrow2::offset::Offset;
use arrow2::types::NativeType;
use std::collections::HashSet;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{any::Any, fmt, sync::Arc};

use arrow_array::builder::{
    make_builder, ArrayBuilder, Float32Builder, Float64Builder, GenericStringBuilder, Int32Builder,
    Int64Builder, LargeStringBuilder, PrimitiveBuilder, StringBuilder, UInt32Builder,
    UInt64Builder,
};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, Int64Array, OffsetSizeTrait, RecordBatch, UInt64Array,
};
use arrow_schema::{DataType, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::common::DFSchemaRef;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{Distribution, ExecutionPlanProperties};
use datafusion::{
    error::DataFusionError,
    execution::TaskContext,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    },
};
use futures::{Stream, StreamExt};

use raphtory::arrow::graph_fragment::TempColGraphFragment;
use raphtory::arrow::prelude::{ArrayOps, BaseArrayOps, PrimitiveCol};
use raphtory::core::entities::VID;
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
        let props = PlanProperties::new(eq_properties, input_partitioning, input.execution_mode());
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

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // tell optimizer this operator doesn't reorder its input
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
            props: self.props.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let batch_size = context.session_config().batch_size();
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(HopStream {
            input,
            graph: self.graph.clone(),
            dir: self.dir,
            input_col: self.input_col,
            batch_size,
            layers: self.layers.clone(),
            right_schema: self.right_schema.clone(),
            output_schema: self.schema(),
            context: None,
        }))
    }
}
pub(crate) struct HopStream {
    input: SendableRecordBatchStream,
    graph: ArrowGraph,
    dir: Direction,
    input_col: usize,
    batch_size: usize,
    layers: Vec<String>,
    right_schema: DFSchemaRef,
    output_schema: SchemaRef,
    context: Option<HopStreamContext>,
}

struct HopStreamContext {
    rb: RecordBatch,
    row: usize,
    edge: usize,
    property: usize,
    layer: usize,
}

impl HopStream {
    fn hop_from_batch(
        &self,
        record_batch: RecordBatch,
    ) -> Option<Result<RecordBatch, DataFusionError>> {
        let hop_col = record_batch
            .column(self.input_col)
            .as_any()
            .downcast_ref::<UInt64Array>()?; // this must be a UInt64Array of node ids

        let graph = self.graph.as_ref();

        let layers = self
            .graph
            .as_ref()
            .layer_names()
            .into_iter()
            .enumerate()
            .filter(|(id, name)| self.layers.contains(&name.to_lowercase()))
            .map(|(id, _)| (id, graph.layer(id)))
            .collect::<Vec<_>>();

        // all properties accross all layers
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

        let mut builders = self
            .right_schema
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

        // build the indices used to take rows from the left hand side
        let mut take_indices = Vec::with_capacity(hop_col.len());
        let mut edge_timestamps = Vec::with_capacity(hop_col.len());
        let mut dst_indices = Vec::with_capacity(hop_col.len());

        let mut edge_ids = Vec::with_capacity(hop_col.len());
        let mut layer_ids = Vec::with_capacity(hop_col.len());

        for (layer_id, layer) in layers.iter() {
            for (col_id, v_id) in hop_col
                .values()
                .into_iter()
                .map(|n| VID(*n as usize))
                .enumerate()
            {
                // handle edge additions and take indices
                for (t, u_id, e_id) in layer
                    .out_edges(v_id)
                    .map(|(e_id, u_id)| (layer.edge(e_id), u_id))
                    .flat_map(|(edge, u_id)| {
                        edge.timestamp_slice().map(move |t| (t, u_id, edge.eid()))
                    })
                {
                    take_indices.push(col_id as u64);
                    edge_timestamps.push(t);
                    dst_indices.push(u_id.0 as u64);
                    edge_ids.push(e_id.0 as u64);
                    layer_ids.push(*layer_id as u64);
                }
            }
        }

        if take_indices.is_empty() {
            return None;
        }

        for (p_builder, p_field, prop_ids) in builders.iter_mut() {
            for (layer_id, (_, layer)) in layers.iter().enumerate() {
                if let Some(p_id) = prop_ids[layer_id] {
                    for v_id in hop_col.values().into_iter().map(|n| VID(*n as usize)) {
                        match p_field.data_type() {
                            DataType::UInt64 => {
                                let builder =
                                    p_builder.as_any_mut().downcast_mut::<UInt64Builder>()?;
                                let prop_iter = prop_iter_primitive::<u64>(layer, v_id, p_id);
                                load_into_primitive_builder(builder, prop_iter);
                            }
                            DataType::UInt32 => {
                                let builder =
                                    p_builder.as_any_mut().downcast_mut::<UInt32Builder>()?;
                                let prop_iter = prop_iter_primitive::<u32>(layer, v_id, p_id);
                                load_into_primitive_builder(builder, prop_iter);
                            }
                            DataType::Int64 => {
                                let builder =
                                    p_builder.as_any_mut().downcast_mut::<Int64Builder>()?;
                                let prop_iter = prop_iter_primitive::<i64>(layer, v_id, p_id);
                                load_into_primitive_builder(builder, prop_iter);
                            }
                            DataType::Int32 => {
                                let builder =
                                    p_builder.as_any_mut().downcast_mut::<Int32Builder>()?;
                                let prop_iter = prop_iter_primitive::<i32>(layer, v_id, p_id);
                                load_into_primitive_builder(builder, prop_iter);
                            }
                            DataType::Float32 => {
                                let builder =
                                    p_builder.as_any_mut().downcast_mut::<Float32Builder>()?;
                                let prop_iter = prop_iter_primitive::<f32>(layer, v_id, p_id);
                                load_into_primitive_builder(builder, prop_iter);
                            }
                            DataType::Float64 => {
                                let builder =
                                    p_builder.as_any_mut().downcast_mut::<Float64Builder>()?;
                                let prop_iter = prop_iter_primitive::<f64>(layer, v_id, p_id);
                                load_into_primitive_builder(builder, prop_iter);
                            }
                            DataType::Utf8 => {
                                let builder =
                                    p_builder.as_any_mut().downcast_mut::<StringBuilder>()?;
                                let utf8_prop_iter = prop_iter_utf8::<i32>(layer, v_id, p_id);
                                load_into_utf8_builder(builder, utf8_prop_iter);
                            }
                            DataType::LargeUtf8 => {
                                let builder = p_builder
                                    .as_any_mut()
                                    .downcast_mut::<LargeStringBuilder>()
                                    .unwrap();
                                let utf8_prop_iter = prop_iter_utf8::<i64>(layer, v_id, p_id);
                                load_into_utf8_builder(builder, utf8_prop_iter);
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        let take_indices = UInt64Array::from(take_indices);
        let left_rb = take_record_batch(&record_batch, &take_indices).expect("take failed");
        let src_ids = left_rb
            .column_by_name("dst")
            .expect("dst not found")
            .clone();

        let edge_timestamps = Arc::new(Int64Array::from(edge_timestamps));
        let dst_ids = Arc::new(UInt64Array::from(dst_indices));
        let edge_ids = Arc::new(UInt64Array::from(edge_ids));
        let layer_ids = Arc::new(UInt64Array::from(layer_ids));

        let mut columns: Vec<ArrayRef> = left_rb.columns().into();

        columns.push(edge_ids);
        columns.push(layer_ids);
        columns.push(src_ids);
        columns.push(dst_ids);
        columns.push(edge_timestamps);

        for (builder, _, _) in builders.iter_mut() {
            columns.push(builder.finish());
        }
        Some(RecordBatch::try_new(self.output_schema.clone(), columns).map_err(Into::into))
    }
}

fn load_into_primitive_builder<T: ArrowPrimitiveType>(
    b: &mut PrimitiveBuilder<T>,
    iter: impl Iterator<Item = Option<T::Native>>,
) {
    for v in iter {
        b.append_option(v);
    }
}

fn load_into_primitive_builder_2<T: ArrowPrimitiveType>(
    layer: &TempColGraphFragment,
    b: &mut PrimitiveBuilder<T>,
    p_id: usize,
    indices: &[Range<usize>],
) -> Option<()>
where
    T::Native: arrow2::types::NativeType,
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

fn load_into_utf8_builder_2<I: OffsetSizeTrait + arrow2::offset::Offset>(
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

fn prop_iter_primitive<T: NativeType>(
    layer: &TempColGraphFragment,
    v_id: VID,
    prop_id: usize,
) -> impl Iterator<Item = Option<T>> + '_ {
    layer
        .out_edges(v_id)
        .map(|(e_id, _)| layer.edge(e_id))
        .flat_map(move |edge| edge.prop_items::<T>(prop_id))
        .map(|(_, v)| v)
}

fn prop_iter_utf8<I: Offset>(
    layer: &TempColGraphFragment,
    v_id: VID,
    prop_id: usize,
) -> impl Iterator<Item = Option<&str>> + '_ {
    layer
        .out_edges(v_id)
        .map(|(e_id, _)| layer.edge(e_id))
        .flat_map(move |edge| edge.prop_items_utf8::<I>(prop_id))
        .map(|(_, v)| v)
}

fn load_into_utf8_builder<'a, I: OffsetSizeTrait>(
    b: &mut GenericStringBuilder<I>,
    iter: impl Iterator<Item = Option<&'a str>>,
) {
    for v in iter {
        b.append_option(v);
    }
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

        if let Some(HopStreamContext {
            rb,
            row,
            edge,
            property,
            layer,
        }) = &mut self.context
        {
            Poll::Ready(produce_next_record(
                rb,
                row,
                edge,
                property,
                layer,
                window_size,
                input_col,
                &graph,
                layers,
                output_schema,
                right_schema,
            ))
        } else {
            let record_batch = match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(record_batch))) => record_batch,
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            };
            self.context = Some(HopStreamContext {
                rb: record_batch,
                row: 0,
                edge: 0,
                property: 0,
                layer: 0,
            });
            return self.poll_next(cx);
        }
    }
}

fn produce_next_record(
    rb: &RecordBatch,
    row_pos: &mut usize,
    edge_pos: &mut usize,
    time_pos: &mut usize,
    layer_pos: &mut usize,
    max_record_rows: usize,
    input_col: usize,
    graph: &ArrowGraph,
    layers: Vec<String>,
    output_schema: SchemaRef,
    right_schema: DFSchemaRef,
) -> Option<Result<RecordBatch, DataFusionError>> {
    println!(
        "produce_next_record row: {} edge: {} layer: {} time: {} max_rows: {}",
        row_pos, edge_pos, time_pos, layer_pos, max_record_rows
    );

    let rb = rb.slice(*row_pos, rb.num_rows() - *row_pos);
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
        .filter(|(id, name)| layers.contains(&name.to_lowercase()))
        .map(|(id, _)| (id, graph.layer(id)))
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

    let mut max_layer_id = *layer_pos;
    'top: for (layer_id, layer) in &layers[*layer_pos..] {
        for (col_id, v_id) in hop_col.into_iter().map(|n| VID(*n as usize)).enumerate() {
            for (edge, u_id) in layer
                .out_edges_from(v_id, *edge_pos)
                .map(|(e_id, u_id)| (layer.edge(e_id), u_id))
            {
                let e_id = edge.eid();
                let slice = edge.timestamp_slice();
                let time_slice = slice.slice(*time_pos..);
                let start = time_slice.range().start;
                let mut end = start;
                for t in time_slice {
                    take_indices.push(col_id as u64);
                    edge_timestamps.push(t);
                    dst_indices.push(u_id.0 as u64);
                    edge_ids.push(e_id.0 as u64);
                    layer_ids.push(*layer_id as u64);

                    *time_pos += 1;
                    end += 1;

                    if take_indices.len() >= max_record_rows {
                        &mut prop_ranges[*layer_id].push(start..end);
                        break 'top;
                    }
                }
                &mut prop_ranges[*layer_id].push(start..end);
                *time_pos = 0;
                *edge_pos += 1;
            }
            *edge_pos = 0;
            *row_pos += 1;
        }

        *layer_pos += 1;
    }

    if take_indices.is_empty() {
        return None;
    }

    // deal with properties
    for (p_builder, p_field, prop_ids) in builders.iter_mut() {
        for (layer_id, layer) in &layers[max_layer_id..] {
            if let Some(p_id) = prop_ids[*layer_id] {
                match p_field.data_type() {
                    DataType::UInt64 => {
                        let builder = p_builder.as_any_mut().downcast_mut::<UInt64Builder>()?;
                        load_into_primitive_builder_2(
                            layer,
                            builder,
                            p_id,
                            &prop_ranges[*layer_id],
                        )?;
                    }
                    DataType::UInt32 => {
                        let builder = p_builder.as_any_mut().downcast_mut::<UInt32Builder>()?;
                        load_into_primitive_builder_2(
                            layer,
                            builder,
                            p_id,
                            &prop_ranges[*layer_id],
                        )?;
                    }
                    DataType::Int64 => {
                        let builder = p_builder.as_any_mut().downcast_mut::<Int64Builder>()?;
                        load_into_primitive_builder_2(
                            layer,
                            builder,
                            p_id,
                            &prop_ranges[*layer_id],
                        )?;
                    }
                    DataType::Int32 => {
                        let builder = p_builder.as_any_mut().downcast_mut::<Int32Builder>()?;
                        load_into_primitive_builder_2(
                            layer,
                            builder,
                            p_id,
                            &prop_ranges[*layer_id],
                        )?;
                    }
                    DataType::Float32 => {
                        let builder = p_builder.as_any_mut().downcast_mut::<Float32Builder>()?;
                        load_into_primitive_builder_2(
                            layer,
                            builder,
                            p_id,
                            &prop_ranges[*layer_id],
                        )?;
                    }
                    DataType::Float64 => {
                        let builder = p_builder.as_any_mut().downcast_mut::<Float64Builder>()?;
                        load_into_primitive_builder_2(
                            layer,
                            builder,
                            p_id,
                            &prop_ranges[*layer_id],
                        )?;
                    }
                    DataType::Utf8 => {
                        let builder = p_builder.as_any_mut().downcast_mut::<StringBuilder>()?;
                        load_into_utf8_builder_2(layer, builder, p_id, &prop_ranges[*layer_id])?;
                    }
                    DataType::LargeUtf8 => {
                        let builder = p_builder
                            .as_any_mut()
                            .downcast_mut::<LargeStringBuilder>()
                            .unwrap();
                        load_into_utf8_builder_2(layer, builder, p_id, &prop_ranges[*layer_id])?;
                    }
                    _ => {}
                }
            }
        }
    }

    let take_indices = UInt64Array::from(take_indices);
    let left_rb = take_record_batch(&rb, &take_indices).expect("take failed");
    let src_ids = left_rb
        .column_by_name("dst")
        .expect("dst not found")
        .clone();

    let edge_timestamps = Arc::new(Int64Array::from(edge_timestamps));
    let dst_ids = Arc::new(UInt64Array::from(dst_indices));
    let edge_ids = Arc::new(UInt64Array::from(edge_ids));
    let layer_ids = Arc::new(UInt64Array::from(layer_ids));

    let mut columns: Vec<ArrayRef> = left_rb.columns().into();

    columns.push(layer_ids);
    columns.push(edge_ids);
    columns.push(src_ids);
    columns.push(dst_ids);
    columns.push(edge_timestamps);

    for (builder, _, _) in builders.iter_mut() {
        columns.push(builder.finish());
    }

    // println!("columns: {:?}", columns);

    Some(RecordBatch::try_new(output_schema, columns).map_err(Into::into))
}

impl RecordBatchStream for HopStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::compute::concat_batches;
    use arrow::util::pretty::print_batches;
    use arrow_array::types::{Float64Type, Int64Type, UInt64Type};
    use arrow_array::{Float64Array, PrimitiveArray};
    use arrow_schema::{ArrowError, Field};
    use datafusion::common::{DFSchema, ToDFSchema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;
    use tempfile::tempdir;

    use pretty_assertions::assert_eq;
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
        check_rb_hop(1, 2, 2, 0..1, 0..1).await;
    }
    #[tokio::test]
    async fn stream_one_hop_from_0_bs4() {
        check_rb_hop(4, 2, 2, 0..1, 0..1).await;
    }

    #[tokio::test]
    async fn stream_one_hop_from_12_bs1() {
        check_rb_hop(1, 2, 2, 1..2, 1..4).await;
    }

    #[tokio::test]
    async fn stream_one_hop_from_02_bs1() {
        check_rb_hop(1, 2, 2, 0..2, 0..4).await;
    }

    #[tokio::test]
    async fn stream_one_hop_from_13_bs1() {
        check_rb_hop(1, 2, 2, 1..3, 1..5).await;
    }

    async fn check_rb_hop(
        batch_size: usize,
        chunk_size: usize,
        t_props_chunk_size: usize,
        input_range: Range<usize>,
        output_range: Range<usize>,
    ) {
        let graph_dir = tempdir().unwrap();
        let graph =
            ArrowGraph::make_simple_graph(graph_dir, &EDGES, chunk_size, t_props_chunk_size);

        let schema = make_input_schema();
        let table_schema: DFSchema = schema.clone().to_dfschema().unwrap();
        let schema = Arc::new(schema);

        let output_schema = make_out_schema();

        let input = RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::once(async move {
                make_rb(schema).map(|rb| rb.slice(input_range.start, input_range.len()))
                // hop from the the first node only
            }),
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

        print_batches(&[actual.clone()]);

        let expected = make_output_rb(output_schema)
            .unwrap()
            .slice(output_range.start, output_range.len());

        assert_eq!(actual, expected);
    }

    fn make_output_rb(output_schema: SchemaRef) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            output_schema.clone(),
            vec![
                arr::<UInt64Type>(vec![0u64, 0, 0, 0, 0]),
                arr::<UInt64Type>(vec![0u64, 1, 1, 1, 2]),
                arr::<UInt64Type>(vec![0u64, 1, 1, 1, 2]),
                arr::<UInt64Type>(vec![1u64, 2, 2, 2, 3]),
                arr::<Int64Type>(vec![0i64, 1, 1, 1, 2]),
                arr::<Float64Type>(vec![3., 4., 4., 4., 5.]),
                arr::<UInt64Type>(vec![0u64, 0, 0, 0, 0]),
                arr::<UInt64Type>(vec![1u64, 2, 3, 4, 5]),
                arr::<UInt64Type>(vec![1u64, 2, 2, 2, 3]),
                arr::<UInt64Type>(vec![2u64, 3, 4, 5, 4]),
                arr::<Int64Type>(vec![1i64, 2, 3, 4, 5]),
                arr::<Float64Type>(vec![4., 5., 6., 7., 8.]),
            ],
        )
    }

    fn arr<T: ArrowPrimitiveType>(v: Vec<T::Native>) -> Arc<PrimitiveArray<T>> {
        Arc::new(PrimitiveArray::from_iter_values(v))
    }

    fn make_input_schema() -> Schema {
        Schema::new(vec![
            Field::new("layer_id", DataType::UInt64, false),
            Field::new("edge_id", DataType::UInt64, false),
            Field::new("src", DataType::UInt64, false),
            Field::new("dst", DataType::UInt64, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("weight", DataType::Float64, true),
        ])
    }

    fn make_out_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("layer_id", DataType::UInt64, false),
            Field::new("edge_id", DataType::UInt64, false),
            Field::new("src", DataType::UInt64, false),
            Field::new("dst", DataType::UInt64, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("weight", DataType::Float64, true),
            Field::new("layer_id", DataType::UInt64, false),
            Field::new("edge_id", DataType::UInt64, false),
            Field::new("src", DataType::UInt64, false),
            Field::new("dst", DataType::UInt64, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("weight", DataType::Float64, true),
        ]))
    }

    fn make_hop_stream(
        batch_size: usize,
        graph: ArrowGraph,
        table_schema: DFSchema,
        output_schema: Arc<Schema>,
        input: RecordBatchStreamAdapter<
            impl stream::Stream<Item = Result<RecordBatch, DataFusionError>> + Send + 'static,
        >,
    ) -> HopStream {
        HopStream {
            input: Box::pin(input),
            graph: graph,
            dir: Direction::OUT,
            input_col: 3,
            batch_size,
            layers: vec!["_default".to_string()],
            right_schema: table_schema.into(),
            output_schema,
            context: None,
        }
    }

    fn make_rb(schema: Arc<Schema>) -> Result<RecordBatch, DataFusionError> {
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(vec![0, 0, 0, 0, 0, 0, 0])), // layer_id
            Arc::new(UInt64Array::from(vec![0, 1, 2, 3, 4, 5, 6])), // edge_id
            Arc::new(UInt64Array::from(vec![0, 1, 2, 2, 2, 3, 5])), // src
            Arc::new(UInt64Array::from(vec![1, 2, 3, 4, 5, 4, 4])), // dst
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5, 6])),  // timestamp
            Arc::new(Float64Array::from(vec![3., 4., 5., 6., 7., 8., 9.])), // weight
        ];
        RecordBatch::try_new(schema.clone(), cols).map_err(Into::into)
    }
}
