use arrow::compute::take_record_batch;
use arrow2::offset::Offset;
use arrow2::types::NativeType;
use std::collections::HashSet;
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
use datafusion::physical_plan::{Distribution, ExecutionMode, ExecutionPlanProperties};
use datafusion::{
    error::DataFusionError,
    execution::TaskContext,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    },
};
use futures::{Stream, StreamExt};

use raphtory::arrow::graph_fragment::TempColGraphFragment;
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

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
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
        let input = self.input.execute(_partition, _context)?;
        Ok(Box::pin(HopStream {
            input,
            graph: self.graph.clone(),
            dir: self.dir,
            input_col: self.input_col,
            layers: self.layers.clone(),
            right_schema: self.right_schema.clone(),
            output_schema: self.schema(),
        }))
    }
}

struct HopStream {
    input: SendableRecordBatchStream,
    graph: ArrowGraph,
    dir: Direction,
    input_col: usize,
    layers: Vec<String>,
    right_schema: DFSchemaRef,
    output_schema: SchemaRef,
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
            .filter(|(id, name)| self.layers.contains(name))
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
        self.input.poll_next_unpin(cx).map(|poll| match poll {
            Some(Ok(batch)) => self.hop_from_batch(batch),
            other => {
                println!("other: {:?}", other);
                other
            }
        })
    }
}

impl RecordBatchStream for HopStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}
