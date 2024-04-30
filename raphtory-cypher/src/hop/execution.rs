use arrow::compute::take_record_batch;
use arrow2::offset::Offset;
use arrow2::types::NativeType;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{any::Any, fmt, sync::Arc};

use arrow_array::builder::{
    make_builder, ArrayBuilder, GenericStringBuilder, Int64Builder, LargeStringBuilder,
    PrimitiveBuilder, StringBuilder, UInt64Builder,
};
use arrow_array::{Array, ArrowPrimitiveType, OffsetSizeTrait, RecordBatch, UInt64Array};
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
            .map(|(id, _)| graph.layer(id))
            .collect::<Vec<_>>();

        let mut layers = layers
            .iter()
            .filter_map(|&layer| {
                let builders = self
                    .right_schema
                    .fields()
                    .iter()
                    .filter_map(|f| layer.edge_property_id(f.name()).map(|id| (f, id)))
                    .map(|(field, prop_id)| {
                        (
                            field,
                            make_builder(field.data_type(), hop_col.len()),
                            prop_id,
                        )
                    })
                    .collect::<Vec<_>>();

                if builders.is_empty() {
                    None
                } else {
                    Some((layer, builders))
                }
            })
            .collect::<Vec<_>>();

        // build the indices used to take rows from the left hand side
        let mut take_indices = vec![];
        let mut edge_timestamps = vec![];

        for (layer, prop_builders) in layers.iter_mut() {
            for (col_id, v_id) in hop_col
                .values()
                .into_iter()
                .map(|n| VID(*n as usize))
                .enumerate()
            {
                // handle edge additions and take indices
                for t in layer
                    .out_edges(v_id)
                    .map(|(e_id, _)| layer.edge(e_id))
                    .flat_map(|edge| edge.timestamp_slice())
                {
                    take_indices.push(col_id as u64);
                    edge_timestamps.push(t);
                }

                // Handle properties
                for (p_field, p_builder, p_id) in prop_builders.iter_mut() {
                    // TODO: these potentially could happen in parallel
                    match p_field.data_type() {
                        DataType::UInt64 => {
                            let builder = p_builder.as_any_mut().downcast_mut::<UInt64Builder>()?;
                            let prop_iter = prop_iter_primitive::<u64>(&layer, v_id, *p_id);
                            load_into_primitive_builder(builder, prop_iter);
                        }
                        DataType::Int64 => {
                            let builder = p_builder.as_any_mut().downcast_mut::<Int64Builder>()?;
                            let prop_iter = prop_iter_primitive::<i64>(&layer, v_id, *p_id);
                            load_into_primitive_builder(builder, prop_iter);
                        }
                        DataType::Utf8 => {
                            let builder = p_builder.as_any_mut().downcast_mut::<StringBuilder>()?;
                            let utf8_prop_iter = prop_iter_utf8::<i32>(&layer, v_id, *p_id);
                            load_into_utf8_builder(builder, utf8_prop_iter);
                        }
                        DataType::LargeUtf8 => {
                            let builder = p_builder
                                .as_any_mut()
                                .downcast_mut::<LargeStringBuilder>()
                                .unwrap();
                            let utf8_prop_iter = prop_iter_utf8::<i64>(&layer, v_id, *p_id);
                            load_into_utf8_builder(builder, utf8_prop_iter);
                        }
                        _ => {}
                    }
                }
            }
        }

        // put all the things together
        let take_indices = UInt64Array::from(take_indices);
        let left_rb = take_record_batch(&record_batch, &take_indices).map(|left_rb| {
            let mut right_columns = vec![];
            for (_, prop_builders) in layers.iter_mut() {
                for (field, builder, _) in prop_builders.iter_mut() {
                    right_rb = right_rb.add_column(field.name(), builder.finish().into());
                }
            }
            right_rb
        });
        None
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
