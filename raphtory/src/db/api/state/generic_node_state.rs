use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::{node_state_ops::NodeStateOps, Index},
            view::{DynamicGraph, IntoDynBoxed, IntoDynamic},
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    prelude::{GraphViewOps, NodeViewOps},
};

use arrow::compute::{cast_with_options, CastOptions};

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaBuilder};
use indexmap::IndexSet;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

use arrow_array::{builder::UInt64Builder, UInt64Array};
use arrow_select::{concat::concat, take::take};
use raphtory_api::core::entities::properties::prop::{Prop, PropType, PropUnwrap};
use rayon::{iter::Either, prelude::*};
use serde::{de::DeserializeOwned, Serialize};
use serde_arrow::{
    from_record_batch,
    schema::{SchemaLike, TracingOptions},
    to_record_batch, Deserializer,
};
use std::{
    cmp::PartialEq,
    collections::HashMap,
    fmt::{Debug, Formatter},
    fs::File,
    hash::BuildHasher,
    marker::PhantomData,
    path::Path,
    sync::Arc,
};

pub trait NodeStateValue:
    Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + Debug
{
}
impl<T> NodeStateValue for T where
    T: Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + Debug
{
}

pub trait InputNodeStateValue: Clone + Serialize + DeserializeOwned {}

impl<T> InputNodeStateValue for T where T: Clone + Serialize + DeserializeOwned {}

#[derive(Clone, PartialEq)]
pub enum MergePriority {
    Left,
    Right,
    Exclude,
}

#[derive(Clone, PartialEq, Debug)]
pub enum NodeStateOutput<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph> = G> {
    Node(NodeView<'graph, G, GH>),
    Nodes(Nodes<'graph, G, GH>),
    Prop(Option<Prop>),
}

#[derive(Clone, PartialEq, Debug)]
pub enum NodeStateOutputType {
    Node,
    Nodes,
    Prop,
}

pub type PropMap = HashMap<String, Option<Prop>>;

pub type TransformedPropMap<'graph, G, GH = G> = HashMap<String, NodeStateOutput<'graph, G, GH>>;

/*
pub type PropMapConverter<'graph, G, GH> =
    fn(&GenericNodeState<'graph, G, GH>, PropMap) -> TransformedPropMap<'graph, G, GH>;
 */

pub type OutputTypedNodeState<'graph, G, GH = G> =
    TypedNodeState<'graph, PropMap, G, GH, TransformedPropMap<'graph, G, GH>>;

pub trait NodeTransform {
    type Input;
    type Output;

    fn transform<'graph, G, GH>(
        state: &GenericNodeState<'graph, G, GH>,
        value: Self::Input,
    ) -> Self::Output
    where
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>;
}

// --- Blanket implementation for identity transforms ---
impl<T> NodeTransform for T
where
    T: NodeStateValue,
{
    type Input = Self;
    type Output = Self;

    fn transform<'graph, G, GH>(
        _state: &GenericNodeState<'graph, G, GH>,
        value: Self::Input,
    ) -> Self::Output
    where
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
    {
        value
    }
}

#[derive(Clone, Debug)]
pub struct GenericNodeState<'graph, G, GH = G> {
    pub base_graph: G,
    pub graph: GH,
    values: RecordBatch,
    keys: Option<Index<VID>>,
    node_cols: HashMap<String, (NodeStateOutputType, Option<G>, Option<GH>)>,
    _marker: PhantomData<&'graph ()>,
}

#[derive(Clone)]
pub struct TypedNodeState<'graph, V: NodeStateValue, G, GH = G, T: Clone + Sync + Send = V> {
    pub state: GenericNodeState<'graph, G, GH>,
    pub converter: fn(&GenericNodeState<'graph, G, GH>, V) -> T,
    _v_marker: PhantomData<V>,
    _t_marker: PhantomData<T>,
}

pub struct RecordBatchIterator<'a, T> {
    deserializer: Deserializer<'a>,
    idx: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T> RecordBatchIterator<'a, T>
where
    T: NodeStateValue,
{
    pub fn new(record_batch: &'a RecordBatch) -> Self {
        let deserializer: Deserializer<'a> = Deserializer::from_record_batch(record_batch).unwrap();
        let idx: usize = 0;
        Self {
            deserializer,
            idx,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn get(&self, idx: usize) -> T {
        T::deserialize(self.deserializer.get(idx).unwrap()).unwrap()
    }
}

impl<'a, T> Iterator for RecordBatchIterator<'a, T>
where
    T: NodeStateValue,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.deserializer.get(self.idx);
        if res.is_none() {
            return None;
        }
        let item = T::deserialize(res.unwrap()).unwrap();
        self.idx += 1;
        Some(item)
    }
}

impl<'graph, G: IntoDynamic, GH: IntoDynamic> GenericNodeState<'graph, G, GH> {
    pub fn into_dyn(self) -> GenericNodeState<'graph, DynamicGraph> {
        let node_cols = Some(
            self.node_cols
                .into_iter()
                .map(|(k, (output_type, bg, g))| {
                    (
                        k,
                        (
                            output_type,
                            bg.map(|bg| bg.into_dynamic()),
                            g.map(|g| g.into_dynamic()),
                        ),
                    )
                })
                .collect(),
        );
        GenericNodeState::new(
            self.base_graph.into_dynamic(),
            self.graph.into_dynamic(),
            self.values,
            self.keys,
            node_cols,
        )
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> GenericNodeState<'graph, G, GH> {
    pub fn new(
        base_graph: G,
        graph: GH,
        values: RecordBatch,
        keys: Option<Index<VID>>,
        node_cols: Option<HashMap<String, (NodeStateOutputType, Option<G>, Option<GH>)>>,
    ) -> Self {
        Self {
            base_graph,
            graph,
            values,
            keys,
            node_cols: node_cols.unwrap_or(HashMap::new()),
            _marker: PhantomData,
        }
    }

    // get node (base_graph, graph, Prop::U64)
    // get nodes(base_graph, graph, Vec<u64>)

    pub fn get_nodes(
        state: &GenericNodeState<'graph, G, GH>,
        value_map: PropMap,
    ) -> TransformedPropMap<'graph, G, GH> {
        // for column in aux data structure
        value_map
            .into_iter()
            .map(|(key, value)| {
                let node_col_entry = state.node_cols.get(&key);
                let mut result = None;
                if value.is_none() || node_col_entry.is_none() {
                    return (key, NodeStateOutput::Prop(value));
                } else {
                    let (node_state_type, base_graph, graph) = node_col_entry.unwrap();
                    if node_state_type == &NodeStateOutputType::Node {
                        if let Some(Prop::U64(vid)) = value {
                            result = Some((
                                key,
                                NodeStateOutput::Node(NodeView::new_one_hop_filtered(
                                    base_graph.as_ref().unwrap_or(&state.base_graph).clone(),
                                    graph.as_ref().unwrap_or(&state.graph).clone(),
                                    VID(vid as usize),
                                )),
                            ));
                        }
                    } else if node_state_type == &NodeStateOutputType::Nodes {
                        if let Some(Prop::Array(vid_arr)) = value {
                            return (
                                key,
                                NodeStateOutput::Nodes(Nodes::new_filtered(
                                    base_graph.as_ref().unwrap_or(&state.base_graph).clone(),
                                    graph.as_ref().unwrap_or(&state.graph).clone(),
                                    Some(Index::from_iter(
                                        vid_arr
                                            .iter_prop()
                                            .map(|vid| VID(vid.into_u64().unwrap() as usize)),
                                    )),
                                    None,
                                )),
                            );
                        } else if let Some(PropType::List(_)) =
                            value.as_ref().map(|value| value.dtype())
                        {
                            if let Some(Prop::List(vid_list)) = value {
                                if let Some(vid_list) = Arc::into_inner(vid_list) {
                                    result = Some((
                                        key,
                                        NodeStateOutput::Nodes(Nodes::new_filtered(
                                            base_graph
                                                .as_ref()
                                                .unwrap_or(&state.base_graph)
                                                .clone(),
                                            graph.as_ref().unwrap_or(&state.graph).clone(),
                                            Some(Index::from_iter(vid_list.into_iter().map(
                                                |vid| {
                                                    VID(vid
                                                        .try_cast(PropType::U64)
                                                        .unwrap()
                                                        .into_u64()
                                                        .unwrap()
                                                        as usize)
                                                },
                                            ))),
                                            None,
                                        )),
                                    ));
                                }
                            }
                        }
                    }
                }
                result.unwrap()
            })
            .collect()
    }

    pub fn transform(self) -> OutputTypedNodeState<'graph, G, GH> {
        TypedNodeState::new_mapped(self, Self::get_nodes)
    }

    pub fn into_inner(self) -> (RecordBatch, Option<Index<VID>>) {
        (self.values, self.keys)
    }

    pub fn values(&self) -> &RecordBatch {
        &self.values
    }

    fn get_index_by_node<N: AsNodeRef>(&self, node: &N) -> Option<usize> {
        let id = self.graph.internalise_node(node.as_node_ref())?;
        match &self.keys {
            Some(index) => index.index(&id),
            None => Some(id.0),
        }
    }

    fn nodes(&self) -> Nodes<'graph, G, GH> {
        Nodes::new_filtered(
            self.base_graph.clone(),
            self.graph.clone(),
            self.keys.clone(),
            None,
        )
    }

    fn len(&self) -> usize {
        self.values.num_rows()
    }

    pub fn to_parquet<P: AsRef<Path>>(&self, file_path: P, id_column: Option<String>) {
        let mut batch: Option<RecordBatch> = None;
        let mut schema = self.values.schema();

        if id_column.is_some() {
            let ids: Vec<String> = self
                .nodes()
                .id()
                .iter()
                .map(|(_, gid)| gid.to_string())
                .collect();
            let ids_array = Arc::new(StringArray::from(ids)) as ArrayRef;

            let mut builder = SchemaBuilder::new();
            for field in &self.values.schema().fields().clone() {
                builder.push(field.clone())
            }
            builder.push(Arc::new(Field::new(
                id_column.unwrap(),
                DataType::Utf8,
                false,
            )));
            schema = Arc::new(Schema::new(builder.finish().fields));

            let mut columns = self.values.columns().to_vec();
            columns.push(ids_array);

            batch = Some(RecordBatch::try_new(schema.clone(), columns).unwrap());
        }

        // Now write this new_batch to Parquet
        let file = File::create(file_path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer
            .write(batch.as_ref().unwrap_or(&self.values))
            .expect("Writing batch");
        writer.close().unwrap();
    }

    fn merge_columns(
        col_name: &String,
        tgt_idx_set: Option<&Index<VID>>,
        tgt_idx_set_len: usize,
        lh: Option<&Index<VID>>,
        rh: Option<&Index<VID>>,
        lh_batch: &RecordBatch,
        rh_batch: &RecordBatch,
    ) -> (Arc<dyn Array>, Field) {
        let iter = if tgt_idx_set.is_some() {
            Either::Left(tgt_idx_set.as_ref().unwrap().iter())
        } else {
            Either::Right((0..tgt_idx_set_len).map(VID))
        };
        let mut idx_builder = UInt64Builder::with_capacity(tgt_idx_set_len);
        let left_col = lh_batch.column_by_name(col_name);
        let left_len = if left_col.is_some() {
            left_col.unwrap().len()
        } else {
            0
        };
        let right_col = rh_batch.column_by_name(col_name);
        let cat = match (left_col, right_col) {
            (Some(l), Some(r)) => &concat(&[l.as_ref(), r.as_ref()]).unwrap(),
            (Some(l), None) => l,
            (None, Some(r)) => r,
            (None, None) => unreachable!("at least one is guaranteed to be Some"),
        };
        for i in iter {
            if left_col.is_some() && (lh.is_none() || lh.unwrap().contains(&i)) {
                if lh.is_none() {
                    idx_builder.append_value(i.0 as u64);
                } else {
                    idx_builder.append_value(lh.unwrap().index(&i).unwrap() as u64);
                }
            } else if right_col.is_some() && (rh.is_none() || rh.unwrap().contains(&i)) {
                if rh.is_none() {
                    idx_builder.append_value((left_len + i.0) as u64);
                } else {
                    idx_builder.append_value((left_len + rh.unwrap().index(&i).unwrap()) as u64);
                }
            } else {
                idx_builder.append_null();
            }
        }
        let take_idx: UInt64Array = idx_builder.finish();
        let mut field = lh_batch.schema_ref().field_with_name(col_name);
        if field.is_err() {
            field = rh_batch.schema_ref().field_with_name(col_name);
        }
        (
            take(cat.as_ref(), &take_idx, None).unwrap(),
            field.unwrap().clone(),
        )
    }

    pub fn merge(
        &self,
        other: &GenericNodeState<'graph, G>,
        index_merge_priority: MergePriority,
        default_column_merge_priority: MergePriority,
        column_merge_priority_map: Option<HashMap<String, MergePriority>>,
    ) -> Self {
        let new_idx_set = if self.keys.is_none() || other.keys.is_none() {
            None
        } else {
            Some(
                IndexSet::union(
                    self.keys.as_ref().unwrap().index.as_ref(),
                    other.keys.as_ref().unwrap().index.as_ref(),
                )
                .clone()
                .into_iter()
                .map(|v| v.to_owned())
                .collect(),
            )
        };
        let tgt_idx_set = match index_merge_priority {
            MergePriority::Left => self.keys.as_ref(),
            MergePriority::Right => other.keys.as_ref(),
            MergePriority::Exclude => new_idx_set.as_ref(),
        };
        let tgt_index_set_len =
            tgt_idx_set.map_or(self.graph.unfiltered_num_nodes(), |idx_set| idx_set.len());

        let mut cols: Vec<Arc<dyn Array>> = vec![];
        let mut fields: Vec<Field> = vec![];

        // iterate over priority map first and merge accordingly
        if column_merge_priority_map.as_ref().is_some() {
            for (col_name, priority) in column_merge_priority_map.as_ref().unwrap() {
                let (lh, rh, lh_batch, rh_batch) = match priority {
                    MergePriority::Left => (
                        self.keys.as_ref(),
                        other.keys.as_ref(),
                        &self.values,
                        &other.values,
                    ),
                    MergePriority::Right => (
                        other.keys.as_ref(),
                        self.keys.as_ref(),
                        &other.values,
                        &self.values,
                    ),
                    MergePriority::Exclude => continue,
                };
                let (col, field) = GenericNodeState::<'graph, G>::merge_columns(
                    col_name,
                    tgt_idx_set,
                    tgt_index_set_len,
                    lh,
                    rh,
                    lh_batch,
                    rh_batch,
                );
                cols.push(col);
                fields.push(field);
            }
        }

        // if default is not exclude
        // merge remaining columns accordingly
        let (lh, rh, lh_batch, rh_batch) = match default_column_merge_priority {
            MergePriority::Left => (
                self.keys.as_ref(),
                other.keys.as_ref(),
                &self.values,
                &other.values,
            ),
            MergePriority::Right => (
                other.keys.as_ref(),
                self.keys.as_ref(),
                &other.values,
                &self.values,
            ),
            MergePriority::Exclude => {
                // TODO: handle node cols merge
                return GenericNodeState::new(
                    self.base_graph.clone(),
                    self.graph.clone(),
                    RecordBatch::try_new(Schema::new(fields).into(), cols).unwrap(),
                    tgt_idx_set.cloned(),
                    Some(self.node_cols.clone()),
                );
            }
        };
        // iterate over columns in lh, if not in priority_map, merge columns
        for column in lh_batch.schema().fields().iter() {
            let col_name = column.name();
            if column_merge_priority_map
                .as_ref()
                .map_or(true, |map| map.contains_key(col_name) == false)
            {
                let (col, field) = GenericNodeState::<'graph, G>::merge_columns(
                    col_name,
                    tgt_idx_set,
                    tgt_index_set_len,
                    lh,
                    rh,
                    lh_batch,
                    rh_batch,
                );
                cols.push(col);
                fields.push(field);
            }
        }
        GenericNodeState::new(
            self.base_graph.clone(),
            self.graph.clone(),
            RecordBatch::try_new(Schema::new(fields).into(), cols).unwrap(),
            tgt_idx_set.cloned(),
            Some(self.node_cols.clone()),
        )
    }

    fn convert_recordbatch(
        recordbatch: RecordBatch,
    ) -> Result<RecordBatch, arrow_schema::ArrowError> {
        // Cast all arrays to nullable
        let new_columns: Vec<Arc<dyn Array>> = recordbatch
            .columns()
            .iter()
            .map(|col| cast_with_options(col, &col.data_type().clone(), &CastOptions::default()))
            .collect::<arrow::error::Result<_>>()?;

        // Rebuild schema with nullable fields
        let new_fields: Vec<Field> = recordbatch
            .schema()
            .fields()
            .iter()
            .map(|f| Field::new(f.name(), f.data_type().clone(), true))
            .collect();

        let new_schema = Arc::new(Schema::new(new_fields));

        RecordBatch::try_new(new_schema.clone(), new_columns)
    }
}

impl<'graph, V, G, GH> TypedNodeState<'graph, V, G, GH>
where
    V: NodeStateValue,
    G: GraphViewOps<'graph>,
    GH: GraphViewOps<'graph>,
{
    pub fn new(state: GenericNodeState<'graph, G, GH>) -> Self {
        Self {
            state,
            converter: V::transform,
            _v_marker: PhantomData,
            _t_marker: PhantomData,
        }
    }
}

impl<
        'graph,
        V: NodeStateValue + 'graph,
        T: Clone + Send + Sync + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
    > TypedNodeState<'graph, V, G, GH, T>
{
    pub fn new_mapped(
        state: GenericNodeState<'graph, G, GH>,
        converter: fn(&GenericNodeState<'graph, G, GH>, V) -> T,
    ) -> Self {
        TypedNodeState {
            state,
            converter,
            _v_marker: PhantomData,
            _t_marker: PhantomData,
        }
    }

    pub fn to_hashmap<Func, TransformedType>(self, f: Func) -> HashMap<String, TransformedType>
    where
        Func: Fn(V) -> TransformedType,
    {
        self.into_iter()
            .map(|(node, value)| (node.name(), f(value)))
            .collect()
    }

    pub fn transform(self) -> OutputTypedNodeState<'graph, G, GH> {
        TypedNodeState::new_mapped(self.state, GenericNodeState::get_nodes)
    }

    pub fn values_to_rows(&self) -> Vec<V> {
        let rows: Vec<V> = from_record_batch(&self.state.values).unwrap();
        rows
    }

    pub fn values_from_rows(&mut self, rows: Vec<V>) {
        let fields = Vec::<FieldRef>::from_type::<V>(TracingOptions::default()).unwrap();
        self.state.values = to_record_batch(&fields, &rows).unwrap();
    }

    pub fn convert(&self, value: V) -> T {
        (self.converter)(&self.state, value)
    }
}

impl<
        'a,
        'graph,
        V: NodeStateValue + 'graph,
        T: Clone + Sync + Send + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
    > PartialEq<TypedNodeState<'graph, V, G, GH, T>> for TypedNodeState<'graph, V, G, GH, T>
{
    fn eq(&self, other: &TypedNodeState<'graph, V, G, GH, T>) -> bool {
        self.len() == other.len()
            && self
                .par_iter()
                .all(|(node, value)| other.get_by_node(node).map(|v| v == value).unwrap_or(false))
    }
}

impl<
        'graph,
        RHS: NodeStateValue + Send + Sync + 'graph,
        T: Clone + Sync + Send + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
    > PartialEq<Vec<RHS>> for TypedNodeState<'graph, RHS, G, GH, T>
{
    fn eq(&self, other: &Vec<RHS>) -> bool {
        self.values_to_rows().par_iter().eq(other)
    }
}

impl<
        'graph,
        K: AsNodeRef,
        RHS: NodeStateValue + 'graph,
        T: Clone + Send + Sync + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
    > PartialEq<HashMap<K, RHS, S>> for TypedNodeState<'graph, RHS, G, GH, T>
{
    fn eq(&self, other: &HashMap<K, RHS, S>) -> bool {
        other.len() == self.len()
            && other
                .iter()
                .all(|(k, rhs)| self.get_by_node(k).filter(|lhs| lhs == rhs).is_none() == false)
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: Debug + GraphViewOps<'graph>>
    OutputTypedNodeState<'graph, G, GH>
{
    pub fn try_eq_hashmap<K: AsNodeRef>(
        &self,
        other: HashMap<K, HashMap<String, Option<Prop>>>,
    ) -> Result<bool, &'static str> {
        if other.len() != self.len() {
            return Ok(false);
        }

        for (k, mut rhs_map) in other {
            let lhs_map = self.get_by_node(&k).ok_or("Key missing in lhs map")?;

            if lhs_map.len() != rhs_map.len() {
                return Ok(false);
            }

            for (key, lhs_val) in lhs_map {
                let rhs_val = rhs_map.remove(&key).ok_or("Key missing in rhs map")?;

                if lhs_val.is_none() {
                    if rhs_val.is_none() {
                        continue;
                    } else {
                        return Ok(false);
                    }
                } else if rhs_val.is_none() {
                    return Ok(false);
                }

                let lhs_val = lhs_val.unwrap();
                let rhs_val = rhs_val.unwrap();

                let casted_rhs = rhs_val
                    .try_cast(lhs_val.dtype())
                    .map_err(|_| "Failed to cast rhs value")?;

                if casted_rhs != lhs_val {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}

impl<
        'graph,
        V: NodeStateValue + 'graph,
        T: Clone + Send + Sync + 'graph,
        G: GraphViewOps<'graph>,
        GH: Debug + GraphViewOps<'graph>,
    > Debug for TypedNodeState<'graph, V, G, GH, T>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<
        'graph,
        V: NodeStateValue + 'graph,
        T: Clone + Sync + Send + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
    > IntoIterator for TypedNodeState<'graph, V, G, GH, T>
{
    type Item = (NodeView<'graph, G, GH>, V);
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'graph>;

    fn into_iter(self) -> Self::IntoIter {
        self.nodes()
            .clone()
            .into_iter()
            .zip(self.into_iter_values())
            .into_dyn_boxed()
    }
}

impl<
        'a,
        'graph: 'a,
        V: NodeStateValue + 'graph,
        T: Clone + Sync + Send + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
    > NodeStateOps<'a, 'graph> for TypedNodeState<'graph, V, G, GH, T>
{
    type Graph = GH;
    type BaseGraph = G;

    type Value = V;
    type OwnedValue = V;

    fn graph(&self) -> &Self::Graph {
        &self.state.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.state.base_graph
    }

    fn iter_values(&'a self) -> impl Iterator<Item = Self::Value> + 'a {
        RecordBatchIterator::new(&self.state.values)
    }

    #[allow(refining_impl_trait)]
    fn par_iter_values(&'a self) -> impl IndexedParallelIterator<Item = Self::Value> + 'a {
        let iter = RecordBatchIterator::<'a, Self::Value>::new(&self.state.values);
        (0..self.len())
            .into_par_iter()
            .map(move |i| RecordBatchIterator::get(&iter, i))
    }

    fn into_iter_values(self) -> impl Iterator<Item = Self::OwnedValue> + Send + Sync {
        (0..self.len()).map(move |i| self.get_by_index(i).unwrap().1)
    }

    #[allow(refining_impl_trait)]
    fn into_par_iter_values(self) -> impl IndexedParallelIterator<Item = Self::OwnedValue> {
        (0..self.len())
            .into_par_iter()
            .map(move |i| self.get_by_index(i).unwrap().1)
    }

    fn iter(
        &'a self,
    ) -> impl Iterator<
        Item = (
            NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
            Self::Value,
        ),
    > + 'a {
        match &self.state.keys {
            Some(index) => index
                .iter()
                .zip(self.iter_values())
                .map(|(n, v)| {
                    (
                        NodeView::new_one_hop_filtered(
                            &self.state.base_graph,
                            &self.state.graph,
                            n,
                        ),
                        v,
                    )
                })
                .into_dyn_boxed(),
            None => self
                .iter_values()
                .enumerate()
                .map(|(i, v)| {
                    (
                        NodeView::new_one_hop_filtered(
                            &self.state.base_graph,
                            &self.state.graph,
                            VID(i),
                        ),
                        v,
                    )
                })
                .into_dyn_boxed(),
        }
    }

    fn nodes(&self) -> Nodes<'graph, Self::BaseGraph, Self::Graph> {
        self.state.nodes()
    }

    fn par_iter(
        &'a self,
    ) -> impl ParallelIterator<
        Item = (
            NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
            Self::Value,
        ),
    > {
        match &self.state.keys {
            Some(index) => {
                Either::Left(index.par_iter().zip(self.par_iter_values()).map(|(n, v)| {
                    (
                        NodeView::new_one_hop_filtered(
                            &self.state.base_graph,
                            &self.state.graph,
                            n,
                        ),
                        v,
                    )
                }))
            }
            None => Either::Right(self.par_iter_values().enumerate().map(|(i, v)| {
                (
                    NodeView::new_one_hop_filtered(
                        &self.state.base_graph,
                        &self.state.graph,
                        VID(i),
                    ),
                    v,
                )
            })),
        }
    }

    fn get_by_index(
        &'a self,
        index: usize,
    ) -> Option<(
        NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
        Self::Value,
    )> {
        let vid = match &self.state.keys {
            Some(node_index) => node_index.key(index).unwrap(),
            None => VID(index),
        };
        Some((
            NodeView::new_one_hop_filtered(&self.state.base_graph, &self.state.graph, vid),
            self.get_by_node(vid).unwrap(), // &self.values[index],
        ))
    }

    fn get_by_node<N: AsNodeRef>(&'a self, node: N) -> Option<Self::Value> {
        let index = self.state.get_index_by_node(&node).unwrap();
        let deserializer = Deserializer::from_record_batch(&self.state.values).unwrap();
        let item = V::deserialize(
            deserializer
                .get(index)
                .ok_or_else(|| tracing::error!("Could not get item"))
                .unwrap(),
        )
        .unwrap();
        Some(item)
    }

    fn len(&self) -> usize {
        self.state.len()
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> GenericNodeState<'graph, G, GH> {
    pub fn new_from_eval_with_index<V: InputNodeStateValue>(
        graph: G,
        filtered_graph: GH,
        values: Vec<V>,
        index: Option<Index<VID>>,
        node_cols: Option<HashMap<String, (NodeStateOutputType, Option<G>, Option<GH>)>>,
    ) -> Self {
        Self::new_from_eval_with_index_mapped(
            graph,
            filtered_graph,
            values,
            index,
            |v| v,
            node_cols,
        )
    }

    pub fn new_from_eval_with_index_mapped<R: Clone, V: InputNodeStateValue>(
        graph: G,
        filtered_graph: GH,
        values: Vec<R>,
        index: Option<Index<VID>>,
        map: impl Fn(R) -> V,
        node_cols: Option<HashMap<String, (NodeStateOutputType, Option<G>, Option<GH>)>>,
    ) -> Self {
        let values: Vec<V> = values.into_iter().map(map).collect();
        /*match &index {
            None => values.into_iter().map(map).collect(),
            Some(index) => index
                .iter()
                .enumerate()
                .map(|vid| map(values[index.index.get_index_of(&vid).unwrap()].clone()))
                .collect(),
        };*/
        let fields = Vec::<FieldRef>::from_type::<V>(TracingOptions::default()).unwrap();
        let values = Self::convert_recordbatch(to_record_batch(&fields, &values).unwrap()).unwrap();

        Self::new(graph, filtered_graph, values, index, node_cols)
    }
}

impl<'graph, G: GraphViewOps<'graph>> GenericNodeState<'graph, G> {
    /// create a new empty NodeState
    pub fn new_empty(graph: G) -> Self {
        Self::new(
            graph.clone(),
            graph,
            RecordBatch::new_empty(Schema::empty().into()),
            Some(Index::default()),
            None,
        )
    }

    /// Construct a node state from an eval result
    ///
    /// # Arguments
    /// - `graph`: the graph view
    /// - `values`: the unfiltered values (i.e., `values.len() == graph.unfiltered_num_nodes()`). This method handles the filtering.
    pub fn new_from_eval<V: InputNodeStateValue>(
        graph: G,
        values: Vec<V>,
        node_cols: Option<HashMap<String, (NodeStateOutputType, Option<G>, Option<G>)>>,
    ) -> Self {
        Self::new_from_eval_mapped(graph, values, |v| v, node_cols)
    }

    /// Construct a node state from an eval result, mapping values
    ///
    /// # Arguments
    /// - `graph`: the graph view
    /// - `values`: the unfiltered values (i.e., `values.len() == graph.unfiltered_num_nodes()`). This method handles the filtering.
    /// - `map`: Closure mapping input to output values
    pub fn new_from_eval_mapped<R: Clone, V: InputNodeStateValue>(
        graph: G,
        values: Vec<R>,
        map: impl Fn(R) -> V,
        node_cols: Option<HashMap<String, (NodeStateOutputType, Option<G>, Option<G>)>>,
    ) -> Self {
        let index = Index::for_graph(graph.clone());
        Self::new_from_eval_with_index_mapped(graph.clone(), graph, values, index, map, node_cols)
    }

    /// create a new NodeState from a list of values for the node (takes care of creating an index for
    /// node filtering when needed)
    pub fn new_from_values(
        graph: G,
        values: impl Into<RecordBatch>,
        node_cols: Option<HashMap<String, (NodeStateOutputType, Option<G>, Option<G>)>>,
    ) -> Self {
        let index = Index::for_graph(&graph);
        Self::new(
            graph.clone(),
            graph,
            Self::convert_recordbatch(values.into()).unwrap(),
            index,
            node_cols,
        )
    }

    /// create a new NodeState from a HashMap of values
    pub fn new_from_map<R: NodeStateValue, S: BuildHasher, V: NodeStateValue>(
        graph: G,
        mut values: HashMap<VID, R, S>,
        map: impl Fn(R) -> V,
        node_cols: Option<HashMap<String, (NodeStateOutputType, Option<G>, Option<G>)>>,
    ) -> Self {
        let fields = Vec::<FieldRef>::from_type::<V>(TracingOptions::default()).unwrap();
        if values.len() == graph.count_nodes() {
            let values: Vec<_> = graph
                .nodes()
                .iter()
                .map(|node| map(values.remove(&node.node).unwrap()))
                .collect();
            let values = to_record_batch(&fields, &values).unwrap();
            Self::new_from_values(graph, values, node_cols)
        } else {
            let (index, values): (IndexSet<VID, ahash::RandomState>, Vec<_>) = graph
                .nodes()
                .iter()
                .flat_map(|node| Some((node.node, map(values.remove(&node.node)?))))
                .unzip();
            let values = to_record_batch(&fields, &values).unwrap();
            Self::new(
                graph.clone(),
                graph,
                values,
                Some(Index::new(index)),
                node_cols,
            )
        }
    }
}
