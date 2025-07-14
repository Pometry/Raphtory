use crate::{
    core::{
        entities::{nodes::node_ref::AsNodeRef, VID},
        Prop,
    },
    db::{
        api::{
            state::{node_state_ops::NodeStateOps, Index},
            view::{DynamicGraph, IntoDynBoxed, IntoDynamic},
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    prelude::GraphViewOps,
};
use arrow_array::RecordBatch;
use arrow_schema::{FieldRef, Schema};
use indexmap::IndexSet;
use rayon::{iter::Either, prelude::*};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_arrow::{
    from_record_batch,
    schema::{SchemaLike, TracingOptions},
    to_record_batch, Deserializer,
};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    hash::BuildHasher,
    io::ErrorKind,
    marker::PhantomData,
};

use super::node_state_ops::ToOwnedValue;

pub trait NodeStateValue:
    Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + Debug
{
}
impl<T> NodeStateValue for T where
    T: Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + Debug
{
}

#[derive(Clone, Debug)]
pub struct GenericNodeState<'graph, G, GH = G> {
    base_graph: G,
    graph: GH,
    values: RecordBatch,
    keys: Option<Index<VID>>,
    _marker: PhantomData<&'graph ()>,
}

#[derive(Clone)]
pub struct TypedNodeState<'graph, V: NodeStateValue, G, GH = G> {
    pub state: GenericNodeState<'graph, G, GH>,
    _marker: PhantomData<V>,
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
        GenericNodeState::new(
            self.base_graph.into_dynamic(),
            self.graph.into_dynamic(),
            self.values,
            self.keys,
        )
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> GenericNodeState<'graph, G, GH> {
    pub fn new(base_graph: G, graph: GH, values: RecordBatch, keys: Option<Index<VID>>) -> Self {
        Self {
            base_graph,
            graph,
            values,
            keys,
            _marker: PhantomData,
        }
    }

    pub fn transform(self) -> TypedNodeState<'graph, HashMap<String, Prop>, G, GH> {
        TypedNodeState::new(self)
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

    /*
    pub fn to_parquet<P: AsRef<Path>>(self, file_path: P) {
        let file = File::create(file_path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, self.values.schema(), Some(props)).unwrap();
        writer.write(&self.values).expect("Writing batch");
        writer.close().unwrap();
    }

    pub fn values_from_parquet<P: AsRef<Path>>(&mut self, file_path: P) {
        let file = File::open(file_path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build().unwrap();
        self.values = reader.next().unwrap().unwrap();
    }
    */
}

impl<'graph, V: NodeStateValue, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    TypedNodeState<'graph, V, G, GH>
{
    pub fn new(state: GenericNodeState<'graph, G, GH>) -> Self {
        TypedNodeState {
            state: state,
            _marker: PhantomData,
        }
    }

    pub fn values_to_rows(&self) -> Vec<V> {
        let rows: Vec<V> = from_record_batch(&self.state.values).unwrap();
        rows
    }

    pub fn values_from_rows(&mut self, rows: Vec<V>) {
        let fields = Vec::<FieldRef>::from_type::<V>(TracingOptions::default()).unwrap();
        self.state.values = to_record_batch(&fields, &rows).unwrap();
    }
}

impl<'a, 'graph, V: NodeStateValue + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    PartialEq<TypedNodeState<'graph, V, G, GH>> for TypedNodeState<'graph, V, G, GH>
{
    fn eq(&self, other: &TypedNodeState<'graph, V, G, GH>) -> bool {
        self.len() == other.len()
            && self
                .par_iter()
                .all(|(node, value)| other.get_by_node(node).map(|v| v == value).unwrap_or(false))
    }
}

impl<
        'graph,
        RHS: NodeStateValue + Send + Sync,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
    > PartialEq<Vec<RHS>> for TypedNodeState<'graph, RHS, G, GH>
{
    fn eq(&self, other: &Vec<RHS>) -> bool {
        self.values_to_rows().par_iter().eq(other)
    }
}

impl<
        'graph,
        K: AsNodeRef,
        RHS: NodeStateValue + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
        S,
    > PartialEq<HashMap<K, RHS, S>> for TypedNodeState<'graph, RHS, G, GH>
{
    fn eq(&self, other: &HashMap<K, RHS, S>) -> bool {
        other.len() == self.len()
            && other
                .iter()
                .all(|(k, rhs)| self.get_by_node(k).filter(|lhs| lhs == rhs).is_none() == false)
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: Debug + GraphViewOps<'graph>>
    TypedNodeState<'graph, HashMap<String, Prop>, G, GH>
{
    pub fn try_eq_hashmap<K: AsNodeRef>(
        &self,
        other: HashMap<K, HashMap<String, Prop>>,
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
        V: 'graph + NodeStateValue,
        G: GraphViewOps<'graph>,
        GH: Debug + GraphViewOps<'graph>,
    > Debug for TypedNodeState<'graph, V, G, GH>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<'graph, V: NodeStateValue + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    IntoIterator for TypedNodeState<'graph, V, G, GH>
{
    type Item = (NodeView<G, GH>, V);
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
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
    > NodeStateOps<'a, 'graph> for TypedNodeState<'graph, V, G, GH>
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
        let iter = RecordBatchIterator::new(&self.state.values);
        (0..self.len()).into_par_iter().map(move |i| iter.get(i))
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
    ) -> impl Iterator<Item = (NodeView<&'a Self::BaseGraph, &'a Self::Graph>, Self::Value)> + 'a
    {
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
        Nodes::new_filtered(
            self.state.base_graph.clone(),
            self.state.graph.clone(),
            self.state.keys.clone(),
            None,
        )
    }

    fn par_iter(
        &'a self,
    ) -> impl ParallelIterator<Item = (NodeView<&'a Self::BaseGraph, &'a Self::Graph>, Self::Value)>
    {
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
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value)> {
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
        self.state.values.num_rows()
    }
}

impl<'graph, G: GraphViewOps<'graph>> GenericNodeState<'graph, G> {
    /// Construct a node state from an eval result
    ///
    /// # Arguments
    /// - `graph`: the graph view
    /// - `values`: the unfiltered values (i.e., `values.len() == graph.unfiltered_num_nodes()`). This method handles the filtering.
    pub fn new_from_eval<V: NodeStateValue>(graph: G, values: Vec<V>) -> Self {
        let index = Index::for_graph(graph.clone());
        let values = match &index {
            None => values,
            Some(index) => index
                .iter()
                .map(|vid| values[vid.index()].clone())
                .collect(),
        };
        let fields = Vec::<FieldRef>::from_type::<V>(TracingOptions::default()).unwrap();
        let values = to_record_batch(&fields, &values).unwrap();
        Self::new(graph.clone(), graph, values, index)
    }

    /// Construct a node state from an eval result, mapping values
    ///
    /// # Arguments
    /// - `graph`: the graph view
    /// - `values`: the unfiltered values (i.e., `values.len() == graph.unfiltered_num_nodes()`). This method handles the filtering.
    /// - `map`: Closure mapping input to output values
    pub fn new_from_eval_mapped<R: NodeStateValue, V: NodeStateValue>(
        graph: G,
        values: Vec<R>,
        map: impl Fn(R) -> V,
    ) -> Self {
        let index = Index::for_graph(graph.clone());
        let values: Vec<V> = match &index {
            None => values.into_iter().map(map).collect(),
            Some(index) => index
                .iter()
                .map(|vid| map(values[vid.index()].clone()))
                .collect(),
        };
        let fields = Vec::<FieldRef>::from_type::<V>(TracingOptions::default()).unwrap();
        let values = to_record_batch(&fields, &values).unwrap();
        Self::new(graph.clone(), graph, values, index)
    }

    /// create a new empty NodeState
    pub fn new_empty(graph: G) -> Self {
        Self::new(
            graph.clone(),
            graph,
            RecordBatch::new_empty(Schema::empty().into()),
            Some(Index::default()),
        )
    }

    /// create a new NodeState from a list of values for the node (takes care of creating an index for
    /// node filtering when needed)
    pub fn new_from_values(graph: G, values: impl Into<RecordBatch>) -> Self {
        let index = Index::for_graph(&graph);
        Self::new(graph.clone(), graph, values.into(), index)
    }

    /// create a new NodeState from a HashMap of values
    pub fn new_from_map<R: NodeStateValue, S: BuildHasher, V: NodeStateValue>(
        graph: G,
        mut values: HashMap<VID, R, S>,
        map: impl Fn(R) -> V,
    ) -> Self {
        let fields = Vec::<FieldRef>::from_type::<V>(TracingOptions::default()).unwrap();
        if values.len() == graph.count_nodes() {
            let values: Vec<_> = graph
                .nodes()
                .iter()
                .map(|node| map(values.remove(&node.node).unwrap()))
                .collect();
            let values = to_record_batch(&fields, &values).unwrap();
            Self::new_from_values(graph, values)
        } else {
            let (index, values): (IndexSet<VID, ahash::RandomState>, Vec<_>) = graph
                .nodes()
                .iter()
                .flat_map(|node| Some((node.node, map(values.remove(&node.node)?))))
                .unzip();
            let values = to_record_batch(&fields, &values).unwrap();
            Self::new(graph.clone(), graph, values, Some(Index::new(index)))
        }
    }
}
