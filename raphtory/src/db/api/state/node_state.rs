use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::{node_state_ops::NodeStateOps, ops::Const},
            view::{
                history::{
                    compose_history_from_items, CompositeHistory, History, HistoryDateTime,
                    HistoryEventId, HistoryTimestamp,
                },
                internal::{FilterOps, NodeList},
                DynamicGraph, IntoDynBoxed, IntoDynamic,
            },
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    prelude::{GraphViewOps, NodeViewOps},
};
use indexmap::IndexSet;
use raphtory_api::core::storage::timeindex::EventTime;
use rayon::{iter::Either, prelude::*};
use std::{
    borrow::Borrow,
    collections::HashMap,
    fmt::{Debug, Formatter},
    hash::{BuildHasher, Hash},
    marker::PhantomData,
    sync::Arc,
};
use storage::state::StateIndex;

#[derive(Debug)]
pub enum Index<K> {
    Full(Arc<StateIndex<K>>),
    Partial(Arc<IndexSet<K, ahash::RandomState>>),
}

impl<K> From<StateIndex<K>> for Index<K> {
    fn from(index: StateIndex<K>) -> Self {
        Self::Full(index.into())
    }
}

impl<K> Default for Index<K> {
    fn default() -> Self {
        Self::Partial(Arc::new(Default::default()))
    }
}

impl<K> Clone for Index<K> {
    fn clone(&self) -> Self {
        match self {
            Index::Full(index) => Index::Full(index.clone()),
            Index::Partial(index) => Index::Partial(index.clone()),
        }
    }
}

impl<K: Copy + Eq + Hash + Into<usize> + From<usize> + Send + Sync> FromIterator<K> for Index<K> {
    fn from_iter<T: IntoIterator<Item = K>>(iter: T) -> Self {
        Self::Partial(Arc::new(IndexSet::from_iter(iter)))
    }
}

impl Index<VID> {
    pub fn for_graph<'graph>(graph: impl GraphViewOps<'graph>) -> Self {
        if graph.filtered() {
            if graph.node_list_trusted() {
                match graph.node_list() {
                    NodeList::All { .. } => {
                        Self::Full(graph.core_graph().node_state_index().into())
                    }
                    NodeList::List { elems } => elems,
                }
            } else {
                Self::from_iter(graph.nodes().iter().map(|node| node.node))
            }
        } else {
            Self::Full(graph.core_graph().node_state_index().into())
        }
    }
}

impl<K: Copy + Eq + Hash + Into<usize> + From<usize> + Send + Sync> Index<K> {
    pub fn new(keys: impl Into<Arc<IndexSet<K, ahash::RandomState>>>) -> Self {
        Self::Partial(keys.into())
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = K> + '_ {
        match self {
            Index::Full(index) => Either::Left(index.iter()),
            Index::Partial(index) => Either::Right(index.iter().copied()),
        }
    }

    pub fn into_par_iter(self) -> impl ParallelIterator<Item = K> {
        match self {
            Index::Full(index) => Either::Left(index.into_par_iter().map(|(_, k)| k)),
            Index::Partial(index) => Either::Right(
                (0..index.len())
                    .into_par_iter()
                    .map(move |i| *index.get_index(i).unwrap()),
            ),
        }
    }

    pub fn into_iter(self) -> impl Iterator<Item = K> {
        match self {
            Index::Full(index) => Either::Left(index.arc_into_iter()),
            Index::Partial(index) => {
                Either::Right((0..index.len()).map(move |i| *index.get_index(i).unwrap()))
            }
        }
    }

    #[inline]
    pub fn index(&self, key: &K) -> Option<usize> {
        // self.index.get_index_of(key)
        match self {
            Index::Full(index) => index.resolve(*key),
            Index::Partial(index) => index.get_index_of(key),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Index::Full(index) => index.len(),
            Index::Partial(index) => index.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn contains(&self, key: &K) -> bool {
        match self {
            Index::Full(index) => index.resolve(*key).is_some(),
            Index::Partial(index) => index.contains(key),
        }
    }

    pub fn par_iter(&self) -> impl ParallelIterator<Item = (usize, K)> + '_ {
        match self {
            Index::Full(index) => Either::Left(index.par_iter()),
            Index::Partial(index) => Either::Right(
                (0..index.len())
                    .into_par_iter()
                    .map(move |i| (i, *index.get_index(i).unwrap())),
            ),
        }
    }

    pub fn intersection(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Full(_), Self::Partial(a)) => Self::Partial(a.clone()),
            (Self::Partial(a), Self::Full(_)) => Self::Partial(a.clone()),
            (Self::Partial(a), Self::Partial(b)) => a.intersection(b).copied().collect(),
            _ => self.clone(),
        }
    }
}

#[derive(Clone)]
pub struct NodeState<'graph, V, G> {
    base_graph: G,
    values: Arc<[V]>,
    keys: Index<VID>,
    _marker: PhantomData<&'graph ()>,
}

impl<'graph, V: Debug + Clone + Send + Sync + 'graph, G: GraphViewOps<'graph>> Debug
    for NodeState<'graph, V, G>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.iter().map(|(node, value)| (node.id(), value)))
            .finish()
    }
}

impl<'graph, RHS: Send + Sync, V: PartialEq<RHS> + Send + Sync + Clone + 'graph, G>
    PartialEq<Vec<RHS>> for NodeState<'graph, V, G>
{
    fn eq(&self, other: &Vec<RHS>) -> bool {
        self.values.par_iter().eq(other)
    }
}

impl<'graph, RHS: Send + Sync, V: PartialEq<RHS> + Send + Sync + Clone + 'graph, G>
    PartialEq<&[RHS]> for NodeState<'graph, V, G>
{
    fn eq(&self, other: &&[RHS]) -> bool {
        self.values.par_iter().eq(*other)
    }
}

impl<
        'graph,
        V: Clone + Send + Sync + PartialEq + 'graph,
        G: GraphViewOps<'graph>,
        RHS: NodeStateOps<'graph, OwnedValue = V>,
    > PartialEq<RHS> for NodeState<'graph, V, G>
{
    fn eq(&self, other: &RHS) -> bool {
        self.len() == other.len()
            && self.par_iter().all(|(node, value)| {
                other
                    .get_by_node(node)
                    .map(|v| v.borrow() == value)
                    .unwrap_or(false)
            })
    }
}

impl<
        'graph,
        K: AsNodeRef,
        RHS: Send + Sync,
        V: PartialEq<RHS> + Send + Sync + Clone + 'graph,
        G: GraphViewOps<'graph>,
        S,
    > PartialEq<HashMap<K, RHS, S>> for NodeState<'graph, V, G>
{
    fn eq(&self, other: &HashMap<K, RHS, S>) -> bool {
        other.len() == self.len()
            && other
                .iter()
                .all(|(k, rhs)| self.get_by_node(k).filter(|&lhs| lhs == rhs).is_some())
    }
}

impl<'graph, V, G: IntoDynamic> NodeState<'graph, V, G> {
    pub fn into_dyn(self) -> NodeState<'graph, V, DynamicGraph> {
        NodeState::new(self.base_graph.into_dynamic(), self.values, self.keys)
    }
}

impl<'graph, V, G: GraphViewOps<'graph>> NodeState<'graph, V, G> {
    /// Construct a node state from an eval result
    ///
    /// # Arguments
    /// - `graph`: the graph view
    /// - `values`: the values indexed by flat position (i.e., `values.len() == index.len()`).
    pub fn new_from_eval(graph: G, values: Vec<V>) -> Self {
        let index = Index::for_graph(graph.clone());
        // Values are already in flat index order from TaskRunner
        Self::new(graph, values.into(), index)
    }

    /// Construct a node state from an eval result
    ///
    /// # Arguments
    /// - `graph`: the graph view
    /// - `values`: the values indexed by flat position (i.e., `values.len() == index.len()`).
    /// - `index`: the index mapping VID to flat position in values
    pub fn new_from_eval_with_index(graph: G, values: Vec<V>, index: Index<VID>) -> Self {
        // Values are already in flat index order from TaskRunner
        Self::new(graph, values.into(), index)
    }

    /// Construct a node state from an eval result, mapping values
    ///
    /// # Arguments
    /// - `graph`: the graph view
    /// - `values`: the values indexed by flat position (i.e., `values.len() == index.len()`).
    /// - `map`: Closure mapping input to output values
    pub fn new_from_eval_mapped_with_index<R: Clone>(
        graph: G,
        values: Vec<R>,
        index: Index<VID>,
        map: impl Fn(R) -> V,
    ) -> Self
    where
        V: std::fmt::Debug,
    {
        // Values are already in flat index order from TaskRunner, just map them
        let values = values.into_iter().map(map).collect();
        Self::new(graph, values, index)
    }

    /// Construct a node state from an eval result, mapping values
    ///
    /// # Arguments
    /// - `graph`: the graph view
    /// - `values`: the values indexed by flat position (i.e., `values.len() == index.len()`).
    /// - `map`: Closure mapping input to output values
    pub fn new_from_eval_mapped<R: Clone>(graph: G, values: Vec<R>, map: impl Fn(R) -> V) -> Self
    where
        V: std::fmt::Debug,
    {
        let index = Index::for_graph(graph.clone());
        // Values are already in flat index order from TaskRunner, just map them
        let values = values.into_iter().map(map).collect();
        Self::new(graph, values, index)
    }

    /// create a new empty NodeState
    pub fn new_empty(graph: G) -> Self {
        let index = Index::for_graph(&graph);
        Self::new(graph, [].into(), index)
    }

    /// create a new NodeState from a list of values for the node (takes care of creating an index for
    /// node filtering when needed)
    pub fn new_from_values(graph: G, values: impl Into<Arc<[V]>>) -> Self {
        let index = Index::for_graph(&graph);
        Self::new(graph, values.into(), index)
    }

    /// create a new NodeState from a HashMap of values
    pub fn new_from_map<R, S: BuildHasher>(
        graph: G,
        mut values: HashMap<VID, R, S>,
        map: impl Fn(R) -> V,
    ) -> Self {
        if values.len() == graph.count_nodes() {
            let values: Vec<_> = graph
                .nodes()
                .iter()
                .map(|node| map(values.remove(&node.node).unwrap()))
                .collect();
            Self::new_from_values(graph, values)
        } else {
            let (index, values): (IndexSet<VID, ahash::RandomState>, Vec<_>) = graph
                .nodes()
                .iter()
                .flat_map(|node| Some((node.node, map(values.remove(&node.node)?))))
                .unzip();
            Self::new(graph, values.into(), Index::Partial(index.into()))
        }
    }

    pub fn keys(&self) -> &Index<VID> {
        &self.keys
    }
}

impl<'graph, V, G: GraphViewOps<'graph>> NodeState<'graph, V, G> {
    pub fn new(base_graph: G, values: Arc<[V]>, keys: Index<VID>) -> Self {
        Self {
            base_graph,
            values,
            keys,
            _marker: PhantomData,
        }
    }

    pub fn values(&self) -> &Arc<[V]> {
        &self.values
    }

    pub fn ids(&self) -> &Index<VID> {
        &self.keys
    }
}

impl<'graph, V: Send + Sync + Clone + 'graph, G: GraphViewOps<'graph>> IntoIterator
    for NodeState<'graph, V, G>
{
    type Item = (NodeView<'graph, G>, V);
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'graph>;

    fn into_iter(self) -> Self::IntoIter {
        self.nodes()
            .clone()
            .into_iter()
            .zip(self.into_iter_values())
            .into_dyn_boxed()
    }
}

impl<'graph, V: Clone + Send + Sync + 'graph, G: GraphViewOps<'graph>> NodeStateOps<'graph>
    for NodeState<'graph, V, G>
{
    type BaseGraph = G;
    type Graph = G;
    type Select = Const<bool>;
    type Value<'a>
        = &'a V
    where
        'graph: 'a;
    type OwnedValue = V;

    fn graph(&self) -> &Self::Graph {
        &self.base_graph
    }

    fn iter_values<'a>(&'a self) -> impl Iterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a,
    {
        self.values.iter()
    }

    fn par_iter_values<'a>(&'a self) -> impl ParallelIterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a,
    {
        self.values.par_iter()
    }

    fn into_iter_values(self) -> impl Iterator<Item = Self::OwnedValue> + 'graph {
        (0..self.values.len()).map(move |i| self.values[i].clone())
    }

    fn into_par_iter_values(self) -> impl ParallelIterator<Item = Self::OwnedValue> + 'graph {
        (0..self.values.len())
            .into_par_iter()
            .map(move |i| self.values[i].clone())
    }

    fn iter<'a>(
        &'a self,
    ) -> impl Iterator<Item = (NodeView<'a, &'a Self::Graph>, Self::Value<'a>)> + 'a
    where
        'graph: 'a,
    {
        self.keys
            .iter()
            .zip(self.values.iter())
            .map(move |(n, v)| (NodeView::new_internal(&self.base_graph, n), v))
    }

    fn nodes(&self) -> Nodes<'graph, Self::BaseGraph, Self::Graph, Self::Select> {
        Nodes::new_filtered(
            self.base_graph.clone(),
            self.base_graph.clone(),
            Const(true),
            self.keys.clone(),
        )
    }

    fn par_iter<'a>(
        &'a self,
    ) -> impl ParallelIterator<
        Item = (
            NodeView<'a, &'a <Self as NodeStateOps<'graph>>::Graph>,
            <Self as NodeStateOps<'graph>>::Value<'a>,
        ),
    >
    where
        'graph: 'a,
    {
        self.keys.par_iter().map(move |(val_id, n)| {
            (
                NodeView::new_internal(&self.base_graph, n),
                &self.values[val_id],
            )
        })
    }

    fn get_by_node<N: AsNodeRef>(&self, node: N) -> Option<Self::Value<'_>> {
        let id = self.base_graph.internalise_node(node.as_node_ref())?;
        self.keys.index(&id).map(|i| &self.values[i])
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

impl<'graph, G: GraphViewOps<'graph>>
    NodeState<'graph, History<'graph, NodeView<'graph, DynamicGraph>>, G>
{
    pub fn t(&self) -> NodeState<'graph, HistoryTimestamp<NodeView<'graph, DynamicGraph>>, G> {
        let values = self
            .values
            .iter()
            .map(|h| h.clone().t())
            .collect::<Vec<HistoryTimestamp<NodeView<DynamicGraph>>>>()
            .into();
        NodeState::new(self.base_graph.clone(), values, self.keys.clone())
    }

    pub fn dt(&self) -> NodeState<'graph, HistoryDateTime<NodeView<'graph, DynamicGraph>>, G> {
        let values = self
            .values
            .iter()
            .map(|h| h.clone().dt())
            .collect::<Vec<HistoryDateTime<NodeView<DynamicGraph>>>>()
            .into();
        NodeState::new(self.base_graph.clone(), values, self.keys.clone())
    }

    pub fn event_id(&self) -> NodeState<'graph, HistoryEventId<NodeView<'graph, DynamicGraph>>, G> {
        let values = self
            .values
            .iter()
            .map(|h| h.clone().event_id())
            .collect::<Vec<HistoryEventId<NodeView<DynamicGraph>>>>()
            .into();
        NodeState::new(self.base_graph.clone(), values, self.keys.clone())
    }

    pub fn earliest_time(&self) -> Option<EventTime> {
        self.values.iter().filter_map(|h| h.earliest_time()).min()
    }

    pub fn latest_time(&self) -> Option<EventTime> {
        self.values.iter().filter_map(|h| h.latest_time()).max()
    }

    /// Collect and return all the contained time entries as a sorted list
    pub fn collect_time_entries(&self) -> Vec<EventTime> {
        let mut entries: Vec<EventTime> = self
            .par_iter_values()
            .flat_map_iter(|hist| hist.iter())
            .collect();
        entries.par_sort_unstable();
        entries
    }

    /// Flattens all history objects into a single history object with all time entries ordered.
    pub fn flatten(
        &self,
    ) -> History<'graph, CompositeHistory<'graph, NodeView<'graph, DynamicGraph>>> {
        let histories: Vec<_> = self.par_iter_values().map(|hist| hist.0.clone()).collect();
        compose_history_from_items(histories)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        db::api::state::{node_state::NodeState, AsOrderedNodeStateOps, OrderedNodeStateOps},
        prelude::*,
    };

    #[test]
    fn float_state() {
        let g = Graph::new();
        g.add_node(0, 0, NO_PROPS, None).unwrap();
        let float_state = NodeState::new_from_values(g.clone(), [0.0f64]);
        let int_state = NodeState::new_from_values(g.clone(), [1i64]);
        let min_float = float_state.min_item().unwrap().1;
        let min_int = int_state.min_item().unwrap().1;
        assert_eq!(min_float, &0.0);
        assert_eq!(min_int, &1);
    }
}
