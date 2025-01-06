use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{
            state::node_state_ops::NodeStateOps,
            view::{internal::NodeList, DynamicGraph, IntoDynBoxed, IntoDynamic},
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    prelude::GraphViewOps,
};
use indexmap::IndexSet;
use rayon::{iter::Either, prelude::*};
use std::{fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc};

#[derive(Clone, Debug)]
pub struct Index<K> {
    index: Arc<IndexSet<K, ahash::RandomState>>,
}

impl Index<VID> {
    pub fn for_graph<'graph>(graph: impl GraphViewOps<'graph>) -> Option<Self> {
        if graph.nodes_filtered() {
            if graph.node_list_trusted() {
                match graph.node_list() {
                    NodeList::All { .. } => None,
                    NodeList::List { nodes } => Some(nodes),
                }
            } else {
                Some(Self::new(graph.nodes().iter().map(|node| node.node)))
            }
        } else {
            None
        }
    }
}

impl<K: Copy + Eq + Hash + Into<usize> + From<usize> + Send + Sync> Index<K> {
    pub fn new(keys: impl IntoIterator<Item = K>) -> Self {
        Self {
            index: Arc::new(IndexSet::from_iter(keys)),
        }
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = K> + '_ {
        self.index.iter().copied()
    }

    pub fn into_par_iter(self) -> impl IndexedParallelIterator<Item = K> {
        (0..self.len())
            .into_par_iter()
            .map(move |i| *self.index.get_index(i).unwrap())
    }

    pub fn into_iter(self) -> impl Iterator<Item = K> {
        (0..self.len()).map(move |i| *self.index.get_index(i).unwrap())
    }

    #[inline]
    pub fn index(&self, key: &K) -> Option<usize> {
        self.index.get_index_of(key)
    }

    #[inline]
    pub fn key(&self, index: usize) -> Option<K> {
        self.index.get_index(index).copied()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.index.len()
    }

    #[inline]
    pub fn contains(&self, key: &K) -> bool {
        self.index.contains(key)
    }

    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = K> + '_ {
        (0..self.len())
            .into_par_iter()
            .map(move |i| *self.index.get_index(i).unwrap())
    }
}

#[derive(Clone)]
pub struct NodeState<'graph, V, G, GH = G> {
    base_graph: G,
    graph: GH,
    values: Arc<[V]>,
    keys: Option<Index<VID>>,
    _marker: PhantomData<&'graph ()>,
}

impl<'graph, V, G: IntoDynamic, GH: IntoDynamic> NodeState<'graph, V, G, GH> {
    pub fn into_dyn(self) -> NodeState<'graph, V, DynamicGraph> {
        NodeState::new(
            self.base_graph.into_dynamic(),
            self.graph.into_dynamic(),
            self.values,
            self.keys,
        )
    }
}

impl<'graph, V, G: GraphViewOps<'graph>> NodeState<'graph, V, G> {
    /// Construct a node state from an eval result
    ///
    /// # Arguments
    ///     - graph: the graph view
    ///     - values: the unfiltered values (i.e., `values.len() == graph.unfiltered_num_nodes()`).
    ///         This method handles the filtering.
    pub fn new_from_eval(graph: G, values: Vec<V>) -> Self
    where
        V: Clone,
    {
        let index = Index::for_graph(graph.clone());
        let values = match &index {
            None => values,
            Some(index) => index
                .iter()
                .map(|vid| values[vid.index()].clone())
                .collect(),
        };
        Self::new(graph.clone(), graph, values.into(), index)
    }

    /// Construct a node state from an eval result, mapping values
    ///
    /// # Arguments
    ///     - graph: the graph view
    ///     - values: the unfiltered values (i.e., `values.len() == graph.unfiltered_num_nodes()`).
    ///         This method handles the filtering.
    ///     - map: Closure mapping input to output values
    pub fn new_from_eval_mapped<R>(graph: G, values: Vec<R>, map: impl Fn(R) -> V) -> Self {
        let index = Index::for_graph(graph.clone());
        let values = match &index {
            None => values.into_iter().map(map).collect(),
            Some(index) => values
                .into_iter()
                .enumerate()
                .filter_map(|(i, v)| index.contains(&VID(i)).then(|| map(v)))
                .collect(),
        };
        Self::new(graph.clone(), graph, values, index)
    }
}

impl<'graph, V, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> NodeState<'graph, V, G, GH> {
    pub fn new(base_graph: G, graph: GH, values: Arc<[V]>, keys: Option<Index<VID>>) -> Self {
        Self {
            base_graph,
            graph,
            values,
            keys,
            _marker: PhantomData,
        }
    }

    pub fn into_inner(self) -> (Arc<[V]>, Option<Index<VID>>) {
        (self.values, self.keys)
    }
}

impl<
        'graph,
        V: Send + Sync + Clone + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
    > IntoIterator for NodeState<'graph, V, G, GH>
{
    type Item = (NodeView<G, GH>, V);
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'graph>;

    fn into_iter(self) -> Self::IntoIter {
        self.nodes()
            .clone()
            .into_iter()
            .zip(self.into_values())
            .into_dyn_boxed()
    }
}

impl<
        'graph,
        V: Clone + Send + Sync + 'graph,
        G: GraphViewOps<'graph>,
        GH: GraphViewOps<'graph>,
    > NodeStateOps<'graph> for NodeState<'graph, V, G, GH>
{
    type Graph = GH;
    type BaseGraph = G;
    type Value<'a>
        = &'a V
    where
        'graph: 'a;
    type OwnedValue = V;

    fn graph(&self) -> &Self::Graph {
        &self.graph
    }

    fn base_graph(&self) -> &Self::BaseGraph {
        &self.base_graph
    }

    fn values<'a>(&'a self) -> impl Iterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a,
    {
        self.values.iter()
    }

    fn par_values<'a>(&'a self) -> impl ParallelIterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a,
    {
        self.values.par_iter()
    }

    fn into_values(self) -> impl Iterator<Item = Self::OwnedValue> + 'graph {
        (0..self.values.len()).map(move |i| self.values[i].clone())
    }

    fn into_par_values(self) -> impl ParallelIterator<Item = Self::OwnedValue> + 'graph {
        (0..self.values.len())
            .into_par_iter()
            .map(move |i| self.values[i].clone())
    }

    fn iter<'a>(
        &'a self,
    ) -> impl Iterator<
        Item = (
            NodeView<&'a Self::BaseGraph, &'a Self::Graph>,
            Self::Value<'a>,
        ),
    > + 'a
    where
        'graph: 'a,
    {
        match &self.keys {
            Some(index) => index
                .iter()
                .zip(self.values.iter())
                .map(|(n, v)| {
                    (
                        NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, n),
                        v,
                    )
                })
                .into_dyn_boxed(),
            None => self
                .values
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    (
                        NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, VID(i)),
                        v,
                    )
                })
                .into_dyn_boxed(),
        }
    }

    fn nodes(&self) -> Nodes<'graph, Self::BaseGraph, Self::Graph> {
        Nodes::new_filtered(
            self.base_graph.clone(),
            self.graph.clone(),
            self.keys.clone(),
            None,
        )
    }

    fn par_iter<'a>(
        &'a self,
    ) -> impl ParallelIterator<
        Item = (
            NodeView<
                &'a <Self as NodeStateOps<'graph>>::BaseGraph,
                &'a <Self as NodeStateOps<'graph>>::Graph,
            >,
            <Self as NodeStateOps<'graph>>::Value<'a>,
        ),
    >
    where
        'graph: 'a,
    {
        match &self.keys {
            Some(index) => {
                Either::Left(index.par_iter().zip(self.values.par_iter()).map(|(n, v)| {
                    (
                        NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, n),
                        v,
                    )
                }))
            }
            None => Either::Right(self.values.par_iter().enumerate().map(|(i, v)| {
                (
                    NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, VID(i)),
                    v,
                )
            })),
        }
    }

    fn get_by_index(
        &self,
        index: usize,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value<'_>)> {
        match &self.keys {
            Some(node_index) => node_index.key(index).map(|n| {
                (
                    NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, n),
                    &self.values[index],
                )
            }),
            None => self.values.get(index).map(|v| {
                (
                    NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, VID(index)),
                    v,
                )
            }),
        }
    }

    fn get_by_node<N: AsNodeRef>(&self, node: N) -> Option<Self::Value<'_>> {
        let id = self.graph.internalise_node(node.as_node_ref())?;
        match &self.keys {
            Some(index) => index.index(&id).map(|i| &self.values[i]),
            None => Some(&self.values[id.0]),
        }
    }

    fn len(&self) -> usize {
        self.values.len()
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
        let float_state = NodeState {
            base_graph: g.clone(),
            graph: g.clone(),
            values: [0.0f64].into(),
            keys: None,
            _marker: Default::default(),
        };

        let int_state = NodeState {
            base_graph: g.clone(),
            graph: g.clone(),
            values: [1i64].into(),
            keys: None,
            _marker: Default::default(),
        };
        let min_float = float_state.min_item().unwrap().1;
        let min_int = int_state.min_item().unwrap().1;
        assert_eq!(min_float, &0.0);
        assert_eq!(min_int, &1);
    }
}
