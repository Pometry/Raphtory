use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, VID},
    db::{
        api::{state::node_state_ops::NodeStateOps, view::IntoDynBoxed},
        graph::node::NodeView,
    },
    prelude::GraphViewOps,
};
use rayon::{iter::Either, prelude::*};
use std::{fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc};

#[derive(Clone, Debug)]
pub struct Index<K> {
    keys: Arc<[K]>,
    map: Arc<[bool]>,
}

impl<K: Copy + Into<usize>> Index<K> {
    pub fn new(keys: impl Into<Arc<[K]>>, n: usize) -> Self {
        let keys = keys.into();
        let mut map = vec![false; n];
        for k in keys.iter().copied() {
            map[k.into()] = true;
        }
        Self {
            keys,
            map: map.into(),
        }
    }
}

impl<K: Copy + Ord + Into<usize> + Send + Sync> Index<K> {
    pub fn iter(&self) -> impl Iterator<Item = &K> + '_ {
        self.keys.iter()
    }

    pub fn into_par_iter(self) -> impl IndexedParallelIterator<Item = K> {
        let keys = self.keys;
        (0..keys.len()).into_par_iter().map(move |i| keys[i])
    }

    pub fn into_iter(self) -> impl Iterator<Item = K> {
        let keys = self.keys;
        (0..keys.len()).map(move |i| keys[i])
    }

    pub fn index(&self, key: &K) -> Option<usize> {
        self.keys.binary_search(key).ok()
    }

    pub fn key(&self, index: usize) -> Option<K> {
        self.keys.get(index).copied()
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn contains(&self, key: &K) -> bool {
        self.map.get((*key).into()).copied().unwrap_or(false)
    }
}

impl<K: Copy + Hash + Eq + Send + Sync> Index<K> {
    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = &K> + '_ {
        self.keys.par_iter()
    }
}

pub struct NodeState<'graph, V, G, GH = G> {
    base_graph: G,
    graph: GH,
    values: Vec<V>,
    keys: Option<Index<VID>>,
    _marker: PhantomData<&'graph ()>,
}

impl<'graph, V, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> NodeState<'graph, V, G, GH> {
    pub(crate) fn new(base_graph: G, graph: GH, values: Vec<V>, keys: Option<Index<VID>>) -> Self {
        Self {
            base_graph,
            graph,
            values,
            keys,
            _marker: PhantomData,
        }
    }
}

impl<'graph, V: Send + Sync + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    IntoIterator for NodeState<'graph, V, G, GH>
{
    type Item = V;
    type IntoIter = std::vec::IntoIter<V>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
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
        self.values.into_iter()
    }

    fn into_par_values(self) -> impl ParallelIterator<Item = Self::OwnedValue> + 'graph {
        self.values.into_par_iter()
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
                .keys
                .iter()
                .zip(self.values.iter())
                .map(|(n, v)| {
                    (
                        NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, *n),
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
            Some(index) => Either::Left(index.keys.par_iter().zip(self.values.par_iter()).map(
                |(n, v)| {
                    (
                        NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, *n),
                        v,
                    )
                },
            )),
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
            Some(node_index) => node_index.keys.get(index).map(|n| {
                (
                    NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, *n),
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
            values: vec![0.0f64],
            keys: None,
            _marker: Default::default(),
        };

        let int_state = NodeState {
            base_graph: g.clone(),
            graph: g.clone(),
            values: vec![1i64],
            keys: None,
            _marker: Default::default(),
        };
        let min_float = float_state.min_item().unwrap().1;
        let min_int = int_state.min_item().unwrap().1;
        assert_eq!(min_float, &0.0);
        assert_eq!(min_int, &1);
    }
}
