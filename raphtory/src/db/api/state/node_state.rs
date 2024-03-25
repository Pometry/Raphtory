use std::{borrow::Borrow, collections::HashMap, fmt::Debug, hash::Hash, marker::PhantomData};

use rayon::{iter::Either, prelude::*};

use crate::{
    core::entities::{nodes::node_ref::NodeRef, VID},
    db::{
        api::{state::ops::NodeStateOps, view::IntoDynBoxed},
        graph::node::NodeView,
    },
    prelude::GraphViewOps,
};

pub struct Index<K> {
    keys: Vec<K>,
    map: HashMap<K, usize>,
}

impl<K: Copy + Hash + Eq> Index<K> {
    pub fn new(keys: Vec<K>) -> Self {
        let map = keys.iter().enumerate().map(|(i, k)| (*k, i)).collect();
        Self { keys, map }
    }
}

pub struct NodeState<'graph, V, G, GH = G> {
    base_graph: G,
    graph: GH,
    values: Vec<V>,
    keys: Option<Index<VID>>,
    _marker: PhantomData<&'graph ()>,
}

impl<'graph, V, G, GH> NodeState<'graph, V, G, GH> {
    pub(crate) fn new(base_graph: G, graph: GH, values: Vec<V>, keys: Option<Vec<VID>>) -> Self {
        let keys = keys.map(Index::new);
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
    type Item = (NodeView<G, GH>, V);
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + Send + 'graph>;

    fn into_iter(self) -> Self::IntoIter {
        let bg = self.base_graph;
        let g = self.graph;
        match self.keys {
            Some(index) => index
                .keys
                .into_iter()
                .zip(self.values.into_iter())
                .map(move |(n, v)| (NodeView::new_one_hop_filtered(bg.clone(), g.clone(), n), v))
                .into_dyn_boxed(),
            None => {
                assert!(
                    !g.nodes_filtered(),
                    "nodes should not be filtered if no keys exist"
                );
                self.values
                    .into_iter()
                    .enumerate()
                    .map(move |(i, v)| {
                        let vid = VID(i);
                        (
                            NodeView::new_one_hop_filtered(bg.clone(), g.clone(), vid),
                            v,
                        )
                    })
                    .into_dyn_boxed()
            }
        }
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
    type Value<'a> = &'a V where 'graph: 'a;
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

    fn get_by_node<N: Into<NodeRef>>(
        &self,
        node: N,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value<'_>)> {
        let id = self.graph.internalise_node(node.into())?;
        match &self.keys {
            Some(index) => index.map.get(&id).map(|i| {
                (
                    NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, id),
                    &self.values[*i],
                )
            }),
            None => Some((
                NodeView::new_one_hop_filtered(&self.base_graph, &self.graph, id),
                &self.values[id.0],
            )),
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
        let min_float = float_state.min().unwrap().1;
        let min_int = int_state.min().unwrap().1;
        assert_eq!(min_float, &0.0);
        assert_eq!(min_int, &1);
    }
}
