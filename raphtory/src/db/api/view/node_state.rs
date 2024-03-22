use crate::{
    core::entities::{nodes::node_ref::NodeRef, VID},
    db::{api::view::IntoDynBoxed, graph::node::NodeView},
    prelude::GraphViewOps,
};
use rayon::prelude::*;
use std::{
    borrow::Borrow, cmp::Ordering, collections::HashMap, hash::Hash, marker::PhantomData, sync::Arc,
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
                    !self.graph.nodes_filtered(),
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

impl<'graph, V: Send + Sync + 'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>>
    NodeStateOps<'graph> for NodeState<'graph, V, G, GH>
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

    fn values(&self) -> impl Iterator<Item = Self::Value<'_>> + '_ {
        self.values.iter()
    }

    fn par_values(&self) -> impl ParallelIterator<Item = Self::Value<'_>> + '_ {
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
    ) -> impl Iterator<Item = (NodeView<&'a Self::BaseGraph, &'a Self::Graph>, Self::Value)> + 'a
    where
        'graph: 'a,
    {
        todo!()
    }

    fn par_iter<'a>(
        &'a self,
    ) -> impl ParallelIterator<Item = (NodeView<&'a Self::BaseGraph, &'a Self::Graph>, Self::Value)>
    where
        'graph: 'a,
    {
        todo!()
    }

    fn get_by_index(
        &self,
        index: usize,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, &Self::Value)> {
        todo!()
    }

    fn get_by_node<N: Into<NodeRef>>(
        &self,
        node: N,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, &Self::Value)> {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn top_k_by<F: Fn(&Self::Value, &Self::Value) -> Ordering>(
        &self,
        cmp: F,
        k: usize,
    ) -> NodeState<'graph, Self::Value, Self::BaseGraph, Self::Graph> {
        todo!()
    }

    fn min_by<F: Fn(&Self::Value, &Self::Value) -> Ordering>(
        &self,
        cmp: F,
    ) -> Option<(NodeView<Self::BaseGraph, Self::Graph>, Self::Value)> {
        todo!()
    }
}

pub trait NodeStateOps<'graph>:
    IntoIterator<Item = (NodeView<Self::BaseGraph, Self::Graph>, Self::OwnedValue)>
{
    type Graph: GraphViewOps<'graph>;
    type BaseGraph: GraphViewOps<'graph>;
    type Value<'a>: Send + Sync + Borrow<Self::OwnedValue>
    where
        'graph: 'a;

    type OwnedValue: Send + Sync + 'graph;

    fn graph(&self) -> &Self::Graph;

    fn base_graph(&self) -> &Self::BaseGraph;

    fn values<'a>(&'a self) -> impl Iterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a;

    fn par_values<'a>(&'a self) -> impl ParallelIterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a;

    fn into_values(self) -> impl Iterator<Item = Self::OwnedValue> + 'graph;

    fn into_par_values(self) -> impl ParallelIterator<Item = Self::OwnedValue> + 'graph;

    fn iter<'a>(
        &'a self,
    ) -> impl Iterator<
        Item = (
            NodeView<&'a Self::BaseGraph, &'a Self::Graph>,
            Self::Value<'_>,
        ),
    > + 'a
    where
        'graph: 'a;

    fn par_iter<'a>(
        &'a self,
    ) -> impl ParallelIterator<
        Item = (
            NodeView<&'a Self::BaseGraph, &'a Self::Graph>,
            Self::Value<'_>,
        ),
    >
    where
        'graph: 'a;

    fn get_by_index(
        &self,
        index: usize,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, &Self::Value<'_>)>;

    fn get_by_node<N: Into<NodeRef>>(
        &self,
        node: N,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value<'_>)>;

    fn len(&self) -> usize;

    /// Sorts the by its values in ascending or descending order.
    ///
    /// Arguments:
    ///
    /// cmp: Comparison function for values
    ///
    /// Returns:
    ///
    /// A sorted vector of tuples containing node names and values.
    fn sort_by_values<F: Fn(Self::Value<'_>, Self::Value<'_>) -> std::cmp::Ordering + Sync>(
        &self,
        cmp: F,
    ) -> NodeState<'graph, Self::Value, Self::BaseGraph, Self::Graph> {
        {
            let mut state: Vec<_> = self.par_iter().map(|(n, v)| (n.node, v)).collect();
            state.par_sort_by(|(_, v1), (_, v2)| cmp(v1, v2));

            let mut keys = Vec::with_capacity(state.len());
            let mut values = Vec::with_capacity(state.len());
            state
                .into_par_iter()
                .unzip_into_vecs(&mut keys, &mut values);

            let index = Index::new(keys);

            NodeState {
                base_graph: self.base_graph().clone(),
                graph: self.graph().clone(),
                values: values.into(),
                keys: Some(index),
                _marker: Default::default(),
            }
        }
    }

    /// Retrieves the top-k elements from the `AlgorithmResult` based on its values.
    ///
    /// Arguments:
    ///
    /// * `k`: The number of elements to retrieve.
    /// * `percentage`: If `true`, the `k` parameter is treated as a percentage of total elements.
    /// * `reverse`: If `true`, retrieves the elements in descending order; otherwise, in ascending order.
    ///
    /// Returns:
    ///
    /// An `a vector of tuples with keys of type `H` and values of type `Y`.
    /// If `percentage` is `true`, the returned vector contains the top `k` percentage of elements.
    /// If `percentage` is `false`, the returned vector contains the top `k` elements.
    /// Returns empty vec if the result is empty or if `k` is 0.
    fn top_k_by<F: Fn(&Self::Value, &Self::Value) -> std::cmp::Ordering>(
        &self,
        cmp: F,
        k: usize,
    ) -> NodeState<'graph, Self::Value, Self::BaseGraph, Self::Graph>;

    fn bottom_k_by<F: Fn(&Self::Value, &Self::Value) -> std::cmp::Ordering>(
        &self,
        cmp: F,
        k: usize,
    ) -> NodeState<'graph, Self::Value, Self::BaseGraph, Self::Graph> {
        self.top_k_by(|v1, v2| cmp(v1, v2).reverse(), k)
    }

    fn min_by<F: Fn(&Self::Value, &Self::Value) -> std::cmp::Ordering>(
        &self,
        cmp: F,
    ) -> Option<(NodeView<Self::BaseGraph, Self::Graph>, Self::Value)>;

    fn max_by<F: Fn(&Self::Value, &Self::Value) -> std::cmp::Ordering>(
        &self,
        cmp: F,
    ) -> Option<(NodeView<Self::BaseGraph, Self::Graph>, Self::Value)> {
        self.min_by(|v1, v2| cmp(v1, v2).reverse())
    }

    fn median_by<F: Fn(&Self::Value, &Self::Value) -> std::cmp::Ordering + Sync>(
        &self,
        cmp: F,
    ) -> Option<(NodeView<Self::BaseGraph, Self::Graph>, Self::Value)> {
        let sorted = self.sort_by_values(cmp);
        let len = sorted.len();
        if len == 0 {
            return None;
        }
        let median_index = len / 2;

        Some((items[median_index].0.clone(), items[median_index].1.clone()))
    }
}
