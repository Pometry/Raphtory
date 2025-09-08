use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::{
        api::{
            state::{group_by::NodeGroups, node_state::NodeState, node_state_ord_ops, Index},
            view::BoxableGraphView,
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    prelude::{GraphViewOps, NodeViewOps},
};
use indexmap::IndexSet;
use num_traits::AsPrimitive;
use rayon::prelude::*;
use std::{borrow::Borrow, fmt::Debug, hash::Hash, iter::Sum};


pub trait ToOwnedValue<T> {
    fn to_owned_value(self) -> T;
}

impl<T> ToOwnedValue<T> for T {
    fn to_owned_value(self) -> T {
        self
    }
}

impl<'a, T: Clone> ToOwnedValue<T> for &'a T {
    fn to_owned_value(self) -> T {
        self.clone()
    }
}

pub trait NodeStateOps<'a, 'graph: 'a>:
    IntoIterator<
        Item = (
            NodeView<'graph, Self::BaseGraph, Self::Graph>,
            Self::OwnedValue,
        ),
    > + Send
    + Sync
    + 'graph
{
    type Graph: GraphViewOps<'graph>;
    type BaseGraph: GraphViewOps<'graph>;
    type Value: Send + Sync + ToOwnedValue<Self::OwnedValue>;

    type OwnedValue: Clone + Send + Sync;

    fn graph(&self) -> &Self::Graph;

    fn base_graph(&self) -> &Self::BaseGraph;

    fn iter_values(&'a self) -> impl Iterator<Item = Self::Value> + 'a;
    fn par_iter_values(&'a self) -> impl ParallelIterator<Item = Self::Value> + 'a;

    fn into_iter_values(self) -> impl Iterator<Item = Self::OwnedValue> + Send + Sync;

    fn into_par_iter_values(self) -> impl ParallelIterator<Item = Self::OwnedValue>;

    fn iter(
        &'a self,
    ) -> impl Iterator<
        Item = (
            NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>,
            Self::Value,
        ),
    > + 'a;

    fn nodes(&self) -> Nodes<'graph, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph;

    fn par_iter(
        &'a self,
    ) -> impl ParallelIterator<Item = (NodeView<'a, &'a Self::BaseGraph, &'a Self::Graph>, Self::Value<'a>)>;

    fn get_by_index(
        &'a self,
        index: usize,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value)>;

    fn get_by_node<N: AsNodeRef>(&'a self, node: N) -> Option<Self::Value>;

    fn len(&self) -> usize;

    fn sort_by<
        F: Fn(
                (NodeView<&Self::BaseGraph, &Self::Graph>, &Self::Value),
                (NodeView<&Self::BaseGraph, &Self::Graph>, &Self::Value),
            ) -> std::cmp::Ordering
            + Sync,
    >(
        &'a self,
        cmp: F,
    ) -> NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph,
    {
        let mut state: Vec<_> = self.par_iter().map(|(n, v)| (n.node, v)).collect();
        let graph = self.graph();
        let base_graph = self.base_graph();
        state.par_sort_by(|(n1, v1), (n2, v2)| {
            cmp(
                (NodeView::new_one_hop_filtered(base_graph, graph, *n1), v1),
                (NodeView::new_one_hop_filtered(base_graph, graph, *n2), v2),
            )
        });

        let (keys, values): (IndexSet<_, ahash::RandomState>, Vec<_>) = state
            .into_par_iter()
            .map(|(n1, v1)| (n1, v1.to_owned_value()))
            .unzip();

        NodeState::new(
            self.base_graph().clone(),
            self.graph().clone(),
            values.into(),
            Some(Index::new(keys)),
        )
    }

    /// Sorts the by its values in ascending or descending order.
    ///
    /// Arguments:
    ///
    /// cmp: Comparison function for values
    ///
    /// Returns:
    ///
    /// A sorted vector of tuples containing node names and values.
    fn sort_by_values_by<F: Fn(&Self::Value, &Self::Value) -> std::cmp::Ordering + Sync>(
        &'a self,
        cmp: F,
    ) -> NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph,
    {
        self.sort_by(|(_, v1), (_, v2)| cmp(v1, v2))
    }

    /// Sort the results by global node id
    fn sort_by_id(&'a self) -> NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph,
    {
        self.sort_by(|(n1, _), (n2, _)| n1.id().cmp(&n2.id()))
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
    fn top_k_by<F: Fn(&Self::Value, &Self::Value) -> std::cmp::Ordering + Sync>(
        &'a self,
        cmp: F,
        k: usize,
    ) -> NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph> {
        let values = node_state_ord_ops::top_k(
            self.iter(),
            |(_, v1), (_, v2)| cmp(v1.borrow(), v2.borrow()),
            k,
        );
        let (keys, values): (IndexSet<_, ahash::RandomState>, Vec<_>) = values
            .into_iter()
            .map(|(n, v)| (n.node, v.to_owned_value()))
            .unzip();

        NodeState::new(
            self.base_graph().clone(),
            self.graph().clone(),
            values.into(),
            Some(Index::new(keys)),
        )
    }

    fn bottom_k_by<F: Fn(&Self::Value, &Self::Value) -> std::cmp::Ordering + Sync>(
        &'a self,
        cmp: F,
        k: usize,
    ) -> NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph>
    where
        Self::BaseGraph: 'graph,
        Self::Graph: 'graph,
    {
        self.top_k_by(|v1, v2| cmp(v1, v2).reverse(), k)
    }

    fn min_item_by<F: Fn(&Self::Value, &Self::Value) -> std::cmp::Ordering + Sync>(
        &'a self,
        cmp: F,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value)> {
        self.par_iter().min_by(|(_, v1), (_, v2)| cmp(v1, v2))
    }

    fn max_item_by<F: Fn(&Self::Value, &Self::Value) -> std::cmp::Ordering + Sync>(
        &'a self,
        cmp: F,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value)> {
        self.par_iter().max_by(|(_, v1), (_, v2)| cmp(v1, v2))
    }

    fn median_item_by<F: Fn(&Self::Value, &Self::Value) -> std::cmp::Ordering + Sync>(
        &'a self,
        cmp: F,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value)> {
        let mut values: Vec<_> = self.par_iter().collect();
        let len = values.len();
        if len == 0 {
            return None;
        }
        values.par_sort_by(|(_, v1), (_, v2)| cmp(v1, v2));
        let median_index = len / 2;
        values.into_iter().nth(median_index)
    }

    fn group_by<V: Hash + Eq + Send + Sync + Clone + Debug, F: Fn(Self::Value) -> V + Sync>(
        &'a self,
        group_fn: F,
    ) -> NodeGroups<V, Self::Graph> {
        NodeGroups::new(
            self.par_iter().map(|(node, v)| (node.node, group_fn(v))),
            self.graph().clone(),
        )
    }

    fn sum<S>(&'a self) -> S
    where
        S: Send + Sum<Self::Value> + Sum<S>,
    {
        self.par_iter_values().sum()
    }

    fn mean(&'a self) -> f64
    where
        Self::OwnedValue: Sum<Self::Value> + Sum + AsPrimitive<f64>,
    {
        let sum: f64 = self.sum::<Self::OwnedValue>().as_();
        sum / (self.len() as f64)
    }
}
