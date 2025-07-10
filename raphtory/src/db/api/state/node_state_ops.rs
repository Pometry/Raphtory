use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::{
        api::state::{group_by::NodeGroups, node_state::NodeState, node_state_ord_ops, Index},
        graph::{node::NodeView, nodes::Nodes},
    },
    prelude::{GraphViewOps, NodeViewOps},
};
use indexmap::IndexSet;
use num_traits::AsPrimitive;
use rayon::prelude::*;
use std::{borrow::Borrow, fmt::Debug, hash::Hash, iter::Sum};

pub trait NodeStateOps<'graph>:
    IntoIterator<
        Item = (
            NodeView<'graph, Self::BaseGraph>,
            Self::OwnedValue,
        ),
    > + Send
    + Sync
    + 'graph
{
    type BaseGraph: GraphViewOps<'graph>;
    type Graph: GraphViewOps<'graph>;
    type Value<'a>: Send + Sync + Borrow<Self::OwnedValue>
    where
        'graph: 'a,
        Self: 'a;

    type OwnedValue: Clone + Send + Sync + 'graph;

    fn graph(&self) -> &Self::Graph;

    fn base_graph(&self) -> &Self::BaseGraph;

    fn iter_values<'a>(&'a self) -> impl Iterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a;
    fn par_iter_values<'a>(&'a self) -> impl ParallelIterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a;

    fn into_iter_values(self) -> impl Iterator<Item = Self::OwnedValue> + Send + Sync + 'graph;

    fn into_par_iter_values(self) -> impl ParallelIterator<Item = Self::OwnedValue> + 'graph;

    fn iter<'a>(
        &'a self,
    ) -> impl Iterator<
        Item = (
            NodeView<'a, &'a Self::BaseGraph>,
            Self::Value<'a>,
        ),
    > + 'a
    where
        'graph: 'a;

    fn nodes(&self) -> Nodes<'graph, Self::BaseGraph, Self::Graph>;

    fn par_iter<'a>(
        &'a self,
    ) -> impl ParallelIterator<
        Item = (
            NodeView<'a, &'a Self::BaseGraph>,
            Self::Value<'a>,
        ),
    >
    where
        'graph: 'a;

    fn get_by_index(
        &self,
        index: usize,
    ) -> Option<(NodeView<&Self::BaseGraph>, Self::Value<'_>)>;

    fn get_by_node<N: AsNodeRef>(&self, node: N) -> Option<Self::Value<'_>>;

    fn len(&self) -> usize;

    fn sort_by<
        F: Fn(
                (NodeView<&Self::BaseGraph>, &Self::OwnedValue),
                (NodeView<&Self::BaseGraph>, &Self::OwnedValue),
            ) -> std::cmp::Ordering
            + Sync,
    >(
        &self,
        cmp: F,
    ) -> NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph> {
        let mut state: Vec<_> = self
            .par_iter()
            .map(|(n, v)| (n.node, v.borrow().clone()))
            .collect();
        let base_graph = self.base_graph();
        state.par_sort_by(|(n1, v1), (n2, v2)| {
            cmp(
                (NodeView::new_one_hop_filtered(base_graph, *n1), v1),
                (NodeView::new_one_hop_filtered(base_graph, *n2), v2),
            )
        });

        let (keys, values): (IndexSet<_, ahash::RandomState>, Vec<_>) =
            state.into_par_iter().unzip();

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
    fn sort_by_values_by<
        F: Fn(&Self::OwnedValue, &Self::OwnedValue) -> std::cmp::Ordering + Sync,
    >(
        &self,
        cmp: F,
    ) -> NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph> {
        self.sort_by(|(_, v1), (_, v2)| cmp(v1, v2))
    }

    /// Sort the results by global node id
    fn sort_by_id(&self) -> NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph> {
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
    fn top_k_by<F: Fn(&Self::OwnedValue, &Self::OwnedValue) -> std::cmp::Ordering + Sync>(
        &self,
        cmp: F,
        k: usize,
    ) -> NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph> {
        let values = node_state_ord_ops::par_top_k(
            self.par_iter(),
            |(_, v1), (_, v2)| cmp(v1.borrow(), v2.borrow()),
            k,
        );
        let (keys, values): (IndexSet<_, ahash::RandomState>, Vec<_>) = values
            .into_iter()
            .map(|(n, v)| (n.node, v.borrow().clone()))
            .unzip();

        NodeState::new(
            self.base_graph().clone(),
            self.graph().clone(),
            values.into(),
            Some(Index::new(keys)),
        )
    }

    fn bottom_k_by<F: Fn(&Self::OwnedValue, &Self::OwnedValue) -> std::cmp::Ordering + Sync>(
        &self,
        cmp: F,
        k: usize,
    ) -> NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph> {
        self.top_k_by(|v1, v2| cmp(v1, v2).reverse(), k)
    }

    fn min_item_by<F: Fn(&Self::OwnedValue, &Self::OwnedValue) -> std::cmp::Ordering + Sync>(
        &self,
        cmp: F,
    ) -> Option<(NodeView<&Self::BaseGraph>, Self::Value<'_>)> {
        self.par_iter()
            .min_by(|(_, v1), (_, v2)| cmp(v1.borrow(), v2.borrow()))
    }

    fn max_item_by<F: Fn(&Self::OwnedValue, &Self::OwnedValue) -> std::cmp::Ordering + Sync>(
        &self,
        cmp: F,
    ) -> Option<(NodeView<&Self::BaseGraph>, Self::Value<'_>)> {
        self.par_iter()
            .max_by(|(_, v1), (_, v2)| cmp(v1.borrow(), v2.borrow()))
    }

    fn median_item_by<F: Fn(&Self::OwnedValue, &Self::OwnedValue) -> std::cmp::Ordering + Sync>(
        &self,
        cmp: F,
    ) -> Option<(NodeView<&Self::BaseGraph>, Self::Value<'_>)> {
        let mut values: Vec<_> = self.par_iter().collect();
        let len = values.len();
        if len == 0 {
            return None;
        }
        values.par_sort_by(|(_, v1), (_, v2)| cmp(v1.borrow(), v2.borrow()));
        let median_index = len / 2;
        values.into_iter().nth(median_index)
    }

    fn group_by<
        V: Hash + Eq + Send + Sync + Clone + Debug,
        F: Fn(&Self::OwnedValue) -> V + Sync,
    >(
        &self,
        group_fn: F,
    ) -> NodeGroups<V, Self::Graph> {
        NodeGroups::new(
            self.par_iter()
                .map(|(node, v)| (node.node, group_fn(v.borrow()))),
            self.graph().clone(),
        )
    }

    fn sum<'a, S>(&'a self) -> S
    where
        'graph: 'a,
        S: Send + Sum<Self::Value<'a>> + Sum<S>,
    {
        self.par_iter_values().sum()
    }

    fn mean<'a>(&'a self) -> f64
    where
        'graph: 'a,
        Self::OwnedValue: Sum<Self::Value<'a>> + Sum + AsPrimitive<f64>,
    {
        let sum: f64 = self.sum::<Self::OwnedValue>().as_();
        sum / (self.len() as f64)
    }
}
