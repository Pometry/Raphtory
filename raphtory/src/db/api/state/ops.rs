use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::{
        api::state::{node_state::NodeState, ord_ops, Index},
        graph::node::NodeView,
    },
    prelude::GraphViewOps,
};
use rayon::{
    iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator},
    prelude::ParallelSliceMut,
};
use std::{borrow::Borrow, iter::Sum};

pub trait NodeStateOps<'graph>: IntoIterator<Item = Self::OwnedValue> {
    type Graph: GraphViewOps<'graph>;
    type BaseGraph: GraphViewOps<'graph>;
    type Value<'a>: Send + Sync + Borrow<Self::OwnedValue>
    where
        'graph: 'a,
        Self: 'a;

    type OwnedValue: Clone + Send + Sync + 'graph;

    fn graph(&self) -> &Self::Graph;

    fn base_graph(&self) -> &Self::BaseGraph;

    fn values<'a>(&'a self) -> impl Iterator<Item = Self::Value<'a>> + 'a
    where
        'graph: 'a;

    fn collect(&self) -> Vec<Self::OwnedValue> {
        self.par_values().map(|v| v.borrow().clone()).collect()
    }

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

    fn nodes<'a>(
        &'a self,
    ) -> impl Iterator<Item = NodeView<&'a Self::BaseGraph, &'a Self::Graph>> + 'a
    where
        'graph: 'a,
    {
        self.iter().map(|(n, _)| n)
    }

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
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value<'_>)>;

    fn get_by_node<N: AsNodeRef>(&self, node: N) -> Option<Self::Value<'_>>;

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
    fn sort_by_values_by<
        F: Fn(&Self::OwnedValue, &Self::OwnedValue) -> std::cmp::Ordering + Sync,
    >(
        &self,
        cmp: F,
    ) -> NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph> {
        {
            let mut state: Vec<_> = self
                .par_iter()
                .map(|(n, v)| (n.node, v.borrow().clone()))
                .collect();
            state.par_sort_by(|(_, v1), (_, v2)| cmp(v1, v2));

            let mut keys = Vec::with_capacity(state.len());
            let mut values = Vec::with_capacity(state.len());
            state
                .into_par_iter()
                .unzip_into_vecs(&mut keys, &mut values);

            NodeState::new(
                self.base_graph().clone(),
                self.graph().clone(),
                values,
                Some(Index::from(keys)),
            )
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
    fn top_k_by<F: Fn(&Self::OwnedValue, &Self::OwnedValue) -> std::cmp::Ordering + Sync>(
        &self,
        cmp: F,
        k: usize,
    ) -> NodeState<'graph, Self::OwnedValue, Self::BaseGraph, Self::Graph> {
        let values = ord_ops::par_top_k(
            self.par_iter(),
            |(_, v1), (_, v2)| cmp(v1.borrow(), v2.borrow()),
            k,
        );
        let (keys, values): (Vec<_>, Vec<_>) = values
            .into_iter()
            .map(|(n, v)| (n.node, v.borrow().clone()))
            .unzip();

        NodeState::new(
            self.base_graph().clone(),
            self.graph().clone(),
            values,
            Some(Index::from(keys)),
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
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value<'_>)> {
        self.par_iter()
            .min_by(|(_, v1), (_, v2)| cmp(v1.borrow(), v2.borrow()))
    }

    fn max_item_by<F: Fn(&Self::OwnedValue, &Self::OwnedValue) -> std::cmp::Ordering + Sync>(
        &self,
        cmp: F,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value<'_>)> {
        self.par_iter()
            .max_by(|(_, v1), (_, v2)| cmp(v1.borrow(), v2.borrow()))
    }

    fn median_item_by<F: Fn(&Self::OwnedValue, &Self::OwnedValue) -> std::cmp::Ordering + Sync>(
        &self,
        cmp: F,
    ) -> Option<(NodeView<&Self::BaseGraph, &Self::Graph>, Self::Value<'_>)> {
        let mut values: Vec<_> = self.par_iter().collect();
        let len = values.len();
        if len == 0 {
            return None;
        }
        values.par_sort_by(|(_, v1), (_, v2)| cmp(v1.borrow(), v2.borrow()));
        let median_index = len / 2;
        values.into_iter().skip(median_index).next()
    }

    fn sum<'a, S>(&'a self) -> S
    where
        'graph: 'a,
        S: Send + Sum<Self::Value<'a>> + Sum<S>,
    {
        self.par_values().sum()
    }
}
