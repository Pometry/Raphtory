use crate::{
    core::entities::nodes::node_ref::{AsNodeRef, NodeRef},
    db::{
        api::{
            state::{
                ops,
                ops::{DynNodeFilter, HistoryOp},
                LazyNodeState, NodeOp, NodeState,
            },
            view::{DynamicGraph, GraphViewOps},
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    impl_lazy_node_state, impl_lazy_node_state_ord, impl_node_state, impl_node_state_ops,
    impl_node_state_ord_ops,
    python::{
        graph::history::PyIntervals,
        types::{repr::Repr, wrappers::iterators::PyBorrowingIterator},
        utils::PyNodeRef,
    },
};
use pyo3::{
    exceptions::{PyKeyError, PyTypeError},
    prelude::*,
    types::{PyDict, PyNotImplemented},
    IntoPyObjectExt,
};
use std::collections::HashMap;

use crate::db::graph::nodes::IntoDynNodes;
pub(crate) use crate::{
    db::api::state::{ops::IntoDynNodeOp, NodeStateOps, OrderedNodeStateOps},
    prelude::*,
    py_borrowing_iter,
    python::graph::node_state::node_state::ops::NodeFilterOp,
};

type HistoryIntervals<G> = ops::Map<HistoryOp<'static, G>, PyIntervals>;
impl_lazy_node_state!(
    IntervalsView<HistoryIntervals<DynamicGraph>>,
    "NodeStateIntervals",
    "Intervals"
);
impl_node_state!(
    NodeStateIntervals<PyIntervals>,
    "NodeStateIntervals",
    "Intervals"
);

// Custom functions for LazyNodeState<Intervals>
#[pymethods]
impl IntervalsView {
    /// Calculate the mean interval in milliseconds for each node.
    ///
    /// Returns:
    ///     IntervalsFloatView: A lazy view over the mean interval between consecutive timestamps for each node. The mean is None if there is fewer than 1 interval.
    pub fn mean(
        &self,
    ) -> LazyNodeState<
        'static,
        ops::Map<HistoryIntervals<DynamicGraph>, Option<f64>>,
        DynamicGraph,
        DynamicGraph,
        DynNodeFilter,
    > {
        let op = self.inner.op.clone().map(|v| v.mean());
        LazyNodeState::new(op, self.inner.nodes())
    }

    /// Calculate the median interval in milliseconds for each node.
    ///
    /// Returns:
    ///     IntervalsIntegerView: A lazy view over the median interval between consecutive timestamps for each node. The median is None if there is fewer than 1 interval.
    pub fn median(
        &self,
    ) -> LazyNodeState<
        'static,
        ops::Map<HistoryIntervals<DynamicGraph>, Option<i64>>,
        DynamicGraph,
        DynamicGraph,
        DynNodeFilter,
    > {
        let op = self.inner.op.clone().map(|v| v.median());
        LazyNodeState::new(op, self.inner.nodes())
    }

    /// Calculate the maximum interval in milliseconds for each node.
    ///
    /// Returns:
    ///     IntervalsIntegerView: A lazy view over the maximum interval between consecutive timestamps for each node. The maximum is None if there is fewer than 1 interval.
    pub fn max(
        &self,
    ) -> LazyNodeState<
        'static,
        ops::Map<HistoryIntervals<DynamicGraph>, Option<i64>>,
        DynamicGraph,
        DynamicGraph,
        DynNodeFilter,
    > {
        let op = self.inner.op.clone().map(|v| v.max());
        LazyNodeState::new(op, self.inner.nodes())
    }

    /// Calculate the minimum interval in milliseconds for each node.
    ///
    /// Returns:
    ///     IntervalsIntegerView: A lazy view over the minimum interval between consecutive timestamps for each node. The minimum is None if there is fewer than 1 interval.
    pub fn min(
        &self,
    ) -> LazyNodeState<
        'static,
        ops::Map<HistoryIntervals<DynamicGraph>, Option<i64>>,
        DynamicGraph,
        DynamicGraph,
        DynNodeFilter,
    > {
        let op = self.inner.op.clone().map(|v| v.min());
        LazyNodeState::new(op, self.inner.nodes())
    }
}

// types needed for LazyNodeStates generated above.
type IntervalsFloat<G> = ops::Map<HistoryIntervals<G>, Option<f64>>;
impl_lazy_node_state_ord!(
    IntervalsFloatView<IntervalsFloat<DynamicGraph>>,
    "NodeStateOptionF64",
    "Optional[float]"
);

type IntervalsI64<G> = ops::Map<HistoryIntervals<G>, Option<i64>>;
impl_lazy_node_state_ord!(
    IntervalsIntegerView<IntervalsI64<DynamicGraph>>,
    "NodeStateOptionI64",
    "Optional[int]"
);

// Custom functions for computed NodeState<Intervals>
#[pymethods]
impl NodeStateIntervals {
    /// Calculate the mean interval in milliseconds for each node.
    ///
    /// Returns:
    ///     NodeStateOptionF64: A NodeState with the computed mean interval between consecutive timestamps for each node. The mean is None if there is fewer than 1 interval.
    pub fn mean(&self) -> NodeState<'static, Option<f64>, DynamicGraph> {
        let values = self
            .inner
            .iter_values()
            .map(|v| v.mean())
            .collect::<Vec<Option<f64>>>()
            .into();
        NodeState::new(self.inner.graph().clone(), values, self.inner.ids().clone())
    }

    /// Calculate the median interval in milliseconds for each node.
    ///
    /// Returns:
    ///     NodeStateOptionI64: A NodeState with the computed median interval between consecutive timestamps for each node. The median is None if there is fewer than 1 interval.
    pub fn median(&self) -> NodeState<'static, Option<i64>, DynamicGraph> {
        let values = self
            .inner
            .iter_values()
            .map(|v| v.median())
            .collect::<Vec<Option<i64>>>()
            .into();
        NodeState::new(self.inner.graph().clone(), values, self.inner.ids().clone())
    }

    /// Calculate the maximum interval in milliseconds for each node.
    ///
    /// Returns:
    ///     NodeStateOptionI64: A NodeState with the computed maximum interval between consecutive timestamps for each node. The maximum is None if there is fewer than 1 interval.
    pub fn max(&self) -> NodeState<'static, Option<i64>, DynamicGraph> {
        let values = self
            .inner
            .iter_values()
            .map(|v| v.max())
            .collect::<Vec<Option<i64>>>()
            .into();
        NodeState::new(self.inner.graph().clone(), values, self.inner.ids().clone())
    }

    /// Calculate the minimum interval in milliseconds for each node.
    ///
    /// Returns:
    ///     NodeStateOptionI64: A NodeState with the computed minimum interval between consecutive timestamps for each node. The minimum is None if there is fewer than 1 interval.
    pub fn min(&self) -> NodeState<'static, Option<i64>, DynamicGraph> {
        let values = self
            .inner
            .iter_values()
            .map(|v| v.min())
            .collect::<Vec<Option<i64>>>()
            .into();
        NodeState::new(self.inner.graph().clone(), values, self.inner.ids().clone())
    }

    /// Collect all intervals in milliseconds into a list for each node.
    ///
    /// Returns:
    ///     list[list[int]]: List of intervals in milliseconds for each node.
    pub fn to_list(&self) -> Vec<Vec<i64>> {
        self.inner
            .iter_values()
            .map(|v| v.to_list())
            .collect::<Vec<Vec<i64>>>()
    }
}
