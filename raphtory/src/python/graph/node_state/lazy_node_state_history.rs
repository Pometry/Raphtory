use crate::{
    db::{
        api::{
            state::{
                ops,
                ops::{node::NodeOp, HistoryOp},
                LazyNodeState, NodeState,
            },
            view::{
                history::{History, InternalHistoryOps},
                internal::Static,
                DynamicGraph, IntoDynHop, IntoDynamic, StaticGraphViewOps,
            },
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    impl_one_hop,
    prelude::{GraphViewOps, LayerOps, NodeStateOps, NodeViewOps, TimeOps},
    python::{
        graph::history::{
            PyHistory, PyHistoryDateTime, PyHistorySecondaryIndex, PyHistoryTimestamp, PyIntervals,
        },
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
use raphtory_api::core::storage::timeindex::TimeIndexEntry;
use raphtory_core::entities::nodes::node_ref::{AsNodeRef, NodeRef};
use std::{collections::HashMap, sync::Arc};

/// A lazy view over History values for each node.
#[pyclass(module = "raphtory.node_state", frozen)]
pub struct HistoryView {
    inner: LazyNodeState<'static, HistoryOp<'static, DynamicGraph>, DynamicGraph, DynamicGraph>,
}

impl HistoryView {
    pub fn inner(
        &self,
    ) -> &LazyNodeState<'static, HistoryOp<'static, DynamicGraph>, DynamicGraph, DynamicGraph> {
        &self.inner
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = History<'static, NodeView<'static, DynamicGraph>>> + '_ {
        self.inner.iter_values()
    }
}

// can't simply call self.inner.t(), dt(), ... because we need LazyNodeState<PyHistoryTimestamp>
// instead of LazyNodeState<HistoryTimestamp<NodeView>> so it matches the node_state macro impls
#[pymethods]
impl HistoryView {
    /// Access history events as timestamps (milliseconds since Unix epoch).
    ///
    /// Returns:
    ///     A lazy view over HistoryTimestamp objects for each node.
    #[getter]
    fn t(
        &self,
    ) -> LazyNodeState<
        'static,
        ops::Map<HistoryOp<'static, DynamicGraph>, PyHistoryTimestamp>,
        DynamicGraph,
        DynamicGraph,
    > {
        let op = self.inner.op.clone().map(|hist| hist.t().into());
        LazyNodeState::new(op, self.inner.nodes())
    }

    /// Access history events as UTC datetimes.
    ///
    /// Returns:
    ///     A lazy view over HistoryDateTime objects for each node.
    #[getter]
    fn dt(
        &self,
    ) -> LazyNodeState<
        'static,
        ops::Map<HistoryOp<'static, DynamicGraph>, PyHistoryDateTime>,
        DynamicGraph,
        DynamicGraph,
    > {
        let op = self.inner.op.clone().map(|hist| hist.dt().into());
        LazyNodeState::new(op, self.inner.nodes())
    }

    /// Access the unique secondary index of each time entry.
    ///
    /// Returns:
    ///     A lazy view over HistorySecondaryIndex objects for each node.
    #[getter]
    fn secondary_index(
        &self,
    ) -> LazyNodeState<
        'static,
        ops::Map<HistoryOp<'static, DynamicGraph>, PyHistorySecondaryIndex>,
        DynamicGraph,
        DynamicGraph,
    > {
        let op = self
            .inner
            .op
            .clone()
            .map(|hist| hist.secondary_index().into());
        LazyNodeState::new(op, self.inner.nodes())
    }

    /// Access the intervals between consecutive timestamps in milliseconds.
    ///
    /// Returns:
    ///     A lazy view over Intervals objects for each node.
    #[getter]
    fn intervals(
        &self,
    ) -> LazyNodeState<
        'static,
        ops::Map<HistoryOp<'static, DynamicGraph>, PyIntervals>,
        DynamicGraph,
        DynamicGraph,
    > {
        let op = self.inner.op.clone().map(|hist| hist.intervals().into());
        LazyNodeState::new(op, self.inner.nodes())
    }

    /// Get the earliest time entry.
    ///
    /// Returns:
    ///     A lazy view over the earliest time of each node as a TimeIndexEntry.
    fn earliest_time(
        &self,
    ) -> LazyNodeState<'static, ops::EarliestTime<DynamicGraph>, DynamicGraph, DynamicGraph> {
        self.inner.earliest_time()
    }

    /// Get the latest time entry.
    /// Returns:
    ///     A lazy view over the latest time of each node as a TimeIndexEntry.
    fn latest_time(
        &self,
    ) -> LazyNodeState<'static, ops::LatestTime<DynamicGraph>, DynamicGraph, DynamicGraph> {
        self.inner.latest_time()
    }

    /// Compute all values and return the result as a node view
    ///
    /// Returns:
    ///     NodeStateHistory: the computed `NodeState`
    fn compute(
        &self,
    ) -> NodeState<
        'static,
        History<'static, NodeView<'static, DynamicGraph>>,
        DynamicGraph,
        DynamicGraph,
    > {
        self.inner.compute()
    }

    /// Compute all History objects and return the result as a list
    ///
    /// Returns:
    ///     list[History]: all History objects as a list
    fn collect(&self) -> Vec<History<'static, NodeView<'static, DynamicGraph>>> {
        self.inner.collect()
    }

    /// Compute all History objects and return the contained time entries as a sorted list
    ///
    /// Returns:
    ///     list[TimeIndexEntry]: all time entries as a list
    fn collect_time_entries(&self) -> Vec<TimeIndexEntry> {
        self.inner.collect_time_entries()
    }

    /// Flattens all history objects into a single history with all time entries ordered.
    ///
    /// Returns:
    ///     History: a history object containing all time entries
    fn flatten(&self) -> PyHistory {
        self.inner.flatten().into_arc_dyn().into()
    }

    /// Get the number of History objects held by this LazyNodeState.
    fn __len__(&self) -> usize {
        NodeStateOps::len(&self.inner)
    }

    /// Iterate over nodes
    ///
    /// Returns:
    ///     Nodes: The nodes
    fn nodes(&self) -> Nodes<'static, DynamicGraph> {
        self.inner.nodes()
    }

    fn __eq__<'py>(
        &self,
        other: &Bound<'py, PyAny>,
        py: Python<'py>,
    ) -> Result<Bound<'py, PyAny>, std::convert::Infallible> {
        let res = if let Ok(other) = other.downcast::<Self>() {
            let other = Bound::get(other);
            self.inner == other.inner
        } else if let Ok(other) =
            other.extract::<Vec<History<'static, Arc<dyn InternalHistoryOps>>>>()
        {
            NodeStateOps::len(&self.inner) == other.len()
                && self
                    .inner
                    .iter_values()
                    .zip(other.into_iter())
                    .all(|(left, right)| left.iter().eq(right.iter()))
        } else if let Ok(other) =
            other.extract::<HashMap<PyNodeRef, History<'static, Arc<dyn InternalHistoryOps>>>>()
        {
            NodeStateOps::len(&self.inner) == other.len()
                && other.into_iter().all(|(node, value)| {
                    self.inner
                        .get_by_node(node)
                        .map(|v| v.iter().eq(value.iter()))
                        .unwrap_or(false)
                })
        } else if let Ok(other) = other.downcast::<PyDict>() {
            NodeStateOps::len(&self.inner) == other.len()
                && other.items().iter().all(|item| {
                    if let Ok((node_ref, value)) = item.extract::<(PyNodeRef, Bound<'py, PyAny>)>()
                    {
                        self.inner
                            .get_by_node(node_ref)
                            .map(|l_value| {
                                if let Ok(l_value_py) = l_value.clone().into_bound_py_any(py) {
                                    l_value_py.eq(value).unwrap_or(false)
                                } else {
                                    false
                                }
                            })
                            .unwrap_or(false)
                    } else {
                        false
                    }
                })
        } else {
            return Ok(PyNotImplemented::get(py).to_owned().into_any());
        };
        Ok(res.into_pyobject(py)?.to_owned().into_any())
    }

    fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.inner.clone(),
            LazyNodeState<'static, HistoryOp<'static, DynamicGraph>, DynamicGraph, DynamicGraph>,
            |inner| inner.iter_values()
        )
    }

    /// Get value for node
    ///
    /// Arguments:
    ///     node (NodeInput): the node
    ///     default (Optional[History]): the default value. Defaults to None.
    ///
    /// Returns:
    ///     Optional[History]: the History object for the node or the default value
    #[pyo3(signature = (node, default=None::<PyHistory>))]
    fn get(&self, node: PyNodeRef, default: Option<PyHistory>) -> Option<PyHistory> {
        self.inner.get_by_node(node).map(|v| v.into()).or(default)
    }

    fn __getitem__(
        &self,
        node: PyNodeRef,
    ) -> PyResult<History<'static, NodeView<'static, DynamicGraph>>> {
        let node = node.as_node_ref();
        self.inner.get_by_node(node).ok_or_else(|| match node {
            NodeRef::External(id) => {
                PyKeyError::new_err(format!("Missing value for node with id {id}"))
            }
            NodeRef::Internal(vid) => {
                let node = self.inner.graph().node(vid);
                match node {
                    Some(node) => PyKeyError::new_err(format!("Missing value {}", node.repr())),
                    None => PyTypeError::new_err("Invalid node reference"),
                }
            }
        })
    }

    /// Iterate over History objects
    ///
    /// Returns:
    ///     Iterator[Tuple[Node, History]]: Iterator over histories
    fn items(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.inner.clone(),
            LazyNodeState<'static, HistoryOp<'static, DynamicGraph>, DynamicGraph, DynamicGraph>,
            |inner| NodeStateOps::iter(inner).map(|(n, v)| (n.cloned(), v))
        )
    }

    /// Iterate over History objects
    ///
    /// Returns:
    ///     Iterator[History]: Iterator over histories
    fn values(&self) -> PyBorrowingIterator {
        self.__iter__()
    }

    /// Sort results by node id
    ///
    /// Returns:
    ///     NodeStateHistory: The sorted node state
    fn sorted_by_id(
        &self,
    ) -> NodeState<'static, History<'static, NodeView<'static, DynamicGraph>>, DynamicGraph> {
        self.inner.sort_by_id()
    }

    fn __repr__(&self) -> String {
        self.inner.repr()
    }

    /// Convert results to pandas DataFrame
    ///
    /// The DataFrame has two columns, "node" with the node ids and "value" with
    /// the corresponding values.
    ///
    /// Returns:
    ///     DataFrame: A Pandas DataFrame.
    fn to_df<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pandas = PyModule::import(py, "pandas")?;
        let columns = PyDict::new(py);
        columns.set_item("node", self.inner.nodes().id())?;
        columns.set_item("value", self.values())?;
        pandas.call_method("DataFrame", (columns,), None)
    }
}

// impl_node_state_ord_ops not here, should History implement Ord ops?
// impl_node_state_group_by_ops not here bc grouping wanted is unclear

impl From<LazyNodeState<'static, HistoryOp<'static, DynamicGraph>, DynamicGraph, DynamicGraph>>
    for HistoryView
{
    fn from(
        inner: LazyNodeState<'static, HistoryOp<'static, DynamicGraph>, DynamicGraph, DynamicGraph>,
    ) -> Self {
        HistoryView { inner }
    }
}

impl<'py> pyo3::IntoPyObject<'py>
    for LazyNodeState<'static, HistoryOp<'static, DynamicGraph>, DynamicGraph, DynamicGraph>
{
    type Target = HistoryView;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as pyo3::IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        HistoryView::from(self).into_pyobject(py)
    }
}

impl<'py> FromPyObject<'py>
    for LazyNodeState<'static, HistoryOp<'static, DynamicGraph>, DynamicGraph, DynamicGraph>
{
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.downcast::<HistoryView>()?.get().inner().clone())
    }
}

type HistoryOpType<G> = HistoryOp<'static, G>;

impl_one_hop!(HistoryView<HistoryOpType>, "HistoryView");
