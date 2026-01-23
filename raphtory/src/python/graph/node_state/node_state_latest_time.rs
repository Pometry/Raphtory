use crate::{
    db::{
        api::{
            state::{ops, ops::DynNodeFilter, LazyNodeState, NodeGroups, NodeOp, NodeState},
            view::DynamicGraph,
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    impl_lazy_node_state, impl_lazy_node_state_ord, impl_node_state_group_by_ops,
    impl_node_state_ops, impl_node_state_ord_ops,
    python::{
        types::{repr::Repr, wrappers::iterators::PyBorrowingIterator},
        utils::PyNodeRef,
    },
};
use chrono::{DateTime, Utc};
use pyo3::{
    exceptions::{PyKeyError, PyTypeError},
    prelude::*,
    types::{PyDict, PyNotImplemented},
    IntoPyObjectExt,
};
use raphtory_api::core::storage::timeindex::{EventTime, TimeError};
use raphtory_core::entities::nodes::node_ref::{AsNodeRef, NodeRef};
use rayon::prelude::*;
use std::{cmp::Ordering, collections::HashMap};

use crate::db::graph::nodes::IntoDynNodes;
pub(crate) use crate::{
    db::api::{
        state::{ops::IntoDynNodeOp, NodeStateGroupBy, NodeStateOps, OrderedNodeStateOps},
        view::GraphViewOps,
    },
    prelude::*,
    py_borrowing_iter,
    python::graph::node_state::node_state::ops::NodeFilterOp,
};

impl_lazy_node_state_ord!(
    LatestTimeView<ops::LatestTime<DynamicGraph>>,
    "NodeStateOptionI64",
    "Optional[int]"
);
impl_node_state_group_by_ops!(LatestTimeView, Option<EventTime>);
// Custom time functions for LazyNodeState<LatestTime>
#[pymethods]
impl LatestTimeView {
    /// Access latest times as timestamps (milliseconds since the Unix epoch).
    ///
    /// Returns:
    ///     LatestTimestampView: A lazy view over the latest times for each node as timestamps.
    #[getter]
    fn t(
        &self,
    ) -> LazyNodeState<
        'static,
        LatestTimestamp<DynamicGraph>,
        DynamicGraph,
        DynamicGraph,
        DynNodeFilter,
    > {
        self.inner.t()
    }

    /// Access latest times as UTC DateTimes.
    ///
    /// Returns:
    ///     LatestDateTimeView: A lazy view over the latest times for each node as datetimes.
    #[getter]
    fn dt(
        &self,
    ) -> LazyNodeState<
        'static,
        ops::Map<ops::LatestTime<DynamicGraph>, Result<Option<DateTime<Utc>>, TimeError>>,
        DynamicGraph,
        DynamicGraph,
        DynNodeFilter,
    > {
        self.inner.dt()
    }

    /// Access the event ids of the latest times.
    ///
    /// Returns:
    ///     LatestEventIdView: A lazy view over the event ids of the latest times for each node.
    #[getter]
    fn event_id(
        &self,
    ) -> LazyNodeState<
        'static,
        LatestEventId<DynamicGraph>,
        DynamicGraph,
        DynamicGraph,
        DynNodeFilter,
    > {
        self.inner.event_id()
    }
}
type LatestTimestamp<G> = ops::Map<ops::LatestTime<G>, Option<i64>>;
impl_lazy_node_state_ord!(
    LatestTimestampView<LatestTimestamp<DynamicGraph>>,
    "NodeStateOptionI64",
    "Optional[int]"
);
impl_node_state_group_by_ops!(LatestTimestampView, Option<i64>);
impl_lazy_node_state_ord!(
    LatestEventIdView<LatestEventId<DynamicGraph>>,
    "NodeStateOptionUsize",
    "Optional[int]"
); // usize gets converted to int in python
impl_node_state_group_by_ops!(LatestEventIdView, Option<usize>);

type LatestEventId<G> = ops::Map<ops::LatestTime<G>, Option<usize>>;

type LatestDateTime<G> = ops::Map<ops::LatestTime<G>, Result<Option<DateTime<Utc>>, TimeError>>;

/// Type: Result<Option<DateTime\<Utc>>, TimeError>
type LatestDateTimeOutput = <LatestDateTime<DynamicGraph> as NodeOp>::Output;

/// A lazy view over EarliestDateTime values for each node.
#[pyclass(module = "raphtory.node_state", frozen)]
pub struct LatestDateTimeView {
    inner: LazyNodeState<
        'static,
        LatestDateTime<DynamicGraph>,
        DynamicGraph,
        DynamicGraph,
        DynNodeFilter,
    >,
}

impl LatestDateTimeView {
    pub fn inner(
        &self,
    ) -> &LazyNodeState<
        'static,
        LatestDateTime<DynamicGraph>,
        DynamicGraph,
        DynamicGraph,
        DynNodeFilter,
    > {
        &self.inner
    }

    pub fn iter(&self) -> impl Iterator<Item = LatestDateTimeOutput> + '_ {
        self.inner.iter_values()
    }
}

#[pymethods]
impl LatestDateTimeView {
    /// Compute all DateTime values and return the result as a NodeState. Fails if any DateTime error is encountered.
    ///
    /// Returns:
    ///     NodeStateOptionDateTime: the computed `NodeState`
    fn compute(
        &self,
    ) -> Result<NodeState<'static, Option<DateTime<Utc>>, DynamicGraph>, TimeError> {
        self.inner.compute_result_type()
    }

    /// Compute all DateTime values and only return the valid results as a NodeState. DateTime errors are ignored.
    ///
    /// Returns:
    ///     NodeStateOptionDateTime: the computed `NodeState`
    fn compute_valid(&self) -> NodeState<'static, Option<DateTime<Utc>>, DynamicGraph> {
        self.inner.compute_valid_results()
    }

    /// Compute all DateTime values and return the result as a list
    ///
    /// Returns:
    ///     list[Optional[datetime]]: all values as a list
    fn collect(&self) -> PyResult<Vec<Option<DateTime<Utc>>>> {
        self.inner
            .iter_values()
            .map(|v| v.map_err(PyErr::from))
            .collect::<PyResult<Vec<_>>>()
    }

    /// Compute all DateTime values and return the valid results as a list. Conversion errors and empty values are ignored
    ///
    /// Returns:
    ///     list[datetime]: all values as a list
    fn collect_valid(&self) -> Vec<DateTime<Utc>> {
        self.inner
            .iter_values()
            .filter_map(|r| r.ok().flatten())
            .collect::<Vec<_>>()
    }

    /// Get the number of DateTimes held by this LazyNodeState.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Iterate over nodes
    ///
    /// Returns:
    ///     Nodes: The nodes
    fn nodes(&self) -> Nodes<'static, DynamicGraph, DynamicGraph, DynNodeFilter> {
        self.inner.nodes().into_dyn()
    }

    fn __eq__<'py>(
        &self,
        other: &Bound<'py, PyAny>,
        py: Python<'py>,
    ) -> Result<Bound<'py, PyAny>, std::convert::Infallible> {
        let res = if let Ok(other) = other.downcast::<Self>() {
            let other = Bound::get(other);
            self.inner == other.inner
        } else if let Ok(other) = other.extract::<Vec<Option<DateTime<Utc>>>>() {
            self.inner
                .iter_values()
                .eq(other.into_iter().map(|o| Ok(o)))
        } else if let Ok(other) = other.extract::<HashMap<PyNodeRef, Option<DateTime<Utc>>>>() {
            self.inner.len() == other.len()
                && other
                    .into_iter()
                    .all(|(node, value)| self.inner.get_by_node(node) == Some(Ok(value)))
        } else if let Ok(other) = other.downcast::<PyDict>() {
            self.inner.len() == other.len()
                && other.items().iter().all(|item| {
                    if let Ok((node_ref, value)) = item.extract::<(PyNodeRef, Bound<'py, PyAny>)>()
                    {
                        self.inner
                            .get_by_node(node_ref)
                            .map(|l_value| {
                                match l_value {
                                    Ok(inner_value) => {
                                        if let Ok(l_value_py) = inner_value.into_bound_py_any(py) {
                                            l_value_py.eq(value).unwrap_or(false)
                                        } else {
                                            false
                                        }
                                    }
                                    Err(_) => false, // error can't be equal to non-error
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
        py_borrowing_iter_result!(
            self.inner.clone(),
            LazyNodeState<
                'static,
                LatestDateTime<DynamicGraph>,
                DynamicGraph,
                DynamicGraph,
                DynNodeFilter,
            >,
            |inner| inner.iter_values()
        )
    }

    /// Returns an iterator over all valid DateTime values. Conversion errors and empty values are ignored
    ///
    /// Returns:
    ///     Iterator[datetime]: Valid DateTime values.
    fn iter_valid(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.inner.clone(),
            LazyNodeState<
                'static,
                LatestDateTime<DynamicGraph>,
                DynamicGraph,
                DynamicGraph,
                DynNodeFilter,
            >,
            |inner| inner.iter_values().filter_map(|r| r.ok().flatten())
        )
    }

    /// Get value for node
    ///
    /// Arguments:
    ///     node (NodeInput): the node
    ///     default (Optional[datetime]): the default value. Defaults to None.
    ///
    /// Returns:
    ///     Optional[datetime]: the value for the node or the default value
    #[pyo3(signature = (node, default=None::<DateTime<Utc>>))]
    fn get(
        &self,
        node: PyNodeRef,
        default: Option<DateTime<Utc>>,
    ) -> PyResult<Option<DateTime<Utc>>> {
        match self.inner.get_by_node(node) {
            Some(v) => v.map_err(PyErr::from),
            None => Ok(default),
        }
    }

    fn __getitem__(&self, node: PyNodeRef) -> PyResult<Option<DateTime<Utc>>> {
        let node = node.as_node_ref();
        match self.inner.get_by_node(node) {
            Some(v) => v.map_err(PyErr::from),
            None => match node {
                NodeRef::External(id) => Err(PyKeyError::new_err(format!(
                    "Missing value for node with id {id}"
                ))),
                NodeRef::Internal(vid) => {
                    let node = self.inner.graph().node(vid);
                    match node {
                        Some(node) => Err(PyKeyError::new_err(format!(
                            "Missing value {}",
                            node.repr()
                        ))),
                        None => Err(PyTypeError::new_err("Invalid node reference")),
                    }
                }
            },
        }
    }

    /// Iterate over items
    ///
    /// Returns:
    ///     Iterator[Tuple[Node, Optional[datetime]]]: Iterator over items
    fn items(&self) -> PyBorrowingIterator {
        py_borrowing_iter_tuple_result!(
            self.inner.clone(),
            LazyNodeState<
                'static,
                LatestDateTime<DynamicGraph>,
                DynamicGraph,
                DynamicGraph,
                DynNodeFilter,
            >,
            |inner| inner.iter().map(|(n, v)| (n.cloned(), v))
        )
    }

    /// Iterate over valid DateTime items only. Ignore error and None values.
    ///
    /// Returns:
    ///     Iterator[Tuple[Node, datetime]]: Iterator over items
    fn items_valid(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.inner.clone(),
            LazyNodeState<
                'static,
                LatestDateTime<DynamicGraph>,
                DynamicGraph,
                DynamicGraph,
                DynNodeFilter,
            >,
            |inner| inner
                .iter()
                .filter(|(_, v)| v.as_ref().is_ok_and(|opt| opt.is_some()))
                .map(|(n, v)| (n.cloned(), v.unwrap().unwrap()))
        )
    }

    /// Iterate over DateTime values
    ///
    /// Returns:
    ///     Iterator[Optional[datetime]]: Iterator over values
    fn values(&self) -> PyBorrowingIterator {
        self.__iter__()
    }

    /// Iterate over valid DateTime values only. Ignore error and None values.
    ///
    /// Returns:
    ///     Iterator[datetime]: Iterator over values
    fn values_valid(&self) -> PyBorrowingIterator {
        self.iter_valid()
    }

    /// Sort results by node id. Fails if any DateTime error is encountered.
    ///
    /// Returns:
    ///     NodeStateOptionDateTime: The sorted node state
    fn sorted_by_id(
        &self,
    ) -> Result<NodeState<'static, Option<DateTime<Utc>>, DynamicGraph>, TimeError> {
        self.compute().map(|ns| ns.sort_by_id())
    }

    /// Sort only non-error DateTimes by node id. DateTime errors are ignored.
    ///
    /// Returns:
    ///     NodeStateOptionDateTime: The sorted node state
    fn sorted_by_id_valid(&self) -> NodeState<'static, Option<DateTime<Utc>>, DynamicGraph> {
        self.compute_valid().sort_by_id()
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

#[pymethods]
impl LatestDateTimeView {
    /// Sort by value. Note that 'None' values will always come after valid DateTime values
    ///
    /// Arguments:
    ///     reverse (bool): If `True`, sort in descending order, otherwise ascending. Defaults to False.
    ///
    /// Returns:
    ///     NodeStateOptionDateTime: Sorted node state
    #[pyo3(signature = (reverse = false))]
    fn sorted(
        &self,
        reverse: bool,
    ) -> PyResult<NodeState<'static, Option<DateTime<Utc>>, DynamicGraph>> {
        if let Some(err) = self.inner.iter_values().find_map(|r| r.err()) {
            return Err(PyErr::from(err));
        }
        // make the Result be on the outside, not inside the NodeState
        let op = self.inner.op.clone().map(|r| r.unwrap());
        let lazy_node_state = LazyNodeState::new(op, self.inner.nodes());
        Ok(if reverse {
            lazy_node_state.sort_by_values_by(|a, b| a.cmp(b).reverse())
        } else {
            lazy_node_state.sort_by_values_by(|a, b| match (a, b) {
                (Some(a), Some(b)) => a.cmp(b),
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (None, None) => Ordering::Equal,
            })
        })
    }

    /// Compute the k largest values
    ///
    /// Arguments:
    ///     k (int): The number of values to return
    ///
    /// Returns:
    ///     NodeStateOptionDateTime: The k largest values as a node state
    fn top_k(
        &self,
        k: usize,
    ) -> Result<NodeState<'static, Option<DateTime<Utc>>, DynamicGraph>, TimeError> {
        self.compute().map(|ns| {
            ns.top_k_by(
                |a, b| a.cmp(b), // None values are always Less than Some(_)
                k,
            )
        })
    }

    /// Compute the k smallest values
    ///
    /// Arguments:
    ///     k (int): The number of values to return
    ///
    /// Returns:
    ///     NodeStateOptionDateTime: The k smallest values as a node state
    fn bottom_k(
        &self,
        k: usize,
    ) -> Result<NodeState<'static, Option<DateTime<Utc>>, DynamicGraph>, TimeError> {
        self.compute().map(|ns| {
            ns.bottom_k_by(
                |a, b| match (a, b) {
                    (Some(a), Some(b)) => a.cmp(b),
                    (None, Some(_)) => Ordering::Greater,
                    (Some(_), None) => Ordering::Less,
                    (None, None) => Ordering::Equal,
                },
                k,
            )
        })
    }

    /// Return smallest value and corresponding node
    ///
    /// Returns:
    ///     Optional[Tuple[Node, datetime]]: The Node and minimum value or `None` if empty
    fn min_item(&self) -> PyResult<Option<(NodeView<'static, DynamicGraph>, DateTime<Utc>)>> {
        let min = self.inner.min_item_by(|a, b| match (a, b) {
            (Ok(a), Ok(b)) => a.cmp(b),
            (Err(_), Ok(_)) => Ordering::Greater,
            (Ok(_), Err(_)) => Ordering::Less,
            _ => Ordering::Equal,
        });
        // both the min_item_by and Result outputs can be None, they both return None to python
        match min {
            Some((n, Ok(Some(o)))) => Ok(Some((n.cloned(), o.clone()))),
            Some((_, Ok(None))) => Ok(None),
            Some((_, Err(e))) => Err(PyErr::from(e.clone())),
            None => Ok(None),
        }
    }

    /// Return the minimum value
    ///
    /// Returns:
    ///     Optional[datetime]: The minimum value or `None` if empty
    fn min(&self) -> PyResult<Option<DateTime<Utc>>> {
        self.min_item().map(|v| v.map(|(_, date)| date))
    }

    /// Return largest value and corresponding node
    ///
    /// Returns:
    ///     Optional[Tuple[Node, datetime]]: The Node and maximum value or `None` if empty
    fn max_item(&self) -> PyResult<Option<(NodeView<'static, DynamicGraph>, DateTime<Utc>)>> {
        let max = self.inner.max_item_by(|a, b| match (a, b) {
            (Ok(a), Ok(b)) => a.cmp(b),
            (Err(_), Ok(_)) => Ordering::Less,
            (Ok(_), Err(_)) => Ordering::Greater,
            _ => Ordering::Equal,
        });
        // both the max_item_by and Result outputs can be None, they both return None to python
        match max {
            Some((n, Ok(Some(o)))) => Ok(Some((n.cloned(), o.clone()))),
            Some((_, Ok(None))) => Ok(None),
            Some((_, Err(e))) => Err(PyErr::from(e.clone())),
            None => Ok(None),
        }
    }

    /// Return the maximum value
    ///
    /// Returns:
    ///     Optional[datetime]: The maximum value or `None` if empty
    fn max(&self) -> PyResult<Option<DateTime<Utc>>> {
        self.max_item().map(|v| v.map(|(_, date)| date))
    }

    /// Return the median value
    ///
    /// Returns:
    ///     Optional[datetime]: The median value or `None` if empty
    fn median(&self) -> Option<DateTime<Utc>> {
        self.median_item().map(|(_, v)| v)
    }

    /// Return median value and corresponding node
    ///
    /// Returns:
    ///     Optional[Tuple[Node, datetime]]: The median value or `None` if empty
    fn median_item(&self) -> Option<(NodeView<'static, DynamicGraph>, DateTime<Utc>)> {
        // median_item_by but we have to exclude error and none values
        let mut values: Vec<_> = self
            .inner
            .par_iter()
            .filter_map(|(n, result)| match result {
                Ok(Some(o)) => Some((n.cloned(), o.clone())),
                _ => None,
            })
            .collect();
        let len = values.len();
        if len == 0 {
            return None;
        }
        values.par_sort_by(|(_, v1), (_, v2)| v1.cmp(v2));
        let median_index = len / 2;
        values.into_iter().nth(median_index).map(|(n, o)| (n, o)) // nodeview and datetime have already been cloned
    }

    /// Group by value
    ///
    /// Returns:
    ///     NodeGroups: The grouped nodes
    fn groups(&self) -> PyResult<NodeGroups<Option<DateTime<Utc>>, DynamicGraph>> {
        if let Some(err) = self.inner.iter_values().find_map(|r| r.err()) {
            return Err(PyErr::from(err));
        }
        Ok(self.inner.group_by(|result| match result {
            Ok(Some(dt)) => Some(*dt),
            _ => None, // can't be an error because we already checked for those
        }))
    }
}

impl
    From<
        LazyNodeState<
            'static,
            LatestDateTime<DynamicGraph>,
            DynamicGraph,
            DynamicGraph,
            DynNodeFilter,
        >,
    > for LatestDateTimeView
{
    fn from(
        inner: LazyNodeState<
            'static,
            LatestDateTime<DynamicGraph>,
            DynamicGraph,
            DynamicGraph,
            DynNodeFilter,
        >,
    ) -> Self {
        LatestDateTimeView { inner }
    }
}

impl<'py> pyo3::IntoPyObject<'py>
    for LazyNodeState<
        'static,
        LatestDateTime<DynamicGraph>,
        DynamicGraph,
        DynamicGraph,
        DynNodeFilter,
    >
{
    type Target = LatestDateTimeView;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as pyo3::IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        LatestDateTimeView::from(self).into_pyobject(py)
    }
}

impl<'py> FromPyObject<'py>
    for LazyNodeState<
        'static,
        LatestDateTime<DynamicGraph>,
        DynamicGraph,
        DynamicGraph,
        DynNodeFilter,
    >
{
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.downcast::<LatestDateTimeView>()?.get().inner().clone())
    }
}
