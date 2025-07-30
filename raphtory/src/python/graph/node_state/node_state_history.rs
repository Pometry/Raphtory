use crate::{
    db::{
        api::{
            state::{NodeGroups, NodeState},
            view::{
                history::{History, InternalHistoryOps},
                DynamicGraph,
            },
        },
        graph::{node::NodeView, nodes::Nodes},
    },
    prelude::{GraphViewOps, NodeStateOps, NodeViewOps},
    python::{
        graph::history::PyHistory,
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
use raphtory_api::core::storage::timeindex::TimeError;
use raphtory_core::entities::nodes::node_ref::{AsNodeRef, NodeRef};
use rayon::{iter::ParallelIterator, prelude::ParallelSliceMut};
use std::{cmp::Ordering, collections::HashMap, sync::Arc};
// $name = NodeStateHistory
// $value = History<'static, NodeView<'static, DynamicGraph>>
// $to_owned = |v| v.clone()    // we need to clone because DynamicGraph doesn't implement copy
// $inner_t = NodeState<'static, History<'static, NodeView<'static, DynamicGraph>>, DynamicGraph, DynamicGraph>
// $py_value = Optional[datetime]
// $computed = NodeStateHistory

#[pyclass(module = "raphtory.node_state", frozen)]
pub struct NodeStateHistory {
    inner: NodeState<
        'static,
        History<'static, NodeView<'static, DynamicGraph>>,
        DynamicGraph,
        DynamicGraph,
    >,
}

impl NodeStateHistory {
    pub fn inner(
        &self,
    ) -> &NodeState<
        'static,
        History<'static, NodeView<'static, DynamicGraph>>,
        DynamicGraph,
        DynamicGraph,
    > {
        &self.inner
    }

    // impl_node_state_ops
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = History<'static, NodeView<'static, DynamicGraph>>> + '_ {
        self.inner.iter_values().map(|v| v.clone())
    }
}

// impl_node_state_ops
#[pymethods]
impl NodeStateHistory {
    fn __len__(&self) -> usize {
        self.inner.len()
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
            self.inner.len() == other.len()
                && self
                    .inner
                    .iter_values()
                    .zip(other.into_iter())
                    .all(|(left, right)| left.iter().eq(right.iter()))
        } else if let Ok(other) =
            other.extract::<HashMap<PyNodeRef, History<'static, Arc<dyn InternalHistoryOps>>>>()
        {
            self.inner.len() == other.len()
                && other.into_iter().all(|(node, value)| {
                    self.inner
                        .get_by_node(node)
                        .map(|v| v.iter().eq(value.iter()))
                        .unwrap_or(false)
                })
        } else if let Ok(other) = other.downcast::<PyDict>() {
            self.inner.len() == other.len()
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
            NodeState<
                'static,
                History<'static, NodeView<'static, DynamicGraph>>,
                DynamicGraph,
                DynamicGraph,
            >,
            |inner| inner.iter_values().map(|v| v.clone())
        )
    }

    /// Get value for node
    ///
    /// Arguments:
    ///     node (NodeInput): the node
    #[doc = "    default (Optional[History]): the default value. Defaults to None."]
    ///
    /// Returns:
    #[doc = "    Optional[History]: the value for the node or the default value"]
    #[pyo3(signature = (node, default=None::<PyHistory>))]
    fn get(&self, node: PyNodeRef, default: Option<PyHistory>) -> Option<PyHistory> {
        self.inner
            .get_by_node(node)
            .map(|v| v.clone().into())
            .or(default)
    }

    fn __getitem__(
        &self,
        node: PyNodeRef,
    ) -> PyResult<History<'static, NodeView<'static, DynamicGraph>>> {
        let node = node.as_node_ref();
        self.inner
            .get_by_node(node)
            .map(|v| v.clone())
            .ok_or_else(|| match node {
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

    /// Iterate over items
    ///
    /// Returns:
    #[doc = "     Iterator[Tuple[Node, History]]: Iterator over items"]
    fn items(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.inner.clone(),
            NodeState<
                'static,
                History<'static, NodeView<'static, DynamicGraph>>,
                DynamicGraph,
                DynamicGraph,
            >,
            |inner| inner.iter().map(|(n, v)| (n.cloned(), v.clone()))
        )
    }

    /// Iterate over values
    ///
    /// Returns:
    #[doc = "     Iterator[History]: Iterator over values"]
    fn values(&self) -> PyBorrowingIterator {
        self.__iter__()
    }

    /// Sort results by node id
    ///
    /// Returns:
    #[doc = "     NodeStateHistory: The sorted node state"]
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
    ///     DataFrame: the pandas DataFrame
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

impl
    From<
        NodeState<
            'static,
            History<'static, NodeView<'static, DynamicGraph>>,
            DynamicGraph,
            DynamicGraph,
        >,
    > for NodeStateHistory
{
    fn from(
        inner: NodeState<
            'static,
            History<'static, NodeView<'static, DynamicGraph>>,
            DynamicGraph,
            DynamicGraph,
        >,
    ) -> Self {
        NodeStateHistory { inner: inner }
    }
}

impl<'py> pyo3::IntoPyObject<'py>
    for NodeState<
        'static,
        History<'static, NodeView<'static, DynamicGraph>>,
        DynamicGraph,
        DynamicGraph,
    >
{
    type Target = NodeStateHistory;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as pyo3::IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        NodeStateHistory::from(self).into_pyobject(py)
    }
}

impl<'py> FromPyObject<'py>
    for NodeState<
        'static,
        History<'static, NodeView<'static, DynamicGraph>>,
        DynamicGraph,
        DynamicGraph,
    >
{
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.downcast::<NodeStateHistory>()?.get().inner().clone())
    }
}
