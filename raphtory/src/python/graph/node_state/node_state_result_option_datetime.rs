use crate::{
    db::{
        api::{state::NodeState, view::DynamicGraph},
        graph::{node::NodeView, nodes::Nodes},
    },
    prelude::{GraphViewOps, NodeStateOps, NodeViewOps, OrderedNodeStateOps},
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
use raphtory_api::core::storage::timeindex::TimeError;
use raphtory_core::entities::nodes::node_ref::{AsNodeRef, NodeRef};
use std::{cmp::Ordering, collections::HashMap};
// $name = NodeStateResultOptionDateTime
// $value = Result<Option<DateTime<Utc>>, TimeError>
// $to_owned = |v| v.clone()
// $inner_t = NodeState<'static, Result<Option<DateTime<Utc>>, TimeError>, DynamicGraph, DynamicGraph>
// $py_value = Optional[datetime]
// $computed = NodeStateResultOptionDateTime

#[pyclass(module = "raphtory.node_state", frozen)]
pub struct NodeStateResultOptionDateTime {
    inner: NodeState<'static, Result<Option<DateTime<Utc>>, TimeError>, DynamicGraph, DynamicGraph>,
}

impl NodeStateResultOptionDateTime {
    pub fn inner(
        &self,
    ) -> &NodeState<'static, Result<Option<DateTime<Utc>>, TimeError>, DynamicGraph, DynamicGraph>
    {
        &self.inner
    }

    // impl_node_state_ops
    pub fn iter(&self) -> impl Iterator<Item = Result<Option<DateTime<Utc>>, TimeError>> + '_ {
        self.inner.iter_values().map(|v| v.clone())
    }
}

// impl_node_state_ops
#[pymethods]
impl NodeStateResultOptionDateTime {
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
        } else if let Ok(other) = other.extract::<Vec<Option<DateTime<Utc>>>>() {
            self.inner
                .iter_values()
                .map(|v| v.clone())
                .eq(other.into_iter().map(|o| Ok(o)))
        } else if let Ok(other) = other.extract::<HashMap<PyNodeRef, Option<DateTime<Utc>>>>() {
            self.inner.len() == other.len()
                && other.into_iter().all(|(node, value)| {
                    self.inner.get_by_node(node).map(|v| v.clone()) == Some(Ok(value))
                })
        } else if let Ok(other) = other.downcast::<PyDict>() {
            self.inner.len() == other.len()
                && other.items().iter().all(|item| {
                    if let Ok((node_ref, value)) = item.extract::<(PyNodeRef, Bound<'py, PyAny>)>()
                    {
                        self.inner
                            .get_by_node(node_ref)
                            .map(|v| v.clone())
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
            NodeState<
                'static,
                Result<Option<DateTime<Utc>>, TimeError>,
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
    #[doc = "    default (Optional[datetime]): the default value. Defaults to None."]
    ///
    /// Returns:
    #[doc = "    Optional[datetime]: the value for the node or the default value"]
    #[pyo3(signature = (node, default=None::<DateTime<Utc>>))]
    fn get(
        &self,
        node: PyNodeRef,
        default: Option<DateTime<Utc>>,
    ) -> PyResult<Option<DateTime<Utc>>> {
        match self.inner.get_by_node(node) {
            Some(v) => v.clone().map_err(PyErr::from),
            None => Ok(default),
        }
    }

    fn __getitem__(&self, node: PyNodeRef) -> PyResult<Option<DateTime<Utc>>> {
        let node = node.as_node_ref();
        match self.inner.get_by_node(node) {
            Some(v) => v.clone().map_err(PyErr::from),
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
    #[doc = "     Iterator[Tuple[Node, Optional[datetime]]]: Iterator over items"]
    fn items(&self) -> PyBorrowingIterator {
        py_borrowing_iter_tuple_result!(
            self.inner.clone(),
            NodeState<
                'static,
                Result<Option<DateTime<Utc>>, TimeError>,
                DynamicGraph,
                DynamicGraph,
            >,
            |inner| inner.iter().map(|(n, v)| (n.cloned(), v.clone()))
        )
    }

    /// Iterate over values
    ///
    /// Returns:
    #[doc = "     Iterator[Optional[datetime]]: Iterator over values"]
    fn values(&self) -> PyBorrowingIterator {
        self.__iter__()
    }

    /// Sort results by node id
    ///
    /// Returns:
    #[doc = "     NodeStateResultOptionDateTime: The sorted node state"]
    fn sorted_by_id(
        &self,
    ) -> NodeState<'static, Result<Option<DateTime<Utc>>, TimeError>, DynamicGraph> {
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

// impl_node_state_ord_ops
#[pymethods]
impl NodeStateResultOptionDateTime {
    /// Sort by value
    ///
    /// Arguments:
    ///     reverse (bool): If `True`, sort in descending order, otherwise ascending. Defaults to False.
    ///
    /// Returns:
    #[doc = "     NodeStateResultOptionDateTime: Sorted node state"]
    #[pyo3(signature = (reverse = false))]
    fn sorted(
        &self,
        reverse: bool,
    ) -> NodeState<'static, Result<Option<DateTime<Utc>>, TimeError>, DynamicGraph> {
        if reverse {
            self.inner.sort_by_values_by(|a, b| match (a, b) {
                (Ok(a), Ok(b)) => a.cmp(b).reverse(),
                (Err(_), Ok(_)) => Ordering::Greater,
                (Ok(_), Err(_)) => Ordering::Less,
                _ => Ordering::Equal,
            })
        } else {
            self.inner.sort_by_values_by(|a, b| match (a, b) {
                (Ok(a), Ok(b)) => a.cmp(b),
                (Err(_), Ok(_)) => Ordering::Greater,
                (Ok(_), Err(_)) => Ordering::Less,
                _ => Ordering::Equal,
            })
        }
    }

    /// Compute the k largest values
    ///
    /// Arguments:
    ///     k (int): The number of values to return
    ///
    /// Returns:
    #[doc = "     NodeStateResultOptionDateTime: The k largest values as a node state"]
    fn top_k(
        &self,
        k: usize,
    ) -> NodeState<'static, Result<Option<DateTime<Utc>>, TimeError>, DynamicGraph> {
        self.inner.top_k_by(
            |a, b| match (a, b) {
                (Ok(a), Ok(b)) => a.cmp(b),
                (Err(_), Ok(_)) => Ordering::Less,
                (Ok(_), Err(_)) => Ordering::Greater,
                _ => Ordering::Equal,
            },
            k,
        )
    }

    /// Compute the k smallest values
    ///
    /// Arguments:
    ///     k (int): The number of values to return
    ///
    /// Returns:
    #[doc = "     NodeStateResultOptionDateTime: The k smallest values as a node state"]
    fn bottom_k(
        &self,
        k: usize,
    ) -> NodeState<'static, Result<Option<DateTime<Utc>>, TimeError>, DynamicGraph> {
        self.inner.bottom_k_by(
            |a, b| match (a, b) {
                (Ok(a), Ok(b)) => a.cmp(b),
                (Err(_), Ok(_)) => Ordering::Greater,
                (Ok(_), Err(_)) => Ordering::Less,
                _ => Ordering::Equal,
            },
            k,
        )
    }

    /// Return smallest value and corresponding node
    ///
    /// Returns:
    #[doc = "     Optional[Tuple[Node, datetime]]: The Node and minimum value or `None` if empty"]
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
    #[doc = "     Optional[datetime]: The minimum value or `None` if empty"]
    fn min(&self) -> PyResult<Option<DateTime<Utc>>> {
        self.min_item().map(|v| v.map(|(_, date)| date))
    }

    /// Return largest value and corresponding node
    ///
    /// Returns:
    #[doc = "     Optional[Tuple[Node, datetime]]: The Node and maximum value or `None` if empty"]
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
            Some((_, Err(e))) => Err(PyErr::from(e)),
            None => Ok(None),
        }
    }

    /// Return the maximum value
    ///
    /// Returns:
    #[doc = "     Optional[datetime]: The maximum value or `None` if empty"]
    fn max(&self) -> PyResult<Option<DateTime<Utc>>> {
        self.max_item().map(|v| v.map(|(_, date)| date))
    }

    /// Return the median value
    ///
    /// Returns:
    #[doc = concat!("     Optional[", $py_value, "]:")]
    fn median(&self) -> PyResult<Option<DateTime<Utc>>> {
        self.inner
            .median()
            .map(|v| v.clone())
            .transpose()
            .map(|v| v.flatten())
            .map_err(PyErr::from)
    }

    /// Return median value and corresponding node
    ///
    /// Returns:
    #[doc = concat!("     Optional[Tuple[Node, ", $py_value,"]]: The median value or `None` if empty")]
    fn median_item(
        &self,
    ) -> Option<(
        NodeView<'static, DynamicGraph>,
        Result<Option<DateTime<Utc>>, TimeError>,
    )> {
        self.inner
            .median_item()
            .map(|(n, v)| (n.cloned(), v.clone()))
    }
}

impl From<NodeState<'static, Result<Option<DateTime<Utc>>, TimeError>, DynamicGraph, DynamicGraph>>
    for NodeStateResultOptionDateTime
{
    fn from(
        inner: NodeState<
            'static,
            Result<Option<DateTime<Utc>>, TimeError>,
            DynamicGraph,
            DynamicGraph,
        >,
    ) -> Self {
        NodeStateResultOptionDateTime { inner: inner }
    }
}

impl<'py> pyo3::IntoPyObject<'py>
    for NodeState<'static, Result<Option<DateTime<Utc>>, TimeError>, DynamicGraph, DynamicGraph>
{
    type Target = NodeStateResultOptionDateTime;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as pyo3::IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        NodeStateResultOptionDateTime::from(self).into_pyobject(py)
    }
}

impl<'py> FromPyObject<'py>
    for NodeState<'static, Result<Option<DateTime<Utc>>, TimeError>, DynamicGraph, DynamicGraph>
{
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob
            .downcast::<NodeStateResultOptionDateTime>()?
            .get()
            .inner()
            .clone())
    }
}
