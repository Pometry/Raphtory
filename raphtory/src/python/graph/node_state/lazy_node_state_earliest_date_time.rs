use crate::{
    db::{
        api::{
            state::{ops, LazyNodeState, NodeOp, NodeState},
            view::DynamicGraph,
        },
        graph::nodes::Nodes,
    },
    prelude::{GraphViewOps, NodeStateOps, NodeViewOps},
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
use rayon::prelude::*;
use std::collections::HashMap;

type EarliestDateTime<G> = ops::Map<ops::EarliestTime<G>, Result<Option<DateTime<Utc>>, TimeError>>;

/// Type: Result<Option<DateTime\<Utc>>, TimeError>
type EarliestDateTimeOutput = <EarliestDateTime<DynamicGraph> as NodeOp>::Output;

// impl_lazy_node_state
/// A lazy view over EarliestDateTime values for node
#[pyclass(module = "raphtory.node_state", frozen)]
pub struct EarliestDateTimeView {
    inner: LazyNodeState<'static, EarliestDateTime<DynamicGraph>, DynamicGraph, DynamicGraph>,
}

impl EarliestDateTimeView {
    pub fn inner(
        &self,
    ) -> &LazyNodeState<'static, EarliestDateTime<DynamicGraph>, DynamicGraph, DynamicGraph> {
        &self.inner
    }

    // impl_node_state_ops
    pub fn iter(&self) -> impl Iterator<Item = EarliestDateTimeOutput> + '_ {
        self.inner.iter_values()
    }
}

#[pymethods]
impl EarliestDateTimeView {
    /// Compute all values and return the result as a node view
    ///
    /// Returns:
    #[doc = "     NodeStateResultOptionDateTime: the computed `NodeState`"]
    fn compute(&self) -> NodeState<'static, EarliestDateTimeOutput, DynamicGraph, DynamicGraph> {
        self.inner.compute()
    }

    /// Compute all values and return the result as a list
    ///
    /// Returns:
    #[doc = "     list[Optional[datetime]]: all values as a list"]
    fn collect(&self) -> PyResult<Vec<Option<DateTime<Utc>>>> {
        self.inner
            .par_iter_values()
            .map(|v| v.map_err(PyErr::from))
            .collect::<PyResult<Vec<_>>>()
    }

    // impl_node_state_ops
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
            LazyNodeState<'static, EarliestDateTime<DynamicGraph>, DynamicGraph, DynamicGraph>,
            |inner| inner.iter_values()
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
    #[doc = "     Iterator[Tuple[Node, Optional[datetime]]]: Iterator over items"]
    fn items(&self) -> PyBorrowingIterator {
        py_borrowing_iter_tuple_result!(
            self.inner.clone(),
            LazyNodeState<'static, EarliestDateTime<DynamicGraph>, DynamicGraph, DynamicGraph>,
            |inner| inner.iter().map(|(n, v)| (n.cloned(), v))
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
    fn sorted_by_id(&self) -> NodeState<'static, EarliestDateTimeOutput, DynamicGraph> {
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

impl From<LazyNodeState<'static, EarliestDateTime<DynamicGraph>, DynamicGraph, DynamicGraph>>
    for EarliestDateTimeView
{
    fn from(
        inner: LazyNodeState<'static, EarliestDateTime<DynamicGraph>, DynamicGraph, DynamicGraph>,
    ) -> Self {
        EarliestDateTimeView { inner }
    }
}

impl<'py> pyo3::IntoPyObject<'py>
    for LazyNodeState<'static, EarliestDateTime<DynamicGraph>, DynamicGraph, DynamicGraph>
{
    type Target = EarliestDateTimeView;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as pyo3::IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        EarliestDateTimeView::from(self).into_pyobject(py)
    }
}

impl<'py> FromPyObject<'py>
    for LazyNodeState<'static, EarliestDateTime<DynamicGraph>, DynamicGraph, DynamicGraph>
{
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.downcast::<EarliestDateTimeView>()?.get().inner().clone())
    }
}
