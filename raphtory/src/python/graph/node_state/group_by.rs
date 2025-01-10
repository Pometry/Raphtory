use crate::{
    db::api::{
        state::NodeGroups,
        view::{internal::IntoDynamicOrMutable, DynamicGraph, IntoDynamic, StaticGraphViewOps},
    },
    python::utils::PyGenericIterator,
};
use pyo3::{exceptions::PyIndexError, prelude::*, IntoPyObjectExt};
use std::hash::Hash;

trait PyNodeGroupOps: Send + Sync + 'static {
    fn iter(&self) -> PyGenericIterator;

    fn iter_subgraphs(&self) -> PyGenericIterator;

    fn group<'py>(
        &self,
        index: usize,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)>;

    fn group_subgraph<'py>(
        &self,
        index: usize,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, DynamicGraph)>;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;
}

impl<
        V: for<'py> IntoPyObject<'py> + Hash + Eq + Clone + Send + Sync + 'static,
        G: StaticGraphViewOps + IntoDynamicOrMutable,
    > PyNodeGroupOps for NodeGroups<V, G>
{
    fn iter(&self) -> PyGenericIterator {
        self.clone().into_iter_groups().into()
    }

    fn iter_subgraphs(&self) -> PyGenericIterator {
        self.clone().into_iter_subgraphs().into()
    }

    fn group<'py>(
        &self,
        index: usize,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        match self.group(index) {
            Some((v, nodes)) => Ok((
                v.clone().into_bound_py_any(py)?,
                nodes.into_bound_py_any(py)?,
            )),
            None => Err(PyIndexError::new_err("Index for group out of bounds")),
        }
    }

    fn group_subgraph<'py>(
        &self,
        index: usize,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, DynamicGraph)> {
        match self.group_subgraph(index) {
            None => Err(PyIndexError::new_err("Index for group out of bounds")),
            Some((v, graph)) => Ok((v.clone().into_bound_py_any(py)?, graph.into_dynamic())),
        }
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

#[pyclass(name = "NodeGroups", module = "raphtory.node_state", frozen)]
pub struct PyNodeGroups {
    inner: Box<dyn PyNodeGroupOps>,
}

#[pymethods]
impl PyNodeGroups {
    fn __iter__(&self) -> PyGenericIterator {
        self.inner.iter()
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __bool__(&self) -> bool {
        !self.inner.is_empty()
    }

    fn __getitem__<'py>(
        &self,
        index: usize,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        self.inner.group(index, py)
    }

    /// Get group nodes and value
    ///
    /// Arguments:
    ///     index (int): the group index
    ///
    /// Returns:
    ///     Tuple[Any, Nodes]: Nodes and corresponding value
    fn group<'py>(
        &self,
        index: usize,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        self.inner.group(index, py)
    }

    /// Get group as subgraph
    ///
    /// Arguments:
    ///     index (int): the group index
    ///
    /// Returns:
    ///     Tuple[Any, GraphView]: The group as a subgraph and corresponding value
    fn group_subgraph<'py>(
        &self,
        index: usize,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, DynamicGraph)> {
        self.inner.group_subgraph(index, py)
    }

    /// Iterate over group subgraphs
    ///
    /// Returns:
    ///     Iterator[Tuple[Any, GraphView]]: Iterator over subgraphs with corresponding value
    fn iter_subgraphs(&self) -> PyGenericIterator {
        self.inner.iter_subgraphs()
    }
}

impl<
        'py,
        V: for<'py2> IntoPyObject<'py2> + Hash + Eq + Clone + Send + Sync + 'static,
        G: StaticGraphViewOps + IntoDynamicOrMutable,
    > IntoPyObject<'py> for NodeGroups<V, G>
{
    type Target = PyNodeGroups;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyNodeGroups {
            inner: Box::new(self),
        }
        .into_pyobject(py)
    }
}
