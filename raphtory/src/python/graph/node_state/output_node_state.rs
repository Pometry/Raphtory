use std::collections::HashMap;

use crate::{
    core::{
        entities::nodes::node_ref::{AsNodeRef, NodeRef},
    }, db::{
        api::{
            state::TypedNodeState,
            view::DynamicGraph,
        },
        graph::nodes::Nodes,
    }, prelude::{GraphViewOps, NodeStateOps, Prop}, py_borrowing_iter, python::{
        types::{repr::Repr, wrappers::iterators::PyBorrowingIterator},
        utils::PyNodeRef,
    }
};
use pyo3::{
    exceptions::{PyKeyError, PyTypeError},
    pyclass, pymethods,
    types::{PyAnyMethods, PyDict, PyDictMethods, PyNotImplemented},
    Bound, IntoPyObject, IntoPyObjectExt, PyAny, PyObject, PyResult, Python,
};


#[pyclass(
    name = "OutputNodeState",
    module = "raphtory.output_node_state",
    frozen
)]
pub struct PyOutputNodeState {
    inner: TypedNodeState<'static, HashMap<String, Prop>, DynamicGraph, DynamicGraph>,
}

#[pymethods]
impl PyOutputNodeState {
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn nodes(&self) -> Nodes<'static, DynamicGraph> {
        self.inner.nodes()
    }

    fn __eq__<'py>(
        &self,
        other: &Bound<'py, PyAny>,
        py: Python<'py>,
    ) -> Result<Bound<'py, PyAny>, std::convert::Infallible> {
        println!("hey, we're about to compare!");
        let res = if let Ok(other) = other.downcast::<Self>() {
            let other = Bound::get(other);
            self.inner == other.inner
        } else if let Ok(other) = other.extract::<Vec<HashMap<String, Prop>>>() {
            self.inner == other //self.inner.iter_values().map($to_owned).eq(other.into_iter())
        } else if let Ok(other) = other.extract::<HashMap<PyNodeRef, HashMap<String, Prop>>>() {
            println!("wow! a hashmap! {:?}", other);
            self.inner.try_eq_hashmap(other).unwrap()
        } else if let Ok(other) = other.downcast::<PyDict>() {
            println!("wow! a pydict!");
            self.inner.len() == other.len()
                && other.items().try_iter().unwrap().all(|item| {
                    if let Ok((node_ref, value)) =
                        item.unwrap().extract::<(PyNodeRef, Bound<'py, PyAny>)>()
                    {
                        println!("{:?}", value);
                        self.inner
                            .get_by_node(node_ref)
                            .map(|l_value| {
                                println!("{:?}", l_value);
                                if let Ok(l_value_py) = l_value.into_bound_py_any(py) {
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
            println!("uh oh...");
            return Ok(PyNotImplemented::get(py).to_owned().into_any());
        };
        Ok(res.into_pyobject(py)?.to_owned().into_any())
    }

    fn __repr__(&self) -> String {
        self.inner.repr()
    }

    fn __getitem__(&self, node: PyNodeRef) -> PyResult<HashMap<String, Prop>> {
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

    /// Get value for node
    ///
    /// Arguments:
    ///     node (NodeInput): the node
    ///
    /// Returns:
    #[pyo3(signature = (node, default=None::<HashMap<String, Prop>>))]
    fn get(
        &self,
        node: PyNodeRef,
        default: Option<HashMap<String, Prop>>,
    ) -> Option<HashMap<String, Prop>> {
        self.inner.get_by_node(node).or(default)
    }

    fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.inner.clone(),
            TypedNodeState<'static, HashMap<String, Prop>, DynamicGraph, DynamicGraph>,
            |inner| inner.iter_values()
        )
    }

    fn items(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.inner.clone(),
            TypedNodeState<'static, HashMap<String, Prop>, DynamicGraph, DynamicGraph>,
            |inner| inner.iter().map(|(n, v)| (n.cloned(), v))
        )
    }

    fn values(&self) -> PyBorrowingIterator {
        self.__iter__()
    }

    #[pyo3(signature = (file_path, node_column="id".to_string()))]
    fn to_parquet(&self, file_path: String, node_column: String) {
        self.inner.state.to_parquet(file_path, Some(node_column));
    }
}

impl<
        'py,
        // V: for<'py2> IntoPyObject<'py2> + NodeStateValue + 'static,
        // G: StaticGraphViewOps + IntoDynamicOrMutable,
    > IntoPyObject<'py> for TypedNodeState<'static, HashMap<String, Prop>, DynamicGraph>
{
    type Target = PyOutputNodeState;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyOutputNodeState { inner: self }.into_pyobject(py)
    }
}
