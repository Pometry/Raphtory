use std::collections::HashMap;

use crate::{
    core::entities::nodes::node_ref::{AsNodeRef, NodeRef},
    db::{
        api::{
            state::{
                ops::Const, GenericNodeState, MergePriority, NodeStateOutput, OutputTypedNodeState,
                TransformedPropMap, TypedNodeState,
            },
            view::DynamicGraph,
        },
        graph::nodes::Nodes,
    },
    prelude::{GraphViewOps, NodeStateOps, Prop},
    python::{
        graph::{
            node::{PyNode, PyNodes},
            node_state::NodeFilterOp,
        },
        types::{repr::Repr, wrappers::iterators::PyBorrowingIterator},
        utils::PyNodeRef,
    },
};
use indexmap::IndexMap;
use pyo3::{
    exceptions::{PyKeyError, PyTypeError},
    pyclass, pymethods,
    types::{PyAnyMethods, PyDict, PyDictMethods, PyNotImplemented},
    Bound, FromPyObject, IntoPyObject, IntoPyObjectExt, PyAny, PyErr, PyObject, PyResult, Python,
};

#[pyclass(
    name = "OutputNodeState",
    module = "raphtory.output_node_state",
    frozen
)]
pub struct PyOutputNodeState {
    inner: OutputTypedNodeState<'static, DynamicGraph>,
}

impl PyOutputNodeState {
    pub fn new(state: GenericNodeState<'static, DynamicGraph>) -> PyOutputNodeState {
        PyOutputNodeState {
            inner: TypedNodeState::new_mapped(state, GenericNodeState::get_nodes)
                .to_output_nodestate(),
        }
    }
}

#[pymethods]
impl PyOutputNodeState {
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn nodes(&self) -> Nodes<'static, DynamicGraph> {
        self.inner.nodes()
    }

    // TODO?
    fn __eq__<'py>(
        &self,
        other: &Bound<'py, PyAny>,
        py: Python<'py>,
    ) -> Result<Bound<'py, PyAny>, std::convert::Infallible> {
        let res = if let Ok(other) = other.downcast::<Self>() {
            let other = Bound::get(other);
            self.inner == other.inner
        } else if let Ok(other) = other.extract::<Vec<IndexMap<String, Option<Prop>>>>() {
            self.inner == other
        } else if let Ok(other) =
            other.extract::<HashMap<PyNodeRef, HashMap<String, Option<Prop>>>>()
        {
            self.inner.try_eq_hashmap(other).unwrap()
        } else if let Ok(other) = other.downcast::<PyDict>() {
            self.inner.len() == other.len()
                && other.items().try_iter().unwrap().all(|item| {
                    if let Ok((node_ref, value)) =
                        item.unwrap().extract::<(PyNodeRef, Bound<'py, PyAny>)>()
                    {
                        self.inner
                            .get_by_node(node_ref)
                            .map(|l_value| {
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
            return Ok(PyNotImplemented::get(py).to_owned().into_any());
        };
        Ok(res.into_pyobject(py)?.to_owned().into_any())
    }

    fn __repr__(&self) -> String {
        self.inner.repr()
    }

    fn __getitem__(&self, node: PyNodeRef) -> PyResult<TransformedPropMap<'static, DynamicGraph>> {
        let node = node.as_node_ref();
        self.inner
            .get_by_node(node)
            .map(|value| self.inner.convert(value))
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

    /// Get value for node
    ///
    /// Arguments:
    ///     node (NodeInput): the node
    ///
    /// Returns:
    #[pyo3(signature = (node, default=None::<TransformedPropMap<'static, DynamicGraph>>))]
    fn get(
        &self,
        node: PyNodeRef,
        default: Option<TransformedPropMap<'static, DynamicGraph>>,
    ) -> Option<TransformedPropMap<'static, DynamicGraph>> {
        self.inner
            .get_by_node(node)
            .map_or(default, |value| Some(self.inner.convert(value)))
    }

    fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.inner.clone(),
            OutputTypedNodeState<'static, DynamicGraph>,
            |inner| inner.iter_values().map(|v| inner.convert(v))
        )
    }

    fn items(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.inner.clone(),
            OutputTypedNodeState<'static, DynamicGraph>,
            |inner| inner.iter().map(|(n, v)| (n.cloned(), inner.convert(v)))
        )
    }

    fn values(&self) -> PyBorrowingIterator {
        self.__iter__()
    }

    //fn sorted_by_id(&self) -> OutputTypedNodeState<'static, DynamicGraph> {
    //    self.inner.sort_by_id()
    //}

    /// Convert TypedNodeState to Parquet
    ///
    /// Arguments:
    ///     file_path (str): filepath to which TypedNodeState is written
    ///     id_column (str): column containing IDs of nodes
    #[pyo3(signature = (file_path, id_column="id".to_string()))]
    fn to_parquet(&self, file_path: String, id_column: String) {
        self.inner.state.to_parquet(file_path, Some(id_column));
    }

    /// Get TypedNodeState from Parquet
    ///
    /// Arguments:
    ///     file_path (str): filepath from which to read TypedNodeState
    ///     id_column (str): column to which node IDs will be written
    ///
    /// Returns:
    ///     TypedNodeState
    #[pyo3(signature = (file_path, id_column="id".to_string()))]
    fn from_parquet(&self, file_path: String, id_column: String) -> PyResult<Self> {
        Ok(PyOutputNodeState {
            inner: OutputTypedNodeState::new_mapped(
                self.inner.state.from_parquet(file_path, Some(id_column))?,
                GenericNodeState::get_nodes,
            ),
        })
    }

    /// Merge with another TypedNodeState (produces new TypedNodeState)
    ///
    /// Arguments:
    ///     other (TypedNodeState): TypedNodeState to merge with
    ///     index_merge_priority (str): "left" or "right" to take left or right index, "union" to union index sets
    ///     default_column_merge_priority (str): "left" or "right" to prioritize left or right columns by default, "exclude" to exclude columns by default
    ///     column_merge_priority_map (dict): map of column names (str) to merge priority ("left", "right", or "exclude")
    ///
    /// Returns:
    ///     TypedNodeState
    #[pyo3(signature = (other, index_merge_priority="left".to_string(), default_column_merge_priority="left".to_string(), column_merge_priority_map=None::<HashMap<String, String>>))]
    fn merge<'py>(
        &self,
        other: &Bound<'py, PyAny>,
        index_merge_priority: String,
        default_column_merge_priority: String,
        column_merge_priority_map: Option<HashMap<String, String>>,
        py: Python<'py>,
    ) -> Result<Bound<'py, PyAny>, pyo3::PyErr> {
        // hashmap of string to enum
        let merge_priority_enum_map: HashMap<String, MergePriority> = HashMap::from([
            ("left".to_string(), MergePriority::Left),
            ("right".to_string(), MergePriority::Right),
            ("union".to_string(), MergePriority::Exclude),
        ]);

        let index_merge_priority = merge_priority_enum_map
            .get(&index_merge_priority.to_lowercase())
            .unwrap()
            .clone();
        let default_column_merge_priority = merge_priority_enum_map
            .get(&default_column_merge_priority.to_lowercase())
            .unwrap()
            .clone();
        let mut column_priority_map = None;
        if column_merge_priority_map.is_some() {
            column_priority_map = Some(
                column_merge_priority_map
                    .unwrap()
                    .into_iter()
                    .map(|(col_name, merge_priority)| {
                        (
                            col_name,
                            merge_priority_enum_map
                                .get(&merge_priority.to_lowercase())
                                .unwrap()
                                .clone(),
                        )
                    })
                    .into_iter()
                    .collect(),
            )
        }

        let res = if let Ok(other) = other.downcast::<Self>() {
            let other = Bound::get(other);
            self.inner.state.merge(
                &other.inner.state,
                index_merge_priority,
                default_column_merge_priority,
                column_priority_map,
            )
        } else {
            return Ok(PyNotImplemented::get(py).to_owned().into_any());
        };

        let new_output_state = PyOutputNodeState::new(res);
        Ok(new_output_state.into_pyobject(py)?.to_owned().into_any())
    }
}

impl<
        'py,
        // V: for<'py2> IntoPyObject<'py2> + NodeStateValue + 'static,
        // G: StaticGraphViewOps + IntoDynamicOrMutable,
    > IntoPyObject<'py> for OutputTypedNodeState<'static, DynamicGraph>
{
    type Target = PyOutputNodeState;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyOutputNodeState { inner: self }.into_pyobject(py)
    }
}

impl<'py> IntoPyObject<'py> for NodeStateOutput<'static, DynamicGraph> {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(match self {
            NodeStateOutput::Node(node_view) => node_view.into_pyobject(py)?.into_any(),
            NodeStateOutput::Nodes(nodes) => nodes.into_pyobject(py)?.into_any(),
            NodeStateOutput::Prop(prop) => prop.into_pyobject(py)?.into_any(),
        })
    }
}

impl<'py> FromPyObject<'py> for NodeStateOutput<'static, DynamicGraph> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(nodes) = ob.downcast::<PyNodes>() {
            if nodes.get().nodes.predicate.is_filtered() {
                return Err(PyTypeError::new_err(
                    "Trying to read filtered Nodes object.",
                ));
            }
            let nodes: Nodes<'static, DynamicGraph, DynamicGraph, Const<bool>> =
                Nodes::new(nodes.get().nodes.graph.clone());
            return Ok(NodeStateOutput::Nodes(nodes));
        }

        if let Ok(node) = ob.downcast::<PyNode>() {
            return Ok(NodeStateOutput::Node(node.get().node.clone()));
        }

        if let Ok(prop) = ob.extract::<Option<Prop>>() {
            return Ok(NodeStateOutput::Prop(prop));
        }

        return Err(PyTypeError::new_err("Invalid type conversion"));
    }
}
