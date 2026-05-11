use crate::{
    core::entities::nodes::node_ref::{AsNodeRef, NodeRef},
    db::{
        api::{
            state::{
                convert_prop_map, ops::Const, GenericNodeState, MergePriority, NodeStateOutput,
                OutputTypedNodeState, TransformedPropMap, TypedNodeState,
            },
            view::{
                internal::IntoDynamicOrMutable, BoxableGraphView, DynamicGraph, StaticGraphViewOps,
            },
        },
        graph::nodes::Nodes,
    },
    prelude::{GraphViewOps, NodeStateOps, Prop},
    python::{
        graph::{
            node::{PyNode, PyNodes},
            node_state::{NodeFilterOp, NodeView},
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
    Borrowed, Bound, FromPyObject, IntoPyObject, IntoPyObjectExt, PyAny, PyErr, PyResult, Python,
};
use raphtory_api::core::entities::properties::prop::PropUntagged;
use std::{collections::HashMap, sync::Arc};

#[pyclass(name = "OutputNodeState", module = "raphtory.node_state", frozen)]
pub struct PyOutputNodeState {
    pub inner: OutputTypedNodeState<'static, DynamicGraph>,
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
        let res = if let Ok(other) = other.cast::<Self>() {
            let other = Bound::get(other);
            self.inner == other.inner
        } else if let Ok(other) = other.extract::<Vec<IndexMap<String, Option<Prop>>>>() {
            self.inner == other
        } else if let Ok(other) =
            other.extract::<HashMap<PyNodeRef, HashMap<String, Option<Prop>>>>()
        {
            self.inner.try_eq_hashmap(other).unwrap_or(false)
        } else if let Ok(other) = other.cast::<PyDict>() {
            self.inner.len() == other.len()
                && other.items().try_iter().unwrap().all(|item| {
                    if let Ok((node_ref, value)) =
                        item.unwrap().extract::<(PyNodeRef, Bound<'py, PyAny>)>()
                    {
                        self.inner
                            .get_by_node(node_ref)
                            .map(|l_value| {
                                let l_value = convert_prop_map::<PropUntagged, Prop>(l_value);
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

    fn __getitem__(
        &self,
        node: PyNodeRef,
    ) -> PyResult<TransformedPropMap<'static, Arc<dyn BoxableGraphView>>> {
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
    ///     default (dict, optional): the default value (dict of field name to value). Defaults to None.
    ///
    /// Returns:
    ///     Optional[dict]: the value for the node or the default value
    #[pyo3(signature = (node, default=None::<TransformedPropMap<'static, Arc<dyn BoxableGraphView>>>))]
    fn get(
        &self,
        node: PyNodeRef,
        default: Option<TransformedPropMap<'static, Arc<dyn BoxableGraphView>>>,
    ) -> Option<TransformedPropMap<'static, Arc<dyn BoxableGraphView>>> {
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

    /// Iterate over items
    ///
    /// Returns:
    ///     Iterator[Tuple[Node, Dict]]: Iterator over items
    fn items(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(
            self.inner.clone(),
            OutputTypedNodeState<'static, DynamicGraph>,
            |inner| inner.iter().map(|(n, v)| (n.cloned(), inner.convert(v)))
        )
    }

    /// Iterate over values
    ///
    /// Returns:
    ///     Iterator[Dict]: Iterator over values (dict of field name to value)
    fn values(&self) -> PyBorrowingIterator {
        self.__iter__()
    }

    /// Get value for node
    ///
    /// Arguments:
    ///     sort_params (Dict): Map of sort keys to sort option ('asc' or 'desc'). None defaults to 'asc'
    ///
    /// Returns:
    ///     OutputNodeState: Sorted NodeState
    fn sort_by(
        &self,
        sort_params: IndexMap<String, Option<String>>,
    ) -> OutputTypedNodeState<'static, DynamicGraph> {
        self.inner
            .state
            .sort_by(sort_params)
            .unwrap()
            .to_output_nodestate()
    }

    /// Get value for node
    ///
    /// Arguments:
    ///     sort_params (Dict): Map of sort keys to sort option ('asc' or 'desc'). None defaults to 'asc'
    ///     k (int): Number of top entries to return.
    ///
    /// Returns:
    ///     OutputNodeState: Sorted NodeState
    fn top_k(
        &self,
        sort_params: IndexMap<String, Option<String>>,
        k: usize,
    ) -> OutputTypedNodeState<'static, DynamicGraph> {
        self.inner
            .state
            .top_k(sort_params, k)
            .unwrap()
            .to_output_nodestate()
    }

    /// Group by value
    ///
    /// Arguments:
    ///     cols (list[str]): columns by which to group nodes
    ///
    /// Returns:
    ///     list[tuple[dict, Nodes]]: The grouped nodes
    fn groups(
        &self,
        cols: Vec<String>,
    ) -> Vec<(
        TransformedPropMap<'static, Arc<dyn BoxableGraphView>>,
        Nodes<'static, DynamicGraph>,
    )> {
        self.inner.get_groups(cols).unwrap()
    }

    //fn sorted_by_id(&self) -> OutputTypedNodeState<'static, DynamicGraph> {
    //    self.inner.sort_by_id()
    //}

    /// Convert OutputNodeState to Parquet
    ///
    /// Arguments:
    ///     file_path (str): filepath to which OutputNodeState is written
    ///     id_column (str): column containing IDs of nodes. Defaults to "id".
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (file_path, id_column="id".to_string()))]
    fn to_parquet(&self, file_path: String, id_column: String) {
        self.inner.state.to_parquet(file_path, Some(id_column));
    }

    /// Get OutputNodeState from Parquet
    ///
    /// Arguments:
    ///     file_path (str): filepath from which to read OutputNodeState
    ///     id_column (str): column to which node IDs will be written. Defaults to "id".
    ///
    /// Returns:
    ///     OutputNodeState:
    #[pyo3(signature = (file_path, id_column="id".to_string()))]
    fn from_parquet(&self, file_path: String, id_column: String) -> PyResult<Self> {
        Ok(PyOutputNodeState {
            inner: OutputTypedNodeState::new_mapped(
                self.inner.state.from_parquet(file_path, Some(id_column))?,
                GenericNodeState::get_nodes,
            ),
        })
    }

    /// Merge with another OutputNodeState (produces new OutputNodeState)
    ///
    /// Arguments:
    ///     other (OutputNodeState): OutputNodeState to merge with
    ///     index_merge_priority (str): "left" or "right" to take left or right index, "union" to union index sets. Defaults to "left".
    ///     default_column_merge_priority (str): "left" or "right" to prioritize left or right columns by default, "exclude" to exclude columns by default. Defaults to "left".
    ///     column_merge_priority_map (dict, optional): map of column names (str) to merge priority ("left", "right", or "exclude"). Defaults to None.
    ///
    /// Returns:
    ///     OutputNodeState:
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

        let res = if let Ok(other) = other.cast::<Self>() {
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

impl<'py> IntoPyObject<'py> for OutputTypedNodeState<'static, DynamicGraph> {
    type Target = PyOutputNodeState;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyOutputNodeState { inner: self }.into_pyobject(py)
    }
}

impl<'py, G: StaticGraphViewOps + IntoDynamicOrMutable> IntoPyObject<'py>
    for NodeStateOutput<'static, G>
{
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(match self {
            NodeStateOutput::Node(node_view) => node_view.into_pyobject(py)?.into_any(),
            NodeStateOutput::Nodes(nodes) => nodes.into_pyobject(py)?.into_any(),
            NodeStateOutput::Prop(prop) => prop.map(Prop::from).into_pyobject(py)?.into_any(),
        })
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for NodeStateOutput<'static, DynamicGraph> {
    type Error = PyErr;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(nodes) = obj.cast::<PyNodes>() {
            if nodes.get().nodes.predicate.is_filtered() {
                return Err(PyTypeError::new_err(
                    "Trying to read filtered Nodes object.",
                ));
            }
            let nodes: Nodes<'static, DynamicGraph, DynamicGraph, Const<bool>> =
                Nodes::new(nodes.get().nodes.graph.clone());
            return Ok(NodeStateOutput::Nodes(nodes));
        }

        if let Ok(node) = obj.cast::<PyNode>() {
            return Ok(NodeStateOutput::Node(node.get().node.clone()));
        }

        if let Ok(prop) = obj.extract::<Option<Prop>>() {
            return Ok(NodeStateOutput::Prop(prop.map(PropUntagged::from)));
        }

        Err(PyTypeError::new_err("Invalid type conversion"))
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for NodeStateOutput<'static, Arc<dyn BoxableGraphView>> {
    type Error = PyErr;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        let obj = NodeStateOutput::<'static, DynamicGraph>::extract(obj)?;
        Ok(match obj {
            NodeStateOutput::Node(node) => {
                NodeStateOutput::Node(NodeView::new_internal(node.graph.0, node.node))
            }
            NodeStateOutput::Nodes(nodes) => NodeStateOutput::Nodes(Nodes::new_filtered(
                nodes.base_graph.0,
                nodes.graph.0,
                nodes.predicate,
                nodes.nodes,
            )),
            NodeStateOutput::Prop(prop) => NodeStateOutput::Prop(prop),
        })
    }
}
