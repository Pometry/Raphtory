use super::view_ops::py_remote_view_ops;
use crate::{
    client::{remote_path_from_graph::RemotePathFromGraph, ClientError},
    python::client::{
        node_subscript,
        remote_collection_metadata::{PyRemoteMetadataView, PyRemotePropertiesView},
        remote_history::PyRemoteHistory,
        remote_nested_edges::PyRemoteNestedEdges,
        remote_node::PyRemoteNode,
        remote_path_from_node::PyRemotePathFromNode,
    },
};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyRef, PyRefMut, PyResult};
use raphtory::python::{filter::filter_expr::PyFilterExpr, utils::execute_async_task};
use raphtory_api::{
    core::{entities::GID, storage::timeindex::EventTime, utils::time::InputTime},
    python::timeindex::PyOptionalEventTime,
};
use std::sync::Arc;

/// A handle to a "path from graph" collection.
///
/// Produced by [RemoteNodes.neighbours][raphtory.graphql.RemoteNodes.neighbours] /
/// [RemoteNodes.in_neighbours][raphtory.graphql.RemoteNodes.in_neighbours] /
/// [RemoteNodes.out_neighbours][raphtory.graphql.RemoteNodes.out_neighbours].
///
/// Distinct from `RemotePathFromNode` because it is **nested** — the server
/// type (`GqlPathFromGraph`) groups results per source node. `.id` returns
/// `list[list[str]]`, `collect()` returns `list[list[RemoteNode]]`, and
/// `count()` is the number of source paths.
#[derive(Clone)]
#[pyclass(
    name = "RemotePathFromGraph",
    module = "raphtory.graphql",
    from_py_object
)]
pub struct PyRemotePathFromGraph {
    pub(crate) path: Arc<RemotePathFromGraph>,
}

impl PyRemotePathFromGraph {
    pub(crate) fn new(path: RemotePathFromGraph) -> Self {
        Self {
            path: Arc::new(path),
        }
    }
}

#[pymethods]
impl PyRemotePathFromGraph {
    /// Filter this collection by a node filter. **Propagates** to downstream
    /// traversals from the matching nodes. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a node filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemotePathFromGraph: a new collection with the filter applied.
    ///
    /// Raises:
    ///     ValueError: if the filter cannot be represented as a GraphQL
    ///         `NodeFilter`.
    pub fn filter(&self, filter: PyFilterExpr) -> PyResult<PyRemotePathFromGraph> {
        let tree = filter
            .try_as_filter_tree()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemotePathFromGraph::new(self.path.filter(tree)?))
    }

    /// `path[filter]` — narrow this collection's membership by a filter
    /// expression, the sugar form of `.select(filter)` (matches the local
    /// `PathFromGraph.__getitem__`). Node predicates, graph views (which
    /// narrow membership to the nodes present in the view), and combinations
    /// all apply. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemotePathFromGraph: a new collection narrowed to matching nodes.
    ///
    /// Raises:
    ///     Exception: if the expression tests edges rather than nodes — the
    ///         same error the local `PathFromGraph.__getitem__` raises.
    ///     ValueError: if the filter cannot be sent over the wire.
    fn __getitem__(&self, filter: PyFilterExpr) -> PyResult<PyRemotePathFromGraph> {
        Ok(PyRemotePathFromGraph::new(
            self.path.select(node_subscript(&filter)?)?,
        ))
    }

    /// Restrict this collection to members whose node type is in the given
    /// list. Lazy — no RPC.
    ///
    /// Arguments:
    ///     node_types (list[str]): the node types to keep.
    ///
    /// Returns:
    ///     RemotePathFromGraph: a new collection restricted to those node types.
    pub fn type_filter(&self, node_types: Vec<String>) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.type_filter(node_types))
    }

    /// The neighbours (both directions) reachable one further hop from each
    /// source path, as a nested `RemotePathFromGraph`. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemotePathFromGraph: the neighbours one further hop from each source path.
    #[getter]
    pub fn neighbours(&self) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.neighbours())
    }

    /// The in-neighbours reachable one further hop from each source path, as a
    /// nested `RemotePathFromGraph`. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemotePathFromGraph: the in-neighbours one further hop from each source path.
    #[getter]
    pub fn in_neighbours(&self) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.in_neighbours())
    }

    /// The out-neighbours reachable one further hop from each source path, as a
    /// nested `RemotePathFromGraph`. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemotePathFromGraph: the out-neighbours one further hop from each source path.
    #[getter]
    pub fn out_neighbours(&self) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.path.out_neighbours())
    }

    /// The incident edges (both directions) of each source path, as a nested
    /// `RemoteNestedEdges` collection. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteNestedEdges: the incident edges of each source path, grouped per source
    ///         node.
    #[getter]
    pub fn edges(&self) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.path.edges())
    }

    /// The incoming edges of each source path, as a nested `RemoteNestedEdges`
    /// collection. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteNestedEdges: the incoming edges of each source path, grouped per source
    ///         node.
    #[getter]
    pub fn in_edges(&self) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.path.in_edges())
    }

    /// The outgoing edges of each source path, as a nested `RemoteNestedEdges`
    /// collection. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteNestedEdges: the outgoing edges of each source path, grouped per source
    ///         node.
    #[getter]
    pub fn out_edges(&self) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.path.out_edges())
    }

    /// The id of each neighbour, grouped per source node. Property — attribute
    /// access fires one RPC.
    ///
    /// Returns:
    ///     list[list[str | int]]: the ids, grouped per source node —
    ///     strings for string-indexed graphs, integers for integer-indexed
    ///     ones.
    #[getter]
    pub fn id(&self) -> Result<Vec<Vec<GID>>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.id().await })
    }

    /// The name of each neighbour, grouped per source node. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///     list[list[str]]: the names, grouped per source node.
    #[getter]
    pub fn name(&self) -> Result<Vec<Vec<String>>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.name().await })
    }

    /// The type of each neighbour (`None` when unset), grouped per source node.
    /// Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///     list[list[Optional[str]]]: the node types, grouped per source node.
    #[getter]
    pub fn node_type(&self) -> Result<Vec<Vec<Option<String>>>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.node_type().await })
    }

    /// The earliest event time of each node, grouped per source node. Property
    /// — attribute access fires one RPC.
    ///
    /// Returns:
    ///   list[list[Optional[EventTime]]]: the earliest times, per source.
    #[getter]
    pub fn earliest_time(&self) -> Result<Vec<Vec<Option<EventTime>>>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.earliest_time().await })
    }

    /// The latest event time of each node, grouped per source node. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///   list[list[Optional[EventTime]]]: the latest times, per source.
    #[getter]
    pub fn latest_time(&self) -> Result<Vec<Vec<Option<EventTime>>>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.latest_time().await })
    }

    /// The non-temporal metadata of this collection as a nested columnar view.
    /// Each accessor returns one value per node, grouped per source. Lazy —
    /// no RPC.
    ///
    /// Returns:
    ///     RemoteMetadataView: the nested columnar metadata view of this collection.
    #[getter]
    pub fn metadata(&self) -> PyRemoteMetadataView {
        PyRemoteMetadataView::new(self.path.metadata())
    }

    /// The properties of this collection as a nested columnar view. Each
    /// accessor returns one value per node, grouped per source. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemotePropertiesView: the nested columnar properties view of this collection.
    #[getter]
    pub fn properties(&self) -> PyRemotePropertiesView {
        PyRemotePropertiesView::new(self.path.properties())
    }

    /// Returns the degree of each node, grouped per source node. Fires one RPC.
    ///
    /// Returns:
    ///   list[list[int]]: the per-node degrees grouped per source node.
    pub fn degree(&self) -> Result<Vec<Vec<i64>>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.degree().await })
    }

    /// Returns the in-degree of each node, grouped per source node. Fires one RPC.
    ///
    /// Returns:
    ///   list[list[int]]: the per-node in-degrees grouped per source node.
    pub fn in_degree(&self) -> Result<Vec<Vec<i64>>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.in_degree().await })
    }

    /// Returns the out-degree of each node, grouped per source node. Fires one RPC.
    ///
    /// Returns:
    ///   list[list[int]]: the per-node out-degrees grouped per source node.
    pub fn out_degree(&self) -> Result<Vec<Vec<i64>>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.out_degree().await })
    }

    /// Returns the number of incident edge updates for each node, grouped per
    /// source node. Fires one RPC.
    ///
    /// Returns:
    ///   list[list[int]]: the per-node edge history counts grouped per source node.
    pub fn edge_history_count(&self) -> Result<Vec<Vec<i64>>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.edge_history_count().await })
    }

    /// Check if this view has a layer named `name`. Fires one RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer to check.
    ///
    /// Returns:
    ///     bool: True if the layer is present.
    pub fn has_layer(&self, name: &str) -> Result<bool, ClientError> {
        let path = Arc::clone(&self.path);
        let name = name.to_string();
        execute_async_task(move || async move { path.has_layer(name).await })
    }

    /// The size of the window covered by this view (`end - start`), or `None`
    /// if the view is unbounded. Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///     Optional[int]: the size of the window, or `None` if the view is unbounded.
    #[getter]
    pub fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let path = Arc::clone(&self.path);
        execute_async_task(move || async move { path.window_size().await })
    }

    /// A single combined event history for all nodes in this view — a
    /// `RemoteHistory` container. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteHistory: the combined event history of the nodes in this view.
    pub fn combined_history(&self) -> PyRemoteHistory {
        PyRemoteHistory::new(self.path.combined_history())
    }

    /// `len(path)` — number of source paths in the collection. Fires one RPC.
    pub fn __len__(&self) -> Result<usize, ClientError> {
        let path = Arc::clone(&self.path);
        Ok(execute_async_task(move || async move { path.len().await })?.max(0) as usize)
    }

    /// `bool(path)` — whether the collection is non-empty. Fires one RPC.
    pub fn __bool__(&self) -> Result<bool, ClientError> {
        let path = Arc::clone(&self.path);
        Ok(execute_async_task(move || async move { path.len().await })? > 0)
    }

    /// View start bound for this collection — `None` if unbounded. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///     OptionalEventTime: the view start bound, or empty if unbounded.
    #[getter]
    pub fn start(&self) -> Result<PyOptionalEventTime, ClientError> {
        let path = Arc::clone(&self.path);
        Ok(execute_async_task(move || async move { path.start().await })?.into())
    }

    /// View end bound for this collection — `None` if unbounded. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///     OptionalEventTime: the view end bound, or empty if unbounded.
    #[getter]
    pub fn end(&self) -> Result<PyOptionalEventTime, ClientError> {
        let path = Arc::clone(&self.path);
        Ok(execute_async_task(move || async move { path.end().await })?.into())
    }

    /// Materialize this collection as a nested list of `RemoteNode` handles —
    /// one inner list per source node. Fires one RPC. Each returned node is
    /// rebased under the same view chain that produced this collection.
    ///
    /// Returns:
    ///   list[list[RemoteNode]]: the neighbours grouped per source node.
    pub fn collect(&self) -> Result<Vec<Vec<PyRemoteNode>>, ClientError> {
        let path = Arc::clone(&self.path);
        let result = execute_async_task(move || async move { path.collect().await })?;
        Ok(result
            .into_iter()
            .map(|row| row.into_iter().map(PyRemoteNode::new).collect())
            .collect())
    }

    /// Enables `for source, path in remote_path_from_graph:` — mirrors the local
    /// `PathFromGraph`, which pairs each source node with that source's own
    /// `PathFromNode`. Fetches the source ids in one RPC; the yielded path is a
    /// lazy handle that still chains (`path.window(..)`, `path.degree()`, ...).
    ///
    /// Returns:
    ///   Iterator[tuple[RemoteNode, RemotePathFromNode]]: one `(source, path)`
    ///     pair per source node.
    fn __iter__(&self) -> Result<PyRemotePathFromGraphIter, ClientError> {
        let path = Arc::clone(&self.path);
        let pairs = execute_async_task(move || async move { path.pairs().await })?;
        Ok(PyRemotePathFromGraphIter {
            inner: pairs
                .into_iter()
                .map(|(source, path)| (PyRemoteNode::new(source), PyRemotePathFromNode::new(path)))
                .collect::<Vec<_>>()
                .into_iter(),
        })
    }
}

#[pyclass(name = "RemotePathFromGraphIter", module = "raphtory.graphql")]
pub struct PyRemotePathFromGraphIter {
    inner: std::vec::IntoIter<(PyRemoteNode, PyRemotePathFromNode)>,
}

#[pymethods]
impl PyRemotePathFromGraphIter {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<(PyRemoteNode, PyRemotePathFromNode)> {
        slf.inner.next()
    }
}

py_remote_view_ops!(PyRemotePathFromGraph, path, "RemotePathFromGraph");
