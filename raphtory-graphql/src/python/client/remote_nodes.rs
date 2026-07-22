use crate::{
    client::{remote_nodes::RemoteNodes, ClientError},
    python::client::{
        remote_history::PyRemoteEventTime, remote_node::PyRemoteNode, remote_sorting::PyNodeSortBy,
    },
};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyRef, PyRefMut, PyResult};
use raphtory::python::{filter::filter_expr::PyFilterExpr, utils::execute_async_task};
use std::sync::Arc;

/// A handle to a remote collection of nodes.
///
/// Returned by [RemoteGraph.nodes][raphtory.graphql.RemoteGraph.nodes] and by
/// [RemoteNode.neighbours][raphtory.graphql.RemoteNode.neighbours] /
/// [RemoteNode.in_neighbours][raphtory.graphql.RemoteNode.in_neighbours] /
/// [RemoteNode.out_neighbours][raphtory.graphql.RemoteNode.out_neighbours].
#[derive(Clone)]
#[pyclass(name = "RemoteNodes", module = "raphtory.graphql", from_py_object)]
pub struct PyRemoteNodes {
    pub(crate) nodes: Arc<RemoteNodes>,
}

impl PyRemoteNodes {
    pub(crate) fn new(nodes: RemoteNodes) -> Self {
        Self {
            nodes: Arc::new(nodes),
        }
    }
}

#[pymethods]
impl PyRemoteNodes {
    /// Time-window this collection. Lazy — no RPC.
    pub fn window(&self, start: i64, end: i64) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.window(start, end))
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    pub fn layer(&self, name: &str) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.layer(name))
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn at(&self, time: i64) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.at(time))
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    pub fn before(&self, time: i64) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.before(time))
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    pub fn after(&self, time: i64) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.after(time))
    }

    /// Latest state. Lazy — no RPC.
    pub fn latest(&self) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.latest())
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    pub fn snapshot_latest(&self) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.snapshot_latest())
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    pub fn snapshot_at(&self, time: i64) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.snapshot_at(time))
    }

    /// Exclude a specific layer. Lazy — no RPC.
    pub fn exclude_layer(&self, name: &str) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.exclude_layer(name))
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    pub fn shrink_window(&self, start: i64, end: i64) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.shrink_window(start, end))
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    pub fn shrink_start(&self, start: i64) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.shrink_start(start))
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    pub fn shrink_end(&self, end: i64) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.shrink_end(end))
    }

    /// Restrict to the default layer. Lazy — no RPC.
    pub fn default_layer(&self) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.default_layer())
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    pub fn layers(&self, names: Vec<String>) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.layers(names))
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    pub fn exclude_layers(&self, names: Vec<String>) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.exclude_layers(names))
    }

    /// Restrict this collection to members whose node type is in the given
    /// list. Filters membership — the returned collection has fewer members.
    /// Lazy — no RPC.
    pub fn type_filter(&self, node_types: Vec<String>) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.type_filter(node_types))
    }

    /// Filter this collection by a filter expression from `raphtory.filter`
    /// (the same builder used by local graphs). The filter **propagates**:
    /// it narrows the current collection AND applies to downstream
    /// traversals from the matching nodes (e.g. their `.neighbours`,
    /// `.edges`). For a narrow-here-only variant, use `.select(...)`.
    /// Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection with the filter applied.
    ///
    /// Raises:
    ///     ValueError: if the filter cannot be represented as a GraphQL
    ///         `NodeFilter` (e.g. references edge fields, or uses an
    ///         unsupported operator like `FuzzySearch`).
    pub fn filter(&self, filter: PyFilterExpr) -> PyResult<PyRemoteNodes> {
        let composite = filter
            .try_as_node_filter()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let gql_filter = composite
            .try_into()
            .map_err(|e: raphtory::errors::GraphError| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemoteNodes::new(self.nodes.filter(gql_filter)))
    }

    /// Narrow this collection's membership by a filter expression. Unlike
    /// `.filter()`, the filter applies **only at this step** — downstream
    /// traversals from the matching nodes see the unfiltered graph. Use
    /// `.filter()` for the propagating variant. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection narrowed to matching nodes.
    pub fn select(&self, filter: PyFilterExpr) -> PyResult<PyRemoteNodes> {
        let composite = filter
            .try_as_node_filter()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let gql_filter = composite
            .try_into()
            .map_err(|e: raphtory::errors::GraphError| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemoteNodes::new(self.nodes.select(gql_filter)))
    }

    /// Reorder this collection by an ordered list of sort keys. Multi-key
    /// sort is lexicographic (ties on key 1 break to key 2). Lazy — no RPC.
    ///
    /// Arguments:
    ///     sort_bys (list[NodeSortBy]): the ordered sort keys.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection in the sorted order.
    pub fn sorted(&self, sort_bys: Vec<PyNodeSortBy>) -> PyRemoteNodes {
        let inner: Vec<_> = sort_bys.into_iter().map(|s| s.inner).collect();
        PyRemoteNodes::new(self.nodes.sorted(inner))
    }

    /// Returns the list of node ids in this collection.
    ///
    /// Fires one RPC.
    ///
    /// Returns:
    ///   list[str]: the ids of the nodes.
    pub fn ids(&self) -> Result<Vec<String>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        execute_async_task(move || async move { nodes.ids().await })
    }

    /// Returns the number of nodes in this collection. Fires one RPC.
    ///
    /// Returns:
    ///   int: the number of nodes.
    pub fn count(&self) -> Result<i64, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        execute_async_task(move || async move { nodes.count().await })
    }

    /// `len(nodes)` — number of nodes in the collection. Fires one RPC.
    pub fn __len__(&self) -> Result<usize, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        Ok(execute_async_task(move || async move { nodes.count().await })?.max(0) as usize)
    }

    /// `bool(nodes)` — whether the collection is non-empty. Fires one RPC.
    pub fn __bool__(&self) -> Result<bool, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        Ok(execute_async_task(move || async move { nodes.count().await })? > 0)
    }

    /// View start bound for this collection — `None` if unbounded. Property —
    /// attribute access fires one RPC.
    #[getter]
    pub fn start(&self) -> Result<Option<PyRemoteEventTime>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        Ok(
            execute_async_task(move || async move { nodes.start().await })?
                .map(PyRemoteEventTime::from),
        )
    }

    /// View end bound for this collection — `None` if unbounded. Property —
    /// attribute access fires one RPC.
    #[getter]
    pub fn end(&self) -> Result<Option<PyRemoteEventTime>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        Ok(
            execute_async_task(move || async move { nodes.end().await })?
                .map(PyRemoteEventTime::from),
        )
    }

    /// Materialize this collection as a list of `RemoteNode` handles.
    ///
    /// Fires one RPC (to fetch the ids); each returned node is rebased under
    /// the same view chain that produced this collection, so terminals on the
    /// returned handles evaluate under the same window / layer / at / etc.
    ///
    /// Returns:
    ///   list[RemoteNode]: one handle per node in the collection.
    pub fn collect(&self) -> Result<Vec<PyRemoteNode>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        let result = execute_async_task(move || async move { nodes.collect().await })?;
        Ok(result.into_iter().map(PyRemoteNode::new).collect())
    }

    /// Enables `for n in remote_nodes:` — fetches all ids in one RPC, then
    /// yields a `RemoteNode` handle for each. No per-node RPC batching yet
    /// (planned as a follow-up); each terminal on a yielded node fires its
    /// own RPC.
    fn __iter__(&self) -> Result<PyRemoteNodesIter, ClientError> {
        let list = self.collect()?;
        Ok(PyRemoteNodesIter {
            inner: list.into_iter(),
        })
    }
}

/// Opaque iterator returned by `PyRemoteNodes::__iter__`.
///
/// Not intended to be constructed directly — Python creates it via
/// `iter(remote_nodes)` (or under the hood in a `for` loop).
#[pyclass(name = "RemoteNodesIter", module = "raphtory.graphql")]
pub struct PyRemoteNodesIter {
    inner: std::vec::IntoIter<PyRemoteNode>,
}

#[pymethods]
impl PyRemoteNodesIter {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<PyRemoteNode> {
        slf.inner.next()
    }
}
