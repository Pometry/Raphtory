use crate::{
    client::{remote_nodes::RemoteNodes, ClientError},
    python::client::{
        remote_collection_metadata::{PyRemoteMetadataView, PyRemotePropertiesView},
        remote_nested_edges::PyRemoteNestedEdges,
        remote_node::PyRemoteNode,
        remote_path_from_graph::PyRemotePathFromGraph,
        remote_sorting::PyNodeSortBy,
    },
};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyRef, PyRefMut, PyResult};
use raphtory::python::{filter::filter_expr::PyFilterExpr, utils::execute_async_task};
use raphtory_api::{
    core::{storage::timeindex::EventTime, utils::time::InputTime},
    python::timeindex::PyOptionalEventTime,
};
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
    ///
    /// Arguments:
    ///     start (TimeInput): inclusive start of the window.
    ///     end (TimeInput): exclusive end of the window.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection restricted to the window.
    pub fn window(&self, start: InputTime, end: InputTime) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.window(start, end))
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection restricted to that layer.
    pub fn layer(&self, name: &str) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.layer(name))
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): the time to snapshot at.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection snapshotted at that time.
    pub fn at(&self, time: InputTime) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.at(time))
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): only events strictly before this time are kept.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection restricted to events before that time.
    pub fn before(&self, time: InputTime) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.before(time))
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): only events strictly after this time are kept.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection restricted to events after that time.
    pub fn after(&self, time: InputTime) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.after(time))
    }

    /// Latest state. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection of the latest state.
    pub fn latest(&self) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.latest())
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection snapshotted at the latest time.
    pub fn snapshot_latest(&self) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.snapshot_latest())
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): the time to snapshot at.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection snapshotted at that time.
    pub fn snapshot_at(&self, time: InputTime) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.snapshot_at(time))
    }

    /// Exclude a specific layer. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer to exclude.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection with that layer excluded.
    pub fn exclude_layer(&self, name: &str) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.exclude_layer(name))
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     start (TimeInput): the new inclusive start of the window.
    ///     end (TimeInput): the new exclusive end of the window.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection with both window bounds shrunk.
    pub fn shrink_window(&self, start: InputTime, end: InputTime) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.shrink_window(start, end))
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     start (TimeInput): the new inclusive start of the window.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection with the window start shrunk.
    pub fn shrink_start(&self, start: InputTime) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.shrink_start(start))
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     end (TimeInput): the new exclusive end of the window.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection with the window end shrunk.
    pub fn shrink_end(&self, end: InputTime) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.shrink_end(end))
    }

    /// Restrict to the default layer. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection restricted to the default layer.
    pub fn default_layer(&self) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.default_layer())
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the layers.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection restricted to those layers.
    pub fn layers(&self, names: Vec<String>) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.layers(names))
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the layers to exclude.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection with those layers excluded.
    pub fn exclude_layers(&self, names: Vec<String>) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.exclude_layers(names))
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the valid layers.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection restricted to those valid layers.
    pub fn valid_layers(&self, names: Vec<String>) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.valid_layers(names))
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the valid layer to exclude.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection with that valid layer excluded.
    pub fn exclude_valid_layer(&self, name: &str) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.exclude_valid_layer(name))
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the valid layers to exclude.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection with those valid layers excluded.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> PyRemoteNodes {
        PyRemoteNodes::new(self.nodes.exclude_valid_layers(names))
    }

    /// Restrict this collection to members whose node type is in the given
    /// list. Filters membership — the returned collection has fewer members.
    /// Lazy — no RPC.
    ///
    /// Arguments:
    ///     node_types (list[str]): the node types to keep.
    ///
    /// Returns:
    ///     RemoteNodes: a new collection restricted to those node types.
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
        let tree = filter
            .try_as_filter_tree()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemoteNodes::new(self.nodes.filter(tree)?))
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
        Ok(PyRemoteNodes::new(self.nodes.select(composite)?))
    }

    /// `nodes[filter]` — sugar for `.select(filter)` (matches the local
    /// `Nodes.__getitem__`). Lazy — no RPC.
    fn __getitem__(&self, filter: PyFilterExpr) -> PyResult<PyRemoteNodes> {
        self.select(filter)
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

    /// Each member's neighbours (both directions). Lazy — no RPC. Returns a
    /// `RemotePathFromGraph` (nested, grouped per source node).
    ///
    /// Returns:
    ///     RemotePathFromGraph: each member's neighbours, grouped per source node.
    #[getter]
    pub fn neighbours(&self) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.nodes.neighbours())
    }

    /// Each member's in-neighbours. Lazy — no RPC. See `neighbours` for
    /// return-type notes.
    ///
    /// Returns:
    ///     RemotePathFromGraph: each member's in-neighbours, grouped per source node.
    #[getter]
    pub fn in_neighbours(&self) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.nodes.in_neighbours())
    }

    /// Each member's out-neighbours. Lazy — no RPC. See `neighbours` for
    /// return-type notes.
    ///
    /// Returns:
    ///     RemotePathFromGraph: each member's out-neighbours, grouped per source node.
    #[getter]
    pub fn out_neighbours(&self) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.nodes.out_neighbours())
    }

    /// Each member's incident edges (both directions). Lazy — no RPC. Returns a
    /// `RemoteNestedEdges` (nested, grouped per source node).
    ///
    /// Returns:
    ///     RemoteNestedEdges: each member's incident edges, grouped per source node.
    #[getter]
    pub fn edges(&self) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.nodes.edges())
    }

    /// Each member's incoming edges. Lazy — no RPC. See `edges` for
    /// return-type notes.
    ///
    /// Returns:
    ///     RemoteNestedEdges: each member's incoming edges, grouped per source node.
    #[getter]
    pub fn in_edges(&self) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.nodes.in_edges())
    }

    /// Each member's outgoing edges. Lazy — no RPC. See `edges` for
    /// return-type notes.
    ///
    /// Returns:
    ///     RemoteNestedEdges: each member's outgoing edges, grouped per source node.
    #[getter]
    pub fn out_edges(&self) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.nodes.out_edges())
    }

    /// The id of each node in this collection. Property — attribute access
    /// fires one RPC.
    ///
    /// Returns:
    ///   list[str]: the ids, in collection order.
    #[getter]
    pub fn id(&self) -> Result<Vec<String>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        execute_async_task(move || async move { nodes.id().await })
    }

    /// The name of each node in this collection. Property — attribute access
    /// fires one RPC.
    ///
    /// Returns:
    ///   list[str]: the names, in collection order.
    #[getter]
    pub fn name(&self) -> Result<Vec<String>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        execute_async_task(move || async move { nodes.name().await })
    }

    /// The type of each node in this collection (`None` when unset). Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///   list[Optional[str]]: the node types, in collection order.
    #[getter]
    pub fn node_type(&self) -> Result<Vec<Option<String>>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        execute_async_task(move || async move { nodes.node_type().await })
    }

    /// The earliest event time of each node in this collection. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///   list[Optional[EventTime]]: the earliest times, in collection order.
    #[getter]
    pub fn earliest_time(&self) -> Result<Vec<Option<EventTime>>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        Ok(
            execute_async_task(move || async move { nodes.earliest_time().await })?
                .into_iter()
                .map(|o| o)
                .collect(),
        )
    }

    /// The latest event time of each node in this collection. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///   list[Optional[EventTime]]: the latest times, in collection order.
    #[getter]
    pub fn latest_time(&self) -> Result<Vec<Option<EventTime>>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        Ok(
            execute_async_task(move || async move { nodes.latest_time().await })?
                .into_iter()
                .map(|o| o)
                .collect(),
        )
    }

    /// The non-temporal metadata of this collection as a columnar view. Each
    /// accessor returns one value per node. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteMetadataView: the columnar metadata view of this collection.
    #[getter]
    pub fn metadata(&self) -> PyRemoteMetadataView {
        PyRemoteMetadataView::new(self.nodes.metadata())
    }

    /// The properties of this collection as a columnar view. Each accessor
    /// returns one value per node. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemotePropertiesView: the columnar properties view of this collection.
    #[getter]
    pub fn properties(&self) -> PyRemotePropertiesView {
        PyRemotePropertiesView::new(self.nodes.properties())
    }

    /// Returns the number of nodes in this collection. Fires one RPC.
    ///
    /// Returns:
    ///   int: the number of nodes.
    pub fn count(&self) -> Result<i64, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        execute_async_task(move || async move { nodes.count().await })
    }

    /// Returns the degree of each node in this collection. Fires one RPC.
    ///
    /// Returns:
    ///   list[int]: the per-node degrees, in collection order.
    pub fn degree(&self) -> Result<Vec<i64>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        execute_async_task(move || async move { nodes.degree().await })
    }

    /// Returns the in-degree of each node in this collection. Fires one RPC.
    ///
    /// Returns:
    ///   list[int]: the per-node in-degrees, in collection order.
    pub fn in_degree(&self) -> Result<Vec<i64>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        execute_async_task(move || async move { nodes.in_degree().await })
    }

    /// Returns the out-degree of each node in this collection. Fires one RPC.
    ///
    /// Returns:
    ///   list[int]: the per-node out-degrees, in collection order.
    pub fn out_degree(&self) -> Result<Vec<i64>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        execute_async_task(move || async move { nodes.out_degree().await })
    }

    /// Returns the number of incident edge updates for each node in this
    /// collection. Fires one RPC.
    ///
    /// Returns:
    ///   list[int]: the per-node edge history counts, in collection order.
    pub fn edge_history_count(&self) -> Result<Vec<i64>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        execute_async_task(move || async move { nodes.edge_history_count().await })
    }

    /// Check if this view has a layer named `name`. Fires one RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer to check.
    ///
    /// Returns:
    ///     bool: True if the layer is present.
    pub fn has_layer(&self, name: &str) -> Result<bool, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        let name = name.to_string();
        execute_async_task(move || async move { nodes.has_layer(name).await })
    }

    /// The size of the window covered by this view (`end - start`), or `None`
    /// if the view is unbounded. Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///     Optional[int]: the size of the window, or `None` if the view is unbounded.
    #[getter]
    pub fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        execute_async_task(move || async move { nodes.window_size().await })
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
    ///
    /// Returns:
    ///     OptionalEventTime: the view start bound, or empty if unbounded.
    #[getter]
    pub fn start(&self) -> Result<PyOptionalEventTime, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        Ok(execute_async_task(move || async move { nodes.start().await })?.into())
    }

    /// View end bound for this collection — `None` if unbounded. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///     OptionalEventTime: the view end bound, or empty if unbounded.
    #[getter]
    pub fn end(&self) -> Result<PyOptionalEventTime, ClientError> {
        let nodes = Arc::clone(&self.nodes);
        Ok(execute_async_task(move || async move { nodes.end().await })?.into())
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
    /// yields a `RemoteNode` handle for each. Node handles are not batched:
    /// each terminal on a yielded node fires its own RPC.
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
