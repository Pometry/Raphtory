use raphtory_api::python::timeindex::PyOptionalEventTime;
use crate::{
    client::{remote_nested_edges::RemoteNestedEdges, ClientError},
    python::client::{
        remote_collection_metadata::{PyRemoteMetadataView, PyRemotePropertiesView},
        remote_edge::PyRemoteEdge,
        remote_path_from_graph::PyRemotePathFromGraph,
    },
};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyRef, PyRefMut, PyResult};
use raphtory::python::{filter::filter_expr::PyFilterExpr, utils::execute_async_task};
use raphtory_api::core::{storage::timeindex::EventTime, utils::time::InputTime};
use std::sync::Arc;

/// A handle to a nested edges collection.
///
/// Produced by [RemoteNodes.edges][raphtory.graphql.RemoteNodes.edges] /
/// [RemoteNodes.in_edges][raphtory.graphql.RemoteNodes.in_edges] /
/// [RemoteNodes.out_edges][raphtory.graphql.RemoteNodes.out_edges].
///
/// Distinct from `RemoteEdges` because it is **nested** — the server type
/// (`GqlNestedEdges`) groups results per source node. `collect()` returns
/// `list[list[RemoteEdge]]`, and `count()` is the number of source edge
/// collections. Edges are identified by `(src, dst)` pairs rather than a
/// single string id; the `.id` accessor returns those pairs, nested per
/// source node.
#[derive(Clone)]
#[pyclass(
    name = "RemoteNestedEdges",
    module = "raphtory.graphql",
    from_py_object
)]
pub struct PyRemoteNestedEdges {
    pub(crate) edges: Arc<RemoteNestedEdges>,
}

impl PyRemoteNestedEdges {
    pub(crate) fn new(edges: RemoteNestedEdges) -> Self {
        Self {
            edges: Arc::new(edges),
        }
    }
}

#[pymethods]
impl PyRemoteNestedEdges {
    /// Time-window this collection. Lazy — no RPC.
    ///
    /// Arguments:
    ///     start (TimeInput): inclusive start of the window.
    ///     end (TimeInput): exclusive end of the window.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection restricted to the window.
    pub fn window(&self, start: InputTime, end: InputTime) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.window(start, end))
    }

    /// Filter this collection by an edge filter. **Propagates** to downstream
    /// traversals from the matching edges. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): an edge filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection with the filter applied.
    ///
    /// Raises:
    ///     ValueError: if the filter cannot be represented as a GraphQL
    ///         `EdgeFilter`.
    pub fn filter(&self, filter: PyFilterExpr) -> PyResult<PyRemoteNestedEdges> {
        let tree = filter
            .try_as_filter_tree()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemoteNestedEdges::new(self.edges.filter(tree)?))
    }

    /// Narrow this collection's membership by an edge filter — applies only at
    /// this step; downstream traversals see the unfiltered graph. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): an edge filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection narrowed to matching edges.
    pub fn select(&self, filter: PyFilterExpr) -> PyResult<PyRemoteNestedEdges> {
        let composite = filter
            .try_as_edge_filter()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemoteNestedEdges::new(self.edges.select(composite)?))
    }

    /// `edges[filter]` — sugar for `.select(filter)`. Lazy — no RPC.
    fn __getitem__(&self, filter: PyFilterExpr) -> PyResult<PyRemoteNestedEdges> {
        self.select(filter)
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection restricted to that layer.
    pub fn layer(&self, name: &str) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.layer(name))
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): the time to snapshot at.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection snapshotted at that time.
    pub fn at(&self, time: InputTime) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.at(time))
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): only events strictly before this time are kept.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection restricted to events before that time.
    pub fn before(&self, time: InputTime) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.before(time))
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): only events strictly after this time are kept.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection restricted to events after that time.
    pub fn after(&self, time: InputTime) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.after(time))
    }

    /// Latest state. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection of the latest state.
    pub fn latest(&self) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.latest())
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection snapshotted at the latest time.
    pub fn snapshot_latest(&self) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.snapshot_latest())
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): the time to snapshot at.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection snapshotted at that time.
    pub fn snapshot_at(&self, time: InputTime) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.snapshot_at(time))
    }

    /// Exclude a specific layer. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer to exclude.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection with that layer excluded.
    pub fn exclude_layer(&self, name: &str) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.exclude_layer(name))
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     start (TimeInput): the new inclusive start of the window.
    ///     end (TimeInput): the new exclusive end of the window.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection with both window bounds shrunk.
    pub fn shrink_window(&self, start: InputTime, end: InputTime) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.shrink_window(start, end))
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     start (TimeInput): the new inclusive start of the window.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection with the window start shrunk.
    pub fn shrink_start(&self, start: InputTime) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.shrink_start(start))
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     end (TimeInput): the new exclusive end of the window.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection with the window end shrunk.
    pub fn shrink_end(&self, end: InputTime) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.shrink_end(end))
    }

    /// Restrict to the default layer. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection restricted to the default layer.
    pub fn default_layer(&self) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.default_layer())
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the layers.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection restricted to those layers.
    pub fn layers(&self, names: Vec<String>) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.layers(names))
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the layers to exclude.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection with those layers excluded.
    pub fn exclude_layers(&self, names: Vec<String>) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.exclude_layers(names))
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the valid layers.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection restricted to those valid layers.
    pub fn valid_layers(&self, names: Vec<String>) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.valid_layers(names))
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the valid layer to exclude.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection with that valid layer excluded.
    pub fn exclude_valid_layer(&self, name: &str) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.exclude_valid_layer(name))
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the valid layers to exclude.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection with those valid layers excluded.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.exclude_valid_layers(names))
    }

    /// Fan out each source's edges into one entry per event. Mirrors the local
    /// `NestedEdges.explode`. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection with one entry per event, grouped per source
    ///         node.
    pub fn explode(&self) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.explode())
    }

    /// Fan out each source's edges into one entry per layer per edge. Mirrors
    /// the local `NestedEdges.explode_layers`. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection with one entry per layer per edge, grouped
    ///         per source node.
    pub fn explode_layers(&self) -> PyRemoteNestedEdges {
        PyRemoteNestedEdges::new(self.edges.explode_layers())
    }

    /// Returns the number of source edge collections in this collection. Fires
    /// one RPC.
    ///
    /// Returns:
    ///     int: the number of source edge collections.
    pub fn count(&self) -> Result<i64, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.count().await })
    }

    /// Check if this view has a layer named `name`. Fires one RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer to check.
    ///
    /// Returns:
    ///     bool: True if the layer is present.
    pub fn has_layer(&self, name: &str) -> Result<bool, ClientError> {
        let edges = Arc::clone(&self.edges);
        let name = name.to_string();
        execute_async_task(move || async move { edges.has_layer(name).await })
    }

    /// The source node of each edge, grouped per source node, as a nested
    /// `RemotePathFromGraph`. Mirrors the local `NestedEdges.src`. Property —
    /// lazy; attribute access fires no RPC.
    ///
    /// Returns:
    ///   RemotePathFromGraph: the source nodes, grouped per source node.
    #[getter]
    pub fn src(&self) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.edges.src())
    }

    /// The destination node of each edge, grouped per source node, as a nested
    /// `RemotePathFromGraph`. Mirrors the local `NestedEdges.dst`. Property —
    /// lazy; attribute access fires no RPC.
    ///
    /// Returns:
    ///   RemotePathFromGraph: the destination nodes, grouped per source node.
    #[getter]
    pub fn dst(&self) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.edges.dst())
    }

    /// The node at the other end of each edge (destination for out-edges,
    /// source for in-edges), grouped per source node, as a nested
    /// `RemotePathFromGraph`. Mirrors the local `NestedEdges.nbr`. Property —
    /// lazy; attribute access fires no RPC.
    ///
    /// Returns:
    ///   RemotePathFromGraph: the other-end nodes, grouped per source node.
    #[getter]
    pub fn nbr(&self) -> PyRemotePathFromGraph {
        PyRemotePathFromGraph::new(self.edges.nbr())
    }

    /// The `(src, dst)` id pair of each edge, grouped per source node.
    /// Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///   list[list[tuple[str, str]]]: id pairs grouped per source node.
    #[getter]
    pub fn id(&self) -> Result<Vec<Vec<(String, String)>>, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.id().await })
    }

    /// The layer names of each edge, grouped per source node. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///   list[list[list[str]]]: layer names per edge, grouped per source node.
    #[getter]
    pub fn layer_names(&self) -> Result<Vec<Vec<Vec<String>>>, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.layer_names().await })
    }

    /// The single layer name of each edge, grouped per source node. Only valid
    /// once the edges have been exploded; raises otherwise. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///   list[list[str]]: layer name per edge, grouped per source node.
    #[getter]
    pub fn layer_name(&self) -> Result<Vec<Vec<String>>, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.layer_name().await })
    }

    /// The earliest event time of each edge, grouped per source node.
    /// Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///   list[list[Optional[EventTime]]]: earliest times, grouped per source node.
    #[getter]
    pub fn earliest_time(&self) -> Result<Vec<Vec<Option<EventTime>>>, ClientError> {
        let edges = Arc::clone(&self.edges);
        Ok(
            execute_async_task(move || async move { edges.earliest_time().await })?
                .into_iter()
                .map(|row| row.into_iter().map(|o| o).collect())
                .collect(),
        )
    }

    /// The latest event time of each edge, grouped per source node. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///   list[list[Optional[EventTime]]]: latest times, grouped per source node.
    #[getter]
    pub fn latest_time(&self) -> Result<Vec<Vec<Option<EventTime>>>, ClientError> {
        let edges = Arc::clone(&self.edges);
        Ok(
            execute_async_task(move || async move { edges.latest_time().await })?
                .into_iter()
                .map(|row| row.into_iter().map(|o| o).collect())
                .collect(),
        )
    }

    /// The event time of each edge, grouped per source node. Only valid once
    /// the edges have been exploded; raises otherwise. Property — attribute
    /// access fires one RPC.
    ///
    /// Returns:
    ///   list[list[Optional[EventTime]]]: event times, grouped per source node.
    #[getter]
    pub fn time(&self) -> Result<Vec<Vec<Option<EventTime>>>, ClientError> {
        let edges = Arc::clone(&self.edges);
        Ok(
            execute_async_task(move || async move { edges.time().await })?
                .into_iter()
                .map(|row| row.into_iter().map(|o| o).collect())
                .collect(),
        )
    }

    /// The non-temporal metadata of this collection as a nested columnar view.
    /// Each accessor returns one value per edge, grouped per source. Lazy —
    /// no RPC.
    ///
    /// Returns:
    ///     RemoteMetadataView: the nested columnar metadata view of this collection.
    #[getter]
    pub fn metadata(&self) -> PyRemoteMetadataView {
        PyRemoteMetadataView::new(self.edges.metadata())
    }

    /// The properties of this collection as a nested columnar view. Each
    /// accessor returns one value per edge, grouped per source. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemotePropertiesView: the nested columnar properties view of this collection.
    #[getter]
    pub fn properties(&self) -> PyRemotePropertiesView {
        PyRemotePropertiesView::new(self.edges.properties())
    }

    /// Whether each edge is active (has an event) in the current view, grouped
    /// per source node. Method — mirrors the local `NestedEdges.is_active`.
    /// Fires one RPC.
    ///
    /// Returns:
    ///   list[list[bool]]: one flag per edge, grouped per source node.
    pub fn is_active(&self) -> Result<Vec<Vec<bool>>, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.is_active().await })
    }

    /// Whether each edge is valid (not deleted) at the current time, grouped
    /// per source node. Method — mirrors the local `NestedEdges.is_valid`.
    /// Fires one RPC.
    ///
    /// Returns:
    ///   list[list[bool]]: one flag per edge, grouped per source node.
    pub fn is_valid(&self) -> Result<Vec<Vec<bool>>, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.is_valid().await })
    }

    /// Whether each edge has been deleted at the current time, grouped per
    /// source node. Method — mirrors the local `NestedEdges.is_deleted`. Fires
    /// one RPC.
    ///
    /// Returns:
    ///   list[list[bool]]: one flag per edge, grouped per source node.
    pub fn is_deleted(&self) -> Result<Vec<Vec<bool>>, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.is_deleted().await })
    }

    /// Whether each edge is a self-loop (`src == dst`), grouped per source
    /// node. Method — mirrors the local `NestedEdges.is_self_loop`. Fires one
    /// RPC.
    ///
    /// Returns:
    ///   list[list[bool]]: one flag per edge, grouped per source node.
    pub fn is_self_loop(&self) -> Result<Vec<Vec<bool>>, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.is_self_loop().await })
    }

    /// The size of the window covered by this view (`end - start`), or `None`
    /// if the view is unbounded. Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///     Optional[int]: the size of the window, or `None` if the view is unbounded.
    #[getter]
    pub fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.window_size().await })
    }

    /// `len(edges)` — number of source edge collections. Fires one RPC.
    pub fn __len__(&self) -> Result<usize, ClientError> {
        let edges = Arc::clone(&self.edges);
        Ok(execute_async_task(move || async move { edges.count().await })?.max(0) as usize)
    }

    /// `bool(edges)` — whether the collection is non-empty. Fires one RPC.
    pub fn __bool__(&self) -> Result<bool, ClientError> {
        let edges = Arc::clone(&self.edges);
        Ok(execute_async_task(move || async move { edges.count().await })? > 0)
    }

    /// View start bound for this collection — `None` if unbounded. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///     OptionalEventTime: the view start bound, or empty if unbounded.
    #[getter]
    pub fn start(&self) -> Result<PyOptionalEventTime, ClientError> {
        let edges = Arc::clone(&self.edges);
        Ok(execute_async_task(
            move || async move { edges.start().await },
        )?.into())
    }

    /// View end bound for this collection — `None` if unbounded. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///     OptionalEventTime: the view end bound, or empty if unbounded.
    #[getter]
    pub fn end(&self) -> Result<PyOptionalEventTime, ClientError> {
        let edges = Arc::clone(&self.edges);
        Ok(execute_async_task(
            move || async move { edges.end().await },
        )?.into())
    }

    /// Materialize this collection as a nested list of `RemoteEdge` handles —
    /// one inner list per source node. Fires one RPC. Each returned edge is
    /// rebased under the same view chain that produced this collection.
    ///
    /// Returns:
    ///   list[list[RemoteEdge]]: the incident edges grouped per source node.
    pub fn collect(&self) -> Result<Vec<Vec<PyRemoteEdge>>, ClientError> {
        let edges = Arc::clone(&self.edges);
        let result = execute_async_task(move || async move { edges.collect().await })?;
        Ok(result
            .into_iter()
            .map(|row| row.into_iter().map(PyRemoteEdge::new).collect())
            .collect())
    }

    /// Enables `for row in remote_nested_edges:` — fetches everything in one
    /// RPC, then yields each per-source `list[RemoteEdge]`.
    fn __iter__(&self) -> Result<PyRemoteNestedEdgesIter, ClientError> {
        let list = self.collect()?;
        Ok(PyRemoteNestedEdgesIter {
            inner: list.into_iter(),
        })
    }
}

#[pyclass(name = "RemoteNestedEdgesIter", module = "raphtory.graphql")]
pub struct PyRemoteNestedEdgesIter {
    inner: std::vec::IntoIter<Vec<PyRemoteEdge>>,
}

#[pymethods]
impl PyRemoteNestedEdgesIter {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<Vec<PyRemoteEdge>> {
        slf.inner.next()
    }
}
