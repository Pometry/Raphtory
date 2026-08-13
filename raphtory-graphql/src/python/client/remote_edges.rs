use crate::{
    client::{remote_edges::RemoteEdges, ClientError},
    python::client::{
        remote_collection_metadata::{PyRemoteMetadataView, PyRemotePropertiesView},
        remote_edge::PyRemoteEdge,
        remote_path_from_node::PyRemotePathFromNode,
        remote_sorting::PyEdgeSortBy,
    },
};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyRef, PyRefMut, PyResult};
use raphtory::python::{filter::filter_expr::PyFilterExpr, utils::execute_async_task};
use raphtory_api::{
    core::{storage::timeindex::EventTime, utils::time::InputTime},
    python::timeindex::PyOptionalEventTime,
};
use std::sync::Arc;

/// A handle to a remote collection of edges.
///
/// Returned by [RemoteGraph.edges][raphtory.graphql.RemoteGraph.edges] and by
/// [RemoteNode.edges][raphtory.graphql.RemoteNode.edges] /
/// [RemoteNode.in_edges][raphtory.graphql.RemoteNode.in_edges] /
/// [RemoteNode.out_edges][raphtory.graphql.RemoteNode.out_edges].
///
/// Edges are identified by `(src, dst)` pairs rather than a single-string id;
/// the `.id` accessor returns those `(src, dst)` pairs. Terminals include
/// `count()` and `collect()`.
#[derive(Clone)]
#[pyclass(name = "RemoteEdges", module = "raphtory.graphql", from_py_object)]
pub struct PyRemoteEdges {
    pub(crate) edges: Arc<RemoteEdges>,
}

impl PyRemoteEdges {
    pub(crate) fn new(edges: RemoteEdges) -> Self {
        Self {
            edges: Arc::new(edges),
        }
    }
}

#[pymethods]
impl PyRemoteEdges {
    /// Time-window this collection. Lazy — no RPC.
    ///
    /// Arguments:
    ///     start (TimeInput): inclusive start of the window.
    ///     end (TimeInput): exclusive end of the window.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection restricted to the window.
    pub fn window(&self, start: InputTime, end: InputTime) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.window(start, end))
    }

    /// Restrict to a single named layer. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection restricted to that layer.
    pub fn layer(&self, name: &str) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.layer(name))
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): the time to snapshot at.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection snapshotted at that time.
    pub fn at(&self, time: InputTime) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.at(time))
    }

    /// Restrict to events strictly before the given time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): only events strictly before this time are kept.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection restricted to events before that time.
    pub fn before(&self, time: InputTime) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.before(time))
    }

    /// Restrict to events strictly after the given time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): only events strictly after this time are kept.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection restricted to events after that time.
    pub fn after(&self, time: InputTime) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.after(time))
    }

    /// Latest state. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection of the latest state.
    pub fn latest(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.latest())
    }

    /// Snapshot at the latest time. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection snapshotted at the latest time.
    pub fn snapshot_latest(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.snapshot_latest())
    }

    /// Snapshot at a specific time. Lazy — no RPC.
    ///
    /// Arguments:
    ///     time (TimeInput): the time to snapshot at.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection snapshotted at that time.
    pub fn snapshot_at(&self, time: InputTime) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.snapshot_at(time))
    }

    /// Exclude a specific layer. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer to exclude.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection with that layer excluded.
    pub fn exclude_layer(&self, name: &str) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.exclude_layer(name))
    }

    /// Shrink both start and end of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     start (TimeInput): the new inclusive start of the window.
    ///     end (TimeInput): the new exclusive end of the window.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection with both window bounds shrunk.
    pub fn shrink_window(&self, start: InputTime, end: InputTime) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.shrink_window(start, end))
    }

    /// Shrink the start of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     start (TimeInput): the new inclusive start of the window.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection with the window start shrunk.
    pub fn shrink_start(&self, start: InputTime) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.shrink_start(start))
    }

    /// Shrink the end of the current window. Lazy — no RPC.
    ///
    /// Arguments:
    ///     end (TimeInput): the new exclusive end of the window.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection with the window end shrunk.
    pub fn shrink_end(&self, end: InputTime) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.shrink_end(end))
    }

    /// Restrict to the default layer. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection restricted to the default layer.
    pub fn default_layer(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.default_layer())
    }

    /// Restrict to the given set of layers. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the layers.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection restricted to those layers.
    pub fn layers(&self, names: Vec<String>) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.layers(names))
    }

    /// Exclude the given set of layers. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the layers to exclude.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection with those layers excluded.
    pub fn exclude_layers(&self, names: Vec<String>) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.exclude_layers(names))
    }

    /// Restrict to the given set of valid layers. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the valid layers.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection restricted to those valid layers.
    pub fn valid_layers(&self, names: Vec<String>) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.valid_layers(names))
    }

    /// Exclude a specific valid layer from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the valid layer to exclude.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection with that valid layer excluded.
    pub fn exclude_valid_layer(&self, name: &str) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.exclude_valid_layer(name))
    }

    /// Exclude the given set of valid layers from the view. Lazy — no RPC.
    ///
    /// Arguments:
    ///     names (list[str]): the names of the valid layers to exclude.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection with those valid layers excluded.
    pub fn exclude_valid_layers(&self, names: Vec<String>) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.exclude_valid_layers(names))
    }

    /// Fan out this collection into one entry per event. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection with one entry per event.
    pub fn explode(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.explode())
    }

    /// Fan out this collection into one entry per layer per edge. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection with one entry per layer per edge.
    pub fn explode_layers(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edges.explode_layers())
    }

    /// Reorder this collection by an ordered list of sort keys. Multi-key
    /// sort is lexicographic (ties on key 1 break to key 2). Lazy — no RPC.
    ///
    /// Arguments:
    ///     sort_bys (list[EdgeSortBy]): the ordered sort keys.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection in the sorted order.
    pub fn sorted(&self, sort_bys: Vec<PyEdgeSortBy>) -> PyRemoteEdges {
        let inner: Vec<_> = sort_bys.into_iter().map(|s| s.inner).collect();
        PyRemoteEdges::new(self.edges.sorted(inner))
    }

    /// Filter this collection by a filter expression. **The filter
    /// propagates**: it applies to the current collection's membership *and*
    /// to downstream traversals from the matching edges. For a
    /// narrow-here-only variant, use `.select(...)`. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection with the filter applied.
    ///
    /// Raises:
    ///     ValueError: if the filter cannot be represented as a GraphQL
    ///         `EdgeFilter` (e.g. references node-only fields).
    pub fn filter(&self, filter: PyFilterExpr) -> PyResult<PyRemoteEdges> {
        let tree = filter
            .try_as_filter_tree()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemoteEdges::new(self.edges.filter(tree)?))
    }

    /// Narrow this collection's membership by a filter expression — edge or
    /// node predicates, graph views, or and/or/not combinations of them.
    /// Unlike `.filter()`, the filter applies **only at this step** —
    /// downstream traversals from the matching edges see the unfiltered
    /// graph. Use `.filter()` for the propagating variant. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection narrowed to matching edges.
    ///
    /// Raises:
    ///     ValueError: if the filter cannot be sent over the wire.
    pub fn select(&self, filter: PyFilterExpr) -> PyResult<PyRemoteEdges> {
        let tree = filter
            .try_as_filter_tree()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemoteEdges::new(self.edges.select(tree)?))
    }

    /// `edges[filter]` — narrow this collection's membership by a filter
    /// expression, the sugar form of `.select(filter)` (matches the local
    /// `Edges.__getitem__`). Edge predicates, node predicates, graph views
    /// and mixed combinations all apply. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemoteEdges: a new collection narrowed to matching edges.
    ///
    /// Raises:
    ///     ValueError: if the filter cannot be sent over the wire.
    fn __getitem__(&self, filter: PyFilterExpr) -> PyResult<PyRemoteEdges> {
        self.select(filter)
    }

    /// Returns the number of edges in this collection. Fires one RPC.
    ///
    /// Returns:
    ///   int: the number of edges.
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

    /// The source node of each edge in this collection, as a flat
    /// `RemotePathFromNode`. Mirrors the local `Edges.src`. Property — lazy;
    /// attribute access fires no RPC.
    ///
    /// Returns:
    ///   RemotePathFromNode: the source nodes, in collection order.
    #[getter]
    pub fn src(&self) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.edges.src())
    }

    /// The destination node of each edge in this collection, as a flat
    /// `RemotePathFromNode`. Mirrors the local `Edges.dst`. Property — lazy;
    /// attribute access fires no RPC.
    ///
    /// Returns:
    ///   RemotePathFromNode: the destination nodes, in collection order.
    #[getter]
    pub fn dst(&self) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.edges.dst())
    }

    /// The node at the other end of each edge (destination for out-edges,
    /// source for in-edges), as a flat `RemotePathFromNode`. Mirrors the local
    /// `Edges.nbr`. Property — lazy; attribute access fires no RPC.
    ///
    /// Returns:
    ///   RemotePathFromNode: the other-end nodes, in collection order.
    #[getter]
    pub fn nbr(&self) -> PyRemotePathFromNode {
        PyRemotePathFromNode::new(self.edges.nbr())
    }

    /// The `(src, dst)` id pair of each edge in this collection. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///   list[tuple[str, str]]: the id pairs, in collection order.
    #[getter]
    pub fn id(&self) -> Result<Vec<(String, String)>, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.id().await })
    }

    /// The layer names of each edge in this collection. Property — attribute
    /// access fires one RPC.
    ///
    /// Returns:
    ///   list[list[str]]: the layer names per edge, in collection order.
    #[getter]
    pub fn layer_names(&self) -> Result<Vec<Vec<String>>, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.layer_names().await })
    }

    /// The single layer name of each edge in this collection. Only valid once
    /// the edges have been exploded via `.explode()` / `.explode_layers()`;
    /// raises otherwise. Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///   list[str]: the layer name per edge, in collection order.
    #[getter]
    pub fn layer_name(&self) -> Result<Vec<String>, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.layer_name().await })
    }

    /// The earliest event time of each edge in this collection. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///   list[Optional[EventTime]]: the earliest times, in collection order.
    #[getter]
    pub fn earliest_time(&self) -> Result<Vec<Option<EventTime>>, ClientError> {
        let edges = Arc::clone(&self.edges);
        Ok(
            execute_async_task(move || async move { edges.earliest_time().await })?
                .into_iter()
                .map(|o| o)
                .collect(),
        )
    }

    /// The latest event time of each edge in this collection. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///   list[Optional[EventTime]]: the latest times, in collection order.
    #[getter]
    pub fn latest_time(&self) -> Result<Vec<Option<EventTime>>, ClientError> {
        let edges = Arc::clone(&self.edges);
        Ok(
            execute_async_task(move || async move { edges.latest_time().await })?
                .into_iter()
                .map(|o| o)
                .collect(),
        )
    }

    /// The event time of each edge in this collection. Only valid once the
    /// edges have been exploded via `.explode()`; raises otherwise. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///   list[Optional[EventTime]]: the event times, in collection order.
    #[getter]
    pub fn time(&self) -> Result<Vec<Option<EventTime>>, ClientError> {
        let edges = Arc::clone(&self.edges);
        Ok(
            execute_async_task(move || async move { edges.time().await })?
                .into_iter()
                .map(|o| o)
                .collect(),
        )
    }

    /// Whether each edge is active (has an event) in the current view. Method
    /// — mirrors the local `Edges.is_active`. Fires one RPC.
    ///
    /// Returns:
    ///   list[bool]: one flag per edge, in collection order.
    pub fn is_active(&self) -> Result<Vec<bool>, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.is_active().await })
    }

    /// Whether each edge is valid (not deleted) at the current time. Method —
    /// mirrors the local `Edges.is_valid`. Fires one RPC.
    ///
    /// Returns:
    ///   list[bool]: one flag per edge, in collection order.
    pub fn is_valid(&self) -> Result<Vec<bool>, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.is_valid().await })
    }

    /// Whether each edge has been deleted at the current time. Method —
    /// mirrors the local `Edges.is_deleted`. Fires one RPC.
    ///
    /// Returns:
    ///   list[bool]: one flag per edge, in collection order.
    pub fn is_deleted(&self) -> Result<Vec<bool>, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.is_deleted().await })
    }

    /// Whether each edge is a self-loop (`src == dst`). Method — mirrors the
    /// local `Edges.is_self_loop`. Fires one RPC.
    ///
    /// Returns:
    ///   list[bool]: one flag per edge, in collection order.
    pub fn is_self_loop(&self) -> Result<Vec<bool>, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.is_self_loop().await })
    }

    /// The non-temporal metadata of this collection as a columnar view. Each
    /// accessor returns one value per edge. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteMetadataView: the columnar metadata view of this collection.
    #[getter]
    pub fn metadata(&self) -> PyRemoteMetadataView {
        PyRemoteMetadataView::new(self.edges.metadata())
    }

    /// The properties of this collection as a columnar view. Each accessor
    /// returns one value per edge. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemotePropertiesView: the columnar properties view of this collection.
    #[getter]
    pub fn properties(&self) -> PyRemotePropertiesView {
        PyRemotePropertiesView::new(self.edges.properties())
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

    /// `len(edges)` — number of edges in the collection. Fires one RPC.
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
        Ok(execute_async_task(move || async move { edges.start().await })?.into())
    }

    /// View end bound for this collection — `None` if unbounded. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///     OptionalEventTime: the view end bound, or empty if unbounded.
    #[getter]
    pub fn end(&self) -> Result<PyOptionalEventTime, ClientError> {
        let edges = Arc::clone(&self.edges);
        Ok(execute_async_task(move || async move { edges.end().await })?.into())
    }

    /// Materialize this collection as a list of `RemoteEdge` handles.
    ///
    /// Fires one RPC (to fetch each edge's `(src, dst)` pair); each returned
    /// edge is rebased under the view chain that produced this collection.
    ///
    /// Returns:
    ///   list[RemoteEdge]: one handle per edge in the collection.
    pub fn collect(&self) -> Result<Vec<PyRemoteEdge>, ClientError> {
        let edges = Arc::clone(&self.edges);
        let result = execute_async_task(move || async move { edges.collect().await })?;
        Ok(result.into_iter().map(PyRemoteEdge::new).collect())
    }

    /// Enables `for e in remote_edges:` — fetches all `(src, dst)` pairs in
    /// one RPC, then yields a `RemoteEdge` handle for each. Edge handles are
    /// not batched: each terminal on a yielded edge fires its own RPC.
    fn __iter__(&self) -> Result<PyRemoteEdgesIter, ClientError> {
        let list = self.collect()?;
        Ok(PyRemoteEdgesIter {
            inner: list.into_iter(),
        })
    }
}

/// Opaque iterator returned by `PyRemoteEdges::__iter__`.
///
/// Not intended to be constructed directly — Python creates it via
/// `iter(remote_edges)` (or under the hood in a `for` loop).
#[pyclass(name = "RemoteEdgesIter", module = "raphtory.graphql")]
pub struct PyRemoteEdgesIter {
    inner: std::vec::IntoIter<PyRemoteEdge>,
}

#[pymethods]
impl PyRemoteEdgesIter {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<PyRemoteEdge> {
        slf.inner.next()
    }
}
