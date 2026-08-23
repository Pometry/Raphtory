use super::view_ops::py_remote_view_ops;
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
use raphtory_api::{
    core::{entities::GID, storage::timeindex::EventTime, utils::time::InputTime},
    python::timeindex::PyOptionalEventTime,
};
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

    /// Narrow this collection's membership by a filter expression — edge or
    /// node predicates, graph views, or and/or/not combinations of them —
    /// applies only at this step; downstream traversals see the unfiltered
    /// graph. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection narrowed to matching edges.
    ///
    /// Raises:
    ///     ValueError: if the filter cannot be sent over the wire.
    pub fn select(&self, filter: PyFilterExpr) -> PyResult<PyRemoteNestedEdges> {
        let tree = filter
            .try_as_filter_tree()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemoteNestedEdges::new(self.edges.select(tree)?))
    }

    /// `edges[filter]` — narrow this collection's membership by a filter
    /// expression, the sugar form of `.select(filter)` (matches the local
    /// `NestedEdges.__getitem__`). Edge predicates, node predicates, graph
    /// views and mixed combinations all apply. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemoteNestedEdges: a new collection narrowed to matching edges.
    ///
    /// Raises:
    ///     ValueError: if the filter cannot be sent over the wire.
    fn __getitem__(&self, filter: PyFilterExpr) -> PyResult<PyRemoteNestedEdges> {
        self.select(filter)
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
    ///   list[list[tuple[str | int, str | int]]]: id pairs grouped per
    ///   source node — endpoint ids are strings for string-indexed graphs,
    ///   integers for integer-indexed ones.
    #[getter]
    pub fn id(&self) -> Result<Vec<Vec<(GID, GID)>>, ClientError> {
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
        execute_async_task(move || async move { edges.earliest_time().await })
    }

    /// The latest event time of each edge, grouped per source node. Property —
    /// attribute access fires one RPC.
    ///
    /// Returns:
    ///   list[list[Optional[EventTime]]]: latest times, grouped per source node.
    #[getter]
    pub fn latest_time(&self) -> Result<Vec<Vec<Option<EventTime>>>, ClientError> {
        let edges = Arc::clone(&self.edges);
        execute_async_task(move || async move { edges.latest_time().await })
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
        execute_async_task(move || async move { edges.time().await })
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
        Ok(execute_async_task(move || async move { edges.len().await })?.max(0) as usize)
    }

    /// `bool(edges)` — whether the collection is non-empty. Fires one RPC.
    pub fn __bool__(&self) -> Result<bool, ClientError> {
        let edges = Arc::clone(&self.edges);
        Ok(execute_async_task(move || async move { edges.len().await })? > 0)
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

py_remote_view_ops!(PyRemoteNestedEdges, edges, "RemoteNestedEdges");
