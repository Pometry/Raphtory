use super::view_ops::py_remote_view_ops;
use crate::{
    client::{op::input_time_from_parts, remote_edge::RemoteEdge, ClientError},
    python::client::{
        remote_edges::PyRemoteEdges,
        remote_history::PyRemoteHistory,
        remote_metadata::{PyRemoteMetadata, PyRemoteProperties},
        remote_node::PyRemoteNode,
    },
};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyResult};
use raphtory::python::{filter::filter_expr::PyFilterExpr, utils::execute_async_task};
use raphtory_api::{
    core::{
        entities::{properties::prop::Prop, GID},
        storage::timeindex::{AsTime, EventTime},
        utils::time::InputTime,
    },
    python::timeindex::{EventTimeComponent, PyOptionalEventTime},
};
use std::{collections::HashMap, sync::Arc};

/// A remote edge reference
///
/// Returned by [RemoteGraph.edge][raphtory.graphql.RemoteGraph.edge],
/// [RemoteGraph.add_edge][raphtory.graphql.RemoteGraph.add_edge],
/// and [RemoteGraph.delete_edge][raphtory.graphql.RemoteGraph.delete_edge].
#[derive(Clone)]
#[pyclass(name = "RemoteEdge", module = "raphtory.graphql", from_py_object)]
pub struct PyRemoteEdge {
    pub(crate) edge: Arc<RemoteEdge>,
}

impl PyRemoteEdge {
    pub(crate) fn new(edge: RemoteEdge) -> Self {
        PyRemoteEdge {
            edge: Arc::new(edge),
        }
    }
}

#[pymethods]
impl PyRemoteEdge {
    /// Return a filtered view of this edge — the filter propagates to
    /// everything reached through it. Accepts node or edge filter
    /// expressions; mirrors the local `Edge.filter`. Lazy — no RPC.
    ///
    /// Arguments:
    ///     filter (FilterExpr): a filter expression from `raphtory.filter`.
    ///
    /// Returns:
    ///     RemoteEdge: a new filtered edge view.
    ///
    /// Raises:
    ///     ValueError: if the filter cannot be represented remotely.
    pub fn filter(&self, filter: PyFilterExpr) -> PyResult<PyRemoteEdge> {
        let tree = filter
            .try_as_filter_tree()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyRemoteEdge::new(self.edge.filter(tree)?))
    }

    /// Add updates to an edge in the remote graph at a specified time.
    ///
    /// This function allows for the addition of property updates to an edge within the graph.
    /// The updates are time-stamped, meaning they are applied at the specified time.
    ///
    /// Arguments:
    ///   t (int | str | datetime): The timestamp at which the updates should be applied.
    ///   properties (dict[str, PropValue], optional): A dictionary of properties to update.
    ///   layer (str, optional): The layer you want the updates to be applied.
    ///   event_id (int, optional): Secondary index to disambiguate multiple
    ///       updates at the same timestamp. If omitted, the server auto-increments it.
    ///
    /// Returns:
    ///   None:
    #[pyo3(signature = (t, properties=None, layer=None, event_id=None))]
    fn add_updates(
        &self,
        t: EventTimeComponent,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
        event_id: Option<usize>,
    ) -> Result<(), ClientError> {
        let edge = Arc::clone(&self.edge);
        let layer_str = layer.map(|s| s.to_string());

        let task = move || async move {
            edge.add_updates(
                input_time_from_parts(t.t(), event_id),
                properties.into_iter().flatten(),
                layer_str,
            )
            .await
        };
        execute_async_task(task)?;

        Ok(())
    }

    /// Mark the edge as deleted at the specified time.
    ///
    /// Arguments:
    ///   t (int | str | datetime): The timestamp at which the deletion should be applied.
    ///   layer (str, optional): The layer you want the deletion applied to.
    ///   event_id (int, optional): Secondary index to disambiguate multiple
    ///       updates at the same timestamp. If omitted, the server auto-increments it.
    ///
    /// Returns:
    ///   None:
    ///
    /// Raises:
    ///   GraphError: If the operation fails.
    #[pyo3(signature = (t, layer=None, event_id=None))]
    fn delete(
        &self,
        t: EventTimeComponent,
        layer: Option<&str>,
        event_id: Option<usize>,
    ) -> Result<(), ClientError> {
        let edge = Arc::clone(&self.edge);
        let layer_str = layer.map(|s| s.to_string());

        let task = move || async move {
            edge.delete(input_time_from_parts(t.t(), event_id), layer_str)
                .await
        };
        execute_async_task(task)?;

        Ok(())
    }

    /// Add metadata to the edge within the remote graph.
    /// This function is used to add metadata to an edge that does not
    /// change over time. This metadata is fundamental information of the edge.
    ///
    /// Arguments:
    ///   metadata (dict[str, PropValue]): A dictionary of metadata to be added to the edge.
    ///   layer (str, optional): The layer you want this metadata to be added on to.
    ///
    /// Returns:
    ///   None:
    #[pyo3(signature = (metadata, layer=None))]
    fn add_metadata(
        &self,
        metadata: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), ClientError> {
        let edge = Arc::clone(&self.edge);
        let layer_str = layer.map(|s| s.to_string());

        let task = move || async move { edge.add_metadata(metadata, layer_str).await };
        execute_async_task(task)?;

        Ok(())
    }

    /// Update metadata of an edge in the remote graph overwriting existing values.
    /// This function is used to add properties to an edge that does not
    /// change over time. These properties are fundamental attributes of the edge.
    ///
    /// Arguments:
    ///   metadata (dict[str, PropValue]): A dictionary of properties to be added to the edge.
    ///   layer (str, optional): The layer you want these properties to be added on to.
    ///
    /// Returns:
    ///   None:
    #[pyo3(signature = (metadata, layer=None))]
    pub fn update_metadata(
        &self,
        metadata: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), ClientError> {
        let edge = Arc::clone(&self.edge);
        let layer_str = layer.map(|s| s.to_string());

        let task = move || async move { edge.update_metadata(metadata, layer_str).await };
        execute_async_task(task)?;

        Ok(())
    }

    /// Navigate to this edge's source node. Lazy — no RPC.
    ///
    /// Returns:
    ///   RemoteNode: a handle to the source node, carrying the accumulated view chain.
    #[getter]
    pub fn src(&self) -> PyRemoteNode {
        PyRemoteNode::new(self.edge.src())
    }

    /// Navigate to this edge's destination node. Property — lazy, no RPC.
    ///
    /// Returns:
    ///   RemoteNode: a handle to the destination node, carrying the accumulated view chain.
    #[getter]
    pub fn dst(&self) -> PyRemoteNode {
        PyRemoteNode::new(self.edge.dst())
    }

    /// Navigate to the "other end" node — destination on out-edges, source
    /// on in-edges. Property — lazy, no RPC.
    ///
    /// Returns:
    ///     RemoteNode: a handle to the other-end node, carrying the accumulated view chain.
    #[getter]
    pub fn nbr(&self) -> PyRemoteNode {
        PyRemoteNode::new(self.edge.nbr())
    }

    /// Earliest event time on this edge under the current view. `None` if the
    /// edge has no events in the view. Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///     OptionalEventTime: the earliest event time on the edge, or empty if it has no
    ///         events in view.
    #[getter]
    pub fn earliest_time(&self) -> Result<PyOptionalEventTime, ClientError> {
        let edge = Arc::clone(&self.edge);
        Ok(execute_async_task(move || async move { edge.earliest_time().await })?.into())
    }

    /// Latest event time on this edge under the current view. Property — RPC.
    ///
    /// Returns:
    ///     OptionalEventTime: the latest event time on the edge, or empty if it has no
    ///         events in view.
    #[getter]
    pub fn latest_time(&self) -> Result<PyOptionalEventTime, ClientError> {
        let edge = Arc::clone(&self.edge);
        Ok(execute_async_task(move || async move { edge.latest_time().await })?.into())
    }

    /// The event time this exploded edge event happened at. Meaningful
    /// primarily on `explode()`'d views. Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///     OptionalEventTime: the event time of this exploded edge event, or empty if
    ///         there is none.
    #[getter]
    pub fn time(&self) -> Result<PyOptionalEventTime, ClientError> {
        let edge = Arc::clone(&self.edge);
        Ok(execute_async_task(move || async move { edge.time().await })?.into())
    }

    /// View start bound as seen by this edge. Property — fires one RPC.
    ///
    /// Returns:
    ///     OptionalEventTime: the view start bound, or empty if unbounded.
    #[getter]
    pub fn start(&self) -> Result<PyOptionalEventTime, ClientError> {
        let edge = Arc::clone(&self.edge);
        Ok(execute_async_task(move || async move { edge.start().await })?.into())
    }

    /// View end bound as seen by this edge. Property — fires one RPC.
    ///
    /// Returns:
    ///     OptionalEventTime: the view end bound, or empty if unbounded.
    #[getter]
    pub fn end(&self) -> Result<PyOptionalEventTime, ClientError> {
        let edge = Arc::clone(&self.edge);
        Ok(execute_async_task(move || async move { edge.end().await })?.into())
    }

    /// Edge id as a `(src, dst)` pair of endpoint ids. Property — fires one RPC.
    ///
    /// Returns:
    ///     tuple[str | int, str | int]: the `(src, dst)` pair of endpoint
    ///         ids — strings for string-indexed graphs, integers for
    ///         integer-indexed ones.
    #[getter]
    pub fn id(&self) -> Result<(GID, GID), ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.id().await })
    }

    /// Layer names this edge is present in. Property — fires one RPC.
    ///
    /// Returns:
    ///     list[str]: the layer names the edge is present in.
    #[getter]
    pub fn layer_names(&self) -> Result<Vec<String>, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.layer_names().await })
    }

    /// Single layer name for a layer-restricted view of this edge. Raises if
    /// the edge isn't scoped to exactly one layer. Property — fires one RPC.
    ///
    /// Returns:
    ///     str: the single layer name of this view.
    #[getter]
    pub fn layer_name(&self) -> Result<String, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.layer_name().await })
    }

    /// Whether the edge has any events in the current view. Fires one RPC.
    ///
    /// Returns:
    ///     bool: True if the edge has events in the current view.
    pub fn is_active(&self) -> Result<bool, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.is_active().await })
    }

    /// Whether the edge is valid at the current time. Fires one RPC.
    ///
    /// Returns:
    ///     bool: True if the edge is valid at the current time.
    pub fn is_valid(&self) -> Result<bool, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.is_valid().await })
    }

    /// Whether the edge has been deleted at the current time. Fires one RPC.
    ///
    /// Returns:
    ///     bool: True if the edge has been deleted at the current time.
    pub fn is_deleted(&self) -> Result<bool, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.is_deleted().await })
    }

    /// Whether the edge is a self-loop (src == dst). Fires one RPC.
    ///
    /// Returns:
    ///     bool: True if the edge is a self-loop.
    pub fn is_self_loop(&self) -> Result<bool, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.is_self_loop().await })
    }

    /// Check if this view has a layer named `name`. Fires one RPC.
    ///
    /// Arguments:
    ///     name (str): the name of the layer to check.
    ///
    /// Returns:
    ///     bool: True if the layer is present.
    pub fn has_layer(&self, name: &str) -> Result<bool, ClientError> {
        let edge = Arc::clone(&self.edge);
        let name = name.to_string();
        execute_async_task(move || async move { edge.has_layer(name).await })
    }

    /// The size of the window covered by this view (`end - start`), or `None`
    /// if the view is unbounded. Property — attribute access fires one RPC.
    ///
    /// Returns:
    ///     Optional[int]: the size of the window, or `None` if the view is unbounded.
    #[getter]
    pub fn window_size(&self) -> Result<Option<i64>, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.window_size().await })
    }

    /// The event history of this edge — a `RemoteHistory` container with
    /// terminals like `count()`, `collect()`, `earliest_time()`, and the
    /// `.t` / `.dt` / `.event_id` / `.intervals` sub-container accessors.
    /// Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteHistory: the edge's event history.
    #[getter]
    pub fn history(&self) -> PyRemoteHistory {
        PyRemoteHistory::new(self.edge.history())
    }

    /// The deletion history of this edge — a `RemoteHistory` container
    /// tracking the times at which the edge was marked deleted. Distinct
    /// from `history` which tracks all events. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteHistory: the edge's deletion history.
    #[getter]
    pub fn deletions(&self) -> PyRemoteHistory {
        PyRemoteHistory::new(self.edge.deletions())
    }

    /// Fan out this edge into one entry per event — returns a `RemoteEdges`
    /// with each member a single-event edge instance. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteEdges: one entry per event of this edge.
    pub fn explode(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edge.explode())
    }

    /// Fan out this edge into one entry per layer — returns a `RemoteEdges`
    /// with each member a single-layer edge instance. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteEdges: one entry per layer of this edge.
    pub fn explode_layers(&self) -> PyRemoteEdges {
        PyRemoteEdges::new(self.edge.explode_layers())
    }

    /// The non-temporal metadata container of this edge. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteMetadata: the edge's metadata container.
    #[getter]
    pub fn metadata(&self) -> PyRemoteMetadata {
        PyRemoteMetadata::new(self.edge.metadata())
    }

    /// The full properties container of this edge (temporal + metadata).
    /// Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteProperties: the edge's properties container.
    #[getter]
    pub fn properties(&self) -> PyRemoteProperties {
        PyRemoteProperties::new(self.edge.properties())
    }
}

py_remote_view_ops!(PyRemoteEdge, edge, "RemoteEdge");
