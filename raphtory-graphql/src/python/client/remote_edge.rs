use crate::{
    client::{remote_edge::RemoteEdge, ClientError},
    python::client::remote_node::PyRemoteNode,
};
use pyo3::{pyclass, pymethods};
use raphtory::python::utils::execute_async_task;
use raphtory_api::core::{entities::properties::prop::Prop, storage::timeindex::EventTime};
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
    /// Add updates to an edge in the remote graph at a specified time.
    ///
    /// This function allows for the addition of property updates to an edge within the graph.
    /// The updates are time-stamped, meaning they are applied at the specified time.
    ///
    /// Arguments:
    ///   t (int | str | datetime): The timestamp at which the updates should be applied.
    ///   properties (dict[str, PropValue], optional): A dictionary of properties to update.
    ///   layer (str, optional): The layer you want the updates to be applied.
    ///
    /// Returns:
    ///   None:
    #[pyo3(signature = (t, properties=None, layer=None))]
    fn add_updates(
        &self,
        t: EventTime,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
    ) -> Result<(), ClientError> {
        let edge = Arc::clone(&self.edge);
        let layer_str = layer.map(|s| s.to_string());

        let task = move || async move { edge.add_updates(t, properties, layer_str).await };
        execute_async_task(task)?;

        Ok(())
    }

    /// Mark the edge as deleted at the specified time.
    ///
    /// Arguments:
    ///   t (int | str | datetime): The timestamp at which the deletion should be applied.
    ///   layer (str, optional): The layer you want the deletion applied to.
    ///
    /// Returns:
    ///   None:
    ///
    /// Raises:
    ///   GraphError: If the operation fails.
    #[pyo3(signature = (t, layer=None))]
    fn delete(&self, t: EventTime, layer: Option<&str>) -> Result<(), ClientError> {
        let edge = Arc::clone(&self.edge);
        let layer_str = layer.map(|s| s.to_string());

        let task = move || async move { edge.delete(t, layer_str).await };
        execute_async_task(task)?;

        Ok(())
    }

    /// Add metadata to the edge within the remote graph.
    /// This function is used to add metadata to an edge that does not
    /// change over time. This metadata is fundamental information of the edge.
    ///
    /// Arguments:
    ///   properties (dict[str, PropValue]): A dictionary of properties to be added to the edge.
    ///   layer (str, optional): The layer you want these properties to be added on to.
    ///
    /// Returns:
    ///   None:
    #[pyo3(signature = (properties, layer=None))]
    fn add_metadata(
        &self,
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), ClientError> {
        let edge = Arc::clone(&self.edge);
        let layer_str = layer.map(|s| s.to_string());

        let task = move || async move { edge.add_metadata(properties, layer_str).await };
        execute_async_task(task)?;

        Ok(())
    }

    /// Update metadata of an edge in the remote graph overwriting existing values.
    /// This function is used to add properties to an edge that does not
    /// change over time. These properties are fundamental attributes of the edge.
    ///
    /// Arguments:
    ///   properties (dict[str, PropValue]): A dictionary of properties to be added to the edge.
    ///   layer (str, optional): The layer you want these properties to be added on to.
    ///
    /// Returns:
    ///   None:
    #[pyo3(signature = (properties, layer=None))]
    pub fn update_metadata(
        &self,
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), ClientError> {
        let edge = Arc::clone(&self.edge);
        let layer_str = layer.map(|s| s.to_string());

        let task = move || async move { edge.update_metadata(properties, layer_str).await };
        execute_async_task(task)?;

        Ok(())
    }

    /// Navigate to this edge's source node. Lazy — no RPC.
    ///
    /// Returns:
    ///   RemoteNode: a handle to the source node, carrying the accumulated view chain.
    pub fn src(&self) -> PyRemoteNode {
        PyRemoteNode::new(self.edge.src())
    }

    /// Navigate to this edge's destination node. Lazy — no RPC.
    ///
    /// Returns:
    ///   RemoteNode: a handle to the destination node, carrying the accumulated view chain.
    pub fn dst(&self) -> PyRemoteNode {
        PyRemoteNode::new(self.edge.dst())
    }

    /// Navigate to the "other end" node — destination on out-edges, source
    /// on in-edges. Lazy — no RPC.
    pub fn nbr(&self) -> PyRemoteNode {
        PyRemoteNode::new(self.edge.nbr())
    }

    /// Earliest event time on this edge under the current view. Returns
    /// `None` if the edge has no events in the view. Fires one RPC.
    pub fn earliest_time(&self) -> Result<Option<i64>, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.earliest_time().await })
    }

    /// Latest event time on this edge under the current view. Fires one RPC.
    pub fn latest_time(&self) -> Result<Option<i64>, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.latest_time().await })
    }

    /// First update timestamp on this edge under the current view. Fires one RPC.
    pub fn first_update(&self) -> Result<Option<i64>, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.first_update().await })
    }

    /// Last update timestamp on this edge under the current view. Fires one RPC.
    pub fn last_update(&self) -> Result<Option<i64>, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.last_update().await })
    }

    /// The event time this exploded edge event happened at. Meaningful
    /// primarily on `explode()`'d views. Fires one RPC.
    pub fn time(&self) -> Result<Option<i64>, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.time().await })
    }

    /// View start bound as seen by this edge. Fires one RPC.
    pub fn start(&self) -> Result<Option<i64>, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.start().await })
    }

    /// View end bound as seen by this edge. Fires one RPC.
    pub fn end(&self) -> Result<Option<i64>, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.end().await })
    }

    /// Edge id as a `(src, dst)` pair of endpoint ids. Fires one RPC.
    pub fn id(&self) -> Result<(String, String), ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.id().await })
    }

    /// Layer names this edge is present in. Fires one RPC.
    pub fn layer_names(&self) -> Result<Vec<String>, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.layer_names().await })
    }

    /// Single layer name for a layer-restricted view of this edge. Raises if
    /// the edge isn't scoped to exactly one layer. Fires one RPC.
    pub fn layer_name(&self) -> Result<String, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.layer_name().await })
    }

    /// Whether the edge has any events in the current view. Fires one RPC.
    pub fn is_active(&self) -> Result<bool, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.is_active().await })
    }

    /// Whether the edge is valid at the current time. Fires one RPC.
    pub fn is_valid(&self) -> Result<bool, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.is_valid().await })
    }

    /// Whether the edge has been deleted at the current time. Fires one RPC.
    pub fn is_deleted(&self) -> Result<bool, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.is_deleted().await })
    }

    /// Whether the edge is a self-loop (src == dst). Fires one RPC.
    pub fn is_self_loop(&self) -> Result<bool, ClientError> {
        let edge = Arc::clone(&self.edge);
        execute_async_task(move || async move { edge.is_self_loop().await })
    }
}
