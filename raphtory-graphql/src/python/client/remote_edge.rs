use crate::{
    client::remote_edge::GraphQLRemoteEdge, python::client::raphtory_client::PyRaphtoryClient,
};
use pyo3::{pyclass, pymethods, Python};
use raphtory::errors::GraphError;
use raphtory_api::core::{entities::properties::prop::Prop, storage::timeindex::EventTime};
use std::collections::HashMap;

/// A remote edge reference
///
/// Returned by [RemoteGraph.edge][raphtory.graphql.RemoteGraph.edge],
/// [RemoteGraph.add_edge][raphtory.graphql.RemoteGraph.add_edge],
/// and [RemoteGraph.delete_edge][raphtory.graphql.RemoteGraph.delete_edge].
#[derive(Clone)]
#[pyclass(name = "RemoteEdge", module = "raphtory.graphql")]
pub struct PyRemoteEdge {
    pub(crate) path: String,
    pub(crate) client: PyRaphtoryClient,
    pub(crate) src: String,
    pub(crate) dst: String,
}

impl PyRemoteEdge {
    pub(crate) fn new(path: String, client: PyRaphtoryClient, src: String, dst: String) -> Self {
        PyRemoteEdge {
            path,
            client,
            src,
            dst,
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
    ) -> Result<(), GraphError> {
        let path = self.path.clone();
        let src = self.src.clone();
        let dst = self.dst.clone();
        let layer_str = layer.map(|s| s.to_string());

        self.client
            .run_async(move |inner_client| async move {
                let remote = GraphQLRemoteEdge::new(path, inner_client, src, dst);
                remote.add_updates(t, properties, layer_str).await
            })
            .map_err(|e| GraphError::from(e))?;

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
    fn delete(&self, t: EventTime, layer: Option<&str>) -> Result<(), GraphError> {
        let path = self.path.clone();
        let src = self.src.clone();
        let dst = self.dst.clone();
        let layer_str = layer.map(|s| s.to_string());

        self.client
            .run_async(move |inner_client| async move {
                let remote = GraphQLRemoteEdge::new(path, inner_client, src, dst);
                remote.delete(t, layer_str).await
            })
            .map_err(|e| GraphError::from(e))?;

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
    ) -> Result<(), GraphError> {
        let path = self.path.clone();
        let src = self.src.clone();
        let dst = self.dst.clone();
        let layer_str = layer.map(|s| s.to_string());

        self.client
            .run_async(move |inner_client| async move {
                let remote = GraphQLRemoteEdge::new(path, inner_client, src, dst);
                remote.add_metadata(properties, layer_str).await
            })
            .map_err(|e| GraphError::from(e))?;

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
    ) -> Result<(), GraphError> {
        let path = self.path.clone();
        let src = self.src.clone();
        let dst = self.dst.clone();
        let layer_str = layer.map(|s| s.to_string());

        self.client
            .run_async(move |inner_client| async move {
                let remote = GraphQLRemoteEdge::new(path, inner_client, src, dst);
                remote.update_metadata(properties, layer_str).await
            })
            .map_err(|e| GraphError::from(e))?;

        Ok(())
    }
}
