use crate::client::{remote_edge::GraphQLRemoteEdge, ClientError};
use pyo3::{pyclass, pymethods};
use raphtory_api::core::{entities::properties::prop::Prop, storage::timeindex::EventTime};
use std::{collections::HashMap, future::Future, sync::Arc};
use tokio::runtime::Runtime;

/// A remote edge reference
///
/// Returned by [RemoteGraph.edge][raphtory.graphql.RemoteGraph.edge],
/// [RemoteGraph.add_edge][raphtory.graphql.RemoteGraph.add_edge],
/// and [RemoteGraph.delete_edge][raphtory.graphql.RemoteGraph.delete_edge].
#[derive(Clone)]
#[pyclass(name = "RemoteEdge", module = "raphtory.graphql")]
pub struct PyRemoteEdge {
    pub(crate) edge: Arc<GraphQLRemoteEdge>,
    pub(crate) runtime: Arc<Runtime>,
}

impl PyRemoteEdge {
    pub(crate) fn new(edge: GraphQLRemoteEdge, runtime: Arc<Runtime>) -> Self {
        PyRemoteEdge {
            edge: Arc::new(edge),
            runtime,
        }
    }

    fn execute_async_task<T, F, O>(&self, task: T) -> O
    where
        T: FnOnce() -> F + Send + 'static,
        F: Future<Output = O> + 'static,
        O: Send + 'static,
    {
        pyo3::Python::attach(|py| py.detach(|| self.runtime.block_on(task())))
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
        self.execute_async_task(task)?;

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
        self.execute_async_task(task)?;

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
        self.execute_async_task(task)?;

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
        self.execute_async_task(task)?;

        Ok(())
    }
}
