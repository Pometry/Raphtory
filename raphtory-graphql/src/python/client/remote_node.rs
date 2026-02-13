use crate::client::{remote_node::GraphQLRemoteNode, ClientError};
use pyo3::{pyclass, pymethods};
use raphtory_api::core::{entities::properties::prop::Prop, storage::timeindex::EventTime};
use std::{collections::HashMap, future::Future, sync::Arc};
use tokio::runtime::Runtime;

#[derive(Clone)]
#[pyclass(name = "RemoteNode", module = "raphtory.graphql")]
pub struct PyRemoteNode {
    pub(crate) node: Arc<GraphQLRemoteNode>,
    pub(crate) runtime: Arc<Runtime>,
}

impl PyRemoteNode {
    /// New node.
    ///
    /// Arguments:
    ///   path (str):
    ///   client (RaphtoryClient):
    ///   id (str):
    ///
    /// Returns:
    ///   None:
    pub(crate) fn new(node: GraphQLRemoteNode, runtime: Arc<Runtime>) -> Self {
        Self {
            node: Arc::new(node),
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
impl PyRemoteNode {
    /// Set the type on the node. This only works if the type has not been previously set, otherwise will
    /// throw an error
    ///
    /// Arguments:
    ///   new_type (str): The new type to be set
    ///
    /// Returns:
    ///   None:
    pub fn set_node_type(&self, new_type: &str) -> Result<(), ClientError> {
        let node = Arc::clone(&self.node);
        let new_type = new_type.to_string();

        let task = move || async move { node.set_node_type(new_type).await };
        self.execute_async_task(task)?;
        Ok(())
    }

    /// Add updates to a node in the remote graph at a specified time.
    /// This function allows for the addition of property updates to a node within the graph. The updates are time-stamped, meaning they are applied at the specified time.
    ///
    /// Arguments:
    ///   t (int | str | datetime): The timestamp at which the updates should be applied.
    ///   properties (dict[str, PropValue], optional): A dictionary of properties to update.
    ///
    /// Returns:
    ///   None:
    #[pyo3(signature = (t, properties=None))]
    pub fn add_updates(
        &self,
        t: EventTime,
        properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), ClientError> {
        let node = Arc::clone(&self.node);

        let task = move || async move { node.add_updates(t, properties).await };
        self.execute_async_task(task)?;

        Ok(())
    }

    /// Add metadata to a node in the remote graph.
    /// This function is used to add properties to a node that do not
    /// change over time. These properties are fundamental attributes of the node.
    ///
    /// Arguments:
    ///   properties (dict[str, PropValue]): A dictionary of properties to be added to the node.
    ///
    /// Returns:
    ///   None:
    pub fn add_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), ClientError> {
        let node = Arc::clone(&self.node);

        let task = move || async move { node.add_metadata(properties).await };
        self.execute_async_task(task)?;
        Ok(())
    }

    /// Update metadata of a node in the remote graph overwriting existing values.
    /// This function is used to add properties to a node that does not
    /// change over time. These properties are fundamental attributes of the node.
    ///
    /// Arguments:
    ///   properties (dict[str, PropValue]): A dictionary of properties to be added to the node.
    ///
    /// Returns:
    ///   None:
    pub fn update_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), ClientError> {
        let node = Arc::clone(&self.node);

        let task = move || async move { node.update_metadata(properties).await };
        self.execute_async_task(task)?;
        Ok(())
    }
}
