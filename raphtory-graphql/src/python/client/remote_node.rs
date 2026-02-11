use crate::{
    client::remote_node::GraphQLRemoteNode, python::client::raphtory_client::PyRaphtoryClient,
};
use pyo3::{pyclass, pymethods, Python};
use raphtory::errors::GraphError;
use raphtory_api::core::{entities::properties::prop::Prop, storage::timeindex::EventTime};
use std::collections::HashMap;

#[derive(Clone)]
#[pyclass(name = "RemoteNode", module = "raphtory.graphql")]
pub struct PyRemoteNode {
    pub(crate) path: String,
    pub(crate) client: PyRaphtoryClient,
    pub(crate) id: String,
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
    pub(crate) fn new(path: String, client: PyRaphtoryClient, id: String) -> Self {
        Self { path, client, id }
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
    pub fn set_node_type(&self, new_type: &str) -> Result<(), GraphError> {
        let path = self.path.clone();
        let id = self.id.clone();
        let new_type = new_type.to_string();

        self.client
            .run_async(move |inner_client| async move {
                let remote = GraphQLRemoteNode::new(path, inner_client, id);
                remote.set_node_type(new_type).await
            })
            .map_err(|e| GraphError::from(e))?;
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
    ) -> Result<(), GraphError> {
        let path = self.path.clone();
        let id = self.id.clone();

        self.client
            .run_async(move |inner_client| async move {
                let remote = GraphQLRemoteNode::new(path, inner_client, id);
                remote.add_updates(t, properties).await
            })
            .map_err(|e| GraphError::from(e))?;

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
    pub fn add_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), GraphError> {
        let path = self.path.clone();
        let id = self.id.clone();

        self.client
            .run_async(move |inner_client| async move {
                let remote = GraphQLRemoteNode::new(path, inner_client, id);
                remote.add_metadata(properties).await
            })
            .map_err(|e| GraphError::from(e))?;
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
    pub fn update_metadata(&self, properties: HashMap<String, Prop>) -> Result<(), GraphError> {
        let path = self.path.clone();
        let id = self.id.clone();

        self.client
            .run_async(move |inner_client| async move {
                let remote = GraphQLRemoteNode::new(path, inner_client, id);
                remote.update_metadata(properties).await
            })
            .map_err(|e| GraphError::from(e))?;
        Ok(())
    }
}
