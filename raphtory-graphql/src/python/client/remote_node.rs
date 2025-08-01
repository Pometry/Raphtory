use crate::python::client::{
    build_property_string, build_query, raphtory_client::PyRaphtoryClient,
};
use minijinja::context;
use pyo3::{pyclass, pymethods, Python};
use raphtory::{core::utils::time::IntoTime, errors::GraphError, python::utils::PyTime};
use raphtory_api::core::entities::properties::prop::Prop;
use std::collections::HashMap;

#[derive(Clone)]
#[pyclass(name = "RemoteNode", module = "raphtory.graphql")]
pub struct PyRemoteNode {
    pub(crate) path: String,
    pub(crate) client: PyRaphtoryClient,
    pub(crate) id: String,
}

impl PyRemoteNode {
    pub(crate) fn new(path: String, client: PyRaphtoryClient, id: String) -> Self {
        Self { path, client, id }
    }
}

#[pymethods]
impl PyRemoteNode {
    /// Set the type on the node. This only works if the type has not been previously set, otherwise will
    /// throw an error
    ///
    /// Parameters:
    ///     new_type (str): The new type to be set
    ///
    /// Returns:
    ///     None:
    pub fn set_node_type(&self, py: Python, new_type: &str) -> Result<(), GraphError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                node(name: "{{name}}") {
                  setNodeType(newType: "{{new_type}}")
                }
              }
            }
        "#;

        let query_context = context! {
            path => self.path,
            name => self.id,
            new_type => new_type
        };

        let query = build_query(template, query_context)?;
        let _ = &self.client.query(py, query, None)?;
        Ok(())
    }

    /// Add updates to a node in the remote graph at a specified time.
    /// This function allows for the addition of property updates to a node within the graph. The updates are time-stamped, meaning they are applied at the specified time.
    ///
    /// Parameters:
    ///     t (int | str | datetime): The timestamp at which the updates should be applied.
    ///     properties (dict[str, PropValue], optional): A dictionary of properties to update.
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (t, properties=None))]
    pub fn add_updates(
        &self,
        py: Python,
        t: PyTime,
        properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                node(name: "{{name}}") {
                  addUpdates(time: {{t}} {% if properties is not none %}, properties:  {{ properties | safe }} {% endif %})
                }
              }
            }
        "#;

        let query_context = context! {
            path => self.path,
            name => self.id,
            t => t.into_time(),
            properties =>  properties.map(|p| build_property_string(p)),
        };

        let query = build_query(template, query_context)?;
        let _ = &self.client.query(py, query, None)?;

        Ok(())
    }

    /// Add metadata to a node in the remote graph.
    /// This function is used to add properties to a node that do not
    /// change over time. These properties are fundamental attributes of the node.
    ///
    /// Parameters:
    ///     properties (dict[str, PropValue]): A dictionary of properties to be added to the node.
    ///
    /// Returns:
    ///     None:
    pub fn add_metadata(
        &self,
        py: Python,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                node(name: "{{name}}") {
                  addMetadata(properties: {{ properties | safe }} )
                }
              }
            }
        "#;

        let query_context = context! {
            path => self.path,
            name => self.id,
            properties =>  build_property_string(properties),
        };

        let query = build_query(template, query_context)?;
        let _ = &self.client.query(py, query, None)?;
        Ok(())
    }

    /// Update metadata of a node in the remote graph overwriting existing values.
    /// This function is used to add properties to a node that does not
    /// change over time. These properties are fundamental attributes of the node.
    ///
    /// Parameters:
    ///     properties (dict[str, PropValue]): A dictionary of properties to be added to the node.
    ///
    /// Returns:
    ///     None:
    pub fn update_metadata(
        &self,
        py: Python,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                node(name: "{{name}}") {
                  updateMetadata(properties: {{ properties | safe }} )
                }
              }
            }
        "#;

        let query_context = context! {
            path => self.path,
            name => self.id,
            properties =>  build_property_string(properties)
        };

        let query = build_query(template, query_context)?;
        let _ = &self.client.query(py, query, None)?;
        Ok(())
    }
}
