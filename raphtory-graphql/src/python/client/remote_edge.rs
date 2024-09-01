use crate::python::client::{
    build_property_string, build_query, raphtory_client::PyRaphtoryClient,
};
use minijinja::context;
use pyo3::{pyclass, pymethods, Python};
use raphtory::{
    core::{
        utils::{errors::GraphError, time::IntoTime},
        Prop,
    },
    python::utils::PyTime,
};
use std::collections::HashMap;

#[derive(Clone)]
#[pyclass(name = "RemoteEdge")]
pub struct PyRemoteEdge {
    pub(crate) path: String,
    pub(crate) client: PyRaphtoryClient,
    pub(crate) src: String,
    pub(crate) dst: String,
}
#[pymethods]
impl PyRemoteEdge {
    #[new]
    pub(crate) fn new(path: String, client: PyRaphtoryClient, src: String, dst: String) -> Self {
        Self {
            path,
            client,
            src,
            dst,
        }
    }

    /// Add updates to an edge in the remote graph at a specified time.
    /// This function allows for the addition of property updates to an edge within the graph. The updates are time-stamped, meaning they are applied at the specified time.
    ///
    /// Parameters:
    ///     t (PyTime): The timestamp at which the updates should be applied.
    ///     properties (Optional[Dict[str, Prop]]): A dictionary of properties to update.
    ///         Each key is a string representing the property name, and each value is of type Prop representing the property value.
    ///         If None, no properties are updated.
    ///
    /// Returns:
    ///     Result: A result object indicating success or failure.
    fn add_updates(
        &self,
        py: Python,
        t: PyTime,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                edge(src: "{{src}}",dst: "{{dst}}") {
                  addUpdates(time: {{t}} {% if properties is not none %}, properties:  {{ properties | safe }} {% endif %} {% if layer is not none %}, layer:  "{{layer}}" {% endif %})
                }
              }
            }
        "#;

        let query_context = context! {
            path => self.path,
            src => self.src,
            dst => self.dst,
            t => t.into_time(),
            properties =>  properties.map(|p| build_property_string(p)),
            layer => layer
        };

        let query = build_query(template, query_context)?;
        let _ = &self.client.query(py, query, None)?;

        Ok(())
    }

    /// Mark the edge as deleted at the specified time.
    ///
    /// Parameters:
    ///     t (PyTime): The timestamp at which the deletion should be applied.
    ///     layer (str): The layer you want the deletion applied to .
    ///
    /// Returns:
    ///     Result: A result object indicating success or failure.
    fn delete(&self, py: Python, t: PyTime, layer: Option<&str>) -> Result<(), GraphError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                edge(src: "{{src}}",dst: "{{dst}}") {
                  delete(time: {{t}}, {% if layer is not none %}, layer:  "{{layer}}" {% endif %})
                }
              }
            }
        "#;

        let query_context = context! {
            path => self.path,
            src => self.src,
            dst => self.dst,
            t => t.into_time(),
            layer => layer
        };

        let query = build_query(template, query_context)?;
        let _ = &self.client.query(py, query, None)?;

        Ok(())
    }

    /// Add constant properties to the edge within the remote graph.
    /// This function is used to add properties to an edge that remain constant and do not
    /// change over time. These properties are fundamental attributes of the edge.
    ///
    /// Parameters:
    ///     properties (Dict[str, Prop]): A dictionary of properties to be added to the edge.
    ///     Each key is a string representing the property name, and each value is of type Prop
    ///     representing the property value.
    ///     layer (str): The layer you want these properties to be added on to.
    ///
    /// Returns:
    ///     Result: A result object indicating success or failure.
    fn add_constant_properties(
        &self,
        py: Python,
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                edge(src: "{{src}}",dst: "{{dst}}") {
                  addConstantProperties(properties:  {{ properties | safe }} {% if layer is not none %}, layer:  "{{layer}}" {% endif %})
                }
              }
            }
        "#;

        let query_context = context! {
            path => self.path,
            src => self.src,
            dst => self.dst,
            properties =>  build_property_string(properties),
            layer => layer
        };

        let query = build_query(template, query_context)?;
        let _ = &self.client.query(py, query, None)?;

        Ok(())
    }

    /// Update constant properties of an edge in the remote graph overwriting existing values.
    /// This function is used to add properties to an edge that remains constant and does not
    /// change over time. These properties are fundamental attributes of the edge.
    ///
    /// Parameters:
    ///     properties (Dict[str, Prop]): A dictionary of properties to be added to the edge.
    ///     Each key is a string representing the property name, and each value is of type Prop
    ///     representing the property value.
    ///     layer (str): The layer you want these properties to be added on to.
    ///
    /// Returns:
    ///     Result: A result object indicating success or failure.
    pub fn update_constant_properties(
        &self,
        py: Python,
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                edge(src: "{{src}}",dst: "{{dst}}") {
                  updateConstantProperties(properties:  {{ properties | safe }} {% if layer is not none %}, layer:  "{{layer}}" {% endif %})
                }
              }
            }
        "#;

        let query_context = context! {
            path => self.path,
            src => self.src,
            dst => self.dst,
            properties =>  build_property_string(properties),
            layer => layer
        };

        let query = build_query(template, query_context)?;
        let _ = &self.client.query(py, query, None)?;

        Ok(())
    }
}
