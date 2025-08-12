use crate::python::client::{
    build_property_string, build_query, raphtory_client::PyRaphtoryClient,
};
use minijinja::context;
use pyo3::{pyclass, pymethods, Python};
use raphtory::{core::utils::time::IntoTime, errors::GraphError, python::utils::PyTime};
use raphtory_api::core::entities::properties::prop::Prop;
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
    /// Parameters:
    ///     t (int | str | datetime): The timestamp at which the updates should be applied.
    ///     properties (dict[str, PropValue], optional): A dictionary of properties to update.
    ///     layer (str, optional): The layer you want the updates to be applied.
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (t, properties=None, layer=None))]
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
                  addUpdates(time: {{t}} {% if properties is not none %}, properties: {{ properties | safe }} {% endif %} {% if layer is not none %}, layer:  "{{layer}}" {% endif %})
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
    ///     t (int | str | datetime): The timestamp at which the deletion should be applied.
    ///     layer (str, optional): The layer you want the deletion applied to.
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (t, layer=None))]
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

    /// Add metadata to the edge within the remote graph.
    /// This function is used to add metadata to an edge that does not
    /// change over time. This metadata is fundamental information of the edge.
    ///
    /// Parameters:
    ///     properties (dict[str, PropValue]): A dictionary of properties to be added to the edge.
    ///     layer (str, optional): The layer you want these properties to be added on to.
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (properties, layer=None))]
    fn add_metadata(
        &self,
        py: Python,
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                edge(src: "{{src}}",dst: "{{dst}}") {
                  addMetadata(properties:  {{ properties | safe }} {% if layer is not none %}, layer:  "{{layer}}" {% endif %})
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

    /// Update metadata of an edge in the remote graph overwriting existing values.
    /// This function is used to add properties to an edge that does not
    /// change over time. These properties are fundamental attributes of the edge.
    ///
    /// Parameters:
    ///     properties (dict[str, PropValue]): A dictionary of properties to be added to the edge.
    ///     layer (str, optional): The layer you want these properties to be added on to.
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (properties, layer=None))]
    pub fn update_metadata(
        &self,
        py: Python,
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        let template = r#"
            {
              updateGraph(path: "{{path}}") {
                edge(src: "{{src}}",dst: "{{dst}}") {
                  updateMetadata(properties:  {{ properties | safe }} {% if layer is not none %}, layer:  "{{layer}}" {% endif %})
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
