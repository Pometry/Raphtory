use pyo3::{pyclass, pymethods};
use raphtory::{
    core::{utils::errors::GraphError, Prop},
    python::utils::PyTime,
};
use std::collections::HashMap;

#[derive(Clone)]
#[pyclass(name = "RemoteGraph")]
pub struct PyRemoteEdge {
    pub(crate) path: String,
    pub(crate) src: String,
    pub(crate) dst: String,
}
#[pymethods]
impl PyRemoteEdge {
    #[new]
    pub(crate) fn new(path: String, src: String, dst: String) -> Self {
        Self { path, src, dst }
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
        t: PyTime,
        properties: Option<HashMap<String, Prop>>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
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
    fn delete(&self, t: PyTime, layer: Option<&str>) -> Result<(), GraphError> {
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
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
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
        properties: HashMap<String, Prop>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        Ok(())
    }
}
