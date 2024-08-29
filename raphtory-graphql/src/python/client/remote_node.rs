use pyo3::{pyclass, pymethods};
use raphtory::{
    core::{utils::errors::GraphError, Prop},
    python::utils::PyTime,
};
use std::collections::HashMap;

#[derive(Clone)]
#[pyclass(name = "RemoteGraph")]
pub struct PyRemoteNode {
    pub(crate) path: String,
    pub(crate) node: String,
}

#[pymethods]
impl PyRemoteNode {
    #[new]
    pub(crate) fn new(path: String, node: String) -> Self {
        Self { path, node }
    }

    /// Set the type on the node. This only works if the type has not been previously set, otherwise will
    /// throw an error
    ///
    /// Parameters:
    ///     new_type (str): The new type to be set
    ///
    /// Returns:
    ///     Result: A result object indicating success or failure.
    pub fn set_node_type(&self, new_type: &str) -> Result<(), GraphError> {
        Ok(())
    }

    /// Add updates to a node in the remote graph at a specified time.
    /// This function allows for the addition of property updates to a node within the graph. The updates are time-stamped, meaning they are applied at the specified time.
    ///
    /// Parameters:
    ///     t (PyTime): The timestamp at which the updates should be applied.
    ///     properties (Optional[Dict[str, Prop]]): A dictionary of properties to update.
    ///         Each key is a string representing the property name, and each value is of type Prop representing the property value.
    ///         If None, no properties are updated.
    ///
    /// Returns:
    ///     Result: A result object indicating success or failure.
    pub fn add_updates(
        &self,
        t: PyTime,
        properties: Option<HashMap<String, Prop>>,
    ) -> Result<(), GraphError> {
        Ok(())
    }

    /// Add constant properties to a node in the remote graph.
    /// This function is used to add properties to a node that remain constant and does not
    /// change over time. These properties are fundamental attributes of the node.
    ///
    /// Parameters:
    ///     properties (Dict[str, Prop]): A dictionary of properties to be added to the node.
    ///     Each key is a string representing the property name, and each value is of type Prop
    ///     representing the property value.
    ///
    /// Returns:
    ///     Result: A result object indicating success or failure.
    pub fn add_constant_properties(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        Ok(())
    }

    /// Update constant properties of a node in the remote graph overwriting existing values.
    /// This function is used to add properties to a node that remain constant and do not
    /// change over time. These properties are fundamental attributes of the node.
    ///
    /// Parameters:
    ///     properties (Dict[str, Prop]): A dictionary of properties to be added to the node.
    ///     Each key is a string representing the property name, and each value is of type Prop
    ///     representing the property value.
    ///
    /// Returns:
    ///     Result: A result object indicating success or failure.
    pub fn update_constant_properties(
        &self,
        properties: HashMap<String, Prop>,
    ) -> Result<(), GraphError> {
        Ok(())
    }
}
