use pyo3::prelude::PyModule;
use pyo3::{PyErr, Python};
use crate::python::graphql::{PyGlobalPlugins, PyRaphtoryClient, PyRaphtoryServer, PyRunningRaphtoryServer};

pub fn base_graphql_module(py: Python<'_>) -> Result<&PyModule, PyErr> {
    let graphql_module = PyModule::new(py, "graphql")?;
    graphql_module.add_class::<PyGlobalPlugins>()?;
    graphql_module.add_class::<PyRaphtoryServer>()?;
    graphql_module.add_class::<PyRunningRaphtoryServer>()?;
    graphql_module.add_class::<PyRaphtoryClient>()?;
    return Ok(graphql_module)
}