use crate::python::{
    client::raphtory_client::PyRaphtoryClient,
    global_plugins::PyGlobalPlugins,
    server::{running_server::PyRunningGraphServer, server::PyGraphServer},
};
use pyo3::{prelude::PyModule, PyErr, Python};

pub fn base_graphql_module(py: Python<'_>) -> Result<&PyModule, PyErr> {
    let graphql_module = PyModule::new(py, "graphql")?;
    graphql_module.add_class::<PyGlobalPlugins>()?;
    graphql_module.add_class::<PyGraphServer>()?;
    graphql_module.add_class::<PyRunningGraphServer>()?;
    graphql_module.add_class::<PyRaphtoryClient>()?;
    Ok(graphql_module)
}
