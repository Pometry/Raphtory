use crate::python::{
    client::{
        raphtory_client::PyRaphtoryClient, remote_edge::PyRemoteEdge, remote_graph::PyRemoteGraph,
        remote_node::PyRemoteNode, PyEdgeAddition, PyNodeAddition, PyUpdate,
    },
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
    graphql_module.add_class::<PyRemoteGraph>()?;
    graphql_module.add_class::<PyRemoteEdge>()?;
    graphql_module.add_class::<PyRemoteNode>()?;
    graphql_module.add_class::<PyNodeAddition>()?;
    graphql_module.add_class::<PyUpdate>()?;
    graphql_module.add_class::<PyEdgeAddition>()?;
    Ok(graphql_module)
}
