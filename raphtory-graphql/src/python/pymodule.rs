use crate::python::{
    client::{
        raphtory_client::PyRaphtoryClient, remote_edge::PyRemoteEdge, remote_graph::PyRemoteGraph,
        remote_node::PyRemoteNode, PyAllPropertySpec, PyEdgeAddition, PyNodeAddition, PyPropsInput,
        PyRemoteIndexSpec, PySomePropertySpec, PyUpdate,
    },
    decode_graph, encode_graph, schema,
    server::{running_server::PyRunningGraphServer, server::PyGraphServer},
};
use pyo3::prelude::*;

pub fn base_graphql_module(py: Python<'_>) -> Result<Bound<PyModule>, PyErr> {
    let graphql_module = PyModule::new(py, "graphql")?;
    graphql_module.add_class::<PyGraphServer>()?;
    graphql_module.add_class::<PyRunningGraphServer>()?;
    graphql_module.add_class::<PyRaphtoryClient>()?;
    graphql_module.add_class::<PyRemoteGraph>()?;
    graphql_module.add_class::<PyRemoteEdge>()?;
    graphql_module.add_class::<PyRemoteNode>()?;
    graphql_module.add_class::<PyNodeAddition>()?;
    graphql_module.add_class::<PyUpdate>()?;
    graphql_module.add_class::<PyEdgeAddition>()?;
    graphql_module.add_class::<PyRemoteIndexSpec>()?;
    graphql_module.add_class::<PyPropsInput>()?;
    graphql_module.add_class::<PySomePropertySpec>()?;
    graphql_module.add_class::<PyAllPropertySpec>()?;

    graphql_module.add_function(wrap_pyfunction!(encode_graph, &graphql_module)?)?;
    graphql_module.add_function(wrap_pyfunction!(decode_graph, &graphql_module)?)?;
    graphql_module.add_function(wrap_pyfunction!(schema, &graphql_module)?)?;

    Ok(graphql_module)
}
