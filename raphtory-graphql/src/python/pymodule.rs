use crate::{
    cli::python_cli,
    python::{
        client::{
            remote_client::PyRaphtoryClient,
            remote_edge::PyRemoteEdge,
            remote_edges::PyRemoteEdges,
            remote_graph::PyRemoteGraph,
            remote_history::{
                PyRemoteEventTime, PyRemoteHistory, PyRemoteHistoryDateTimes,
                PyRemoteHistoryEventIds, PyRemoteHistoryTimestamps, PyRemoteIntervals,
            },
            remote_metadata::{
                PyRemoteMetadata, PyRemoteProperties, PyRemoteProperty, PyRemotePropertyTuple,
                PyRemoteTemporalProperties, PyRemoteTemporalProperty,
            },
            remote_node::PyRemoteNode,
            remote_nodes::PyRemoteNodes,
            remote_schema::{
                PyRemoteEdgeSchema, PyRemoteGraphSchema, PyRemoteLayerSchema, PyRemoteNodeSchema,
                PyRemotePropertySchema,
            },
            PyAllPropertySpec, PyEdgeAddition, PyNodeAddition, PyPropsInput, PyRemoteIndexSpec,
            PySomePropertySpec, PyUpdate,
        },
        decode_graph, encode_graph, schema,
        server::{running_server::PyRunningGraphServer, server::PyGraphServer},
    },
};
use pyo3::prelude::*;

/// Returns True if the permissions extension (raphtory-auth) is compiled in.
///
/// Returns:
///     bool: True if the extension is built in, False otherwise.
#[pyfunction]
pub fn has_permissions_extension() -> bool {
    crate::server::has_server_extension()
}

pub fn base_graphql_module(py: Python<'_>) -> Result<Bound<'_, PyModule>, PyErr> {
    let graphql_module = PyModule::new(py, "graphql")?;
    graphql_module.add_class::<PyGraphServer>()?;
    graphql_module.add_class::<PyRunningGraphServer>()?;
    graphql_module.add_class::<PyRaphtoryClient>()?;
    graphql_module.add_class::<PyRemoteGraph>()?;
    graphql_module.add_class::<PyRemoteEdge>()?;
    graphql_module.add_class::<PyRemoteNode>()?;
    graphql_module.add_class::<PyRemoteNodes>()?;
    graphql_module.add_class::<PyRemoteEdges>()?;
    graphql_module.add_class::<PyRemoteHistory>()?;
    graphql_module.add_class::<PyRemoteEventTime>()?;
    graphql_module.add_class::<PyRemoteHistoryTimestamps>()?;
    graphql_module.add_class::<PyRemoteHistoryEventIds>()?;
    graphql_module.add_class::<PyRemoteHistoryDateTimes>()?;
    graphql_module.add_class::<PyRemoteIntervals>()?;
    graphql_module.add_class::<PyRemoteMetadata>()?;
    graphql_module.add_class::<PyRemoteProperties>()?;
    graphql_module.add_class::<PyRemoteProperty>()?;
    graphql_module.add_class::<PyRemoteTemporalProperties>()?;
    graphql_module.add_class::<PyRemoteTemporalProperty>()?;
    graphql_module.add_class::<PyRemotePropertyTuple>()?;
    graphql_module.add_class::<PyRemoteGraphSchema>()?;
    graphql_module.add_class::<PyRemoteNodeSchema>()?;
    graphql_module.add_class::<PyRemoteLayerSchema>()?;
    graphql_module.add_class::<PyRemoteEdgeSchema>()?;
    graphql_module.add_class::<PyRemotePropertySchema>()?;
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
    graphql_module.add_function(wrap_pyfunction!(python_cli, &graphql_module)?)?;
    graphql_module.add_function(wrap_pyfunction!(
        has_permissions_extension,
        &graphql_module
    )?)?;

    Ok(graphql_module)
}
