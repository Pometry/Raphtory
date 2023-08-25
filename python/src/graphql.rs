use pyo3::{exceptions, prelude::*};
use raphtory_core::{
    db::api::view::internal::{DynamicGraph, MaterializedGraph},
    python::{graph::graph::PyGraph, utils::errors::adapt_err_value},
};
use raphtory_graphql::{url_decode_graph, url_encode_graph, RaphtoryServer};
use std::collections::HashMap;

#[pyfunction]
pub fn from_map(
    py: Python,
    graphs: HashMap<String, PyGraph>,
    port: Option<u16>,
) -> PyResult<&PyAny> {
    let graphs: HashMap<String, DynamicGraph> = graphs
        .into_iter()
        .map(|(key, value)| (key, value.into()))
        .collect();
    let server = RaphtoryServer::from_map(graphs);
    let port = port.unwrap_or(1736);
    pyo3_asyncio::tokio::future_into_py(py, async move {
        server
            .run_with_port(port)
            .await
            .map_err(|e| adapt_err_value(&e))
    })
}

#[pyfunction]
pub fn from_directory(py: Python, path: String, port: Option<u16>) -> PyResult<&PyAny> {
    let server = RaphtoryServer::from_directory(path.as_str());
    let port = port.unwrap_or(1736);
    pyo3_asyncio::tokio::future_into_py(py, async move {
        server
            .run_with_port(port)
            .await
            .map_err(|e| adapt_err_value(&e))
    })
}

#[pyfunction]
pub fn from_map_and_directory(
    py: Python,
    graphs: HashMap<String, PyGraph>,
    path: String,
    port: Option<u16>,
) -> PyResult<&PyAny> {
    let graphs: HashMap<String, DynamicGraph> = graphs
        .into_iter()
        .map(|(key, value)| (key, value.into()))
        .collect();
    let port = port.unwrap_or(1736);
    let server = RaphtoryServer::from_map_and_directory(graphs, path.as_str());
    pyo3_asyncio::tokio::future_into_py(py, async move {
        server
            .run_with_port(port)
            .await
            .map_err(|e| adapt_err_value(&e))
    })
}

#[pyfunction]
pub fn encode_graph(py: Python, graph: MaterializedGraph) -> PyResult<String> {
    let result = url_encode_graph(graph);
    match result {
        Ok(s) => Ok(s),
        Err(e) => Err(exceptions::PyValueError::new_err(format!(
            "Error encoding: {:?}",
            e
        ))),
    }
}

#[pyfunction]
pub fn decode_graph(py: Python, encoded_graph: String) -> PyResult<PyObject> {
    let result = url_decode_graph(encoded_graph);
    match result {
        Ok(s) => Ok(s.into_py(py)),
        Err(e) => Err(exceptions::PyValueError::new_err(format!(
            "Error decoding: {:?}",
            e
        ))),
    }
}
