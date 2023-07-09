use pyo3::prelude::*;
use raphtory_core::{
    db::api::view::internal::DynamicGraph,
    python::{graph::graph::PyGraph, utils::errors::adapt_err_value},
};
use raphtory_graphql::RaphtoryServer;
use std::collections::HashMap;

#[pyfunction]
pub fn from_map(py: Python, graphs: HashMap<String, PyGraph>) -> PyResult<&PyAny> {
    let graphs: HashMap<String, DynamicGraph> = graphs
        .into_iter()
        .map(|(key, value)| (key, value.into()))
        .collect();
    let server = RaphtoryServer::from_map(graphs);
    pyo3_asyncio::tokio::future_into_py(py, async {
        server.run().await.map_err(|e| adapt_err_value(&e))
    })
}

#[pyfunction]
pub fn from_directory(py: Python, path: String) -> PyResult<&PyAny> {
    let server = RaphtoryServer::from_directory(path.as_str());
    pyo3_asyncio::tokio::future_into_py(py, async {
        server.run().await.map_err(|e| adapt_err_value(&e))
    })
}

#[pyfunction]
pub fn from_map_and_directory(py: Python,graphs: HashMap<String, PyGraph>, path: String) -> PyResult<&PyAny> {
    let graphs: HashMap<String, DynamicGraph> = graphs
        .into_iter()
        .map(|(key, value)| (key, value.into()))
        .collect();
    let server = RaphtoryServer::from_map_and_directory(graphs,path.as_str());
    pyo3_asyncio::tokio::future_into_py(py, async {
        server.run().await.map_err(|e| adapt_err_value(&e))
    })
}