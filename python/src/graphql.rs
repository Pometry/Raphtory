use pyo3::prelude::*;
use raphtory_core::{
    python::{graph::graph::PyGraph, utils::errors::adapt_err_value},
};
use raphtory_graphql::RaphtoryServer;
use std::collections::HashMap;
use raphtory_core::db::api::view::internal::DynamicGraph;

#[pyfunction]
pub fn run_from_dict(py: Python, graphs: HashMap<String, PyGraph>) -> PyResult<&PyAny> {
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
pub fn run_from_file(py: Python, path: String) -> PyResult<&PyAny> {
    let server = RaphtoryServer::new(path.as_str());
    pyo3_asyncio::tokio::future_into_py(py, async {
        server.run().await.map_err(|e| adapt_err_value(&e))
    })
}
