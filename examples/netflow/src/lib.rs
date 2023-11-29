mod netflow_one_path_vertex;

use crate::netflow_one_path_vertex::netflow_one_path_vertex;
use pyo3::prelude::*;
use raphtory::db::api::view::{DynamicGraph, StaticGraphViewOps};

#[pyfunction(name = "netflow_one_path_vertex")]
fn py_netflow_one_path_vertex(graph: DynamicGraph, no_time: bool, threads: Option<usize>) -> usize {
    netflow_one_path_vertex(&graph, no_time, threads)
}

#[pymodule]
fn netflow_algorithm(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_netflow_one_path_vertex, m)?)?;
    Ok(())
}
