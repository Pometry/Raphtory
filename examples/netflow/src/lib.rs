mod netflow_one_path_node;

use crate::netflow_one_path_node::netflow_one_path_node;
use pyo3::prelude::*;
use raphtory::db::api::view::DynamicGraph;

#[pyfunction(name = "netflow_one_path_node")]
fn py_netflow_one_path_node(graph: DynamicGraph, no_time: bool, threads: Option<usize>) -> usize {
    netflow_one_path_node(&graph, no_time, threads)
}

#[pymodule]
fn netflow_algorithm(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_netflow_one_path_node, m)?)?;
    Ok(())
}
