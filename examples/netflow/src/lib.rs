mod netflow_one_path_node;

use crate::netflow_one_path_node::netflow_one_path_node;
use ::raphtory_core::python::packages::base_modules::{
    add_raphtory_classes, base_algorithm_module, base_graph_gen_module, base_graph_loader_module,
    base_vectors_module,
};
use pyo3::prelude::*;
use raphtory_core::db::api::view::DynamicGraph;
use raphtory_graphql::python::pymodule::base_graphql_module;

#[pyfunction(name = "netflow_one_path_node")]
fn py_netflow_one_path_node(graph: DynamicGraph, no_time: bool, threads: Option<usize>) -> usize {
    netflow_one_path_node(&graph, no_time, threads)
}

#[pymodule]
fn raphtory_netflow(py: Python<'_>, m: &Bound<PyModule>) -> PyResult<()> {
    let _ = add_raphtory_classes(m);

    let graphql_module = base_graphql_module(py)?;
    let algorithm_module = base_algorithm_module(py)?;
    let graph_loader_module = base_graph_loader_module(py)?;
    let graph_gen_module = base_graph_gen_module(py)?;
    let vectors_module = base_vectors_module(py)?;
    m.add_submodule(&graphql_module)?;
    m.add_submodule(&algorithm_module)?;
    m.add_submodule(&graph_loader_module)?;
    m.add_submodule(&graph_gen_module)?;
    m.add_submodule(&vectors_module)?;

    //new content
    algorithm_module.add_function(wrap_pyfunction!(py_netflow_one_path_node, m)?)?;

    Ok(())
}

#[pymodule]
fn netflow_algorithm(_py: Python<'_>, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_netflow_one_path_node, m)?)?;
    Ok(())
}
