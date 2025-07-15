extern crate core;
use pyo3::prelude::*;
use raphtory_core::python::{
    filter::base_filter_module,
    graph::node_state::base_node_state_module,
    packages::base_modules::{
        add_raphtory_classes, base_algorithm_module, base_graph_gen_module,
        base_graph_loader_module, base_vectors_module,
    },
};
use raphtory_graphql::python::pymodule::base_graphql_module;

/// Raphtory graph analytics library
#[pymodule]
fn _raphtory(py: Python<'_>, m: &Bound<PyModule>) -> PyResult<()> {
    let _ = add_raphtory_classes(m);

    let graphql_module = base_graphql_module(py)?;
    let algorithm_module = base_algorithm_module(py)?;
    let graph_loader_module = base_graph_loader_module(py)?;
    let graph_gen_module = base_graph_gen_module(py)?;
    let vectors_module = base_vectors_module(py)?;
    let node_state_module = base_node_state_module(py)?;
    let filter_module = base_filter_module(py)?;
    m.add_submodule(&graphql_module)?;
    m.add_submodule(&algorithm_module)?;
    m.add_submodule(&graph_loader_module)?;
    m.add_submodule(&graph_gen_module)?;
    m.add_submodule(&vectors_module)?;
    m.add_submodule(&node_state_module)?;
    m.add_submodule(&filter_module)?;
    Ok(())
}
