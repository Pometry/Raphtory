use pyo3::prelude::*;
#[cfg(feature = "vectors")]
use raphtory::python::packages::base_modules::base_vectors_module;
use raphtory::python::{
    filter::base_filter_module,
    graph::node_state::base_node_state_module,
    packages::base_modules::{
        add_raphtory_classes, base_algorithm_module, base_graph_gen_module,
        base_graph_loader_module, base_iterables_module,
    },
};

use raphtory_graphql::python::pymodule::base_graphql_module;

#[cfg(target_os = "macos")]
use tikv_jemallocator::Jemalloc;
#[cfg(target_os = "macos")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// Raphtory graph analytics library
#[pymodule]
fn _raphtory(py: Python<'_>, m: &Bound<PyModule>) -> PyResult<()> {
    auth::init();
    add_raphtory_classes(m)?;

    let graphql_module = base_graphql_module(py)?;
    let algorithm_module = base_algorithm_module(py)?;
    let graph_loader_module = base_graph_loader_module(py)?;
    let graph_gen_module = base_graph_gen_module(py)?;
    #[cfg(feature = "vectors")]
    let vectors_module = base_vectors_module(py)?;
    let node_state_module = base_node_state_module(py)?;
    let filter_module = base_filter_module(py)?;
    let iterables = base_iterables_module(py)?;
    m.add_submodule(&graphql_module)?;
    m.add_submodule(&algorithm_module)?;
    m.add_submodule(&graph_loader_module)?;
    m.add_submodule(&graph_gen_module)?;
    #[cfg(feature = "vectors")]
    m.add_submodule(&vectors_module)?;
    m.add_submodule(&node_state_module)?;
    m.add_submodule(&filter_module)?;
    m.add_submodule(&iterables)?;
    Ok(())
}
