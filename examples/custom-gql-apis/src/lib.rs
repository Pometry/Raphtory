use crate::python::py_add_custom_gql_apis;
use ::raphtory_core::python::packages::base_modules::{
    add_raphtory_classes, base_algorithm_module, base_graph_gen_module, base_graph_loader_module,
    base_vectors_module,
};
use pyo3::prelude::*;
use raphtory_core::python::types::wrappers::filter_expr::base_filter_module;
use raphtory_graphql::python::pymodule::base_graphql_module;

pub mod python;

mod mutation;
mod query;

#[pymodule]
fn raphtory(py: Python<'_>, m: &Bound<PyModule>) -> PyResult<()> {
    let _ = add_raphtory_classes(m);

    let graphql_module = base_graphql_module(py)?;
    let algorithm_module = base_algorithm_module(py)?;
    let graph_loader_module = base_graph_loader_module(py)?;
    let graph_gen_module = base_graph_gen_module(py)?;
    let vectors_module = base_vectors_module(py)?;
    let filter_module = base_filter_module(py)?;
    m.add_submodule(&graphql_module)?;
    m.add_submodule(&algorithm_module)?;
    m.add_submodule(&graph_loader_module)?;
    m.add_submodule(&graph_gen_module)?;
    m.add_submodule(&vectors_module)?;
    m.add_submodule(&filter_module)?;

    //new content
    graphql_module.add_function(wrap_pyfunction!(py_add_custom_gql_apis, m)?)?;

    Ok(())
}
