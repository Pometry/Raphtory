use pyo3::prelude::*;
use raphtory::{db::api::view::internal::DynamicGraph, prelude::GraphViewOps};

fn custom_algorithm<G: GraphViewOps>(graph: &G) -> usize {
    graph.num_vertices()
}

#[pyfunction(name = "custom_algorithm")]
fn py_custom_algorithm(graph: DynamicGraph) -> usize {
    custom_algorithm(&graph)
}

#[pymodule]
fn custom_python_extension(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_custom_algorithm, m)?)?;
    Ok(())
}
