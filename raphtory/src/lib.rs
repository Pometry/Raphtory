pub mod wrappers;
pub mod graph;
pub mod graph_window;
pub mod algorithms;
pub mod graph_gen;

use pyo3::prelude::*;

use crate::wrappers::Direction;
use crate::graph::Graph;
use crate::algorithms::triangle_count;
use crate::graph_gen::random_attachment;
use crate::graph_gen::ba_preferential_attachment;

#[pymodule]
fn raphtory(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Direction>()?;
    m.add_class::<Graph>()?;


    let algorithm_module = PyModule::new(py, "algorithms")?;
    algorithm_module.add_function(wrap_pyfunction!(triangle_count, algorithm_module)?)?;
    m.add_submodule(algorithm_module)?;

    let graph_gen_module = PyModule::new(py, "graph_gen")?;
    graph_gen_module.add_function(wrap_pyfunction!(random_attachment, graph_gen_module)?)?;
    graph_gen_module.add_function(wrap_pyfunction!(ba_preferential_attachment, graph_gen_module)?)?;
    m.add_submodule(graph_gen_module)?;

    Ok(())
}
