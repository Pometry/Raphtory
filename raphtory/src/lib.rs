pub mod algorithms;
pub mod graph;
pub mod graph_gen;
pub mod graph_loader;

use pyo3::prelude::*;

use crate::wrappers::{Direction, Perspective};
use crate::graph::Graph;
pub mod graph_window;
pub mod wrappers;

use pyo3::prelude::*;

use crate::algorithms::avg_degree;
use crate::algorithms::clustering_coefficient;
use crate::algorithms::graph_density;
use crate::algorithms::max_indegree;
use crate::algorithms::max_outdegree;
use crate::algorithms::min_indegree;
use crate::algorithms::min_outdegree;
use crate::algorithms::triangle_count;

use crate::graph_gen::ba_preferential_attachment;
use crate::graph_loader::lotr_graph;
use crate::graph_loader::twitter_graph;
use crate::graph_gen::random_attachment;


#[pymodule]
fn raphtory(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Direction>()?;
    m.add_class::<Graph>()?;
    m.add_class::<Perspective>()?;

    let algorithm_module = PyModule::new(py, "algorithms")?;
    algorithm_module.add_function(wrap_pyfunction!(triangle_count, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(clustering_coefficient, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(avg_degree, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(graph_density, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(max_outdegree, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(max_indegree, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(min_outdegree, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(min_indegree, algorithm_module)?)?;
    m.add_submodule(algorithm_module)?;

    let graph_loader_module = PyModule::new(py, "graph_loader")?;
    graph_loader_module.add_function(wrap_pyfunction!(lotr_graph, graph_loader_module)?)?;
    graph_loader_module.add_function(wrap_pyfunction!(twitter_graph, graph_loader_module)?)?;
    m.add_submodule(graph_loader_module)?;

    let graph_gen_module = PyModule::new(py, "graph_gen")?;
    graph_gen_module.add_function(wrap_pyfunction!(random_attachment, graph_gen_module)?)?;
    graph_gen_module.add_function(wrap_pyfunction!(
        ba_preferential_attachment,
        graph_gen_module
    )?)?;
    m.add_submodule(graph_gen_module)?;

    Ok(())
}
