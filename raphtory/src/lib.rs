pub mod algorithms;
pub mod graph;
pub mod graph_gen;
pub mod graph_loader;
pub mod graph_window;
pub mod wrappers;

use pyo3::prelude::*;

use crate::algorithms::{all_local_reciprocity, global_reciprocity, local_reciprocity};
use crate::graph::Graph;
use crate::wrappers::{Direction, Perspective};

use pyo3::prelude::*;

use crate::algorithms::average_degree;
use crate::algorithms::directed_graph_density;
use crate::algorithms::local_clustering_coefficient;
use crate::algorithms::local_triangle_count;
use crate::algorithms::max_in_degree;
use crate::algorithms::max_out_degree;
use crate::algorithms::min_in_degree;
use crate::algorithms::min_out_degree;

use crate::graph_gen::ba_preferential_attachment;
use crate::graph_gen::random_attachment;
use crate::graph_loader::lotr_graph;
use crate::graph_loader::twitter_graph;

#[pymodule]
fn raphtory(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Graph>()?;
    m.add_class::<Perspective>()?;

    let algorithm_module = PyModule::new(py, "algorithms")?;
    algorithm_module.add_function(wrap_pyfunction!(global_reciprocity, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(local_reciprocity, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(all_local_reciprocity, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(local_triangle_count, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(
        local_clustering_coefficient,
        algorithm_module
    )?)?;
    algorithm_module.add_function(wrap_pyfunction!(average_degree, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(directed_graph_density, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(max_out_degree, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(max_in_degree, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(min_out_degree, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(min_in_degree, algorithm_module)?)?;
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
