use crate::graph_window::WindowedGraph;
use docbrown_db::algorithms::degree::average_degree;
use docbrown_db::algorithms::degree::max_in_degree;
use docbrown_db::algorithms::degree::max_out_degree;
use docbrown_db::algorithms::degree::min_in_degree;
use docbrown_db::algorithms::degree::min_out_degree;
use docbrown_db::algorithms::directed_graph_density::directed_graph_density;
use docbrown_db::algorithms::local_clustering_coefficient::local_clustering_coefficient;
use docbrown_db::algorithms::local_triangle_count::local_triangle_count;
use pyo3::prelude::*;

#[pyfunction]
pub(crate) fn triangle_count(g: &WindowedGraph, v: u64) -> u32 {
    local_triangle_count(&g.graph_w, v)
}

#[pyfunction]
pub(crate) fn clustering_coefficient(g: &WindowedGraph, v: u64) -> f32 {
    local_clustering_coefficient(&g.graph_w, v)
}

#[pyfunction]
pub(crate) fn graph_density(g: &WindowedGraph) -> f32 {
    directed_graph_density(&g.graph_w)
}

#[pyfunction]
pub(crate) fn avg_degree(g: &WindowedGraph) -> f64 {
    average_degree(&g.graph_w)
}

#[pyfunction]
pub(crate) fn max_outdegree(g: &WindowedGraph) -> usize {
    max_out_degree(&g.graph_w)
}

#[pyfunction]
pub(crate) fn max_indegree(g: &WindowedGraph) -> usize {
    max_in_degree(&g.graph_w)
}

#[pyfunction]
pub(crate) fn min_outdegree(g: &WindowedGraph) -> usize {
    min_out_degree(&g.graph_w)
}

#[pyfunction]
pub(crate) fn min_indegree(g: &WindowedGraph) -> usize {
    min_in_degree(&g.graph_w)
}
