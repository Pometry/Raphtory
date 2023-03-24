use crate::graph_window::WindowedGraph;
use docbrown_db::algorithms::reciprocity::{
    all_local_reciprocity as all_local_reciprocity_rs, global_reciprocity as global_reciprocity_rs,
    local_reciprocity as local_reciprocity_rs,
};

use docbrown_db::algorithms::degree::{
    average_degree as average_degree_rs, max_in_degree as max_in_degree_rs,
    max_out_degree as max_out_degree_rs, min_in_degree as min_in_degree_rs,
    min_out_degree as min_out_degree_rs,
};
use docbrown_db::algorithms::directed_graph_density::directed_graph_density as directed_graph_density_rs;
use docbrown_db::algorithms::local_clustering_coefficient::local_clustering_coefficient as local_clustering_coefficient_rs;
use docbrown_db::algorithms::local_triangle_count::local_triangle_count as local_triangle_count_rs;
use pyo3::prelude::*;

#[pyfunction]
pub(crate) fn local_triangle_count(g: &WindowedGraph, v: u64) -> usize {
    local_triangle_count_rs(&g.graph_w, v)
}

#[pyfunction]
pub(crate) fn local_clustering_coefficient(g: &WindowedGraph, v: u64) -> f32 {
    local_clustering_coefficient_rs(&g.graph_w, v)
}

#[pyfunction]
pub(crate) fn directed_graph_density(g: &WindowedGraph) -> f32 {
    directed_graph_density_rs(&g.graph_w)
}

#[pyfunction]
pub(crate) fn average_degree(g: &WindowedGraph) -> f64 {
    average_degree_rs(&g.graph_w)
}

#[pyfunction]
pub(crate) fn max_out_degree(g: &WindowedGraph) -> usize {
    max_out_degree_rs(&g.graph_w)
}

#[pyfunction]
pub(crate) fn max_in_degree(g: &WindowedGraph) -> usize {
    max_in_degree_rs(&g.graph_w)
}

#[pyfunction]
pub(crate) fn min_out_degree(g: &WindowedGraph) -> usize {
    min_out_degree_rs(&g.graph_w)
}

#[pyfunction]
pub(crate) fn min_in_degree(g: &WindowedGraph) -> usize {
    min_in_degree_rs(&g.graph_w)
}

#[pyfunction]
pub(crate) fn global_reciprocity(g: &WindowedGraph) -> f64 {
    global_reciprocity_rs(&g.graph_w)
}

#[pyfunction]
pub(crate) fn local_reciprocity(g: &WindowedGraph, v: u64) -> f64 {
    local_reciprocity_rs(&g.graph_w, v)
}

#[pyfunction]
pub(crate) fn all_local_reciprocity(g: &WindowedGraph) -> Vec<(u64, f64)> {
    all_local_reciprocity_rs(&g.graph_w)
}
