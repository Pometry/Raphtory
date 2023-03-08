use crate::graph_window::WindowedGraph;
use docbrown_db::algorithms::local_triangle_count::local_triangle_count;

use pyo3::prelude::*;


#[pyfunction]
pub(crate) fn triangle_count(g: &WindowedGraph, v: u64) -> u32 {
    local_triangle_count(&g.graph_w, v)
}
