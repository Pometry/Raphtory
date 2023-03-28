use crate::wrappers::adapt_err;
use crate::Graph;
use docbrown_db::graphgen::preferential_attachment::ba_preferential_attachment as pa;
use docbrown_db::graphgen::random_attachment::random_attachment as ra;
use pyo3::prelude::*;

#[pyfunction]
pub(crate) fn random_attachment(
    g: &Graph,
    vertices_to_add: usize,
    edges_per_step: usize,
) -> PyResult<()> {
    adapt_err(ra(&g.graph, vertices_to_add, edges_per_step))
}

#[pyfunction]
pub(crate) fn ba_preferential_attachment(
    g: &Graph,
    vertices_to_add: usize,
    edges_per_step: usize,
) -> PyResult<()> {
    adapt_err(pa(&g.graph, vertices_to_add, edges_per_step))
}
