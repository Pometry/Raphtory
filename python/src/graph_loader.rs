use crate::graph::PyGraph;
use pyo3::prelude::*;

#[pyfunction]
#[pyo3(signature = (shards=1))]
pub(crate) fn lotr_graph(shards: usize) -> PyResult<Py<PyGraph>> {
    PyGraph::py_from_db_graph(docbrown::graph_loader::lotr_graph::lotr_graph(shards))
}

#[pyfunction]
#[pyo3(signature = (shards=1,timeout_seconds=600))]
pub(crate) fn reddit_hyperlink_graph(shards: usize, timeout_seconds: u64) -> PyResult<Py<PyGraph>> {
    PyGraph::py_from_db_graph(docbrown::graph_loader::reddit_hyperlinks::reddit_graph(
        shards,
        timeout_seconds,
    ))
}
