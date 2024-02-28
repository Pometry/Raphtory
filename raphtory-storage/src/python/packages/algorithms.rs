use pyo3::prelude::*;
use raphtory::arrow::graph_impl::ArrowGraph;
use raphtory::python::graph::arrow::PyArrowGraph;

use crate::lanl;

#[pyfunction]
pub fn lanl_query1(graph: PyArrowGraph) -> Option<usize> {
    lanl::query1::run(&ArrowGraph::from(graph))
}

#[pyfunction]
pub fn lanl_query2(graph: PyArrowGraph) -> Option<usize> {
    lanl::query2::run(&ArrowGraph::from(graph))
}

#[pyfunction]
pub fn lanl_query3(graph: PyArrowGraph) -> Option<usize> {
    lanl::query3::run(&ArrowGraph::from(graph))
}

#[pyfunction]
pub fn lanl_query3b(graph: PyArrowGraph) -> Option<usize> {
    lanl::query3b::run(&ArrowGraph::from(graph))
}

#[pyfunction]
pub fn lanl_query3c(graph: PyArrowGraph) -> Option<usize> {
    lanl::query3c::run(&ArrowGraph::from(graph))
}

#[pyfunction]
pub fn lanl_query4(graph: PyArrowGraph) -> Option<usize> {
    lanl::query4::run(&ArrowGraph::from(graph))
}
