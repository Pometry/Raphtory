use pyo3::prelude::*;
use raphtory::arrow::graph_impl::Graph2;
use raphtory::python::graph::arrow::PyArrowGraph;

use crate::lanl;

#[pyfunction]
pub fn lanl_query1(graph: PyArrowGraph) -> Option<usize> {
    lanl::query1::run(&Graph2::from(graph))
}

#[pyfunction]
pub fn lanl_query2(graph: PyArrowGraph) -> Option<usize> {
    lanl::query2::run(&Graph2::from(graph))
}

#[pyfunction]
pub fn lanl_query3(graph: PyArrowGraph) -> Option<usize> {
    lanl::query3::run(&Graph2::from(graph))
}

#[pyfunction]
pub fn lanl_query3b(graph: PyArrowGraph) -> Option<usize> {
    lanl::query3b::run(&Graph2::from(graph))
}

#[pyfunction]
pub fn lanl_query3c(graph: PyArrowGraph) -> Option<usize> {
    lanl::query3c::run(&Graph2::from(graph))
}

#[pyfunction]
pub fn lanl_query4(graph: PyArrowGraph) -> Option<usize> {
    lanl::query4::run(&Graph2::from(graph))
}
