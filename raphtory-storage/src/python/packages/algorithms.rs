use pyo3::prelude::*;
use raphtory::python::graph::arrow::PyArrowGraph;

use crate::lanl;

#[pyfunction]
pub fn lanl_query1(graph: PyArrowGraph) -> Option<usize> {
    lanl::query1::run(graph.as_ref())
}

#[pyfunction]
pub fn lanl_query2(graph: PyArrowGraph) -> Option<usize> {
    lanl::query2::run(graph.as_ref())
}

#[pyfunction]
pub fn lanl_query3(graph: PyArrowGraph) -> Option<usize> {
    lanl::query3::run(graph.as_ref())
}

#[pyfunction]
pub fn lanl_query3b(graph: PyArrowGraph) -> Option<usize> {
    lanl::query3b::run(graph.as_ref())
}

#[pyfunction]
pub fn lanl_query3c(graph: PyArrowGraph) -> Option<usize> {
    lanl::query3c::run(graph.as_ref())
}

#[pyfunction]
pub fn lanl_query4(graph: PyArrowGraph) -> Option<usize> {
    lanl::query4::run2(graph.as_ref())
}

#[pyfunction]
pub fn exfilteration_query1(graph: PyArrowGraph) -> Option<usize> {
    lanl::exfiltration::query1::run(graph.as_ref())
}

#[pyfunction]
pub fn exfilteration_count_query_total(graph: PyArrowGraph, window: i64) -> usize {
    lanl::exfiltration::count::query_total(graph.as_ref(), window)
}

#[pyfunction]
pub fn exfiltration_list_query_count(graph: PyArrowGraph, window: i64) -> usize {
    lanl::exfiltration::list::query_count(graph.as_ref(), window)
}
