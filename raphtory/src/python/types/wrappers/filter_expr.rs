use crate::db::graph::views::property_filter::{Filter, FilterExpr};
use pyo3::prelude::*;

#[pyclass(frozen, name = "FilterExpr", module = "raphtory")]
#[derive(Clone)]
pub struct PyFilterExpr(pub FilterExpr);

impl PyFilterExpr {
    fn new(filter_expr: FilterExpr) -> Self {
        PyFilterExpr(filter_expr)
    }
}

#[pyclass(frozen, name = "NodeFilterOp", module = "raphtory")]
#[derive(Clone)]
pub struct PyNodeFilterOp {
    field: String,
}

#[pymethods]
impl PyNodeFilterOp {
    #[new]
    fn new(field: String) -> Self {
        PyNodeFilterOp { field }
    }

    fn __eq__(&self, value: String) -> PyFilterExpr {
        PyFilterExpr::new(FilterExpr::Node(Filter::eq(self.field.clone(), value)))
    }

    fn __ne__(&self, value: String) -> PyFilterExpr {
        PyFilterExpr::new(FilterExpr::Node(Filter::ne(self.field.clone(), value)))
    }

    fn includes(&self, values: Vec<String>) -> PyFilterExpr {
        PyFilterExpr::new(FilterExpr::Node(Filter::includes(
            self.field.clone(),
            values.into_iter(),
        )))
    }

    fn excludes(&self, values: Vec<String>) -> PyFilterExpr {
        PyFilterExpr::new(FilterExpr::Node(Filter::excludes(
            self.field.clone(),
            values.into_iter(),
        )))
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr::new(FilterExpr::Node(Filter::fuzzy_search(
            self.field.clone(),
            value,
            levenshtein_distance,
            prefix_match,
        )))
    }
}

#[pyclass(frozen, name = "Node", module = "raphtory")]
#[derive(Clone)]
pub struct PyNodeFilter;

#[pymethods]
impl PyNodeFilter {
    #[staticmethod]
    fn node_name() -> PyNodeFilterOp {
        PyNodeFilterOp::new("node_name".to_string())
    }

    #[staticmethod]
    fn node_type() -> PyNodeFilterOp {
        PyNodeFilterOp::new("node_type".to_string())
    }
}

#[pyclass(frozen, name = "EdgeFilterOp", module = "raphtory")]
#[derive(Clone)]
pub struct PyEdgeFilterOp {
    field: String,
}

#[pymethods]
impl PyEdgeFilterOp {
    #[new]
    fn new(field: String) -> Self {
        PyEdgeFilterOp { field }
    }

    fn __eq__(&self, value: String) -> PyFilterExpr {
        PyFilterExpr::new(FilterExpr::Edge(Filter::eq(self.field.clone(), value)))
    }

    fn __ne__(&self, value: String) -> PyFilterExpr {
        PyFilterExpr::new(FilterExpr::Edge(Filter::ne(self.field.clone(), value)))
    }

    fn includes(&self, values: Vec<String>) -> PyFilterExpr {
        PyFilterExpr::new(FilterExpr::Edge(Filter::includes(
            self.field.clone(),
            values.into_iter(),
        )))
    }

    fn excludes(&self, values: Vec<String>) -> PyFilterExpr {
        PyFilterExpr::new(FilterExpr::Edge(Filter::excludes(
            self.field.clone(),
            values.into_iter(),
        )))
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr::new(FilterExpr::Edge(Filter::fuzzy_search(
            self.field.clone(),
            value,
            levenshtein_distance,
            prefix_match,
        )))
    }
}

#[pyclass(frozen, name = "Edge", module = "raphtory")]
#[derive(Clone)]
pub struct PyEdgeFilter;

#[pymethods]
impl PyEdgeFilter {
    #[staticmethod]
    fn from() -> PyEdgeFilterOp {
        PyEdgeFilterOp::new("from".to_string())
    }

    #[staticmethod]
    fn to() -> PyEdgeFilterOp {
        PyEdgeFilterOp::new("to".to_string())
    }
}

pub fn base_filter_module(py: Python<'_>) -> Result<Bound<PyModule>, PyErr> {
    let filter_module = PyModule::new(py, "filter")?;

    filter_module.add_class::<PyFilterExpr>()?;
    filter_module.add_class::<PyNodeFilterOp>()?;
    filter_module.add_class::<PyNodeFilter>()?;
    filter_module.add_class::<PyEdgeFilterOp>()?;
    filter_module.add_class::<PyEdgeFilter>()?;

    Ok(filter_module)
}
