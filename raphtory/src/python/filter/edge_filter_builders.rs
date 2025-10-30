use crate::{
    db::graph::views::filter::model::{
        EdgeEndpointFilter, EdgeFilter, EdgeFilterOps, InternalEdgeFilterBuilderOps,
    },
    python::{
        filter::filter_expr::{PyFilterExpr, PyInnerFilterExpr},
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods};
use std::sync::Arc;

#[pyclass(frozen, name = "EdgeFilterOp", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeFilterOp(Arc<dyn InternalEdgeFilterBuilderOps>);

impl<T: InternalEdgeFilterBuilderOps + 'static> From<T> for PyEdgeFilterOp {
    fn from(value: T) -> Self {
        PyEdgeFilterOp(Arc::new(value))
    }
}

#[pymethods]
impl PyEdgeFilterOp {
    fn __eq__(&self, value: String) -> PyFilterExpr {
        let field = self.0.eq(value);
        PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(field)))
    }

    fn __ne__(&self, value: String) -> PyFilterExpr {
        let field = self.0.ne(value);
        PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(field)))
    }

    fn is_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        let field = self.0.is_in(values);
        PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(field)))
    }

    fn is_not_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        let field = self.0.is_not_in(values);
        PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(field)))
    }

    fn contains(&self, value: String) -> PyFilterExpr {
        let field = self.0.contains(value);
        PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(field)))
    }

    fn not_contains(&self, value: String) -> PyFilterExpr {
        let field = self.0.not_contains(value);
        PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(field)))
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        let field = self
            .0
            .fuzzy_search(value, levenshtein_distance, prefix_match);
        PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(field)))
    }
}

#[pyclass(frozen, name = "EdgeEndpoint", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeEndpoint(pub EdgeEndpointFilter);

#[pymethods]
impl PyEdgeEndpoint {
    fn name(&self) -> PyEdgeFilterOp {
        PyEdgeFilterOp(self.0.name())
    }
}

#[pyclass(frozen, name = "Edge", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeFilter;

#[pymethods]
impl PyEdgeFilter {
    #[staticmethod]
    fn src() -> PyEdgeEndpoint {
        PyEdgeEndpoint(EdgeFilter::src())
    }

    #[staticmethod]
    fn dst() -> PyEdgeEndpoint {
        PyEdgeEndpoint(EdgeFilter::dst())
    }
}
