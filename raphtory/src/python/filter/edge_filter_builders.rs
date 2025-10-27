use crate::{
    db::graph::views::filter::model::{
        edge_filter::{
            EdgeEndpointFilter, EdgeFilter, EdgeFilterOps, EdgeIdFilterBuilder, ExplodedEdgeFilter,
            InternalEdgeFilterBuilderOps,
        },
        property_filter::{MetadataFilterBuilder, PropertyFilterBuilder},
        PropertyFilterFactory,
    },
    python::{
        filter::{
            filter_expr::PyFilterExpr,
            property_filter_builders::{PyMetadataFilterBuilder, PyPropertyFilterBuilder},
        },
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods, IntoPyObject, Py, PyResult, Python};
use raphtory_api::core::entities::GID;
use std::sync::Arc;

#[pyclass(frozen, name = "EdgeIdFilterOp", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeIdFilterOp(pub EdgeIdFilterBuilder);

#[pymethods]
impl PyEdgeIdFilterOp {
    fn __eq__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.eq(value)))
    }

    fn __ne__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.ne(value)))
    }

    fn __lt__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.lt(value)))
    }

    fn __le__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.le(value)))
    }

    fn __gt__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.gt(value)))
    }

    fn __ge__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.ge(value)))
    }

    fn is_in(&self, values: FromIterable<GID>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.is_in(values)))
    }

    fn is_not_in(&self, values: FromIterable<GID>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.is_not_in(values)))
    }

    fn starts_with(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.starts_with(value)))
    }

    fn ends_with(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.ends_with(value)))
    }

    fn contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.contains(value)))
    }

    fn not_contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.not_contains(value)))
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.fuzzy_search(
            value,
            levenshtein_distance,
            prefix_match,
        )))
    }
}

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
        PyFilterExpr(Arc::new(field))
    }

    fn __ne__(&self, value: String) -> PyFilterExpr {
        let field = self.0.ne(value);
        PyFilterExpr(Arc::new(field))
    }

    fn is_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        let field = self.0.is_in(values);
        PyFilterExpr(Arc::new(field))
    }

    fn is_not_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        let field = self.0.is_not_in(values);
        PyFilterExpr(Arc::new(field))
    }

    fn starts_with(&self, value: String) -> PyFilterExpr {
        let field = self.0.starts_with(value);
        PyFilterExpr(Arc::new(field))
    }

    fn ends_with(&self, value: String) -> PyFilterExpr {
        let field = self.0.ends_with(value);
        PyFilterExpr(Arc::new(field))
    }

    fn contains(&self, value: String) -> PyFilterExpr {
        let field = self.0.contains(value);
        PyFilterExpr(Arc::new(field))
    }

    fn not_contains(&self, value: String) -> PyFilterExpr {
        let field = self.0.not_contains(value);
        PyFilterExpr(Arc::new(field))
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
        PyFilterExpr(Arc::new(field))
    }
}

#[pyclass(frozen, name = "EdgeEndpoint", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeEndpoint(pub EdgeEndpointFilter);

#[pymethods]
impl PyEdgeEndpoint {
    fn id(&self) -> PyEdgeIdFilterOp {
        PyEdgeIdFilterOp(self.0.id())
    }

    fn name(&self) -> PyEdgeFilterOp {
        PyEdgeFilterOp(self.0.name())
    }

    fn property(&self, py: Python<'_>, name: String) -> PyResult<Py<PyPropertyFilterBuilder>> {
        match self.0 {
            EdgeEndpointFilter::Src => EdgeEndpointFilter::src_property(&self.0, name)
                .into_pyobject(py)
                .map(|b| b.unbind()),
            EdgeEndpointFilter::Dst => EdgeEndpointFilter::dst_property(&self.0, name)
                .into_pyobject(py)
                .map(|b| b.unbind()),
        }
    }

    fn metadata(&self, py: Python<'_>, name: String) -> PyResult<Py<PyMetadataFilterBuilder>> {
        match self.0 {
            EdgeEndpointFilter::Src => EdgeEndpointFilter::src_metadata(&self.0, name)
                .into_pyobject(py)
                .map(|b| b.unbind()),
            EdgeEndpointFilter::Dst => EdgeEndpointFilter::dst_metadata(&self.0, name)
                .into_pyobject(py)
                .map(|b| b.unbind()),
        }
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

    #[staticmethod]
    fn property(name: String) -> PropertyFilterBuilder<EdgeFilter> {
        EdgeFilter::property(name)
    }

    #[staticmethod]
    fn metadata(name: String) -> MetadataFilterBuilder<EdgeFilter> {
        EdgeFilter::metadata(name)
    }
}

#[pyclass(frozen, name = "ExplodedEdge", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyExplodedEdgeFilter;

#[pymethods]
impl PyExplodedEdgeFilter {
    #[staticmethod]
    fn property(name: String) -> PropertyFilterBuilder<ExplodedEdgeFilter> {
        ExplodedEdgeFilter::property(name)
    }

    #[staticmethod]
    fn metadata(name: String) -> MetadataFilterBuilder<ExplodedEdgeFilter> {
        ExplodedEdgeFilter::metadata(name)
    }
}
