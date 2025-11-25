use crate::{
    db::graph::views::filter::model::{
        edge_filter::{EdgeFilter, EndpointWrapper},
        node_filter::{
            NodeFilter, NodeFilterBuilderOps, NodeIdFilterBuilder, NodeIdFilterBuilderOps,
            NodeNameFilterBuilder, NodeTypeFilterBuilder,
        },
        property_filter::{MetadataFilterBuilder, PropertyFilterBuilder, PropertyFilterOps},
        PropertyFilterFactory, Windowed,
    },
    prelude::TimeOps,
    python::{
        filter::{
            filter_expr::PyFilterExpr,
            property_filter_builders::{
                PyFilterOps, PyPropertyFilterBuilder, PyPropertyFilterFactory,
            },
        },
        types::iterable::FromIterable,
        utils::PyTime,
    },
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyResult, Python};
use raphtory_api::core::entities::GID;
use std::sync::Arc;

#[pyclass(frozen, name = "EdgeIdFilterOp", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeIdFilterOp(pub EndpointWrapper<NodeIdFilterBuilder>);

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
pub struct PyEdgeFilterOp(EdgeTextBuilder);

#[derive(Clone)]
enum EdgeTextBuilder {
    Name(EndpointWrapper<NodeNameFilterBuilder>),
    Type(EndpointWrapper<NodeTypeFilterBuilder>),
}

impl From<EndpointWrapper<NodeNameFilterBuilder>> for PyEdgeFilterOp {
    fn from(v: EndpointWrapper<NodeNameFilterBuilder>) -> Self {
        PyEdgeFilterOp(EdgeTextBuilder::Name(v))
    }
}

impl From<EndpointWrapper<NodeTypeFilterBuilder>> for PyEdgeFilterOp {
    fn from(v: EndpointWrapper<NodeTypeFilterBuilder>) -> Self {
        PyEdgeFilterOp(EdgeTextBuilder::Type(v))
    }
}

impl PyEdgeFilterOp {
    #[inline]
    fn map<T>(
        &self,
        f_name: impl FnOnce(&EndpointWrapper<NodeNameFilterBuilder>) -> T,
        f_type: impl FnOnce(&EndpointWrapper<NodeTypeFilterBuilder>) -> T,
    ) -> T {
        match &self.0 {
            EdgeTextBuilder::Name(n) => f_name(n),
            EdgeTextBuilder::Type(t) => f_type(t),
        }
    }
}

#[pymethods]
impl PyEdgeFilterOp {
    fn __eq__(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| PyFilterExpr(Arc::new(n.eq(value.clone()))),
            |t| PyFilterExpr(Arc::new(t.eq(value.clone()))),
        )
    }

    fn __ne__(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| PyFilterExpr(Arc::new(n.ne(value.clone()))),
            |t| PyFilterExpr(Arc::new(t.ne(value.clone()))),
        )
    }

    fn is_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        let vals: Vec<String> = values.into_iter().collect();
        self.map(
            |n| PyFilterExpr(Arc::new(n.is_in(vals.clone()))),
            |t| PyFilterExpr(Arc::new(t.is_in(vals.clone()))),
        )
    }

    fn is_not_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        let vals: Vec<String> = values.into_iter().collect();
        self.map(
            |n| PyFilterExpr(Arc::new(n.is_not_in(vals.clone()))),
            |t| PyFilterExpr(Arc::new(t.is_not_in(vals.clone()))),
        )
    }

    fn starts_with(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| PyFilterExpr(Arc::new(n.starts_with(value.clone()))),
            |t| PyFilterExpr(Arc::new(t.starts_with(value.clone()))),
        )
    }

    fn ends_with(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| PyFilterExpr(Arc::new(n.ends_with(value.clone()))),
            |t| PyFilterExpr(Arc::new(t.ends_with(value.clone()))),
        )
    }

    fn contains(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| PyFilterExpr(Arc::new(n.contains(value.clone()))),
            |t| PyFilterExpr(Arc::new(t.contains(value.clone()))),
        )
    }

    fn not_contains(&self, value: String) -> PyFilterExpr {
        self.map(
            |n| PyFilterExpr(Arc::new(n.not_contains(value.clone()))),
            |t| PyFilterExpr(Arc::new(t.not_contains(value.clone()))),
        )
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        self.map(
            |n| {
                PyFilterExpr(Arc::new(n.fuzzy_search(
                    value.clone(),
                    levenshtein_distance,
                    prefix_match,
                )))
            },
            |t| {
                PyFilterExpr(Arc::new(t.fuzzy_search(
                    value.clone(),
                    levenshtein_distance,
                    prefix_match,
                )))
            },
        )
    }
}

#[pyclass(frozen, name = "EdgeEndpoint", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeEndpoint(pub EndpointWrapper<NodeFilter>);

#[pymethods]
impl PyEdgeEndpoint {
    fn id(&self) -> PyEdgeIdFilterOp {
        PyEdgeIdFilterOp(self.0.id())
    }

    fn name(&self) -> PyEdgeFilterOp {
        PyEdgeFilterOp::from(self.0.name())
    }

    fn node_type(&self) -> PyEdgeFilterOp {
        PyEdgeFilterOp::from(self.0.node_type())
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
    fn property<'py>(
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyPropertyFilterBuilder>> {
        let b: PropertyFilterBuilder<EdgeFilter> =
            PropertyFilterFactory::property(&EdgeFilter, name);
        b.into_pyobject(py)
    }

    #[staticmethod]
    fn metadata<'py>(py: Python<'py>, name: String) -> PyResult<Bound<'py, PyFilterOps>> {
        let b: MetadataFilterBuilder<EdgeFilter> =
            PropertyFilterFactory::metadata(&EdgeFilter, name);
        b.into_pyobject(py)
    }

    #[staticmethod]
    fn window(start: PyTime, end: PyTime) -> PyPropertyFilterFactory {
        PyPropertyFilterFactory::wrap(EdgeFilter::window(start, end))
    }
}
