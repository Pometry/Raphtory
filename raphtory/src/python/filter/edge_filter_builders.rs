use crate::{
    db::graph::views::filter::model::{
        edge_filter::{EdgeEndpointWrapper, EdgeFilter},
        node_filter::{
            builders::{NodeIdFilterBuilder, NodeNameFilterBuilder, NodeTypeFilterBuilder},
            ops::{NodeFilterOps, NodeIdFilterOps},
            NodeFilter,
        },
        property_filter::builders::{MetadataFilterBuilder, PropertyFilterBuilder},
        PropertyFilterFactory,
    },
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

#[pyclass(frozen, name = "EdgeEndpointIdFilter", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeEndpointIdFilter(pub EdgeEndpointWrapper<NodeIdFilterBuilder>);

#[pymethods]
impl PyEdgeEndpointIdFilter {
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

#[pyclass(frozen, name = "EdgeEndpointNameFilter", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeEndpointNameFilter(pub EdgeEndpointWrapper<NodeNameFilterBuilder>);

#[pyclass(frozen, name = "EdgeEndpointTypeFilter", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeEndpointTypeFilter(pub EdgeEndpointWrapper<NodeTypeFilterBuilder>);

macro_rules! impl_edge_endpoint_text_filter {
    ($wrapper:ident) => {
        #[pymethods]
        impl $wrapper {
            fn __eq__(&self, value: String) -> PyFilterExpr {
                PyFilterExpr(Arc::new(self.0.eq(value)))
            }

            fn __ne__(&self, value: String) -> PyFilterExpr {
                PyFilterExpr(Arc::new(self.0.ne(value)))
            }

            fn is_in(&self, values: FromIterable<String>) -> PyFilterExpr {
                let vals: Vec<String> = values.into_iter().collect();
                PyFilterExpr(Arc::new(self.0.is_in(vals)))
            }

            fn is_not_in(&self, values: FromIterable<String>) -> PyFilterExpr {
                let vals: Vec<String> = values.into_iter().collect();
                PyFilterExpr(Arc::new(self.0.is_not_in(vals)))
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
    };
}

impl_edge_endpoint_text_filter!(PyEdgeEndpointNameFilter);
impl_edge_endpoint_text_filter!(PyEdgeEndpointTypeFilter);

#[pyclass(frozen, name = "EdgeEndpoint", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeEndpoint(pub EdgeEndpointWrapper<NodeFilter>);

#[pymethods]
impl PyEdgeEndpoint {
    fn id(&self) -> PyEdgeEndpointIdFilter {
        PyEdgeEndpointIdFilter(self.0.id())
    }

    fn name(&self) -> PyEdgeEndpointNameFilter {
        PyEdgeEndpointNameFilter(self.0.name())
    }

    fn node_type(&self) -> PyEdgeEndpointTypeFilter {
        PyEdgeEndpointTypeFilter(self.0.node_type())
    }

    fn property<'py>(
        &self,
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyPropertyFilterBuilder>> {
        let b = PropertyFilterFactory::property(&self.0, name);
        b.into_pyobject(py)
    }

    fn metadata<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyFilterOps>> {
        let b = PropertyFilterFactory::metadata(&self.0, name);
        b.into_pyobject(py)
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
