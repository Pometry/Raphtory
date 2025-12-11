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
    impl_node_text_filter_builder,
    python::{
        filter::{
            filter_expr::PyFilterExpr,
            property_filter_builders::{
                PyPropertyExprBuilder, PyPropertyFilterBuilder, PyPropertyFilterFactory,
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
pub struct PyEdgeEndpointIdFilterBuilder(pub EdgeEndpointWrapper<NodeIdFilterBuilder>);

#[pymethods]
impl PyEdgeEndpointIdFilterBuilder {
    /// Returns a filter expression that checks whether the endpoint ID
    /// is equal to the given value.
    ///
    /// Arguments:
    ///     value (int): Node ID to compare against.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating equality.
    fn __eq__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.eq(value)))
    }

    /// Returns a filter expression that checks whether the endpoint ID
    /// is not equal to the given value.
    ///
    /// Arguments:
    ///     value (int): Node ID to compare against.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating inequality.
    fn __ne__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.ne(value)))
    }

    /// Returns a filter expression that checks whether the endpoint ID
    /// is less than the given value.
    ///
    /// Arguments:
    ///     value (int): Upper bound (exclusive) for the node ID.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating a `<` comparison.
    fn __lt__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.lt(value)))
    }

    /// Returns a filter expression that checks whether the endpoint ID
    /// is less than or equal to the given value.
    ///
    /// Arguments:
    ///     value (int): Upper bound (inclusive) for the node ID.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating a `<=` comparison.
    fn __le__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.le(value)))
    }

    /// Returns a filter expression that checks whether the endpoint ID
    /// is greater than the given value.
    ///
    /// Arguments:
    ///     value (int): Lower bound (exclusive) for the node ID.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating a `>` comparison.
    fn __gt__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.gt(value)))
    }

    /// Returns a filter expression that checks whether the endpoint ID
    /// is greater than or equal to the given value.
    ///
    /// Arguments:
    ///     value (int): Lower bound (inclusive) for the node ID.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating a `>=` comparison.
    fn __ge__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.ge(value)))
    }

    /// Returns a filter expression that checks whether the endpoint ID
    /// is contained within the specified iterable of IDs.
    ///
    /// Arguments:
    ///     values (list[int]): Iterable of node IDs to match against.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating membership.
    fn is_in(&self, values: FromIterable<GID>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.is_in(values)))
    }

    /// Returns a filter expression that checks whether the endpoint ID
    /// is **not** contained within the specified iterable of IDs.
    ///
    /// Arguments:
    ///     values (list[int]): Iterable of node IDs to exclude.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating non-membership.
    fn is_not_in(&self, values: FromIterable<GID>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.is_not_in(values)))
    }

    /// Returns a filter expression that checks whether the string
    /// representation of the endpoint ID starts with the given prefix.
    ///
    /// Arguments:
    ///     value (str): Prefix to check for.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating prefix matching.
    fn starts_with(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.starts_with(value)))
    }

    /// Returns a filter expression that checks whether the string
    /// representation of the endpoint ID ends with the given suffix.
    ///
    /// Arguments:
    ///     value (str): Suffix to check for.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating suffix matching.
    fn ends_with(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.ends_with(value)))
    }

    /// Returns a filter expression that checks whether the string
    /// representation of the endpoint ID contains the given substring.
    ///
    /// Arguments:
    ///     value (str): Substring to search for.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating substring search.
    fn contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.contains(value)))
    }

    /// Returns a filter expression that checks whether the string
    /// representation of the endpoint ID **does not** contain the given substring.
    ///
    /// Arguments:
    ///     value (str): Substring to exclude.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating substring exclusion.
    fn not_contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.not_contains(value)))
    }

    /// Returns a filter expression that performs fuzzy matching
    /// against the string representation of the endpoint ID.
    ///
    /// Uses a specified Levenshtein distance and optional prefix matching.
    ///
    /// Arguments:
    ///     value (str): String to approximately match against.
    ///     levenshtein_distance (int): Maximum allowed Levenshtein distance.
    ///     prefix_match (bool): Whether to require a matching prefix.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression performing approximate text matching.
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
pub struct PyEdgeEndpointNameFilterBuilder(pub EdgeEndpointWrapper<NodeNameFilterBuilder>);

#[pyclass(frozen, name = "EdgeEndpointTypeFilter", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeEndpointTypeFilterBuilder(pub EdgeEndpointWrapper<NodeTypeFilterBuilder>);

impl_node_text_filter_builder!(PyEdgeEndpointNameFilterBuilder);
impl_node_text_filter_builder!(PyEdgeEndpointTypeFilterBuilder);

#[pyclass(frozen, name = "EdgeEndpoint", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeEndpoint(pub EdgeEndpointWrapper<NodeFilter>);

#[pymethods]
impl PyEdgeEndpoint {
    fn id(&self) -> PyEdgeEndpointIdFilterBuilder {
        PyEdgeEndpointIdFilterBuilder(self.0.id())
    }

    fn name(&self) -> PyEdgeEndpointNameFilterBuilder {
        PyEdgeEndpointNameFilterBuilder(self.0.name())
    }

    fn node_type(&self) -> PyEdgeEndpointTypeFilterBuilder {
        PyEdgeEndpointTypeFilterBuilder(self.0.node_type())
    }

    fn property<'py>(
        &self,
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyPropertyFilterBuilder>> {
        let b = PropertyFilterFactory::property(&self.0, name);
        b.into_pyobject(py)
    }

    fn metadata<'py>(
        &self,
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyPropertyExprBuilder>> {
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
    fn metadata<'py>(py: Python<'py>, name: String) -> PyResult<Bound<'py, PyPropertyExprBuilder>> {
        let b: MetadataFilterBuilder<EdgeFilter> =
            PropertyFilterFactory::metadata(&EdgeFilter, name);
        b.into_pyobject(py)
    }

    #[staticmethod]
    fn window(start: PyTime, end: PyTime) -> PyPropertyFilterFactory {
        PyPropertyFilterFactory::wrap(EdgeFilter::window(start, end))
    }
}
