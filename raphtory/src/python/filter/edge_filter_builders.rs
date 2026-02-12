use crate::{
    db::graph::views::filter::model::{
        edge_filter::{EdgeEndpointWrapper, EdgeFilter},
        node_filter::{
            builders::{NodeIdFilterBuilder, NodeNameFilterBuilder, NodeTypeFilterBuilder},
            ops::{NodeFilterOps, NodeIdFilterOps},
            NodeFilter,
        },
        property_filter::builders::{MetadataFilterBuilder, PropertyFilterBuilder},
        EdgeViewFilterOps, PropertyFilterFactory, ViewWrapOps,
    },
    impl_node_text_filter_builder,
    python::{
        filter::{
            filter_expr::PyFilterExpr,
            property_filter_builders::{
                PyEdgeViewPropsFilterBuilder, PyPropertyExprBuilder, PyPropertyFilterBuilder,
            },
        },
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyResult, Python};
use raphtory_api::core::{entities::GID, storage::timeindex::EventTime};
use std::sync::Arc;

/// Filters an edge endpoint by its node ID.
///
/// This builder produces `FilterExpr` predicates over the **source** or
/// **destination** endpoint of an edge (depending on where it was obtained).
///
/// Examples:
///     Edge.src().id() == 1
///     Edge.dst().id().is_in([1, 2, 3])
///     Edge.src().id().starts_with("user:")
#[pyclass(frozen, name = "EdgeEndpointIdFilter", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeEndpointIdFilterBuilder(pub EdgeEndpointWrapper<NodeIdFilterBuilder>);

#[pymethods]
impl PyEdgeEndpointIdFilterBuilder {
    /// Checks whether the endpoint ID is equal to the given value.
    ///
    /// Arguments:
    ///     value (int): Node ID to compare against.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating equality.
    fn __eq__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.eq(value)))
    }

    /// Checks whether the endpoint ID is not equal to the given value.
    ///
    /// Arguments:
    ///     value (int): Node ID to compare against.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating inequality.
    fn __ne__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.ne(value)))
    }

    /// Checks whether the endpoint ID is less than the given value (exclusive).
    ///
    /// Arguments:
    ///     value (int): Upper bound (exclusive) for the node ID.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating a `<` comparison.
    fn __lt__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.lt(value)))
    }

    /// Checks whether the endpoint ID is less than or equal to the given value.
    ///
    /// Arguments:
    ///     value (int): Upper bound (inclusive) for the node ID.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating a `<=` comparison.
    fn __le__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.le(value)))
    }

    /// Checks whether the endpoint ID is greater than the given value (exclusive).
    ///
    /// Arguments:
    ///     value (int): Lower bound (exclusive) for the node ID.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating a `>` comparison.
    fn __gt__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.gt(value)))
    }

    /// Checks whether the endpoint ID is greater than or equal to the given value.
    ///
    /// Arguments:
    ///     value (int): Lower bound (inclusive) for the node ID.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating a `>=` comparison.
    fn __ge__(&self, value: GID) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.ge(value)))
    }

    /// Checks whether the endpoint ID is contained within the specified iterable of IDs.
    ///
    /// Arguments:
    ///     values (list[int]): Iterable of node IDs to match against.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating membership.
    fn is_in(&self, values: FromIterable<GID>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.is_in(values)))
    }

    /// Checks whether the endpoint ID is **not** contained within the specified iterable of IDs.
    ///
    /// Arguments:
    ///     values (list[int]): Iterable of node IDs to exclude.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating non-membership.
    fn is_not_in(&self, values: FromIterable<GID>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.is_not_in(values)))
    }

    /// Checks whether the string representation of the endpoint ID starts with the given prefix.
    ///
    /// Arguments:
    ///     value (str): Prefix to check for.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating prefix matching.
    fn starts_with(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.starts_with(value)))
    }

    /// Checks whether the string representation of the endpoint ID ends with the given suffix.
    ///
    /// Arguments:
    ///     value (str): Suffix to check for.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating suffix matching.
    fn ends_with(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.ends_with(value)))
    }

    /// Checks whether the string representation of the endpoint ID contains the given substring.
    ///
    /// Arguments:
    ///     value (str): Substring to search for.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating substring search.
    fn contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.contains(value)))
    }

    /// Checks whether the string representation of the endpoint ID **does not** contain the given substring.
    ///
    /// Arguments:
    ///     value (str): Substring to exclude.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating substring exclusion.
    fn not_contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.not_contains(value)))
    }

    /// Performs fuzzy matching against the string representation of the endpoint ID.
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

/// Filters an edge endpoint by its node name.
///
/// This builder produces `FilterExpr` predicates over the **source** or
/// **destination** endpoint node name.
///
/// Examples:
///     Edge.src().name() == "alice"
///     Edge.dst().name().contains("ali")
#[pyclass(frozen, name = "EdgeEndpointNameFilter", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeEndpointNameFilterBuilder(pub EdgeEndpointWrapper<NodeNameFilterBuilder>);

/// Filters an edge endpoint by its node type.
///
/// This builder produces `FilterExpr` predicates over the **source** or
/// **destination** endpoint node type.
///
/// Examples:
///     Edge.src().node_type() == "fire_nation"
///     Edge.dst().node_type().is_not_in(["air_nomads"])
#[pyclass(frozen, name = "EdgeEndpointTypeFilter", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeEndpointTypeFilterBuilder(pub EdgeEndpointWrapper<NodeTypeFilterBuilder>);

impl_node_text_filter_builder!(PyEdgeEndpointNameFilterBuilder);
impl_node_text_filter_builder!(PyEdgeEndpointTypeFilterBuilder);

/// Entry point for filtering an edge endpoint (source or destination).
///
/// An `EdgeEndpoint` is obtained from `Edge.src()` or `Edge.dst()` and allows
/// you to filter on endpoint fields (id, name, type) as well as endpoint
/// properties and metadata.
///
/// Examples:
///     Edge.src().id() == 1
///     Edge.dst().name().starts_with("user:")
///     Edge.src().property("country") == "UK"
#[pyclass(frozen, name = "EdgeEndpoint", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeEndpoint(pub EdgeEndpointWrapper<NodeFilter>);

#[pymethods]
impl PyEdgeEndpoint {
    /// Selects the endpoint node ID field for filtering.
    ///
    /// Returns:
    ///     filter.EdgeEndpointIdFilter
    fn id(&self) -> PyEdgeEndpointIdFilterBuilder {
        PyEdgeEndpointIdFilterBuilder(self.0.id())
    }

    /// Selects the endpoint node name field for filtering.
    ///
    /// Returns:
    ///     filter.EdgeEndpointNameFilter
    fn name(&self) -> PyEdgeEndpointNameFilterBuilder {
        PyEdgeEndpointNameFilterBuilder(self.0.name())
    }

    /// Selects the endpoint node type field for filtering.
    ///
    /// Returns:
    ///     filter.EdgeEndpointTypeFilter
    fn node_type(&self) -> PyEdgeEndpointTypeFilterBuilder {
        PyEdgeEndpointTypeFilterBuilder(self.0.node_type())
    }

    /// Filters an endpoint node property by name.
    ///
    /// The property may be static or temporal depending on the query context.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyFilterOps
    fn property<'py>(
        &self,
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyPropertyFilterBuilder>> {
        let b = PropertyFilterFactory::property(&self.0, name);
        b.into_pyobject(py)
    }

    /// Filters an endpoint node metadata field by name.
    ///
    /// Metadata is shared across all temporal versions of a node.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.FilterOps
    fn metadata<'py>(
        &self,
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyPropertyExprBuilder>> {
        let b = PropertyFilterFactory::metadata(&self.0, name);
        b.into_pyobject(py)
    }
}

/// Entry point for constructing edge filter expressions.
///
/// The `Edge` filter provides:
/// - endpoint filters via `src()` and `dst()`,
/// - property and metadata filters,
/// - view restrictions (time windows, snapshots, layers),
/// - and structural predicates over edge state (active/valid/deleted/self-loop).
///
/// Examples:
///     Edge.src().id() == 1
///     Edge.property("weight") > 0.5
///     Edge.window(0, 10).is_active()
///     Edge.layer("fire_nation").is_valid()
#[pyclass(frozen, name = "Edge", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeFilter;

#[pymethods]
impl PyEdgeFilter {
    /// Selects the edge **source endpoint** for filtering.
    ///
    /// Returns:
    ///     filter.EdgeEndpoint
    #[staticmethod]
    fn src() -> PyEdgeEndpoint {
        PyEdgeEndpoint(EdgeFilter::src())
    }

    /// Selects the edge **destination endpoint** for filtering.
    ///
    /// Returns:
    ///     filter.EdgeEndpoint
    #[staticmethod]
    fn dst() -> PyEdgeEndpoint {
        PyEdgeEndpoint(EdgeFilter::dst())
    }

    /// Filters an edge property by name.
    ///
    /// The property may be static or temporal depending on the query context.
    ///
    /// Arguments:
    ///     name (str): Property key.
    ///
    /// Returns:
    ///     filter.PropertyFilterOps
    #[staticmethod]
    fn property<'py>(
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyPropertyFilterBuilder>> {
        let b: PropertyFilterBuilder<EdgeFilter> =
            PropertyFilterFactory::property(&EdgeFilter, name);
        b.into_pyobject(py)
    }

    /// Filters an edge metadata field by name.
    ///
    /// Metadata is shared across all temporal versions of an edge.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.FilterOps
    #[staticmethod]
    fn metadata<'py>(py: Python<'py>, name: String) -> PyResult<Bound<'py, PyPropertyExprBuilder>> {
        let b: MetadataFilterBuilder<EdgeFilter> =
            PropertyFilterFactory::metadata(&EdgeFilter, name);
        b.into_pyobject(py)
    }

    /// Restricts edge evaluation to the given time window.
    ///
    /// The window is inclusive of `start` and exclusive of `end`.
    ///
    /// Arguments:
    ///     start (int): Start time.
    ///     end (int): End time.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn window(start: EventTime, end: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(EdgeFilter.window(start, end)))
    }

    /// Restricts edge evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn at(time: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(EdgeFilter.at(time)))
    }

    /// Restricts edge evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (int): Lower time bound.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn after(time: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(EdgeFilter.after(time)))
    }

    /// Restricts edge evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (int): Upper time bound.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn before(time: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(EdgeFilter.before(time)))
    }

    /// Evaluates edge predicates against the latest available edge state.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn latest() -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(EdgeFilter.latest()))
    }

    /// Evaluates edge predicates against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (int): Snapshot time.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn snapshot_at(time: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(EdgeFilter.snapshot_at(time)))
    }

    /// Evaluates edge predicates against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn snapshot_latest() -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(EdgeFilter.snapshot_latest()))
    }

    /// Restricts evaluation to edges belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn layer(layer: String) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(EdgeFilter.layer(layer)))
    }

    /// Restricts evaluation to edges belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.EdgeViewPropsFilterBuilder
    #[staticmethod]
    fn layers(layers: FromIterable<String>) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(EdgeFilter.layer(layers)))
    }

    /// Matches edges that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr
    #[staticmethod]
    fn is_active() -> PyFilterExpr {
        PyFilterExpr(Arc::new(EdgeFilter.is_active()))
    }

    /// Matches edges that are structurally valid in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr
    #[staticmethod]
    fn is_valid() -> PyFilterExpr {
        PyFilterExpr(Arc::new(EdgeFilter.is_valid()))
    }

    /// Matches edges that have been deleted.
    ///
    /// Returns:
    ///     filter.FilterExpr
    #[staticmethod]
    fn is_deleted() -> PyFilterExpr {
        PyFilterExpr(Arc::new(EdgeFilter.is_deleted()))
    }

    /// Matches edges that are self-loops (source == destination).
    ///
    /// Returns:
    ///     filter.FilterExpr
    #[staticmethod]
    fn is_self_loop() -> PyFilterExpr {
        PyFilterExpr(Arc::new(EdgeFilter.is_self_loop()))
    }
}
