use crate::{
    db::graph::views::filter::model::{
        node_filter::{
            builders::{NodeIdFilterBuilder, NodeNameFilterBuilder, NodeTypeFilterBuilder},
            ops::{NodeFilterOps, NodeIdFilterOps},
            NodeFilter,
        },
        property_filter::builders::{MetadataFilterBuilder, PropertyFilterBuilder},
        NodeViewFilterOps, PropertyFilterFactory, ViewWrapOps,
    },
    python::{
        filter::{
            filter_expr::PyFilterExpr,
            property_filter_builders::{
                PyNodeViewPropsFilterBuilder, PyPropertyExprBuilder, PyPropertyFilterBuilder,
            },
        },
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyResult, Python};
use raphtory_api::core::{entities::GID, storage::timeindex::EventTime};
use std::sync::Arc;

/// Filters nodes by their ID value.
///
/// Supports numeric and string IDs and produces a `FilterExpr`
/// that can be used in node queries.
///
/// Examples:
///     Node.id() == 1
///     Node.id().is_in([1, 2, 3])
///     Node.id().starts_with("user:")
#[pyclass(frozen, name = "NodeIdFilterBuilder", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeIdFilterBuilder(Arc<NodeIdFilterBuilder>);

#[pymethods]
impl PyNodeIdFilterBuilder {
    /// Returns a filter expression that checks whether the node ID
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

    /// Returns a filter expression that checks whether the node ID
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

    /// Returns a filter expression that checks whether the node ID
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

    /// Returns a filter expression that checks whether the node ID
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

    /// Returns a filter expression that checks whether the node ID
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

    /// Returns a filter expression that checks whether the node ID
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

    /// Returns a filter expression that checks whether the node ID
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

    /// Returns a filter expression that checks whether the node ID
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
    /// representation of the node ID starts with the given prefix.
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
    /// representation of the node ID ends with the given suffix.
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
    /// representation of the node ID contains the given substring.
    ///
    /// Arguments:
    ///     value (str): Substring that must appear within the value.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating substring search.
    fn contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.contains(value)))
    }

    /// Returns a filter expression that checks whether the string
    /// representation of the node ID **does not** contain the given substring.
    ///
    /// Arguments:
    ///     value (str): Substring that must not appear within the value.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating substring exclusion.
    fn not_contains(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.0.not_contains(value)))
    }

    /// Returns a filter expression that performs fuzzy matching
    /// against the string representation of the node ID.
    ///
    /// Uses a specified Levenshtein distance and optional prefix matching.
    ///
    /// Arguments:
    ///     value (str): String to approximately match against.
    ///     levenshtein_distance (int): Maximum allowed edit distance.
    ///     prefix_match (bool): If true, the value must also match as a prefix.
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

/// Filters nodes by their name.
///
/// Comparisons are performed on the node's string name.
///
/// Examples:
///     Node.name() == "alice"
///     Node.name().contains("ali")
#[pyclass(frozen, name = "NodeNameFilterBuilder", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeNameFilterBuilder(Arc<NodeNameFilterBuilder>);

/// Filters nodes by their node type.
///
/// The node type corresponds to the optional type assigned at node creation.
///
/// Examples:
///     Node.node_type() == "fire_nation"
///     Node.node_type().is_not_in(["air_nomads"])
#[pyclass(frozen, name = "NodeTypeFilterBuilder", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeTypeFilterBuilder(Arc<NodeTypeFilterBuilder>);

#[macro_export]
macro_rules! impl_node_text_filter_builder {
    ($py_ty:ident) => {
        #[pymethods]
        impl $py_ty {
            /// Returns a filter expression that checks whether the entity's
            /// string value is equal to the specified string.
            ///
            /// Arguments:
            ///     value (str): String value to compare against.
            ///
            /// Returns:
            ///     filter.FilterExpr: A filter expression evaluating equality.
            fn __eq__(&self, value: String) -> PyFilterExpr {
                PyFilterExpr(Arc::new(self.0.eq(value)))
            }

            /// Returns a filter expression that checks whether the entity's
            /// string value is not equal to the specified string.
            ///
            /// Arguments:
            ///     value (str): String value to compare against.
            ///
            /// Returns:
            ///     filter.FilterExpr: A filter expression evaluating inequality.
            fn __ne__(&self, value: String) -> PyFilterExpr {
                PyFilterExpr(Arc::new(self.0.ne(value)))
            }

            /// Returns a filter expression that checks whether the entity's
            /// string value is contained within the given iterable of strings.
            ///
            /// Arguments:
            ///     values (list[str]): Iterable of allowed string values.
            ///
            /// Returns:
            ///     filter.FilterExpr: A filter expression evaluating membership.
            fn is_in(&self, values: FromIterable<String>) -> PyFilterExpr {
                let vals: Vec<String> = values.into_iter().collect();
                PyFilterExpr(Arc::new(self.0.is_in(vals)))
            }

            /// Returns a filter expression that checks whether the entity's
            /// string value is **not** contained within the given iterable of strings.
            ///
            /// Arguments:
            ///     values (list[str]): Iterable of string values to exclude.
            ///
            /// Returns:
            ///     filter.FilterExpr: A filter expression evaluating non-membership.
            fn is_not_in(&self, values: FromIterable<String>) -> PyFilterExpr {
                let vals: Vec<String> = values.into_iter().collect();
                PyFilterExpr(Arc::new(self.0.is_not_in(vals)))
            }

            /// Returns a filter expression that checks whether the entity's
            /// string value starts with the specified prefix.
            ///
            /// Arguments:
            ///     value (str): Prefix to check for.
            ///
            /// Returns:
            ///     filter.FilterExpr: A filter expression evaluating prefix matching.
            fn starts_with(&self, value: String) -> PyFilterExpr {
                PyFilterExpr(Arc::new(self.0.starts_with(value)))
            }

            /// Returns a filter expression that checks whether the entity's
            /// string value ends with the specified suffix.
            ///
            /// Arguments:
            ///     value (str): Suffix to check for.
            ///
            /// Returns:
            ///     filter.FilterExpr: A filter expression evaluating suffix matching.
            fn ends_with(&self, value: String) -> PyFilterExpr {
                PyFilterExpr(Arc::new(self.0.ends_with(value)))
            }

            /// Returns a filter expression that checks whether the entity's
            /// string value contains the given substring.
            ///
            /// Arguments:
            ///     value (str): Substring that must appear within the value.
            ///
            /// Returns:
            ///     filter.FilterExpr: A filter expression evaluating substring search.
            fn contains(&self, value: String) -> PyFilterExpr {
                PyFilterExpr(Arc::new(self.0.contains(value)))
            }

            /// Returns a filter expression that checks whether the entity's
            /// string value **does not** contain the given substring.
            ///
            /// Arguments:
            ///     value (str): Substring that must not appear within the value.
            ///
            /// Returns:
            ///     filter.FilterExpr: A filter expression evaluating substring exclusion.
            fn not_contains(&self, value: String) -> PyFilterExpr {
                PyFilterExpr(Arc::new(self.0.not_contains(value)))
            }

            /// Returns a filter expression that performs fuzzy matching
            /// against the entity's string value.
            ///
            /// Uses a specified Levenshtein distance and optional prefix matching.
            ///
            /// Arguments:
            ///     value (str): String to approximately match against.
            ///     levenshtein_distance (int): Maximum allowed edit distance.
            ///     prefix_match (bool): If true, the value must also match as a prefix.
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
    };
}

impl_node_text_filter_builder!(PyNodeNameFilterBuilder);
impl_node_text_filter_builder!(PyNodeTypeFilterBuilder);

/// Constructs node filter expressions.
///
/// Each method returns either:
/// - a field-specific filter builder, or
/// - a view-restricted filter context, or
/// - a boolean predicate over node state.
#[pyclass(frozen, name = "Node", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeFilter;

#[pymethods]
impl PyNodeFilter {
    /// Selects the node ID field for filtering.
    ///
    /// Returns:
    ///     filter.NodeIdFilterBuilder
    #[staticmethod]
    fn id() -> PyNodeIdFilterBuilder {
        PyNodeIdFilterBuilder(Arc::new(NodeFilter::id()))
    }

    /// Selects the node name field for filtering.
    ///
    /// Returns:
    ///     filter.NodeNameFilterBuilder
    #[staticmethod]
    fn name() -> PyNodeNameFilterBuilder {
        PyNodeNameFilterBuilder(Arc::new(NodeFilter::name()))
    }

    /// Selects the node type field for filtering.
    ///
    /// Returns:
    ///     filter.NodeTypeFilterBuilder
    #[staticmethod]
    fn node_type() -> PyNodeTypeFilterBuilder {
        PyNodeTypeFilterBuilder(Arc::new(NodeFilter::node_type()))
    }

    /// Filters a node property by name.
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
        let b: PropertyFilterBuilder<NodeFilter> =
            PropertyFilterFactory::property(&NodeFilter, name);
        b.into_pyobject(py)
    }

    /// Filters a node metadata field by name.
    ///
    /// Metadata is shared across all temporal versions of a node.
    ///
    /// Arguments:
    ///     name (str): Metadata key.
    ///
    /// Returns:
    ///     filter.FilterOps
    #[staticmethod]
    fn metadata<'py>(py: Python<'py>, name: String) -> PyResult<Bound<'py, PyPropertyExprBuilder>> {
        let b: MetadataFilterBuilder<NodeFilter> =
            PropertyFilterFactory::metadata(&NodeFilter, name);
        b.into_pyobject(py)
    }

    /// Restricts node evaluation to the given time window.
    ///
    /// The window is inclusive of `start` and exclusive of `end`.
    ///
    /// Arguments:
    ///     start (int): Start time.
    ///     end (int): End time.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder
    #[staticmethod]
    fn window(start: EventTime, end: EventTime) -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(Arc::new(NodeFilter.window(start, end)))
    }

    /// Restricts node evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder
    #[staticmethod]
    fn at(time: EventTime) -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(Arc::new(NodeFilter.at(time)))
    }

    /// Restricts node evaluation to times strictly after the given time.
    ///
    /// Arguments:
    ///     time (int): Lower time bound.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder
    #[staticmethod]
    fn after(time: EventTime) -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(Arc::new(NodeFilter.after(time)))
    }

    /// Restricts node evaluation to times strictly before the given time.
    ///
    /// Arguments:
    ///     time (int): Upper time bound.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder
    #[staticmethod]
    fn before(time: EventTime) -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(Arc::new(NodeFilter.before(time)))
    }

    /// Evaluates filters against the latest available state of each node.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder
    #[staticmethod]
    fn latest() -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(Arc::new(NodeFilter.latest()))
    }

    /// Evaluates filters against a snapshot of the graph at a given time.
    ///
    /// Arguments:
    ///     time (int): Snapshot time.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder
    #[staticmethod]
    fn snapshot_at(time: EventTime) -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(Arc::new(NodeFilter.snapshot_at(time)))
    }

    /// Evaluates filters against the most recent snapshot of the graph.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder
    #[staticmethod]
    fn snapshot_latest() -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(Arc::new(NodeFilter.snapshot_latest()))
    }

    /// Restricts evaluation to nodes belonging to the given layer.
    ///
    /// Arguments:
    ///     layer (str): Layer name.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder
    #[staticmethod]
    fn layer(layer: String) -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(Arc::new(NodeFilter.layer(layer)))
    }

    /// Restricts evaluation to nodes belonging to any of the given layers.
    ///
    /// Arguments:
    ///     layers (list[str]): Layer names.
    ///
    /// Returns:
    ///     filter.NodeViewPropsFilterBuilder
    #[staticmethod]
    fn layers(layers: FromIterable<String>) -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(Arc::new(NodeFilter.layer(layers)))
    }

    /// Matches nodes that have at least one event in the current view.
    ///
    /// Returns:
    ///     filter.FilterExpr
    #[staticmethod]
    fn is_active() -> PyFilterExpr {
        PyFilterExpr(Arc::new(NodeFilter.is_active()))
    }
}
