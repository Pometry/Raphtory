//! Python wrappers over the typed property-expression chain.
//!
//! Two classes:
//! - `PyPropertyExprBuilder` (`filter.FilterOps`) — wraps `Arc<dyn DynPropertyExpr>`
//!   and exposes the comparator/string/set/aggregator/quantifier chain methods.
//! - `PyPropertyFilterBuilder` (`filter.PropertyFilterOps`) — extends `FilterOps`
//!   with `.temporal()`. Wraps `Arc<dyn DynTemporalPropertyExpr>`.
//!
//! Plus three view-builder wrappers that combine view restrictions (window /
//! layer / snapshot) with property predicates:
//! - `PyViewFilterBuilder` (`filter.ViewFilterBuilder`)
//! - `PyNodeViewPropsFilterBuilder` (`filter.NodeViewPropsFilterBuilder`)
//! - `PyEdgeViewPropsFilterBuilder` (`filter.EdgeViewPropsFilterBuilder`)

use crate::{
    db::graph::views::filter::model::{
        edge_filter::EdgeEndpointWrapper,
        node_expr::{DynPropertyExpr, DynTemporalPropertyExpr, EntityExpr},
        DynEdgeViewFilterOps, DynEdgeViewProps, DynNodeViewProps, DynView, MetadataExpr,
        PropertyExpr, ViewWrapOps,
    },
    python::{filter::filter_expr::PyFilterExpr, types::iterable::FromIterable},
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyErr, Python};
use raphtory_api::core::{entities::properties::prop::Prop, storage::timeindex::EventTime};
use std::sync::Arc;

/// Python wrapper over a chainable property expression.
///
/// Represents a "property access" plus optional aggregator / quantifier
/// qualifiers (`first`, `len`, `sum`, `any`, …). Comparison operators
/// (`==`, `<`, `is_in`, …) terminate the chain by returning a
/// `filter.FilterExpr`.
///
/// Combine `FilterExpr`s with `&`, `|`, `~`.
#[pyclass(frozen, name = "FilterOps", module = "raphtory.filter", subclass)]
pub struct PyPropertyExprBuilder(pub Arc<dyn DynPropertyExpr>);

impl PyPropertyExprBuilder {
    pub fn wrap<T: DynPropertyExpr + 'static>(t: T) -> Self {
        Self(Arc::new(t))
    }

    pub fn from_arc(inner: Arc<dyn DynPropertyExpr>) -> Self {
        Self(inner)
    }
}

#[pymethods]
impl PyPropertyExprBuilder {
    fn __eq__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_eq(value))
    }

    fn __ne__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_ne(value))
    }

    fn __lt__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_lt(value))
    }

    fn __le__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_le(value))
    }

    fn __gt__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_gt(value))
    }

    fn __ge__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_ge(value))
    }

    fn is_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_in(values.into_iter().collect()))
    }

    fn is_not_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_not_in(values.into_iter().collect()))
    }

    fn is_none(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_none())
    }

    fn is_some(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_some())
    }

    fn starts_with(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_starts_with(value))
    }

    fn ends_with(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_ends_with(value))
    }

    fn contains(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_contains(value))
    }

    fn not_contains(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_not_contains(value))
    }

    fn fuzzy_search(
        &self,
        prop_value: Prop,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(
            self.0
                .dyn_fuzzy_search(prop_value, levenshtein_distance, prefix_match),
        )
    }

    pub fn first(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.dyn_first())
    }

    pub fn last(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.dyn_last())
    }

    pub fn any(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.dyn_any())
    }

    pub fn all(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.dyn_all())
    }

    fn len(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.dyn_len())
    }

    fn sum(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.dyn_sum())
    }

    fn avg(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.dyn_avg())
    }

    fn min(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.dyn_min())
    }

    fn max(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.dyn_max())
    }
}

/// Python wrapper that adds `.temporal()` to the property-expression chain.
///
/// Exported as: `filter.PropertyFilterOps`.
#[pyclass(
    frozen,
    name = "PropertyFilterOps",
    module = "raphtory.filter",
    extends = PyPropertyExprBuilder
)]
pub struct PyPropertyFilterBuilder(pub(crate) Arc<dyn DynTemporalPropertyExpr>);

impl PyPropertyFilterBuilder {
    pub(crate) fn from_arc(inner: Arc<dyn DynTemporalPropertyExpr>) -> Self {
        Self(inner)
    }
}

#[pymethods]
impl PyPropertyFilterBuilder {
    /// Switch to temporal evaluation — chain methods now operate over the
    /// list of values at each timestamp.
    fn temporal(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.dyn_temporal())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// IntoPyObject bridges — used by `Node.property(name)` / `Edge.property(name)`
// etc. to surface a `PyPropertyFilterBuilder` from the typed Rust expression.
// ─────────────────────────────────────────────────────────────────────────────

impl<'py, E> IntoPyObject<'py> for PropertyExpr<E>
where
    E: EntityExpr + Clone + Send + Sync + 'static,
    PropertyExpr<E>: DynTemporalPropertyExpr,
{
    type Target = PyPropertyFilterBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let inner: Arc<PropertyExpr<E>> = Arc::new(self);
        let child = PyPropertyFilterBuilder::from_arc(inner.clone());
        let parent = PyPropertyExprBuilder::from_arc(inner);
        Bound::new(py, (child, parent))
    }
}

impl<'py, E> IntoPyObject<'py> for MetadataExpr<E>
where
    E: EntityExpr + Clone + Send + Sync + 'static,
    MetadataExpr<E>: DynPropertyExpr,
{
    type Target = PyPropertyExprBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyPropertyExprBuilder::wrap(self).into_pyobject(py)
    }
}

impl<'py, E> IntoPyObject<'py> for EdgeEndpointWrapper<PropertyExpr<E>>
where
    E: EntityExpr + Clone + Send + Sync + 'static,
    EdgeEndpointWrapper<PropertyExpr<E>>: DynTemporalPropertyExpr,
{
    type Target = PyPropertyFilterBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let inner: Arc<EdgeEndpointWrapper<PropertyExpr<E>>> = Arc::new(self);
        let child = PyPropertyFilterBuilder::from_arc(inner.clone());
        let parent = PyPropertyExprBuilder::from_arc(inner);
        Bound::new(py, (child, parent))
    }
}

impl<'py, E> IntoPyObject<'py> for EdgeEndpointWrapper<MetadataExpr<E>>
where
    E: EntityExpr + Clone + Send + Sync + 'static,
    EdgeEndpointWrapper<MetadataExpr<E>>: DynPropertyExpr,
{
    type Target = PyPropertyExprBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let inner: Arc<EdgeEndpointWrapper<MetadataExpr<E>>> = Arc::new(self);
        PyPropertyExprBuilder::from_arc(inner).into_pyobject(py)
    }
}

impl<'py> IntoPyObject<'py> for PyPropertyFilterBuilder {
    type Target = PyPropertyFilterBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let parent = PyPropertyExprBuilder::from_arc(self.0.clone());
        Bound::new(py, (self, parent))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// View-builder wrappers (graph / node / edge views restricting when+where
// predicates are evaluated, then exposing `.property()` / `.metadata()` etc.)
// ─────────────────────────────────────────────────────────────────────────────

/// Graph-level view filter — composes time / snapshot / layer restrictions
/// before applying node or edge predicates.
#[pyclass(
    name = "ViewFilterBuilder",
    module = "raphtory.filter",
    extends = PyFilterExpr,
    frozen
)]
pub struct PyViewFilterBuilder(pub(crate) DynView);

#[pymethods]
impl PyViewFilterBuilder {
    /// Restricts evaluation to events in the given time window.
    ///
    /// The window is inclusive of `start` and exclusive of `end`.
    ///
    /// Arguments:
    ///     start (int): Start time.
    ///     end (int): End time.
    ///
    /// Returns:
    ///     filter.ViewFilterBuilder:
    fn window(&self, start: EventTime, end: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(self.0.clone().window(start, end))
    }

    /// Restricts evaluation to a single point in time.
    ///
    /// Arguments:
    ///     time (int): Event time.
    ///
    /// Returns:
    ///     filter.ViewFilterBuilder:
    fn at(&self, time: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(self.0.clone().at(time))
    }

    /// Restricts evaluation to times strictly after the given time.
    fn after(&self, time: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(self.0.clone().after(time))
    }

    /// Restricts evaluation to times strictly before the given time.
    fn before(&self, time: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(self.0.clone().before(time))
    }

    /// Evaluates against the latest available state.
    fn latest(&self) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(self.0.clone().latest()))
    }

    /// Evaluates against a snapshot of the graph at a specific time.
    fn snapshot_at(&self, time: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(self.0.clone().snapshot_at(time)))
    }

    /// Evaluates against the most recent snapshot of the graph.
    fn snapshot_latest(&self) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(self.0.clone().snapshot_latest()))
    }

    /// Restricts evaluation to a single layer.
    fn layer(&self, layer: String) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(self.0.clone().layer(layer)))
    }

    /// Restricts evaluation to any of the given layers.
    fn layers(&self, layers: FromIterable<String>) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(self.0.clone().layer(layers)))
    }
}

impl<'py> IntoPyObject<'py> for PyViewFilterBuilder {
    type Target = PyViewFilterBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let parent = PyFilterExpr(self.0.clone());
        Bound::new(py, (self, parent))
    }
}

/// Node-side view + property predicate builder.
#[pyclass(
    name = "NodeViewPropsFilterBuilder",
    module = "raphtory.filter",
    frozen
)]
pub struct PyNodeViewPropsFilterBuilder(pub(crate) DynNodeViewProps);

#[pymethods]
impl PyNodeViewPropsFilterBuilder {
    fn is_active(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_active())
    }

    fn property(&self, name: String) -> PyPropertyFilterBuilder {
        PyPropertyFilterBuilder(Arc::new(self.0.property(name)))
    }

    fn metadata(&self, name: String) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(Arc::new(self.0.metadata(name)))
    }

    fn window(&self, start: EventTime, end: EventTime) -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(self.0.clone().window(start, end))
    }

    fn at(&self, time: EventTime) -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(self.0.clone().at(time))
    }

    fn after(&self, time: EventTime) -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(self.0.clone().after(time))
    }

    fn before(&self, time: EventTime) -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(self.0.clone().before(time))
    }

    fn latest(&self) -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(Arc::new(self.0.clone().latest()))
    }

    fn snapshot_at(&self, time: EventTime) -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(Arc::new(self.0.clone().snapshot_at(time)))
    }

    fn snapshot_latest(&self) -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(Arc::new(self.0.clone().snapshot_latest()))
    }

    fn layer(&self, layer: String) -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(Arc::new(self.0.clone().layer(layer)))
    }

    fn layers(&self, layers: FromIterable<String>) -> PyNodeViewPropsFilterBuilder {
        PyNodeViewPropsFilterBuilder(Arc::new(self.0.clone().layer(layers)))
    }
}

/// Edge-side view + property predicate builder.
#[pyclass(
    name = "EdgeViewPropsFilterBuilder",
    module = "raphtory.filter",
    frozen
)]
pub struct PyEdgeViewPropsFilterBuilder(pub(crate) DynEdgeViewProps);

#[pymethods]
impl PyEdgeViewPropsFilterBuilder {
    fn is_active(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_active())
    }

    fn is_valid(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_valid())
    }

    fn is_deleted(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_deleted())
    }

    fn is_self_loop(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_self_loop())
    }

    fn property(&self, name: String) -> PyPropertyFilterBuilder {
        PyPropertyFilterBuilder(Arc::new(self.0.property(name)))
    }

    fn metadata(&self, name: String) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(Arc::new(self.0.metadata(name)))
    }

    fn window(&self, start: EventTime, end: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(self.0.clone().window(start, end))
    }

    fn at(&self, time: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(self.0.clone().at(time))
    }

    fn after(&self, time: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(self.0.clone().after(time))
    }

    fn before(&self, time: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(self.0.clone().before(time))
    }

    fn latest(&self) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(self.0.clone().latest()))
    }

    fn snapshot_at(&self, time: EventTime) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(self.0.clone().snapshot_at(time)))
    }

    fn snapshot_latest(&self) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(self.0.clone().snapshot_latest()))
    }

    fn layer(&self, layer: String) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(self.0.clone().layer(layer)))
    }

    fn layers(&self, layers: FromIterable<String>) -> PyEdgeViewPropsFilterBuilder {
        PyEdgeViewPropsFilterBuilder(Arc::new(self.0.clone().layer(layers)))
    }
}
