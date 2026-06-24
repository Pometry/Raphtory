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
        edge_filter::EdgeEndpointWrapper, node_expr::EntityExpr, DynEdgeViewFilterOps,
        DynEdgeViewProps, DynNodeViewProps, MetadataExpr, PropertyExpr, ViewWrapOps,
    },
    python::{filter::filter_expr::PyFilterExpr, types::iterable::FromIterable},
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyErr, Python};
use raphtory_api::core::storage::timeindex::EventTime;
use std::sync::Arc;
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
