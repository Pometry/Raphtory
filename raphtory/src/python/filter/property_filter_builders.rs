use crate::{
    db::graph::views::filter::{
        model::{
            edge_filter::EdgeEndpointWrapper,
            property_filter::{
                builders::{MetadataFilterBuilder, PropertyExprBuilder, PropertyFilterBuilder},
                ops::{ElemQualifierOps, ListAggOps, PropertyFilterOps},
            },
            DynEdgeViewFilterOps, DynEdgeViewProps, DynNodeViewProps, DynPropertyFilterBuilder,
            DynTemporalPropertyFilterBuilder, DynView, EntityMarker, InternalPropertyFilterBuilder,
            PropertyFilterFactory, TemporalPropertyFilterFactory, TryAsCompositeFilter,
            ViewWrapOps,
        },
        CreateFilter,
    },
    prelude::PropertyFilter,
    python::{filter::filter_expr::PyFilterExpr, types::iterable::FromIterable},
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyErr, Python};
use raphtory_api::core::{entities::properties::prop::Prop, storage::timeindex::EventTime};
use std::sync::Arc;

#[pyclass(frozen, name = "FilterOps", module = "raphtory.filter", subclass)]
#[derive(Clone)]
pub struct PyPropertyExprBuilder(pub Arc<dyn DynPropertyFilterBuilder>);

impl PyPropertyExprBuilder {
    pub fn wrap<T: DynPropertyFilterBuilder + 'static>(t: T) -> Self {
        Self(Arc::new(t))
    }

    pub fn from_arc(inner: Arc<dyn DynPropertyFilterBuilder>) -> Self {
        Self(inner)
    }
}

#[pymethods]
impl PyPropertyExprBuilder {
    /// Returns a filter expression that checks whether the property
    /// is equal to the given value.
    ///
    /// Arguments:
    ///     value (Prop): Property value to compare against.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating equality.
    fn __eq__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.eq(value))
    }

    /// Returns a filter expression that checks whether the property
    /// is not equal to the given value.
    ///
    /// Arguments:
    ///     value (Prop): Property value to compare against.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating inequality.
    fn __ne__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.ne(value))
    }

    /// Returns a filter expression that checks whether the property
    /// is less than the given value.
    ///
    /// Arguments:
    ///     value (Prop): Upper bound (exclusive) for the property.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating a `<` comparison.
    fn __lt__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.lt(value))
    }

    /// Returns a filter expression that checks whether the property
    /// is less than or equal to the given value.
    ///
    /// Arguments:
    ///     value (Prop): Upper bound (inclusive) for the property.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating a `<=` comparison.
    fn __le__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.le(value))
    }

    /// Returns a filter expression that checks whether the property
    /// is greater than the given value.
    ///
    /// Arguments:
    ///     value (Prop): Lower bound (exclusive) for the property.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating a `>` comparison.
    fn __gt__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.gt(value))
    }

    /// Returns a filter expression that checks whether the property
    /// is greater than or equal to the given value.
    ///
    /// Arguments:
    ///     value (Prop): Lower bound (inclusive) for the property.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating a `>=` comparison.
    fn __ge__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.ge(value))
    }

    /// Returns a filter expression that checks whether the property
    /// is contained within the specified iterable of values.
    ///
    /// Arguments:
    ///     values (list[Prop]): Iterable of property values to match against.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating membership.
    fn is_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        PyFilterExpr(self.0.is_in(values))
    }

    /// Returns a filter expression that checks whether the property
    /// is **not** contained within the specified iterable of values.
    ///
    /// Arguments:
    ///     values (list[Prop]): Iterable of property values to exclude.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating non-membership.
    fn is_not_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        PyFilterExpr(self.0.is_not_in(values))
    }

    /// Returns a filter expression that checks whether the property
    /// value is `None` / missing.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating `value is None`.
    fn is_none(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.is_none())
    }

    /// Returns a filter expression that checks whether the property
    /// value is present (not `None`).
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating `value is not None`.
    fn is_some(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.is_some())
    }

    /// Returns a filter expression that checks whether the property's
    /// string representation starts with the given value.
    ///
    /// Arguments:
    ///     value (Prop): Prefix to check for.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating prefix matching.
    fn starts_with(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.starts_with(value))
    }

    /// Returns a filter expression that checks whether the property's
    /// string representation ends with the given value.
    ///
    /// Arguments:
    ///     value (Prop): Suffix to check for.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating suffix matching.
    fn ends_with(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.ends_with(value))
    }

    /// Returns a filter expression that checks whether the property's
    /// string representation contains the given value.
    ///
    /// Arguments:
    ///     value (Prop): Substring that must appear within the value.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating substring search.
    fn contains(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.contains(value))
    }

    /// Returns a filter expression that checks whether the property's
    /// string representation **does not** contain the given value.
    ///
    /// Arguments:
    ///     value (Prop): Substring that must not appear within the value.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating substring exclusion.
    fn not_contains(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.not_contains(value))
    }

    /// Returns a filter expression that performs fuzzy matching
    /// against the property's string value.
    ///
    /// Uses a specified Levenshtein distance and optional prefix matching.
    ///
    /// Arguments:
    ///     prop_value (str): String to approximately match against.
    ///     levenshtein_distance (int): Maximum allowed Levenshtein distance.
    ///     prefix_match (bool): Whether to require a matching prefix.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression performing approximate text matching.
    fn fuzzy_search(
        &self,
        prop_value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(
            self.0
                .fuzzy_search(prop_value, levenshtein_distance, prefix_match),
        )
    }

    pub fn first(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.first())
    }

    pub fn last(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.last())
    }

    pub fn any(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.any())
    }

    pub fn all(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.all())
    }

    fn len(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.len())
    }

    fn sum(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.sum())
    }

    fn avg(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.avg())
    }

    fn min(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.min())
    }

    fn max(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.max())
    }
}

#[pyclass(
    frozen,
    name = "PropertyFilterOps",
    module = "raphtory.filter",
    extends = PyPropertyExprBuilder
)]
#[derive(Clone)]
pub struct PyPropertyFilterBuilder(pub(crate) Arc<dyn DynTemporalPropertyFilterBuilder>);

impl PyPropertyFilterBuilder {
    pub(crate) fn from_arc(inner: Arc<dyn DynTemporalPropertyFilterBuilder>) -> Self {
        Self(inner)
    }
}

#[pymethods]
impl PyPropertyFilterBuilder {
    fn temporal(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.temporal())
    }
}

impl<'py, M: Into<EntityMarker> + Clone + Send + Sync + 'static> IntoPyObject<'py>
    for PropertyFilterBuilder<M>
where
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
    PropertyExprBuilder<M>: InternalPropertyFilterBuilder<Marker = M>,
{
    type Target = PyPropertyFilterBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let inner: Arc<PropertyFilterBuilder<M>> = Arc::new(self);
        let child = PyPropertyFilterBuilder::from_arc(inner.clone());
        let parent = PyPropertyExprBuilder::from_arc(inner);
        Bound::new(py, (child, parent))
    }
}

impl<'py, M: Into<EntityMarker> + Send + Sync + Clone + 'static> IntoPyObject<'py>
    for MetadataFilterBuilder<M>
where
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
    PropertyExprBuilder<M>: InternalPropertyFilterBuilder<Marker = M>,
{
    type Target = PyPropertyExprBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyPropertyExprBuilder::wrap(self).into_pyobject(py)
    }
}

impl<'py, M> IntoPyObject<'py> for EdgeEndpointWrapper<PropertyFilterBuilder<M>>
where
    M: Into<EntityMarker> + Clone + Send + Sync + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
    PropertyExprBuilder<M>: InternalPropertyFilterBuilder<Marker = M>,
{
    type Target = PyPropertyFilterBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let inner: Arc<EdgeEndpointWrapper<PropertyFilterBuilder<M>>> = Arc::new(self);
        let child = PyPropertyFilterBuilder::from_arc(inner.clone());
        let parent = PyPropertyExprBuilder::from_arc(inner);
        Bound::new(py, (child, parent))
    }
}

impl<'py, M> IntoPyObject<'py> for EdgeEndpointWrapper<MetadataFilterBuilder<M>>
where
    M: Into<EntityMarker> + Clone + Send + Sync + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
    PropertyExprBuilder<M>: InternalPropertyFilterBuilder<Marker = M>,
{
    type Target = PyPropertyExprBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let inner: Arc<EdgeEndpointWrapper<MetadataFilterBuilder<M>>> = Arc::new(self);
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

#[pyclass(
    name = "ViewFilterBuilder",
    module = "raphtory.filter",
    extends = PyFilterExpr,
    frozen
)]
pub struct PyViewFilterBuilder(pub(crate) DynView);

#[pymethods]
impl PyViewFilterBuilder {
    fn window(&self, start: EventTime, end: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(self.0.clone().window(start, end))
    }

    fn at(&self, time: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(self.0.clone().at(time))
    }

    fn after(&self, time: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(self.0.clone().after(time))
    }

    fn before(&self, time: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(self.0.clone().before(time))
    }

    fn latest(&self) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(self.0.clone().latest()))
    }

    fn snapshot_at(&self, time: EventTime) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(self.0.clone().snapshot_at(time)))
    }

    fn snapshot_latest(&self) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(self.0.clone().snapshot_latest()))
    }

    fn layer(&self, layer: String) -> PyViewFilterBuilder {
        PyViewFilterBuilder(Arc::new(self.0.clone().layer(layer)))
    }

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

#[pyclass(
    name = "NodeViewPropsFilterBuilder",
    module = "raphtory.filter",
    subclass,
    frozen
)]
pub struct PyNodeViewPropsFilterBuilder(pub(crate) DynNodeViewProps);

#[pymethods]
impl PyNodeViewPropsFilterBuilder {
    fn is_active(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.dyn_is_active())
    }

    fn property(&self, name: String) -> PyPropertyFilterBuilder {
        PyPropertyFilterBuilder(self.0.property(name))
    }

    fn metadata(&self, name: String) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.metadata(name))
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

#[pyclass(
    name = "EdgeViewPropsFilterBuilder",
    module = "raphtory.filter",
    subclass,
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
        PyPropertyFilterBuilder(self.0.property(name))
    }

    fn metadata(&self, name: String) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder(self.0.metadata(name))
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
