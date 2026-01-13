use crate::{
    db::graph::views::filter::{
        model::{
            edge_filter::EdgeEndpointWrapper,
            property_filter::{
                builders::{MetadataFilterBuilder, PropertyExprBuilder, PropertyFilterBuilder},
                ops::{ElemQualifierOps, ListAggOps, PropertyFilterOps},
            },
            InternalPropertyFilterBuilder, PropertyFilterFactory, TemporalPropertyFilterFactory,
            TryAsCompositeFilter,
        },
        CreateFilter,
    },
    prelude::PropertyFilter,
    python::{filter::filter_expr::PyFilterExpr, types::iterable::FromIterable},
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyErr, Python};
use raphtory_api::core::entities::properties::prop::Prop;
use std::sync::Arc;

pub trait DynPropertyFilterOps: Send + Sync {
    fn __eq__(&self, value: Prop) -> PyFilterExpr;

    fn __ne__(&self, value: Prop) -> PyFilterExpr;

    fn __lt__(&self, value: Prop) -> PyFilterExpr;

    fn __le__(&self, value: Prop) -> PyFilterExpr;

    fn __gt__(&self, value: Prop) -> PyFilterExpr;

    fn __ge__(&self, value: Prop) -> PyFilterExpr;

    fn is_in(&self, values: FromIterable<Prop>) -> PyFilterExpr;

    fn is_not_in(&self, values: FromIterable<Prop>) -> PyFilterExpr;

    fn is_none(&self) -> PyFilterExpr;

    fn is_some(&self) -> PyFilterExpr;

    fn starts_with(&self, value: Prop) -> PyFilterExpr;

    fn ends_with(&self, value: Prop) -> PyFilterExpr;

    fn contains(&self, value: Prop) -> PyFilterExpr;

    fn not_contains(&self, value: Prop) -> PyFilterExpr;

    fn fuzzy_search(
        &self,
        prop_value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr;
}

impl<T: PropertyFilterOps> DynPropertyFilterOps for T {
    fn __eq__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(PropertyFilterOps::eq(self, value)))
    }

    fn __ne__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(PropertyFilterOps::ne(self, value)))
    }

    fn __lt__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(PropertyFilterOps::lt(self, value)))
    }

    fn __le__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(PropertyFilterOps::le(self, value)))
    }

    fn __gt__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(PropertyFilterOps::gt(self, value)))
    }

    fn __ge__(&self, value: Prop) -> PyFilterExpr {
        let filter = Arc::new(PropertyFilterOps::ge(self, value));
        PyFilterExpr(filter)
    }

    fn is_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(PropertyFilterOps::is_in(self, values)))
    }

    fn is_not_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(PropertyFilterOps::is_not_in(self, values)))
    }

    fn is_none(&self) -> PyFilterExpr {
        PyFilterExpr(Arc::new(PropertyFilterOps::is_none(self)))
    }

    fn is_some(&self) -> PyFilterExpr {
        PyFilterExpr(Arc::new(PropertyFilterOps::is_some(self)))
    }

    fn starts_with(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(PropertyFilterOps::starts_with(self, value)))
    }

    fn ends_with(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(PropertyFilterOps::ends_with(self, value)))
    }

    fn contains(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(PropertyFilterOps::contains(self, value)))
    }

    fn not_contains(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(PropertyFilterOps::not_contains(self, value)))
    }

    fn fuzzy_search(
        &self,
        prop_value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(Arc::new(PropertyFilterOps::fuzzy_search(
            self,
            prop_value,
            levenshtein_distance,
            prefix_match,
        )))
    }
}

pub trait DynPropertyExprBuilderOps: Send + Sync {
    fn any(&self) -> PyPropertyExprBuilder;

    fn all(&self) -> PyPropertyExprBuilder;

    fn len(&self) -> PyPropertyExprBuilder;

    fn sum(&self) -> PyPropertyExprBuilder;

    fn avg(&self) -> PyPropertyExprBuilder;

    fn min(&self) -> PyPropertyExprBuilder;

    fn max(&self) -> PyPropertyExprBuilder;

    fn first(&self) -> PyPropertyExprBuilder;

    fn last(&self) -> PyPropertyExprBuilder;
}

impl<T> DynPropertyExprBuilderOps for T
where
    T: InternalPropertyFilterBuilder + 'static,
{
    fn any(&self) -> PyPropertyExprBuilder {
        let filter = ElemQualifierOps::any(self);
        PyPropertyExprBuilder::wrap(filter)
    }

    fn all(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder::wrap(ElemQualifierOps::all(self))
    }

    fn len(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder::wrap(ListAggOps::len(self))
    }

    fn sum(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder::wrap(ListAggOps::sum(self))
    }

    fn avg(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder::wrap(ListAggOps::avg(self))
    }

    fn min(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder::wrap(ListAggOps::min(self))
    }

    fn max(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder::wrap(ListAggOps::max(self))
    }

    fn first(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder::wrap(ListAggOps::first(self))
    }

    fn last(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder::wrap(ListAggOps::last(self))
    }
}

pub trait DynPropertyExprOps: DynPropertyExprBuilderOps + DynPropertyFilterOps {}

impl<T: DynPropertyExprBuilderOps + DynPropertyFilterOps + ?Sized> DynPropertyExprOps for T {}

#[pyclass(frozen, name = "FilterOps", module = "raphtory.filter", subclass)]
#[derive(Clone)]
pub struct PyPropertyExprBuilder {
    inner: Arc<dyn DynPropertyExprOps>,
}

impl PyPropertyExprBuilder {
    pub fn wrap<T: DynPropertyExprOps + 'static>(t: T) -> Self {
        Self { inner: Arc::new(t) }
    }

    pub fn from_arc(inner: Arc<dyn DynPropertyExprOps>) -> Self {
        Self { inner }
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
        self.inner.__eq__(value)
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
        self.inner.__ne__(value)
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
        self.inner.__lt__(value)
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
        self.inner.__le__(value)
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
        self.inner.__gt__(value)
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
        self.inner.__ge__(value)
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
        self.inner.is_in(values)
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
        self.inner.is_not_in(values)
    }

    /// Returns a filter expression that checks whether the property
    /// value is `None` / missing.
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating `value is None`.
    fn is_none(&self) -> PyFilterExpr {
        self.inner.is_none()
    }

    /// Returns a filter expression that checks whether the property
    /// value is present (not `None`).
    ///
    /// Returns:
    ///     filter.FilterExpr: A filter expression evaluating `value is not None`.
    fn is_some(&self) -> PyFilterExpr {
        self.inner.is_some()
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
        self.inner.starts_with(value)
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
        self.inner.ends_with(value)
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
        self.inner.contains(value)
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
        self.inner.not_contains(value)
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
        self.inner
            .fuzzy_search(prop_value, levenshtein_distance, prefix_match)
    }

    pub fn first(&self) -> PyPropertyExprBuilder {
        self.inner.first()
    }

    pub fn last(&self) -> PyPropertyExprBuilder {
        self.inner.last()
    }

    pub fn any(&self) -> PyPropertyExprBuilder {
        self.inner.any()
    }

    pub fn all(&self) -> PyPropertyExprBuilder {
        self.inner.all()
    }

    fn len(&self) -> PyPropertyExprBuilder {
        self.inner.len()
    }

    fn sum(&self) -> PyPropertyExprBuilder {
        self.inner.sum()
    }

    fn avg(&self) -> PyPropertyExprBuilder {
        self.inner.avg()
    }

    fn min(&self) -> PyPropertyExprBuilder {
        self.inner.min()
    }

    fn max(&self) -> PyPropertyExprBuilder {
        self.inner.max()
    }
}

#[pyclass(
    frozen,
    name = "PropertyFilterOps",
    module = "raphtory.filter",
    extends = PyPropertyExprBuilder
)]
#[derive(Clone)]
pub struct PyPropertyFilterBuilder {
    inner: Arc<dyn DynTemporalPropertyFilterFactory>,
}

impl PyPropertyFilterBuilder {
    pub fn from_arc(inner: Arc<dyn DynTemporalPropertyFilterFactory>) -> Self {
        Self { inner }
    }
}

pub trait DynTemporalPropertyFilterFactory: DynPropertyExprOps {
    fn temporal(&self) -> PyPropertyExprBuilder;
}

impl<T: DynPropertyExprOps + TemporalPropertyFilterFactory> DynTemporalPropertyFilterFactory for T
where
    T::ExprBuilder: 'static,
{
    fn temporal(&self) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder::wrap(self.temporal())
    }
}

#[pymethods]
impl PyPropertyFilterBuilder {
    fn temporal(&self) -> PyPropertyExprBuilder {
        self.inner.temporal()
    }
}

impl<'py, M: Clone + Send + Sync + 'static> IntoPyObject<'py> for PropertyFilterBuilder<M>
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

impl<'py, M: Send + Sync + Clone + 'static> IntoPyObject<'py> for MetadataFilterBuilder<M>
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
    M: Clone + Send + Sync + 'static,
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
    M: Clone + Send + Sync + 'static,
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
        let parent = PyPropertyExprBuilder::from_arc(self.inner.clone());
        Bound::new(py, (self, parent))
    }
}

pub trait DynPropertyFilterFactory: Send + Sync + 'static {
    fn property(&self, name: String) -> PyPropertyFilterBuilder;

    fn metadata(&self, name: String) -> PyPropertyExprBuilder;
}

impl<T: PropertyFilterFactory + Send + Sync + 'static> DynPropertyFilterFactory for T {
    fn property(&self, name: String) -> PyPropertyFilterBuilder {
        PyPropertyFilterBuilder::from_arc(Arc::new(self.property(name)))
    }

    fn metadata(&self, name: String) -> PyPropertyExprBuilder {
        PyPropertyExprBuilder::wrap(self.metadata(name))
    }
}

#[pyclass(
    name = "PropertyFilterFactory",
    module = "raphtory.filter",
    subclass,
    frozen
)]
pub struct PyPropertyFilterFactory(Arc<dyn DynPropertyFilterFactory>);

impl PyPropertyFilterFactory {
    pub fn wrap<T: DynPropertyFilterFactory>(value: T) -> Self {
        Self(Arc::new(value))
    }
}

#[pymethods]
impl PyPropertyFilterFactory {
    fn property(&self, name: String) -> PyPropertyFilterBuilder {
        self.0.property(name)
    }

    fn metadata(&self, name: String) -> PyPropertyExprBuilder {
        self.0.metadata(name)
    }
}
