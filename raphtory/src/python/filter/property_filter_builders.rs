use crate::{
    db::graph::views::filter::model::{
        edge_filter::EndpointWrapper,
        property_filter::{
            builders::{MetadataFilterBuilder, OpChainBuilder, PropertyFilterBuilder},
            ops::{ElemQualifierOps, ListAggOps, PropertyFilterOps},
        },
        InternalPropertyFilterBuilderOps, PropertyFilterFactory, TemporalPropertyFilterFactory,
        TryAsCompositeFilter,
    },
    prelude::PropertyFilter,
    python::{filter::filter_expr::PyFilterExpr, types::iterable::FromIterable},
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyErr, Python};
use raphtory_api::core::entities::properties::prop::Prop;
use std::sync::Arc;
use crate::db::graph::views::filter::CreateFilter;

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

pub trait DynListFilterOps: Send + Sync {
    fn any(&self) -> PyFilterOps;

    fn all(&self) -> PyFilterOps;

    fn len(&self) -> PyFilterOps;

    fn sum(&self) -> PyFilterOps;

    fn avg(&self) -> PyFilterOps;

    fn min(&self) -> PyFilterOps;

    fn max(&self) -> PyFilterOps;

    fn first(&self) -> PyFilterOps;

    fn last(&self) -> PyFilterOps;
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

impl<T> DynListFilterOps for T
where
    T: InternalPropertyFilterBuilderOps + 'static,
{
    fn any(&self) -> PyFilterOps {
        let filter = ElemQualifierOps::any(self);
        PyFilterOps::wrap(filter)
    }

    fn all(&self) -> PyFilterOps {
        PyFilterOps::wrap(ElemQualifierOps::all(self))
    }

    fn len(&self) -> PyFilterOps {
        PyFilterOps::wrap(ListAggOps::len(self))
    }

    fn sum(&self) -> PyFilterOps {
        PyFilterOps::wrap(ListAggOps::sum(self))
    }

    fn avg(&self) -> PyFilterOps {
        PyFilterOps::wrap(ListAggOps::avg(self))
    }

    fn min(&self) -> PyFilterOps {
        PyFilterOps::wrap(ListAggOps::min(self))
    }

    fn max(&self) -> PyFilterOps {
        PyFilterOps::wrap(ListAggOps::max(self))
    }

    fn first(&self) -> PyFilterOps {
        PyFilterOps::wrap(ListAggOps::first(self))
    }

    fn last(&self) -> PyFilterOps {
        PyFilterOps::wrap(ListAggOps::last(self))
    }
}

pub trait DynFilterOps: DynListFilterOps + DynPropertyFilterOps {}

impl<T: DynListFilterOps + DynPropertyFilterOps + ?Sized> DynFilterOps for T {}

#[pyclass(frozen, name = "FilterOps", module = "raphtory.filter", subclass)]
#[derive(Clone)]
pub struct PyFilterOps {
    ops: Arc<dyn DynFilterOps>,
}

impl PyFilterOps {
    pub fn wrap<T: DynFilterOps + 'static>(t: T) -> Self {
        Self { ops: Arc::new(t) }
    }

    pub fn from_arc(ops: Arc<dyn DynFilterOps>) -> Self {
        Self { ops }
    }
}

#[pymethods]
impl PyFilterOps {
    /// Returns a filter expression that checks if a specified property is equal to a given value.
    ///
    /// Arguments:
    ///     PropValue:
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn __eq__(&self, value: Prop) -> PyFilterExpr {
        self.ops.__eq__(value)
    }

    /// Returns a filter expression that checks if a specified property is not equal to a given value.
    ///
    /// Arguments:
    ///     PropValue:
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn __ne__(&self, value: Prop) -> PyFilterExpr {
        self.ops.__ne__(value)
    }

    /// Returns a filter expression that checks if a specified property is less than to a given value.
    ///
    /// Arguments:
    ///     PropValue:
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn __lt__(&self, value: Prop) -> PyFilterExpr {
        self.ops.__lt__(value)
    }

    /// Returns a filter expression that checks if a specified property is less than or equal to a given value.
    ///
    /// Arguments:
    ///     PropValue:
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn __le__(&self, value: Prop) -> PyFilterExpr {
        self.ops.__le__(value)
    }

    /// Returns a filter expression that checks if a specified property is greater than a given value.
    ///
    /// Arguments:
    ///     PropValue:
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn __gt__(&self, value: Prop) -> PyFilterExpr {
        self.ops.__gt__(value)
    }

    /// Returns a filter expression that checks if a specified property is greater than or equal to a given value.
    ///
    /// Arguments:
    ///     PropValue:
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn __ge__(&self, value: Prop) -> PyFilterExpr {
        self.ops.__ge__(value)
    }

    /// Returns a filter expression that checks if a given value is in a specified iterable of properties.
    ///
    /// Arguments:
    ///     values (list[PropValue]):
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        self.ops.is_in(values)
    }

    /// Returns a filter expression that checks if a given value is not in a specified iterable of properties.
    ///
    /// Arguments:
    ///     values (list[PropValue]):
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_not_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        self.ops.is_not_in(values)
    }

    /// Returns a filter expression that checks if a given value is none.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_none(&self) -> PyFilterExpr {
        self.ops.is_none()
    }

    /// Returns a filter expression that checks if a given value is some.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn is_some(&self) -> PyFilterExpr {
        self.ops.is_some()
    }

    fn starts_with(&self, value: Prop) -> PyFilterExpr {
        self.ops.starts_with(value)
    }

    fn ends_with(&self, value: Prop) -> PyFilterExpr {
        self.ops.ends_with(value)
    }

    /// Returns a filter expression that checks if this object contains a specified property.
    ///
    /// Arguments:
    ///     PropValue:
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn contains(&self, value: Prop) -> PyFilterExpr {
        self.ops.contains(value)
    }

    /// Returns a filter expression that checks if this object does not contain a specified property.
    ///
    /// Arguments:
    ///     PropValue:
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn not_contains(&self, value: Prop) -> PyFilterExpr {
        self.ops.not_contains(value)
    }

    /// Returns a filter expression that checks if the specified properties approximately match the specified string.
    ///
    /// Uses a specified Levenshtein distance and optional prefix matching.
    ///
    /// Arguments:
    ///     prop_value (str): Property to match against.
    ///     levenshtein_distance (int): Maximum levenshtein distance between the specified prop_value and the result.
    ///     prefix_match (bool): Enable prefix matching.
    ///
    /// Returns:
    ///     filter.FilterExpr:
    fn fuzzy_search(
        &self,
        prop_value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        self.ops
            .fuzzy_search(prop_value, levenshtein_distance, prefix_match)
    }

    pub fn first(&self) -> PyFilterOps {
        self.ops.first()
    }

    pub fn last(&self) -> PyFilterOps {
        self.ops.last()
    }

    pub fn any(&self) -> PyFilterOps {
        self.ops.any()
    }

    pub fn all(&self) -> PyFilterOps {
        self.ops.all()
    }

    fn len(&self) -> PyFilterOps {
        self.ops.len()
    }

    fn sum(&self) -> PyFilterOps {
        self.ops.sum()
    }

    fn avg(&self) -> PyFilterOps {
        self.ops.avg()
    }

    fn min(&self) -> PyFilterOps {
        self.ops.min()
    }

    fn max(&self) -> PyFilterOps {
        self.ops.max()
    }
}

#[pyclass(
    frozen,
    name = "PropertyFilterOps",
    module = "raphtory.filter",
    extends = PyFilterOps
)]
#[derive(Clone)]
pub struct PyPropertyFilterBuilder {
    ops: Arc<dyn DynPropertyFilterBuilderOps>,
}

impl PyPropertyFilterBuilder {
    pub fn from_arc(ops: Arc<dyn DynPropertyFilterBuilderOps>) -> Self {
        Self { ops }
    }
}

pub trait DynPropertyFilterBuilderOps: DynFilterOps {
    fn temporal(&self) -> PyFilterOps;
}

impl<T: DynFilterOps + TemporalPropertyFilterFactory> DynPropertyFilterBuilderOps for T
where
    T::Chained: 'static,
{
    fn temporal(&self) -> PyFilterOps {
        PyFilterOps::wrap(self.temporal())
    }
}

#[pymethods]
impl PyPropertyFilterBuilder {
    fn temporal(&self) -> PyFilterOps {
        self.ops.temporal()
    }
}

impl<'py, M: Clone + Send + Sync + 'static> IntoPyObject<'py> for PropertyFilterBuilder<M>
where
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
    OpChainBuilder<M>: InternalPropertyFilterBuilderOps<Marker = M>,
{
    type Target = PyPropertyFilterBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let inner: Arc<PropertyFilterBuilder<M>> = Arc::new(self);
        let child = PyPropertyFilterBuilder::from_arc(inner.clone());
        let parent = PyFilterOps::from_arc(inner);
        Bound::new(py, (child, parent))
    }
}

impl<'py, M: Send + Sync + Clone + 'static> IntoPyObject<'py> for MetadataFilterBuilder<M>
where
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
    OpChainBuilder<M>: InternalPropertyFilterBuilderOps<Marker = M>,
{
    type Target = PyFilterOps;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyFilterOps::wrap(self).into_pyobject(py)
    }
}

impl<'py, M> IntoPyObject<'py> for EndpointWrapper<PropertyFilterBuilder<M>>
where
    M: Clone + Send + Sync + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
    OpChainBuilder<M>: InternalPropertyFilterBuilderOps<Marker = M>,
{
    type Target = PyPropertyFilterBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let inner: Arc<EndpointWrapper<PropertyFilterBuilder<M>>> = Arc::new(self);
        let child = PyPropertyFilterBuilder::from_arc(inner.clone());
        let parent = PyFilterOps::from_arc(inner);
        Bound::new(py, (child, parent))
    }
}

impl<'py, M> IntoPyObject<'py> for EndpointWrapper<MetadataFilterBuilder<M>>
where
    M: Clone + Send + Sync + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
    OpChainBuilder<M>: InternalPropertyFilterBuilderOps<Marker = M>,
{
    type Target = PyFilterOps;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let inner: Arc<EndpointWrapper<MetadataFilterBuilder<M>>> = Arc::new(self);
        PyFilterOps::from_arc(inner).into_pyobject(py)
    }
}

impl<'py> IntoPyObject<'py> for PyPropertyFilterBuilder {
    type Target = PyPropertyFilterBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let parent = PyFilterOps::from_arc(self.ops.clone());
        Bound::new(py, (self, parent))
    }
}

pub trait DynPropertyFilterFactory: Send + Sync + 'static {
    fn property(&self, name: String) -> PyPropertyFilterBuilder;

    fn metadata(&self, name: String) -> PyFilterOps;
}

impl<T: PropertyFilterFactory + Send + Sync + 'static> DynPropertyFilterFactory for T {
    fn property(&self, name: String) -> PyPropertyFilterBuilder {
        PyPropertyFilterBuilder::from_arc(Arc::new(self.property(name)))
    }

    fn metadata(&self, name: String) -> PyFilterOps {
        PyFilterOps::wrap(self.metadata(name))
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

    fn metadata(&self, name: String) -> PyFilterOps {
        self.0.metadata(name)
    }
}
