use crate::{
    db::graph::views::filter::{
        internal::CreateFilter,
        model::{
            property_filter::{
                ElemQualifierOps, ListAggOps, MetadataFilterBuilder, OpChainBuilder,
                PropertyFilterBuilder, PropertyFilterOps,
            },
            TryAsCompositeFilter,
        },
    },
    prelude::PropertyFilter,
    python::{filter::filter_expr::PyFilterExpr, types::iterable::FromIterable},
};
use pyo3::{
    exceptions::PyTypeError, pyclass, pymethods, Bound, IntoPyObject, PyErr, PyResult, Python,
};
use raphtory_api::core::entities::properties::prop::Prop;
use std::sync::Arc;

pub trait DynFilterOps: Send + Sync {
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

    fn any(&self) -> PyResult<PyPropertyFilterOps>;

    fn all(&self) -> PyResult<PyPropertyFilterOps>;

    fn len(&self) -> PyResult<PyPropertyFilterOps>;

    fn sum(&self) -> PyResult<PyPropertyFilterOps>;

    fn avg(&self) -> PyResult<PyPropertyFilterOps>;

    fn min(&self) -> PyResult<PyPropertyFilterOps>;

    fn max(&self) -> PyResult<PyPropertyFilterOps>;

    fn first(&self) -> PyResult<PyPropertyFilterOps>;

    fn last(&self) -> PyResult<PyPropertyFilterOps>;

    fn temporal(&self) -> PyResult<PyPropertyFilterOps>;
}

impl DynFilterOps for Arc<dyn DynFilterOps> {
    #[inline]
    fn __eq__(&self, v: Prop) -> PyFilterExpr {
        (**self).__eq__(v)
    }

    #[inline]
    fn __ne__(&self, v: Prop) -> PyFilterExpr {
        (**self).__ne__(v)
    }

    #[inline]
    fn __lt__(&self, v: Prop) -> PyFilterExpr {
        (**self).__lt__(v)
    }

    #[inline]
    fn __le__(&self, v: Prop) -> PyFilterExpr {
        (**self).__le__(v)
    }

    #[inline]
    fn __gt__(&self, v: Prop) -> PyFilterExpr {
        (**self).__gt__(v)
    }

    #[inline]
    fn __ge__(&self, v: Prop) -> PyFilterExpr {
        (**self).__ge__(v)
    }

    #[inline]
    fn is_in(&self, vs: FromIterable<Prop>) -> PyFilterExpr {
        (**self).is_in(vs)
    }

    #[inline]
    fn is_not_in(&self, vs: FromIterable<Prop>) -> PyFilterExpr {
        (**self).is_not_in(vs)
    }

    #[inline]
    fn is_none(&self) -> PyFilterExpr {
        (**self).is_none()
    }

    #[inline]
    fn is_some(&self) -> PyFilterExpr {
        (**self).is_some()
    }

    #[inline]
    fn starts_with(&self, v: Prop) -> PyFilterExpr {
        (**self).starts_with(v)
    }

    #[inline]
    fn ends_with(&self, v: Prop) -> PyFilterExpr {
        (**self).ends_with(v)
    }

    #[inline]
    fn contains(&self, v: Prop) -> PyFilterExpr {
        (**self).contains(v)
    }

    #[inline]
    fn not_contains(&self, v: Prop) -> PyFilterExpr {
        (**self).not_contains(v)
    }

    #[inline]
    fn fuzzy_search(
        &self,
        prop_value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        (**self).fuzzy_search(prop_value, levenshtein_distance, prefix_match)
    }

    #[inline]
    fn any(&self) -> PyResult<PyPropertyFilterOps> {
        (**self).any()
    }

    #[inline]
    fn all(&self) -> PyResult<PyPropertyFilterOps> {
        (**self).all()
    }

    #[inline]
    fn len(&self) -> PyResult<PyPropertyFilterOps> {
        (**self).len()
    }

    #[inline]
    fn sum(&self) -> PyResult<PyPropertyFilterOps> {
        (**self).sum()
    }

    #[inline]
    fn avg(&self) -> PyResult<PyPropertyFilterOps> {
        (**self).avg()
    }

    #[inline]
    fn min(&self) -> PyResult<PyPropertyFilterOps> {
        (**self).min()
    }

    #[inline]
    fn max(&self) -> PyResult<PyPropertyFilterOps> {
        (**self).max()
    }

    #[inline]
    fn first(&self) -> PyResult<PyPropertyFilterOps> {
        (**self).first()
    }

    #[inline]
    fn last(&self) -> PyResult<PyPropertyFilterOps> {
        (**self).last()
    }

    #[inline]
    fn temporal(&self) -> PyResult<PyPropertyFilterOps> {
        (**self).temporal()
    }
}

impl<M> DynFilterOps for PropertyFilterBuilder<M>
where
    M: Clone + Send + Sync + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
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
        PyFilterExpr(Arc::new(PropertyFilterOps::ge(self, value)))
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

    fn any(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(ElemQualifierOps::any(self)))
    }

    fn all(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(ElemQualifierOps::all(self)))
    }

    fn len(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(ListAggOps::len(self)))
    }

    fn sum(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(ListAggOps::sum(self)))
    }

    fn avg(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(ListAggOps::avg(self)))
    }

    fn min(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(ListAggOps::min(self)))
    }

    fn max(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(ListAggOps::max(self)))
    }

    fn first(&self) -> PyResult<PyPropertyFilterOps> {
        Err(PyTypeError::new_err(
            "first() is only valid on temporal properties. Call temporal() first.",
        ))
    }

    fn last(&self) -> PyResult<PyPropertyFilterOps> {
        Err(PyTypeError::new_err(
            "last() is only valid on temporal properties. Call temporal() first.",
        ))
    }

    fn temporal(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(self.clone().temporal()))
    }
}

impl<M> DynFilterOps for MetadataFilterBuilder<M>
where
    M: Clone + Send + Sync + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
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
        PyFilterExpr(Arc::new(PropertyFilterOps::ge(self, value)))
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

    fn any(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(ElemQualifierOps::any(self)))
    }

    fn all(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(ElemQualifierOps::all(self)))
    }

    fn len(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(ListAggOps::len(self)))
    }

    fn sum(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(ListAggOps::sum(self)))
    }

    fn avg(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(ListAggOps::avg(self)))
    }

    fn min(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(ListAggOps::min(self)))
    }

    fn max(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(ListAggOps::max(self)))
    }

    fn first(&self) -> PyResult<PyPropertyFilterOps> {
        Err(PyTypeError::new_err(
            "first() is only valid on temporal properties.",
        ))
    }

    fn last(&self) -> PyResult<PyPropertyFilterOps> {
        Err(PyTypeError::new_err(
            "last() is only valid on temporal properties.",
        ))
    }

    fn temporal(&self) -> PyResult<PyPropertyFilterOps> {
        Err(PyTypeError::new_err(
            "temporal() is only valid on standard properties, not metadata.",
        ))
    }
}

impl<M> DynFilterOps for OpChainBuilder<M>
where
    M: Send + Sync + Clone + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
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
        PyFilterExpr(Arc::new(PropertyFilterOps::ge(self, value)))
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

    fn any(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(self.clone().any()))
    }

    fn all(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(self.clone().all()))
    }

    fn len(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(self.clone().len()))
    }

    fn sum(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(self.clone().sum()))
    }

    fn avg(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(self.clone().avg()))
    }

    fn min(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(self.clone().min()))
    }

    fn max(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(self.clone().max()))
    }

    fn first(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(self.clone().first()))
    }

    fn last(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::wrap(self.clone().last()))
    }

    fn temporal(&self) -> PyResult<PyPropertyFilterOps> {
        Err(PyTypeError::new_err(
            "temporal() must be called on a property builder, not on an existing chain.",
        ))
    }
}

#[pyclass(
    frozen,
    name = "PropertyFilterOps",
    module = "raphtory.filter",
    subclass
)]
pub struct PyPropertyFilterOps {
    ops: Arc<dyn DynFilterOps>,
}

impl PyPropertyFilterOps {
    fn wrap<T: DynFilterOps + 'static>(t: T) -> Self {
        Self { ops: Arc::new(t) }
    }

    fn from_arc(ops: Arc<dyn DynFilterOps>) -> Self {
        Self { ops }
    }
}

#[pymethods]
impl PyPropertyFilterOps {
    fn __eq__(&self, value: Prop) -> PyFilterExpr {
        self.ops.__eq__(value)
    }

    fn __ne__(&self, value: Prop) -> PyFilterExpr {
        self.ops.__ne__(value)
    }

    fn __lt__(&self, value: Prop) -> PyFilterExpr {
        self.ops.__lt__(value)
    }

    fn __le__(&self, value: Prop) -> PyFilterExpr {
        self.ops.__le__(value)
    }

    fn __gt__(&self, value: Prop) -> PyFilterExpr {
        self.ops.__gt__(value)
    }

    fn __ge__(&self, value: Prop) -> PyFilterExpr {
        self.ops.__ge__(value)
    }

    fn is_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        self.ops.is_in(values)
    }

    fn is_not_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        self.ops.is_not_in(values)
    }

    fn is_none(&self) -> PyFilterExpr {
        self.ops.is_none()
    }

    fn is_some(&self) -> PyFilterExpr {
        self.ops.is_some()
    }

    fn starts_with(&self, value: Prop) -> PyFilterExpr {
        self.ops.starts_with(value)
    }

    fn ends_with(&self, value: Prop) -> PyFilterExpr {
        self.ops.ends_with(value)
    }

    fn contains(&self, value: Prop) -> PyFilterExpr {
        self.ops.contains(value)
    }

    fn not_contains(&self, value: Prop) -> PyFilterExpr {
        self.ops.not_contains(value)
    }

    fn fuzzy_search(
        &self,
        prop_value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        self.ops
            .fuzzy_search(prop_value, levenshtein_distance, prefix_match)
    }

    pub fn first(&self) -> PyResult<PyPropertyFilterOps> {
        self.ops.first()
    }

    pub fn last(&self) -> PyResult<PyPropertyFilterOps> {
        self.ops.last()
    }

    pub fn any(&self) -> PyResult<PyPropertyFilterOps> {
        self.ops.any()
    }

    pub fn all(&self) -> PyResult<PyPropertyFilterOps> {
        self.ops.all()
    }

    fn len(&self) -> PyResult<PyPropertyFilterOps> {
        self.ops.len()
    }

    fn sum(&self) -> PyResult<PyPropertyFilterOps> {
        self.ops.sum()
    }

    fn avg(&self) -> PyResult<PyPropertyFilterOps> {
        self.ops.avg()
    }

    fn min(&self) -> PyResult<PyPropertyFilterOps> {
        self.ops.min()
    }

    fn max(&self) -> PyResult<PyPropertyFilterOps> {
        self.ops.max()
    }
}

trait DynPropertyFilterBuilderOps: DynFilterOps {
    fn as_dyn(self: Arc<Self>) -> Arc<dyn DynFilterOps>;
}

impl<T> DynPropertyFilterBuilderOps for T
where
    T: DynFilterOps + 'static,
{
    fn as_dyn(self: Arc<Self>) -> Arc<dyn DynFilterOps> {
        self
    }
}

#[pyclass(
    frozen,
    name = "Property",
    module = "raphtory.filter",
    extends = PyPropertyFilterOps
)]
#[derive(Clone)]
pub struct PyPropertyFilterBuilder(Arc<dyn DynPropertyFilterBuilderOps>);

impl<'py, M: Clone + Send + Sync + 'static> IntoPyObject<'py> for PropertyFilterBuilder<M>
where
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    type Target = PyPropertyFilterBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let inner: Arc<PropertyFilterBuilder<M>> = Arc::new(self);
        let parent = PyPropertyFilterOps::from_arc(inner.clone().as_dyn());
        let child = PyPropertyFilterBuilder(inner);
        Bound::new(py, (child, parent))
    }
}

#[pymethods]
impl PyPropertyFilterBuilder {
    fn temporal(&self) -> PyResult<PyPropertyFilterOps> {
        self.0.temporal()
    }
}

#[pyclass(
    frozen,
    name = "Metadata",
    module = "raphtory.filter",
    extends = PyPropertyFilterOps
)]
#[derive(Clone)]
pub struct PyMetadataFilterBuilder;

impl<'py, M: Send + Sync + Clone + 'static> IntoPyObject<'py> for MetadataFilterBuilder<M>
where
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    type Target = PyMetadataFilterBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let inner: Arc<MetadataFilterBuilder<M>> = Arc::new(self);
        let parent = PyPropertyFilterOps::from_arc(inner.clone().as_dyn());
        let child = PyMetadataFilterBuilder;
        Bound::new(py, (child, parent))
    }
}
