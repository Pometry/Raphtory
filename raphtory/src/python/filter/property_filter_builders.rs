use crate::{
    db::graph::views::filter::{
        internal::CreateFilter,
        model::{
            property_filter::{
                ElemQualifierOps, ListAggOps, MetadataFilterBuilder, PropertyFilterBuilder,
                PropertyFilterOps,
            },
            TryAsCompositeFilter,
        },
    },
    prelude::PropertyFilter,
    python::{filter::filter_expr::PyFilterExpr, types::iterable::FromIterable},
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyErr, Python};
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

impl<T> DynFilterOps for T
where
    T: PropertyFilterOps + ElemQualifierOps + ListAggOps + Clone + Send + Sync + 'static,
    PropertyFilter<T::Marker>: CreateFilter + TryAsCompositeFilter,
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

    fn any(&self) -> PyFilterOps {
        PyFilterOps::wrap(ElemQualifierOps::any(self))
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

#[pyclass(frozen, name = "FilterOps", module = "raphtory.filter", subclass)]
#[derive(Clone)]
pub struct PyFilterOps {
    ops: Arc<dyn DynFilterOps>,
}

impl PyFilterOps {
    fn wrap<T: DynFilterOps + 'static>(t: T) -> Self {
        Self { ops: Arc::new(t) }
    }

    pub fn from_arc(ops: Arc<dyn DynFilterOps>) -> Self {
        Self { ops }
    }
}

#[pymethods]
impl PyFilterOps {
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

impl<T> DynPropertyFilterBuilderOps for PropertyFilterBuilder<T>
where
    PropertyFilter<T>: CreateFilter + TryAsCompositeFilter,
    T: Clone + Send + Sync + 'static,
{
    fn temporal(&self) -> PyFilterOps {
        PyFilterOps::wrap(self.clone().temporal())
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
{
    type Target = PyFilterOps;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyFilterOps::wrap(self).into_pyobject(py)
    }
}

// impl<'py, T: Clone> IntoPyObject<'py> for PropertyFilter<T>
// where
//     PropertyFilter<T>: CreateFilter + TryAsCompositeFilter,
// {
//     type Target = PyFilterExpr;
//     type Output = Bound<'py, Self::Target>;
//     type Error = PyErr;
//
//     fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
//         PyFilterExpr(Arc::new(self)).into_pyobject(py)
//     }
// }

impl<'py> IntoPyObject<'py> for PyPropertyFilterBuilder {
    type Target = PyPropertyFilterBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let parent = PyFilterOps::from_arc(self.ops.clone());
        Bound::new(py, (self, parent))
    }
}

// impl<'py, M> IntoPyObject<'py> for OpChainBuilder<M>
// where
//     PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
// {
//     type Target = PyPropertyFilterBuilder;
//     type Output = Bound<'py, Self::Target>;
//     type Error = PyErr;
//
//     fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
//         PyPropertyFilterBuilder::from_arc(Arc::new(self)).into_pyobject(py)
//     }
// }
