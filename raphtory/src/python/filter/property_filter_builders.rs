use crate::{
    db::graph::views::filter::{
        internal::CreateFilter,
        model::{
            property_filter::{
                PropertyFilterBuilder, PropertyFilterOps, TemporalPropertyFilterBuilder,
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

    fn contains(&self, value: Prop) -> PyFilterExpr;

    fn not_contains(&self, value: Prop) -> PyFilterExpr;

    fn fuzzy_search(
        &self,
        prop_value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr;
}

impl<F> DynPropertyFilterOps for F
where
    F: PropertyFilterOps,
    PropertyFilter<F::Marker>: CreateFilter + TryAsCompositeFilter,
{
    fn __eq__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.eq(value)))
    }

    fn __ne__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.ne(value)))
    }

    fn __lt__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.lt(value)))
    }

    fn __le__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.le(value)))
    }

    fn __gt__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.gt(value)))
    }

    fn __ge__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.ge(value)))
    }

    fn is_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.is_in(values)))
    }

    fn is_not_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.is_not_in(values)))
    }

    fn is_none(&self) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.is_none()))
    }

    fn is_some(&self) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.is_some()))
    }

    fn contains(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.contains(value)))
    }

    fn not_contains(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.not_contains(value)))
    }

    fn fuzzy_search(
        &self,
        prop_value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.fuzzy_search(
            prop_value,
            levenshtein_distance,
            prefix_match,
        )))
    }
}

#[pyclass(
    frozen,
    name = "PropertyFilterOps",
    module = "raphtory.filter",
    subclass
)]
pub struct PyPropertyFilterOps(Arc<dyn DynPropertyFilterOps>);

impl<T> From<T> for PyPropertyFilterOps
where
    T: DynPropertyFilterOps + 'static,
{
    fn from(value: T) -> Self {
        PyPropertyFilterOps(Arc::new(value))
    }
}

#[pymethods]
impl PyPropertyFilterOps {
    fn __eq__(&self, value: Prop) -> PyFilterExpr {
        self.0.__eq__(value)
    }

    fn __ne__(&self, value: Prop) -> PyFilterExpr {
        self.0.__ne__(value)
    }

    fn __lt__(&self, value: Prop) -> PyFilterExpr {
        self.0.__lt__(value)
    }

    fn __le__(&self, value: Prop) -> PyFilterExpr {
        self.0.__le__(value)
    }

    fn __gt__(&self, value: Prop) -> PyFilterExpr {
        self.0.__gt__(value)
    }

    fn __ge__(&self, value: Prop) -> PyFilterExpr {
        self.0.__ge__(value)
    }

    fn is_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        self.0.is_in(values)
    }

    fn is_not_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        self.0.is_not_in(values)
    }

    fn is_none(&self) -> PyFilterExpr {
        self.0.is_none()
    }

    fn is_some(&self) -> PyFilterExpr {
        self.0.is_some()
    }

    fn contains(&self, value: Prop) -> PyFilterExpr {
        self.0.contains(value)
    }

    fn not_contains(&self, value: Prop) -> PyFilterExpr {
        self.0.not_contains(value)
    }

    fn fuzzy_search(
        &self,
        prop_value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        self.0
            .fuzzy_search(prop_value, levenshtein_distance, prefix_match)
    }
}

trait DynTemporalPropertyFilterBuilderOps: Send + Sync {
    fn any(&self) -> PyPropertyFilterOps;

    fn latest(&self) -> PyPropertyFilterOps;
}

impl<M: Clone + Send + Sync + 'static> DynTemporalPropertyFilterBuilderOps
    for TemporalPropertyFilterBuilder<M>
where
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    fn any(&self) -> PyPropertyFilterOps {
        PyPropertyFilterOps(Arc::new(self.clone().any()))
    }

    fn latest(&self) -> PyPropertyFilterOps {
        PyPropertyFilterOps(Arc::new(self.clone().latest()))
    }
}

#[pyclass(
    frozen,
    name = "TemporalPropertyFilterBuilder",
    module = "raphtory.filter"
)]
#[derive(Clone)]
pub struct PyTemporalPropertyFilterBuilder(Arc<dyn DynTemporalPropertyFilterBuilderOps>);

#[pymethods]
impl PyTemporalPropertyFilterBuilder {
    pub fn any(&self) -> PyPropertyFilterOps {
        self.0.any()
    }

    pub fn latest(&self) -> PyPropertyFilterOps {
        self.0.latest()
    }
}

trait DynPropertyFilterBuilderOps: Send + Sync {
    fn constant(&self) -> PyPropertyFilterOps;
    fn temporal(&self) -> PyTemporalPropertyFilterBuilder;
}

impl<M: Clone + Send + Sync + 'static> DynPropertyFilterBuilderOps for PropertyFilterBuilder<M>
where
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    fn constant(&self) -> PyPropertyFilterOps {
        PyPropertyFilterOps(Arc::new(self.clone().constant()))
    }

    fn temporal(&self) -> PyTemporalPropertyFilterBuilder {
        PyTemporalPropertyFilterBuilder(Arc::new(self.clone().temporal()))
    }
}

#[pyclass(frozen, name = "PropertyFilterBuilder", module = "raphtory.filter", extends=PyPropertyFilterOps
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
        let inner = Arc::new(self);
        Bound::new(
            py,
            (
                PyPropertyFilterBuilder(inner.clone()),
                PyPropertyFilterOps(inner),
            ),
        )
    }
}

#[pymethods]
impl PyPropertyFilterBuilder {
    fn constant(&self) -> PyPropertyFilterOps {
        self.0.constant()
    }

    fn temporal(&self) -> PyTemporalPropertyFilterBuilder {
        self.0.temporal()
    }
}
