use crate::{
    db::graph::views::filter::{
        internal::CreateFilter,
        model::{
            property_filter::{
                AllTemporalPropertyFilterBuilder, AnyTemporalPropertyFilterBuilder,
                FirstTemporalPropertyFilterBuilder, LatestTemporalPropertyFilterBuilder,
                ListAggOps, MetadataFilterBuilder, PropertyFilterBuilder, PropertyFilterOps,
                TemporalPropertyFilterBuilder,
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

    fn starts_with(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.starts_with(value)))
    }

    fn ends_with(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(Arc::new(self.ends_with(value)))
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
pub struct PyPropertyFilterOps {
    ops: Arc<dyn DynPropertyFilterOps>,
    agg: Arc<dyn DynListAggOps>,
}

impl PyPropertyFilterOps {
    fn from_parts<O, A>(ops_provider: Arc<O>, agg_provider: Arc<A>) -> Self
    where
        O: DynPropertyFilterOps + 'static,
        A: DynListAggOps + 'static,
    {
        let ops: Arc<dyn DynPropertyFilterOps> = ops_provider;
        let agg: Arc<dyn DynListAggOps> = agg_provider;
        PyPropertyFilterOps { ops, agg }
    }

    fn from_builder<B>(builder: B) -> Self
    where
        B: DynPropertyFilterOps + DynListAggOps + 'static,
    {
        let shared: Arc<B> = Arc::new(builder);
        let ops: Arc<dyn DynPropertyFilterOps> = shared.clone();
        let agg: Arc<dyn DynListAggOps> = shared;
        PyPropertyFilterOps { ops, agg }
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

    fn len(&self) -> PyPropertyFilterOps {
        self.agg.len()
    }

    fn sum(&self) -> PyPropertyFilterOps {
        self.agg.sum()
    }

    fn avg(&self) -> PyPropertyFilterOps {
        self.agg.avg()
    }

    fn min(&self) -> PyPropertyFilterOps {
        self.agg.min()
    }

    fn max(&self) -> PyPropertyFilterOps {
        self.agg.max()
    }
}

pub trait DynListAggOps: Send + Sync {
    fn len(&self) -> PyPropertyFilterOps;
    fn sum(&self) -> PyPropertyFilterOps;
    fn avg(&self) -> PyPropertyFilterOps;
    fn min(&self) -> PyPropertyFilterOps;
    fn max(&self) -> PyPropertyFilterOps;
}

impl<M> DynListAggOps for PropertyFilterBuilder<M>
where
    M: Clone + Send + Sync + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    fn len(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(<PropertyFilterBuilder<M> as ListAggOps<M>>::len(
            self.clone(),
        ));
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn sum(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(<PropertyFilterBuilder<M> as ListAggOps<M>>::sum(
            self.clone(),
        ));
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn avg(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(<PropertyFilterBuilder<M> as ListAggOps<M>>::avg(
            self.clone(),
        ));
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn min(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(<PropertyFilterBuilder<M> as ListAggOps<M>>::min(
            self.clone(),
        ));
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn max(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(<PropertyFilterBuilder<M> as ListAggOps<M>>::max(
            self.clone(),
        ));
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
}

impl<M> DynListAggOps for MetadataFilterBuilder<M>
where
    M: Clone + Send + Sync + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    fn len(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(<MetadataFilterBuilder<M> as ListAggOps<M>>::len(
            self.clone(),
        ));
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn sum(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(<MetadataFilterBuilder<M> as ListAggOps<M>>::sum(
            self.clone(),
        ));
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn avg(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(<MetadataFilterBuilder<M> as ListAggOps<M>>::avg(
            self.clone(),
        ));
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn min(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(<MetadataFilterBuilder<M> as ListAggOps<M>>::min(
            self.clone(),
        ));
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn max(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(<MetadataFilterBuilder<M> as ListAggOps<M>>::max(
            self.clone(),
        ));
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
}

impl<M> DynListAggOps for AnyTemporalPropertyFilterBuilder<M>
where
    M: Clone + Send + Sync + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    fn len(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().len());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn sum(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().sum());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn avg(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().avg());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn min(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().min());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn max(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().max());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
}

impl<M> DynListAggOps for LatestTemporalPropertyFilterBuilder<M>
where
    M: Clone + Send + Sync + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    fn len(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().len());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn sum(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().sum());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn avg(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().avg());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn min(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().min());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn max(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().max());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
}

impl<M> DynListAggOps for FirstTemporalPropertyFilterBuilder<M>
where
    M: Clone + Send + Sync + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    fn len(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().len());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn sum(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().sum());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn avg(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().avg());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn min(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().min());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn max(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().max());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
}

impl<M> DynListAggOps for AllTemporalPropertyFilterBuilder<M>
where
    M: Clone + Send + Sync + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    fn len(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().len());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn sum(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().sum());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn avg(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().avg());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn min(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().min());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
    fn max(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(self.clone().max());
        let agg = Arc::new(self.clone());
        PyPropertyFilterOps::from_parts(ops, agg)
    }
}

trait DynTemporalPropertyFilterBuilderOps: Send + Sync {
    fn any(&self) -> PyPropertyFilterOps;

    fn latest(&self) -> PyPropertyFilterOps;

    fn first(&self) -> PyPropertyFilterOps;

    fn all(&self) -> PyPropertyFilterOps;
}

impl<M: Clone + Send + Sync + 'static> DynTemporalPropertyFilterBuilderOps
    for TemporalPropertyFilterBuilder<M>
where
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    fn any(&self) -> PyPropertyFilterOps {
        PyPropertyFilterOps::from_builder(self.clone().any())
    }

    fn latest(&self) -> PyPropertyFilterOps {
        PyPropertyFilterOps::from_builder(self.clone().latest())
    }

    fn first(&self) -> PyPropertyFilterOps {
        PyPropertyFilterOps::from_builder(self.clone().first())
    }

    fn all(&self) -> PyPropertyFilterOps {
        PyPropertyFilterOps::from_builder(self.clone().all())
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

    pub fn first(&self) -> PyPropertyFilterOps {
        self.0.first()
    }

    pub fn all(&self) -> PyPropertyFilterOps {
        self.0.all()
    }
}

trait DynPropertyFilterBuilderOps: Send + Sync {
    fn temporal(&self) -> PyTemporalPropertyFilterBuilder;
}

impl<M: Clone + Send + Sync + 'static> DynPropertyFilterBuilderOps for PropertyFilterBuilder<M>
where
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    fn temporal(&self) -> PyTemporalPropertyFilterBuilder {
        PyTemporalPropertyFilterBuilder(Arc::new(self.clone().temporal()))
    }
}

/// Construct a property filter
///
/// Arguments:
///     name (str): the name of the property to filter
#[pyclass(frozen, name = "Property", module = "raphtory.filter", extends=PyPropertyFilterOps
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
        let parent = PyPropertyFilterOps {
            ops: inner.clone(),
            agg: inner.clone(),
        };
        let child = PyPropertyFilterBuilder(inner as Arc<dyn DynPropertyFilterBuilderOps>);
        Bound::new(py, (child, parent))
    }
}

#[pymethods]
impl PyPropertyFilterBuilder {
    fn temporal(&self) -> PyTemporalPropertyFilterBuilder {
        self.0.temporal()
    }
}

/// Construct a metadata filter
///
/// Arguments:
///     name (str): the name of the property to filter
#[pyclass(frozen, name = "Metadata", module = "raphtory.filter", extends=PyPropertyFilterOps
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
        let parent = PyPropertyFilterOps {
            ops: Arc::new(self.clone()),
            agg: Arc::new(self),
        };
        let child = PyMetadataFilterBuilder;
        Bound::new(py, (child, parent))
    }
}
