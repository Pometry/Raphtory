use crate::{
    db::graph::views::filter::{
        internal::CreateFilter,
        model::{
            property_filter::{
                ElemQualifierOps, InternalPropertyFilterOps, ListAggOps, MetadataFilterBuilder,
                OpChainBuilder, PropertyFilterBuilder, PropertyFilterOps,
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
    qual: Arc<dyn DynElemQualifierOps>,
    sel: Arc<dyn DynSelectorOps>,
}

impl PyPropertyFilterOps {
    fn from_parts<O, A, Q, S>(
        ops_provider: Arc<O>,
        agg_provider: Arc<A>,
        qual_provider: Arc<Q>,
        sel_provider: Arc<S>,
    ) -> Self
    where
        O: DynPropertyFilterOps + 'static,
        A: DynListAggOps + 'static,
        Q: DynElemQualifierOps + 'static,
        S: DynSelectorOps + 'static,
    {
        PyPropertyFilterOps {
            ops: ops_provider,
            agg: agg_provider,
            qual: qual_provider,
            sel: sel_provider,
        }
    }

    fn from_builder<B>(builder: B) -> Self
    where
        B: DynPropertyFilterOps + DynListAggOps + DynElemQualifierOps + DynSelectorOps + 'static,
    {
        let shared: Arc<B> = Arc::new(builder);
        PyPropertyFilterOps {
            ops: shared.clone(),
            agg: shared.clone(),
            qual: shared.clone(),
            sel: shared,
        }
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
        self.sel.first()
    }

    pub fn last(&self) -> PyResult<PyPropertyFilterOps> {
        self.sel.last()
    }

    pub fn any(&self) -> PyResult<PyPropertyFilterOps> {
        self.qual.any()
    }

    pub fn all(&self) -> PyResult<PyPropertyFilterOps> {
        self.qual.all()
    }

    fn len(&self) -> PyResult<PyPropertyFilterOps> {
        self.agg.len()
    }

    fn sum(&self) -> PyResult<PyPropertyFilterOps> {
        self.agg.sum()
    }

    fn avg(&self) -> PyResult<PyPropertyFilterOps> {
        self.agg.avg()
    }

    fn min(&self) -> PyResult<PyPropertyFilterOps> {
        self.agg.min()
    }

    fn max(&self) -> PyResult<PyPropertyFilterOps> {
        self.agg.max()
    }
}

pub trait DynListAggOps: Send + Sync {
    fn len(&self) -> PyResult<PyPropertyFilterOps>;

    fn sum(&self) -> PyResult<PyPropertyFilterOps>;

    fn avg(&self) -> PyResult<PyPropertyFilterOps>;

    fn min(&self) -> PyResult<PyPropertyFilterOps>;

    fn max(&self) -> PyResult<PyPropertyFilterOps>;
}

trait DynElemQualifierOps: Send + Sync {
    fn any(&self) -> PyResult<PyPropertyFilterOps>;

    fn all(&self) -> PyResult<PyPropertyFilterOps>;
}

#[derive(Clone)]
struct NoElemQualifiers;

impl DynElemQualifierOps for NoElemQualifiers {
    fn any(&self) -> PyResult<PyPropertyFilterOps> {
        Err(PyTypeError::new_err("Element qualifiers (any/all) cannot be used after a list aggregation (len/sum/avg/min/max)."))
    }

    fn all(&self) -> PyResult<PyPropertyFilterOps> {
        Err(PyTypeError::new_err("Element qualifiers (any/all) cannot be used after a list aggregation (len/sum/avg/min/max)."))
    }
}

#[derive(Clone)]
struct NoListAggOps;

impl DynListAggOps for NoListAggOps {
    fn len(&self) -> PyResult<PyPropertyFilterOps> {
        Err(PyTypeError::new_err(
            "List aggregation len cannot be used after an element qualifier (any/all).",
        ))
    }

    fn sum(&self) -> PyResult<PyPropertyFilterOps> {
        Err(PyTypeError::new_err(
            "List aggregation sum cannot be used after an element qualifier (any/all).",
        ))
    }

    fn avg(&self) -> PyResult<PyPropertyFilterOps> {
        Err(PyTypeError::new_err(
            "List aggregation avg cannot be used after an element qualifier (any/all).",
        ))
    }

    fn min(&self) -> PyResult<PyPropertyFilterOps> {
        Err(PyTypeError::new_err(
            "List aggregation min cannot be used after an element qualifier (any/all).",
        ))
    }

    fn max(&self) -> PyResult<PyPropertyFilterOps> {
        Err(PyTypeError::new_err(
            "List aggregation max cannot be used after an element qualifier (any/all).",
        ))
    }
}

trait DynSelectorOps: Send + Sync {
    fn first(&self) -> PyResult<PyPropertyFilterOps>;

    fn last(&self) -> PyResult<PyPropertyFilterOps>;
}

#[derive(Clone)]
struct NoSelector;

impl DynSelectorOps for NoSelector {
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
}

impl<T> DynListAggOps for T
where
    T: ListAggOps + InternalPropertyFilterOps + Clone + Send + Sync + 'static,
    PropertyFilter<<T as InternalPropertyFilterOps>::Marker>: CreateFilter + TryAsCompositeFilter,
{
    fn len(&self) -> PyResult<PyPropertyFilterOps> {
        let ops = Arc::new(<T as ListAggOps>::len(self.clone()));
        let agg = Arc::new(self.clone());
        let qual = Arc::new(NoElemQualifiers);
        let sel = Arc::new(NoSelector);
        Ok(PyPropertyFilterOps::from_parts(ops, agg, qual, sel))
    }

    fn sum(&self) -> PyResult<PyPropertyFilterOps> {
        let ops = Arc::new(<T as ListAggOps>::sum(self.clone()));
        let agg = Arc::new(self.clone());
        let qual = Arc::new(NoElemQualifiers);
        let sel = Arc::new(NoSelector);
        Ok(PyPropertyFilterOps::from_parts(ops, agg, qual, sel))
    }

    fn avg(&self) -> PyResult<PyPropertyFilterOps> {
        let ops = Arc::new(<T as ListAggOps>::avg(self.clone()));
        let agg = Arc::new(self.clone());
        let qual = Arc::new(NoElemQualifiers);
        let sel = Arc::new(NoSelector);
        Ok(PyPropertyFilterOps::from_parts(ops, agg, qual, sel))
    }

    fn min(&self) -> PyResult<PyPropertyFilterOps> {
        let ops = Arc::new(<T as ListAggOps>::min(self.clone()));
        let agg = Arc::new(self.clone());
        let qual = Arc::new(NoElemQualifiers);
        let sel = Arc::new(NoSelector);
        Ok(PyPropertyFilterOps::from_parts(ops, agg, qual, sel))
    }

    fn max(&self) -> PyResult<PyPropertyFilterOps> {
        let ops = Arc::new(<T as ListAggOps>::max(self.clone()));
        let agg = Arc::new(self.clone());
        let qual = Arc::new(NoElemQualifiers);
        let sel = Arc::new(NoSelector);
        Ok(PyPropertyFilterOps::from_parts(ops, agg, qual, sel))
    }
}

trait QualifierBehavior:
    InternalPropertyFilterOps + ElemQualifierOps + Clone + Send + Sync + 'static
where
    PropertyFilter<Self::Marker>: CreateFilter + TryAsCompositeFilter,
{
    fn build_any(&self) -> PyPropertyFilterOps;

    fn build_all(&self) -> PyPropertyFilterOps;
}

impl<M> QualifierBehavior for PropertyFilterBuilder<M>
where
    M: Clone + Send + Sync + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    fn build_any(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(ElemQualifierOps::any(self.clone()));
        let agg = Arc::new(NoListAggOps);
        let qual = Arc::new(NoElemQualifiers);
        let sel = Arc::new(NoSelector);
        PyPropertyFilterOps::from_parts(ops, agg, qual, sel)
    }

    fn build_all(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(ElemQualifierOps::all(self.clone()));
        let agg = Arc::new(NoListAggOps);
        let qual = Arc::new(NoElemQualifiers);
        let sel = Arc::new(NoSelector);
        PyPropertyFilterOps::from_parts(ops, agg, qual, sel)
    }
}

impl<M> QualifierBehavior for MetadataFilterBuilder<M>
where
    M: Clone + Send + Sync + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    fn build_any(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(ElemQualifierOps::any(self.clone()));
        let agg = Arc::new(NoListAggOps);
        let qual = Arc::new(NoElemQualifiers);
        let sel = Arc::new(NoSelector);
        PyPropertyFilterOps::from_parts(ops, agg, qual, sel)
    }
    fn build_all(&self) -> PyPropertyFilterOps {
        let ops = Arc::new(ElemQualifierOps::all(self.clone()));
        let agg = Arc::new(NoListAggOps);
        let qual = Arc::new(NoElemQualifiers);
        let sel = Arc::new(NoSelector);
        PyPropertyFilterOps::from_parts(ops, agg, qual, sel)
    }
}

impl<M> DynSelectorOps for OpChainBuilder<M>
where
    M: Send + Sync + Clone + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    fn first(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::from_builder(self.clone().first()))
    }

    fn last(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(PyPropertyFilterOps::from_builder(self.clone().last()))
    }
}

impl<M> QualifierBehavior for OpChainBuilder<M>
where
    M: Send + Sync + Clone + 'static,
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    fn build_any(&self) -> PyPropertyFilterOps {
        let chain = self.clone().any();
        let ops = Arc::new(chain.clone());
        let agg = Arc::new(chain.clone());
        let qual = Arc::new(chain.clone());
        let sel = Arc::new(NoSelector);
        PyPropertyFilterOps::from_parts(ops, agg, qual, sel)
    }

    fn build_all(&self) -> PyPropertyFilterOps {
        let chain = self.clone().all();
        let ops = Arc::new(chain.clone());
        let agg = Arc::new(chain.clone());
        let qual = Arc::new(chain.clone());
        let sel = Arc::new(NoSelector);
        PyPropertyFilterOps::from_parts(ops, agg, qual, sel)
    }
}

impl<T> DynElemQualifierOps for T
where
    T: QualifierBehavior,
    PropertyFilter<T::Marker>: CreateFilter + TryAsCompositeFilter,
{
    fn any(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(self.build_any())
    }

    fn all(&self) -> PyResult<PyPropertyFilterOps> {
        Ok(self.build_all())
    }
}

trait DynPropertyFilterBuilderOps: Send + Sync {
    fn temporal(&self) -> PyPropertyFilterOps;
}

impl<M: Clone + Send + Sync + 'static> DynPropertyFilterBuilderOps for PropertyFilterBuilder<M>
where
    PropertyFilter<M>: CreateFilter + TryAsCompositeFilter,
{
    fn temporal(&self) -> PyPropertyFilterOps {
        PyPropertyFilterOps::from_builder(self.clone().temporal())
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
            qual: inner.clone(),
            sel: Arc::new(NoSelector),
        };
        let child = PyPropertyFilterBuilder(inner as Arc<dyn DynPropertyFilterBuilderOps>);
        Bound::new(py, (child, parent))
    }
}

#[pymethods]
impl PyPropertyFilterBuilder {
    fn temporal(&self) -> PyPropertyFilterOps {
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
            agg: Arc::new(self.clone()),
            qual: Arc::new(self),
            sel: Arc::new(NoSelector),
        };
        let child = PyMetadataFilterBuilder;
        Bound::new(py, (child, parent))
    }
}
