use crate::{
    db::graph::views::filter::{
        internal::CreateFilter,
        model::{
            edge_filter::{
                CompositeEdgeFilter, EdgeEndpointFilter, EdgeFilter, EdgeFilterOps,
                InternalEdgeFilterBuilderOps,
            },
            node_filter::{CompositeNodeFilter, InternalNodeFilterBuilderOps, NodeFilter},
            not_filter::NotFilter,
            or_filter::OrFilter,
            property_filter::{PropertyFilterBuilder, TemporalPropertyFilterBuilder},
            AndFilter, PropertyFilterFactory, TryAsEdgeFilter, TryAsNodeFilter,
        },
    },
    errors::GraphError,
    prelude::PropertyFilter,
    python::types::{
        iterable::FromIterable,
        wrappers::prop::{DynInternalFilterOps, DynNodeFilterBuilderOps, DynPropertyFilterOps},
    },
};
use pyo3::prelude::*;
use raphtory_api::core::entities::properties::prop::Prop;
use std::sync::Arc;

#[pyclass(frozen, name = "FilterExpr", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyFilterExpr(pub Arc<dyn DynInternalFilterOps>);

impl PyFilterExpr {
    pub fn try_as_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        self.0.try_as_node_filter()
    }

    pub fn try_as_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        self.0.try_as_edge_filter()
    }
}

#[pymethods]
impl PyFilterExpr {
    pub fn __and__(&self, other: &Self) -> Self {
        let left = self.0.clone();
        let right = other.0.clone();
        PyFilterExpr(Arc::new(AndFilter { left, right }))
    }

    pub fn __or__(&self, other: &Self) -> Self {
        let left = self.0.clone();
        let right = other.0.clone();
        PyFilterExpr(Arc::new(OrFilter { left, right }))
    }

    fn __invert__(&self) -> Self {
        PyFilterExpr(Arc::new(NotFilter(self.0.clone())))
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
    PropertyFilter<M>: CreateFilter + TryAsEdgeFilter + TryAsNodeFilter,
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
    PropertyFilter<M>: CreateFilter + TryAsEdgeFilter + TryAsNodeFilter,
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
    PropertyFilter<M>: CreateFilter + TryAsEdgeFilter + TryAsNodeFilter,
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

#[pyclass(frozen, name = "NodeFilterOp", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeFilterOp(Arc<dyn DynNodeFilterBuilderOps>);

impl<T: InternalNodeFilterBuilderOps + 'static> From<T> for PyNodeFilterOp {
    fn from(value: T) -> Self {
        PyNodeFilterOp(Arc::new(value))
    }
}

#[pymethods]
impl PyNodeFilterOp {
    fn __eq__(&self, value: String) -> PyFilterExpr {
        self.0.eq(value)
    }

    fn __ne__(&self, value: String) -> PyFilterExpr {
        self.0.ne(value)
    }

    fn is_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        self.0.is_in(values.into())
    }

    fn is_not_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        self.0.is_not_in(values.into())
    }

    fn contains(&self, value: String) -> PyFilterExpr {
        self.0.contains(value)
    }

    fn not_contains(&self, value: String) -> PyFilterExpr {
        self.0.not_contains(value)
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        self.0
            .fuzzy_search(value, levenshtein_distance, prefix_match)
    }
}

#[pyclass(frozen, name = "Node", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeFilter;

#[pymethods]
impl PyNodeFilter {
    #[staticmethod]
    fn name() -> PyNodeFilterOp {
        PyNodeFilterOp(Arc::new(NodeFilter::name()))
    }

    #[staticmethod]
    fn node_type() -> PyNodeFilterOp {
        PyNodeFilterOp(Arc::new(NodeFilter::node_type()))
    }

    #[staticmethod]
    fn property(py: Python<'_>, name: String) -> PyResult<Bound<PyPropertyFilterBuilder>> {
        NodeFilter::property(name).into_pyobject(py)
    }
}

#[pyclass(frozen, name = "EdgeFilterOp", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeFilterOp(Arc<dyn InternalEdgeFilterBuilderOps>);

impl<T: InternalEdgeFilterBuilderOps + 'static> From<T> for PyEdgeFilterOp {
    fn from(value: T) -> Self {
        PyEdgeFilterOp(Arc::new(value))
    }
}

#[pymethods]
impl PyEdgeFilterOp {
    fn __eq__(&self, value: String) -> PyFilterExpr {
        let field = self.0.eq(value);
        PyFilterExpr(Arc::new(field))
    }

    fn __ne__(&self, value: String) -> PyFilterExpr {
        let field = self.0.ne(value);
        PyFilterExpr(Arc::new(field))
    }

    fn is_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        let field = self.0.is_in(values);
        PyFilterExpr(Arc::new(field))
    }

    fn is_not_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        let field = self.0.is_not_in(values);
        PyFilterExpr(Arc::new(field))
    }

    fn contains(&self, value: String) -> PyFilterExpr {
        let field = self.0.contains(value);
        PyFilterExpr(Arc::new(field))
    }

    fn not_contains(&self, value: String) -> PyFilterExpr {
        let field = self.0.not_contains(value);
        PyFilterExpr(Arc::new(field))
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        let field = self
            .0
            .fuzzy_search(value, levenshtein_distance, prefix_match);
        PyFilterExpr(Arc::new(field))
    }
}

#[pyclass(frozen, name = "EdgeEndpoint", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeEndpoint(pub EdgeEndpointFilter);

#[pymethods]
impl PyEdgeEndpoint {
    fn name(&self) -> PyEdgeFilterOp {
        PyEdgeFilterOp(self.0.name())
    }
}

#[pyclass(frozen, name = "Edge", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeFilter;

#[pymethods]
impl PyEdgeFilter {
    #[staticmethod]
    fn src() -> PyEdgeEndpoint {
        PyEdgeEndpoint(EdgeFilter::src())
    }

    #[staticmethod]
    fn dst() -> PyEdgeEndpoint {
        PyEdgeEndpoint(EdgeFilter::dst())
    }

    #[staticmethod]
    fn property(py: Python<'_>, name: String) -> PyResult<Bound<PyPropertyFilterBuilder>> {
        EdgeFilter::property(name).into_pyobject(py)
    }
}

#[pyclass(frozen, name = "ExplodedEdge", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyExplodedEdgeFilter;

// #[pymethods]
// impl PyExplodedEdgeFilter {
//     #[staticmethod]
//     fn property(py: Python<'_>, name: String) -> PyResult<Bound<PyPropertyFilterBuilder>> {
//         ExplodedEdgeFilter::property(name).into_pyobject(py)
//     }
// }

pub fn base_filter_module(py: Python<'_>) -> Result<Bound<PyModule>, PyErr> {
    let filter_module = PyModule::new(py, "filter")?;

    filter_module.add_class::<PyNodeFilterOp>()?;
    filter_module.add_class::<PyNodeFilter>()?;
    filter_module.add_class::<PyEdgeFilterOp>()?;
    filter_module.add_class::<PyEdgeEndpoint>()?;
    filter_module.add_class::<PyEdgeFilter>()?;
    filter_module.add_class::<PyPropertyFilterBuilder>()?;
    filter_module.add_class::<PyTemporalPropertyFilterBuilder>()?;

    Ok(filter_module)
}
