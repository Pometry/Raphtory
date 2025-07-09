use crate::{
    db::graph::views::filter::model::{
        edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter, AndFilter,
        AsEdgeFilter, AsNodeFilter, EdgeEndpointFilter, EdgeFilter, EdgeFilterOps,
        InternalEdgeFilterBuilderOps, InternalNodeFilterBuilderOps, InternalPropertyFilterOps,
        NodeFilter, NotFilter, OrFilter, PropertyFilterBuilder, PropertyFilterOps,
        TemporalPropertyFilterBuilder,
    },
    errors::GraphError,
    python::types::{
        iterable::FromIterable,
        wrappers::prop::{
            DynInternalEdgeFilterOps, DynInternalNodeFilterOps, DynNodeFilterBuilderOps,
        },
    },
};
use pyo3::prelude::*;
use raphtory_api::core::entities::properties::prop::Prop;
use std::sync::Arc;

pub trait AsPropertyFilter: DynInternalNodeFilterOps + DynInternalEdgeFilterOps {}

impl<T: DynInternalNodeFilterOps + DynInternalEdgeFilterOps + ?Sized> AsPropertyFilter for T {}

#[derive(Clone)]
pub enum PyInnerFilterExpr {
    Node(Arc<dyn DynInternalNodeFilterOps>),
    Edge(Arc<dyn DynInternalEdgeFilterOps>),
    Property(Arc<dyn AsPropertyFilter>),
}

#[pyclass(frozen, name = "FilterExpr", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyFilterExpr(pub PyInnerFilterExpr);

impl PyFilterExpr {
    pub fn try_as_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        match &self.0 {
            PyInnerFilterExpr::Node(i) => Ok(i.as_node_filter()),
            PyInnerFilterExpr::Property(i) => Ok(i.as_node_filter()),
            PyInnerFilterExpr::Edge(_) => Err(GraphError::ParsingError),
        }
    }

    pub fn try_as_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        match &self.0 {
            PyInnerFilterExpr::Edge(i) => Ok(i.as_edge_filter()),
            PyInnerFilterExpr::Property(i) => Ok(i.as_edge_filter()),
            PyInnerFilterExpr::Node(_) => Err(GraphError::ParsingError),
        }
    }
}

#[pymethods]
impl PyFilterExpr {
    pub fn __and__(&self, other: &Self) -> Result<Self, GraphError> {
        match &self.0 {
            PyInnerFilterExpr::Node(i) => match &other.0 {
                PyInnerFilterExpr::Node(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(AndFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Property(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(AndFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Edge(_) => Err(GraphError::ParsingError),
            },
            PyInnerFilterExpr::Edge(i) => match &other.0 {
                PyInnerFilterExpr::Edge(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(AndFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Property(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(AndFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Node(_) => Err(GraphError::ParsingError),
            },
            PyInnerFilterExpr::Property(i) => match &other.0 {
                PyInnerFilterExpr::Property(j) => Ok(PyFilterExpr(PyInnerFilterExpr::Property(
                    Arc::new(AndFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }),
                ))),
                PyInnerFilterExpr::Node(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(AndFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Edge(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(AndFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
            },
        }
    }

    pub fn __or__(&self, other: &Self) -> Result<Self, GraphError> {
        match &self.0 {
            PyInnerFilterExpr::Node(i) => match &other.0 {
                PyInnerFilterExpr::Node(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(OrFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Property(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(OrFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Edge(_) => Err(GraphError::ParsingError),
            },
            PyInnerFilterExpr::Edge(i) => match &other.0 {
                PyInnerFilterExpr::Edge(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(OrFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Property(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(OrFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Node(_) => Err(GraphError::ParsingError),
            },
            PyInnerFilterExpr::Property(i) => match &other.0 {
                PyInnerFilterExpr::Property(j) => Ok(PyFilterExpr(PyInnerFilterExpr::Property(
                    Arc::new(OrFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }),
                ))),
                PyInnerFilterExpr::Node(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(OrFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
                PyInnerFilterExpr::Edge(j) => {
                    Ok(PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(OrFilter {
                        left: i.clone(),
                        right: j.clone(),
                    }))))
                }
            },
        }
    }

    fn __invert__(&self) -> Result<Self, GraphError> {
        match &self.0 {
            PyInnerFilterExpr::Node(i) => Ok(PyFilterExpr(PyInnerFilterExpr::Node(Arc::new(
                NotFilter(i.clone()),
            )))),
            PyInnerFilterExpr::Edge(i) => Ok(PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(
                NotFilter(i.clone()),
            )))),
            PyInnerFilterExpr::Property(i) => Ok(PyFilterExpr(PyInnerFilterExpr::Property(
                Arc::new(NotFilter(i.clone())),
            ))),
        }
    }
}

#[pyclass(
    frozen,
    name = "PropertyFilterOps",
    module = "raphtory.filter",
    subclass
)]
pub struct PyPropertyFilterOps(Arc<dyn InternalPropertyFilterOps>);

impl<T: InternalPropertyFilterOps + 'static> From<T> for PyPropertyFilterOps {
    fn from(value: T) -> Self {
        PyPropertyFilterOps(Arc::new(value))
    }
}

#[pymethods]
impl PyPropertyFilterOps {
    fn __eq__(&self, value: Prop) -> PyFilterExpr {
        let property = self.0.eq(value);
        PyFilterExpr(PyInnerFilterExpr::Property(Arc::new(property)))
    }

    fn __ne__(&self, value: Prop) -> PyFilterExpr {
        let property = self.0.ne(value);
        PyFilterExpr(PyInnerFilterExpr::Property(Arc::new(property)))
    }

    fn __lt__(&self, value: Prop) -> PyFilterExpr {
        let property = self.0.lt(value);
        PyFilterExpr(PyInnerFilterExpr::Property(Arc::new(property)))
    }

    fn __le__(&self, value: Prop) -> PyFilterExpr {
        let property = self.0.le(value);
        PyFilterExpr(PyInnerFilterExpr::Property(Arc::new(property)))
    }

    fn __gt__(&self, value: Prop) -> PyFilterExpr {
        let property = self.0.gt(value);
        PyFilterExpr(PyInnerFilterExpr::Property(Arc::new(property)))
    }

    fn __ge__(&self, value: Prop) -> PyFilterExpr {
        let property = self.0.ge(value);
        PyFilterExpr(PyInnerFilterExpr::Property(Arc::new(property)))
    }

    fn is_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        let property = self.0.is_in(values);
        PyFilterExpr(PyInnerFilterExpr::Property(Arc::new(property)))
    }

    fn is_not_in(&self, values: FromIterable<Prop>) -> PyFilterExpr {
        let property = self.0.is_not_in(values);
        PyFilterExpr(PyInnerFilterExpr::Property(Arc::new(property)))
    }

    fn is_none(&self) -> PyFilterExpr {
        let property = self.0.is_none();
        PyFilterExpr(PyInnerFilterExpr::Property(Arc::new(property)))
    }

    fn is_some(&self) -> PyFilterExpr {
        let property = self.0.is_some();
        PyFilterExpr(PyInnerFilterExpr::Property(Arc::new(property)))
    }

    fn contains(&self, value: Prop) -> PyFilterExpr {
        let property = self.0.contains(value);
        PyFilterExpr(PyInnerFilterExpr::Property(Arc::new(property)))
    }

    fn not_contains(&self, value: Prop) -> PyFilterExpr {
        let property = self.0.not_contains(value);
        PyFilterExpr(PyInnerFilterExpr::Property(Arc::new(property)))
    }

    fn fuzzy_search(
        &self,
        prop_value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        let property = self
            .0
            .fuzzy_search(prop_value, levenshtein_distance, prefix_match);
        PyFilterExpr(PyInnerFilterExpr::Property(Arc::new(property)))
    }
}

#[pyclass(
    frozen,
    name = "TemporalPropertyFilterBuilder",
    module = "raphtory.filter"
)]
#[derive(Clone)]
pub struct PyTemporalPropertyFilterBuilder(TemporalPropertyFilterBuilder);

#[pymethods]
impl PyTemporalPropertyFilterBuilder {
    pub fn any(&self) -> PyPropertyFilterOps {
        self.0.clone().any().into()
    }

    pub fn latest(&self) -> PyPropertyFilterOps {
        self.0.clone().latest().into()
    }
}

#[pyclass(frozen, name = "PropertyFilterBuilder", module = "raphtory.filter", extends=PyPropertyFilterOps
)]
#[derive(Clone)]
pub struct PyPropertyFilterBuilder(PropertyFilterBuilder);

impl<'py> IntoPyObject<'py> for PropertyFilterBuilder {
    type Target = PyPropertyFilterBuilder;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Bound::new(
            py,
            (
                PyPropertyFilterBuilder(self.clone()),
                PyPropertyFilterOps(Arc::new(self.clone())),
            ),
        )
    }
}

#[pymethods]
impl PyPropertyFilterBuilder {
    #[new]
    fn new(name: String) -> (Self, PyPropertyFilterOps) {
        let builder = PropertyFilterBuilder(name);
        (
            PyPropertyFilterBuilder(builder.clone()),
            PyPropertyFilterOps(Arc::new(builder)),
        )
    }

    fn constant(&self) -> PyPropertyFilterOps {
        PyPropertyFilterOps(Arc::new(self.0.clone().constant()))
    }

    fn temporal(&self) -> PyTemporalPropertyFilterBuilder {
        PyTemporalPropertyFilterBuilder(self.0.clone().temporal())
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

#[derive(Clone)]
#[pyclass(frozen, name = "Node", module = "raphtory.filter")]
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
        PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(field)))
    }

    fn __ne__(&self, value: String) -> PyFilterExpr {
        let field = self.0.ne(value);
        PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(field)))
    }

    fn is_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        let field = self.0.is_in(values);
        PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(field)))
    }

    fn is_not_in(&self, values: FromIterable<String>) -> PyFilterExpr {
        let field = self.0.is_not_in(values);
        PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(field)))
    }

    fn contains(&self, value: String) -> PyFilterExpr {
        let field = self.0.contains(value);
        PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(field)))
    }

    fn not_contains(&self, value: String) -> PyFilterExpr {
        let field = self.0.not_contains(value);
        PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(field)))
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
        PyFilterExpr(PyInnerFilterExpr::Edge(Arc::new(field)))
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
}

#[pyfunction(name = "Property")]
fn property(name: String) -> PropertyFilterBuilder {
    PropertyFilterBuilder(name)
}

pub fn base_filter_module(py: Python<'_>) -> Result<Bound<PyModule>, PyErr> {
    let filter_module = PyModule::new(py, "filter")?;

    filter_module.add_class::<PyNodeFilterOp>()?;
    filter_module.add_class::<PyNodeFilter>()?;
    filter_module.add_class::<PyEdgeFilterOp>()?;
    filter_module.add_class::<PyEdgeEndpoint>()?;
    filter_module.add_class::<PyEdgeFilter>()?;
    filter_module.add_class::<PyPropertyFilterBuilder>()?;
    filter_module.add_class::<PyTemporalPropertyFilterBuilder>()?;

    filter_module.add_function(wrap_pyfunction!(property, filter_module.clone())?)?;

    Ok(filter_module)
}
