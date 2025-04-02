use crate::{
    core::{utils::errors::GraphError, Prop},
    db::graph::views::filter::{
        CompositeEdgeFilter, CompositeNodeFilter, EdgeFieldFilter, EdgeFilter, EdgeFilterOps,
        InternalEdgeFilterOps, InternalNodeFilterOps, InternalPropertyFilterOps, IntoEdgeFilter,
        IntoNodeFilter, NodeFieldFilter, NodeFilter, NodeFilterOps, PropertyFilterBuilder,
        PropertyFilterOps, TemporalPropertyFilterBuilder,
    },
    prelude::PropertyFilter,
};
use pyo3::prelude::*;
use std::sync::Arc;

#[derive(Clone)]
pub enum PyInnerFilterExpr {
    NodeField(NodeFieldFilter),
    EdgeField(EdgeFieldFilter),
    Property(PropertyFilter),
    And(Box<PyInnerFilterExpr>, Box<PyInnerFilterExpr>),
    Or(Box<PyInnerFilterExpr>, Box<PyInnerFilterExpr>),
}

pub trait TryIntoNodeFilter {
    fn try_into_node_filter(self) -> Result<CompositeNodeFilter, GraphError>;
}

impl TryIntoNodeFilter for PyInnerFilterExpr {
    fn try_into_node_filter(self) -> Result<CompositeNodeFilter, GraphError> {
        match self {
            PyInnerFilterExpr::NodeField(f) => Ok(f.into_node_filter()),
            PyInnerFilterExpr::Property(p) => Ok(p.into_node_filter()),
            PyInnerFilterExpr::And(left, right) => Ok(CompositeNodeFilter::And(
                Box::new(left.try_into_node_filter()?),
                Box::new(right.try_into_node_filter()?),
            )),
            PyInnerFilterExpr::Or(left, right) => Ok(CompositeNodeFilter::Or(
                Box::new(left.try_into_node_filter()?),
                Box::new(right.try_into_node_filter()?),
            )),
            PyInnerFilterExpr::EdgeField(_) => Err(GraphError::ParsingError),
        }
    }
}

pub trait TryIntoEdgeFilter {
    fn try_into_edge_filter(self) -> Result<CompositeEdgeFilter, GraphError>;
}

impl TryIntoEdgeFilter for PyInnerFilterExpr {
    fn try_into_edge_filter(self) -> Result<CompositeEdgeFilter, GraphError> {
        match self {
            PyInnerFilterExpr::EdgeField(f) => Ok(f.into_edge_filter()),
            PyInnerFilterExpr::Property(p) => Ok(p.into_edge_filter()),
            PyInnerFilterExpr::And(left, right) => Ok(CompositeEdgeFilter::And(
                Box::new(left.try_into_edge_filter()?),
                Box::new(right.try_into_edge_filter()?),
            )),
            PyInnerFilterExpr::Or(left, right) => Ok(CompositeEdgeFilter::Or(
                Box::new(left.try_into_edge_filter()?),
                Box::new(right.try_into_edge_filter()?),
            )),
            PyInnerFilterExpr::NodeField(_) => Err(GraphError::ParsingError),
        }
    }
}

#[pyclass(name = "FilterExpr", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyFilterExpr {
    pub inner: PyInnerFilterExpr,
}

#[pymethods]
impl PyFilterExpr {
    pub fn __and__(&self, other: &Self) -> Self {
        Self {
            inner: PyInnerFilterExpr::And(
                Box::new(self.inner.clone()),
                Box::new(other.inner.clone()),
            ),
        }
    }

    pub fn __or__(&self, other: &Self) -> Self {
        Self {
            inner: PyInnerFilterExpr::Or(
                Box::new(self.inner.clone()),
                Box::new(other.inner.clone()),
            ),
        }
    }
}

impl TryIntoNodeFilter for PyFilterExpr {
    fn try_into_node_filter(self) -> Result<CompositeNodeFilter, GraphError> {
        self.inner.try_into_node_filter()
    }
}

impl TryIntoEdgeFilter for PyFilterExpr {
    fn try_into_edge_filter(self) -> Result<CompositeEdgeFilter, GraphError> {
        self.inner.try_into_edge_filter()
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
        PyFilterExpr {
            inner: PyInnerFilterExpr::Property(property),
        }
    }

    fn __ne__(&self, value: Prop) -> PyFilterExpr {
        let property = self.0.ne(value);
        PyFilterExpr {
            inner: PyInnerFilterExpr::Property(property),
        }
    }

    fn __lt__(&self, value: Prop) -> PyFilterExpr {
        let property = self.0.lt(value);
        PyFilterExpr {
            inner: PyInnerFilterExpr::Property(property),
        }
    }

    fn __le__(&self, value: Prop) -> PyFilterExpr {
        let property = self.0.le(value);
        PyFilterExpr {
            inner: PyInnerFilterExpr::Property(property),
        }
    }

    fn __gt__(&self, value: Prop) -> PyFilterExpr {
        let property = self.0.gt(value);
        PyFilterExpr {
            inner: PyInnerFilterExpr::Property(property),
        }
    }

    fn __ge__(&self, value: Prop) -> PyFilterExpr {
        let property = self.0.ge(value);
        PyFilterExpr {
            inner: PyInnerFilterExpr::Property(property),
        }
    }

    fn includes(&self, values: Vec<Prop>) -> PyFilterExpr {
        let property = self.0.includes(values);
        PyFilterExpr {
            inner: PyInnerFilterExpr::Property(property),
        }
    }

    fn excludes(&self, values: Vec<Prop>) -> PyFilterExpr {
        let property = self.0.excludes(values);
        PyFilterExpr {
            inner: PyInnerFilterExpr::Property(property),
        }
    }

    fn is_none(&self) -> PyFilterExpr {
        let property = self.0.is_none();
        PyFilterExpr {
            inner: PyInnerFilterExpr::Property(property),
        }
    }

    fn is_some(&self) -> PyFilterExpr {
        let property = self.0.is_some();
        PyFilterExpr {
            inner: PyInnerFilterExpr::Property(property),
        }
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
        PyFilterExpr {
            inner: PyInnerFilterExpr::Property(property),
        }
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

#[pyclass(frozen, name = "PropertyFilterBuilder", module = "raphtory.filter", extends=PyPropertyFilterOps)]
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
pub struct PyNodeFilterOp(Arc<dyn InternalNodeFilterOps>);

impl<T: InternalNodeFilterOps + 'static> From<T> for PyNodeFilterOp {
    fn from(value: T) -> Self {
        PyNodeFilterOp(Arc::new(value))
    }
}

#[pymethods]
impl PyNodeFilterOp {
    fn __eq__(&self, value: String) -> PyFilterExpr {
        let field = self.0.eq(value);
        PyFilterExpr {
            inner: PyInnerFilterExpr::NodeField(field),
        }
    }

    fn __ne__(&self, value: String) -> PyFilterExpr {
        let field = self.0.ne(value);
        PyFilterExpr {
            inner: PyInnerFilterExpr::NodeField(field),
        }
    }

    fn includes(&self, values: Vec<String>) -> PyFilterExpr {
        let field = self.0.includes(values);
        PyFilterExpr {
            inner: PyInnerFilterExpr::NodeField(field),
        }
    }

    fn excludes(&self, values: Vec<String>) -> PyFilterExpr {
        let field = self.0.excludes(values);
        PyFilterExpr {
            inner: PyInnerFilterExpr::NodeField(field),
        }
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
        PyFilterExpr {
            inner: PyInnerFilterExpr::NodeField(field),
        }
    }
}

#[pyclass(frozen, name = "Node", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeFilter;

#[pymethods]
impl PyNodeFilter {
    #[staticmethod]
    fn node_name() -> PyNodeFilterOp {
        PyNodeFilterOp(Arc::new(NodeFilter::node_name()))
    }

    #[staticmethod]
    fn node_type() -> PyNodeFilterOp {
        PyNodeFilterOp(Arc::new(NodeFilter::node_type()))
    }

    #[staticmethod]
    fn property(name: String) -> PropertyFilterBuilder {
        PropertyFilterBuilder(name)
    }
}

#[pyclass(frozen, name = "EdgeFilterOp", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeFilterOp(Arc<dyn InternalEdgeFilterOps>);

impl<T: InternalEdgeFilterOps + 'static> From<T> for PyEdgeFilterOp {
    fn from(value: T) -> Self {
        PyEdgeFilterOp(Arc::new(value))
    }
}

#[pymethods]
impl PyEdgeFilterOp {
    fn __eq__(&self, value: String) -> PyFilterExpr {
        let field = self.0.eq(value);
        PyFilterExpr {
            inner: PyInnerFilterExpr::EdgeField(field),
        }
    }

    fn __ne__(&self, value: String) -> PyFilterExpr {
        let field = self.0.ne(value);
        PyFilterExpr {
            inner: PyInnerFilterExpr::EdgeField(field),
        }
    }

    fn includes(&self, values: Vec<String>) -> PyFilterExpr {
        let field = self.0.includes(values);
        PyFilterExpr {
            inner: PyInnerFilterExpr::EdgeField(field),
        }
    }

    fn excludes(&self, values: Vec<String>) -> PyFilterExpr {
        let field = self.0.excludes(values);
        PyFilterExpr {
            inner: PyInnerFilterExpr::EdgeField(field),
        }
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
        PyFilterExpr {
            inner: PyInnerFilterExpr::EdgeField(field),
        }
    }
}

#[pyclass(frozen, name = "Edge", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeFilter;

#[pymethods]
impl PyEdgeFilter {
    #[staticmethod]
    fn src() -> PyEdgeFilterOp {
        PyEdgeFilterOp(Arc::new(EdgeFilter::src()))
    }

    #[staticmethod]
    fn dst() -> PyEdgeFilterOp {
        PyEdgeFilterOp(Arc::new(EdgeFilter::dst()))
    }

    #[staticmethod]
    fn property(name: String) -> PropertyFilterBuilder {
        PropertyFilterBuilder(name)
    }
}

pub fn base_filter_module(py: Python<'_>) -> Result<Bound<PyModule>, PyErr> {
    let filter_module = PyModule::new(py, "filter")?;

    filter_module.add_class::<PyNodeFilterOp>()?;
    filter_module.add_class::<PyNodeFilter>()?;
    filter_module.add_class::<PyEdgeFilterOp>()?;
    filter_module.add_class::<PyEdgeFilter>()?;
    filter_module.add_class::<PyPropertyFilterBuilder>()?;
    filter_module.add_class::<PyTemporalPropertyFilterBuilder>()?;

    Ok(filter_module)
}
