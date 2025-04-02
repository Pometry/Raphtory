use crate::{
    core::Prop,
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

#[pyclass(frozen, name = "PropertyFilter", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyPropertyFilter(pub PropertyFilter);

#[derive(Clone)]
pub enum PyInnerNodeFilterExpr {
    Field(NodeFieldFilter),
    Property(PropertyFilter),
    And(Box<PyInnerNodeFilterExpr>, Box<PyInnerNodeFilterExpr>),
    Or(Box<PyInnerNodeFilterExpr>, Box<PyInnerNodeFilterExpr>),
}

impl IntoNodeFilter for PyInnerNodeFilterExpr {
    fn into_node_filter(self) -> CompositeNodeFilter {
        match self {
            PyInnerNodeFilterExpr::Field(f) => f.into_node_filter(),
            PyInnerNodeFilterExpr::Property(p) => p.into_node_filter(),
            PyInnerNodeFilterExpr::And(left, right) => CompositeNodeFilter::And(
                Box::new(left.into_node_filter()),
                Box::new(right.into_node_filter()),
            ),
            PyInnerNodeFilterExpr::Or(left, right) => CompositeNodeFilter::Or(
                Box::new(left.into_node_filter()),
                Box::new(right.into_node_filter()),
            ),
        }
    }
}

#[pyclass(name = "NodeFilterExpr", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeFilterExpr {
    pub inner: PyInnerNodeFilterExpr,
}

#[pymethods]
impl PyNodeFilterExpr {
    pub fn __and__(&self, other: &Self) -> Self {
        Self {
            inner: PyInnerNodeFilterExpr::And(
                Box::new(self.inner.clone()),
                Box::new(other.inner.clone()),
            ),
        }
    }

    pub fn __or__(&self, other: &Self) -> Self {
        Self {
            inner: PyInnerNodeFilterExpr::Or(
                Box::new(self.inner.clone()),
                Box::new(other.inner.clone()),
            ),
        }
    }
}

impl IntoNodeFilter for PyNodeFilterExpr {
    fn into_node_filter(self) -> CompositeNodeFilter {
        self.inner.into_node_filter()
    }
}

#[derive(Clone)]
pub enum PyInnerEdgeFilterExpr {
    Field(EdgeFieldFilter),
    Property(PyPropertyFilter),
    And(Box<PyInnerEdgeFilterExpr>, Box<PyInnerEdgeFilterExpr>),
    Or(Box<PyInnerEdgeFilterExpr>, Box<PyInnerEdgeFilterExpr>),
}

impl IntoEdgeFilter for PyInnerEdgeFilterExpr {
    fn into_edge_filter(self) -> CompositeEdgeFilter {
        match self {
            PyInnerEdgeFilterExpr::Field(f) => f.into_edge_filter(),
            PyInnerEdgeFilterExpr::Property(p) => p.0.into_edge_filter(),
            PyInnerEdgeFilterExpr::And(left, right) => CompositeEdgeFilter::And(
                Box::new(left.into_edge_filter()),
                Box::new(right.into_edge_filter()),
            ),
            PyInnerEdgeFilterExpr::Or(left, right) => CompositeEdgeFilter::Or(
                Box::new(left.into_edge_filter()),
                Box::new(right.into_edge_filter()),
            ),
        }
    }
}

#[pyclass(name = "EdgeFilterExpr", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeFilterExpr {
    pub inner: PyInnerEdgeFilterExpr,
}

#[pymethods]
impl PyEdgeFilterExpr {
    pub fn __and__(&self, other: &Self) -> Self {
        Self {
            inner: PyInnerEdgeFilterExpr::And(
                Box::new(self.inner.clone()),
                Box::new(other.inner.clone()),
            ),
        }
    }

    pub fn __or__(&self, other: &Self) -> Self {
        Self {
            inner: PyInnerEdgeFilterExpr::Or(
                Box::new(self.inner.clone()),
                Box::new(other.inner.clone()),
            ),
        }
    }
}

impl IntoEdgeFilter for PyEdgeFilterExpr {
    fn into_edge_filter(self) -> CompositeEdgeFilter {
        self.inner.into_edge_filter()
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
    fn __eq__(&self, value: Prop) -> PyPropertyFilter {
        PyPropertyFilter(self.0.eq(value))
    }

    fn __ne__(&self, value: Prop) -> PyPropertyFilter {
        PyPropertyFilter(self.0.ne(value))
    }

    fn __lt__(&self, value: Prop) -> PyPropertyFilter {
        PyPropertyFilter(self.0.lt(value))
    }

    fn __le__(&self, value: Prop) -> PyPropertyFilter {
        PyPropertyFilter(self.0.le(value))
    }

    fn __gt__(&self, value: Prop) -> PyPropertyFilter {
        PyPropertyFilter(self.0.gt(value))
    }

    fn __ge__(&self, value: Prop) -> PyPropertyFilter {
        PyPropertyFilter(self.0.ge(value))
    }

    fn includes(&self, values: Vec<Prop>) -> PyPropertyFilter {
        PyPropertyFilter(self.0.includes(values))
    }

    fn excludes(&self, values: Vec<Prop>) -> PyPropertyFilter {
        PyPropertyFilter(self.0.excludes(values))
    }

    fn is_none(&self) -> PyPropertyFilter {
        PyPropertyFilter(self.0.is_none())
    }

    fn is_some(&self) -> PyPropertyFilter {
        PyPropertyFilter(self.0.is_some())
    }

    fn fuzzy_search(
        &self,
        prop_value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyPropertyFilter {
        PyPropertyFilter(
            self.0
                .fuzzy_search(prop_value, levenshtein_distance, prefix_match),
        )
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
    fn __eq__(&self, value: String) -> PyNodeFilterExpr {
        let field = self.0.eq(value);
        PyNodeFilterExpr {
            inner: PyInnerNodeFilterExpr::Field(field),
        }
    }

    fn __ne__(&self, value: String) -> PyNodeFilterExpr {
        let field = self.0.ne(value);
        PyNodeFilterExpr {
            inner: PyInnerNodeFilterExpr::Field(field),
        }
    }

    fn includes(&self, values: Vec<String>) -> PyNodeFilterExpr {
        let field = self.0.includes(values);
        PyNodeFilterExpr {
            inner: PyInnerNodeFilterExpr::Field(field),
        }
    }

    fn excludes(&self, values: Vec<String>) -> PyNodeFilterExpr {
        let field = self.0.excludes(values);
        PyNodeFilterExpr {
            inner: PyInnerNodeFilterExpr::Field(field),
        }
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyNodeFilterExpr {
        let field = self
            .0
            .fuzzy_search(value, levenshtein_distance, prefix_match);
        PyNodeFilterExpr {
            inner: PyInnerNodeFilterExpr::Field(field),
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
    fn __eq__(&self, value: String) -> PyEdgeFilterExpr {
        let field = self.0.eq(value);
        PyEdgeFilterExpr {
            inner: PyInnerEdgeFilterExpr::Field(field),
        }
    }

    fn __ne__(&self, value: String) -> PyEdgeFilterExpr {
        let field = self.0.ne(value);
        PyEdgeFilterExpr {
            inner: PyInnerEdgeFilterExpr::Field(field),
        }
    }

    fn includes(&self, values: Vec<String>) -> PyEdgeFilterExpr {
        let field = self.0.includes(values);
        PyEdgeFilterExpr {
            inner: PyInnerEdgeFilterExpr::Field(field),
        }
    }

    fn excludes(&self, values: Vec<String>) -> PyEdgeFilterExpr {
        let field = self.0.excludes(values);
        PyEdgeFilterExpr {
            inner: PyInnerEdgeFilterExpr::Field(field),
        }
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyEdgeFilterExpr {
        let field = self
            .0
            .fuzzy_search(value, levenshtein_distance, prefix_match);
        PyEdgeFilterExpr {
            inner: PyInnerEdgeFilterExpr::Field(field),
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
