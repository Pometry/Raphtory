use crate::{
    core::Prop,
    db::graph::views::property_filter::{
        Filter, FilterExpr, InternalPropertyFilterOps, PropertyFilterBuilder, PropertyFilterOps,
        TemporalPropertyFilterBuilder,
    },
};
use pyo3::prelude::*;
use std::sync::Arc;

#[pyclass(frozen, name = "FilterExpr", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyFilterExpr(pub FilterExpr);

#[pymethods]
impl PyFilterExpr {
    fn __and__(&self, other: &PyFilterExpr) -> PyFilterExpr {
        PyFilterExpr(self.0.clone().and(other.0.clone()))
    }

    fn __or__(&self, other: &PyFilterExpr) -> PyFilterExpr {
        PyFilterExpr(self.0.clone().or(other.0.clone()))
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
        PyFilterExpr(self.0.eq(value))
    }

    fn __ne__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.ne(value))
    }

    fn __lt__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.lt(value))
    }

    fn __le__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.le(value))
    }

    fn __gt__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.gt(value))
    }

    fn __ge__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(self.0.ge(value))
    }

    fn includes(&self, values: Vec<Prop>) -> PyFilterExpr {
        PyFilterExpr(self.0.includes(values))
    }

    fn excludes(&self, values: Vec<Prop>) -> PyFilterExpr {
        PyFilterExpr(self.0.excludes(values))
    }

    fn is_none(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.is_none())
    }

    fn is_some(&self) -> PyFilterExpr {
        PyFilterExpr(self.0.is_some())
    }

    fn fuzzy_search(
        &self,
        prop_value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(
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
pub struct PyPropertyFilter(PropertyFilterBuilder);

impl<'py> IntoPyObject<'py> for PropertyFilterBuilder {
    type Target = PyPropertyFilter;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Bound::new(
            py,
            (
                PyPropertyFilter(self.clone()),
                PyPropertyFilterOps(Arc::new(self.clone())),
            ),
        )
    }
}

#[pymethods]
impl PyPropertyFilter {
    #[new]
    fn new(name: String) -> (Self, PyPropertyFilterOps) {
        let builder = PropertyFilterBuilder(name);
        (
            PyPropertyFilter(builder.clone()),
            PyPropertyFilterOps(Arc::new(builder)),
        )
    }

    fn constant<'py>(&self) -> PyPropertyFilterOps {
        PyPropertyFilterOps(Arc::new(self.0.clone().constant()))
    }

    fn temporal(&self) -> PyTemporalPropertyFilterBuilder {
        PyTemporalPropertyFilterBuilder(self.0.clone().temporal())
    }
}

#[pyclass(frozen, name = "NodeFilterOp", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeFilterOp {
    field: String,
}

#[pymethods]
impl PyNodeFilterOp {
    #[new]
    fn new(field: String) -> Self {
        PyNodeFilterOp { field }
    }

    fn __eq__(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Node(Filter::eq(self.field.clone(), value)))
    }

    fn __ne__(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Node(Filter::ne(self.field.clone(), value)))
    }

    fn includes(&self, values: Vec<String>) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Node(Filter::includes(
            self.field.clone(),
            values.into_iter(),
        )))
    }

    fn excludes(&self, values: Vec<String>) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Node(Filter::excludes(
            self.field.clone(),
            values.into_iter(),
        )))
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Node(Filter::fuzzy_search(
            self.field.clone(),
            value,
            levenshtein_distance,
            prefix_match,
        )))
    }
}

#[pyclass(frozen, name = "Node", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyNodeFilter;

#[pymethods]
impl PyNodeFilter {
    #[staticmethod]
    fn node_name() -> PyNodeFilterOp {
        PyNodeFilterOp::new("node_name".to_string())
    }

    #[staticmethod]
    fn node_type() -> PyNodeFilterOp {
        PyNodeFilterOp::new("node_type".to_string())
    }

    #[staticmethod]
    fn property(name: String) -> PropertyFilterBuilder {
        PropertyFilterBuilder(name)
    }
}

#[pyclass(frozen, name = "EdgeFilterOp", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeFilterOp {
    field: String,
}

#[pymethods]
impl PyEdgeFilterOp {
    #[new]
    fn new(field: String) -> Self {
        PyEdgeFilterOp { field }
    }

    fn __eq__(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Edge(Filter::eq(self.field.clone(), value)))
    }

    fn __ne__(&self, value: String) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Edge(Filter::ne(self.field.clone(), value)))
    }

    fn includes(&self, values: Vec<String>) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Edge(Filter::includes(
            self.field.clone(),
            values.into_iter(),
        )))
    }

    fn excludes(&self, values: Vec<String>) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Edge(Filter::excludes(
            self.field.clone(),
            values.into_iter(),
        )))
    }

    fn fuzzy_search(
        &self,
        value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Edge(Filter::fuzzy_search(
            self.field.clone(),
            value,
            levenshtein_distance,
            prefix_match,
        )))
    }
}

#[pyclass(frozen, name = "Edge", module = "raphtory.filter")]
#[derive(Clone)]
pub struct PyEdgeFilter;

#[pymethods]
impl PyEdgeFilter {
    #[staticmethod]
    fn src() -> PyEdgeFilterOp {
        PyEdgeFilterOp::new("src".to_string())
    }

    #[staticmethod]
    fn dst() -> PyEdgeFilterOp {
        PyEdgeFilterOp::new("dst".to_string())
    }

    #[staticmethod]
    fn property(name: String) -> PropertyFilterBuilder {
        PropertyFilterBuilder(name)
    }
}

pub fn base_filter_module(py: Python<'_>) -> Result<Bound<PyModule>, PyErr> {
    let filter_module = PyModule::new(py, "filter")?;

    filter_module.add_class::<PyFilterExpr>()?;
    filter_module.add_class::<PyNodeFilterOp>()?;
    filter_module.add_class::<PyNodeFilter>()?;
    filter_module.add_class::<PyEdgeFilterOp>()?;
    filter_module.add_class::<PyEdgeFilter>()?;
    filter_module.add_class::<PyPropertyFilter>()?;
    filter_module.add_class::<PyTemporalPropertyFilterBuilder>()?;

    Ok(filter_module)
}
