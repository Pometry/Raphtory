use crate::{
    core::Prop,
    db::graph::views::filter::{Filter, FilterExpr, PropertyRef, Temporal},
    prelude::PropertyFilter,
};
use pyo3::prelude::*;

#[pyclass(frozen, name = "FilterExpr", module = "raphtory")]
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

#[pyclass(frozen, name = "ConstantPropertyFilterOps", module = "raphtory")]
#[derive(Clone)]
pub struct PyConstantPropertyFilterOps(String);

#[pymethods]
impl PyConstantPropertyFilterOps {
    #[new]
    fn new(name: String) -> Self {
        PyConstantPropertyFilterOps(name)
    }

    fn __eq__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::eq(
            PropertyRef::ConstantProperty(self.0.clone()),
            value,
        )))
    }

    fn __ne__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::ne(
            PropertyRef::ConstantProperty(self.0.clone()),
            value,
        )))
    }

    fn __lt__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::lt(
            PropertyRef::ConstantProperty(self.0.clone()),
            value,
        )))
    }

    fn __le__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::le(
            PropertyRef::ConstantProperty(self.0.clone()),
            value,
        )))
    }

    fn __gt__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::gt(
            PropertyRef::ConstantProperty(self.0.clone()),
            value,
        )))
    }

    fn __ge__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::ge(
            PropertyRef::ConstantProperty(self.0.clone()),
            value,
        )))
    }

    fn includes(&self, values: Vec<Prop>) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::includes(
            PropertyRef::ConstantProperty(self.0.clone()),
            values,
        )))
    }

    fn excludes(&self, values: Vec<Prop>) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::excludes(
            PropertyRef::ConstantProperty(self.0.clone()),
            values,
        )))
    }

    fn is_none(&self) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::is_none(
            PropertyRef::ConstantProperty(self.0.clone()),
        )))
    }

    fn is_some(&self) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::is_some(
            PropertyRef::ConstantProperty(self.0.clone()),
        )))
    }

    fn fuzzy_search(
        &self,
        prop_value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::fuzzy_search(
            PropertyRef::ConstantProperty(self.0.clone()),
            prop_value,
            levenshtein_distance,
            prefix_match,
        )))
    }
}

#[pyclass(frozen, name = "TemporalPropertyFilterOps", module = "raphtory")]
#[derive(Clone)]
pub struct PyTemporalPropertyFilterOps(String, Temporal);

#[pymethods]
impl PyTemporalPropertyFilterOps {
    #[new]
    fn new(name: String, temporal: &str) -> Self {
        let temporal = match temporal {
            "any" => Temporal::Any,
            "latest" => Temporal::Latest,
            _ => panic!("Temporal type {} not supported", temporal),
        };
        PyTemporalPropertyFilterOps(name, temporal)
    }

    fn __eq__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::eq(
            PropertyRef::TemporalProperty(self.0.clone(), self.1.clone()),
            value,
        )))
    }

    fn __ne__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::ne(
            PropertyRef::TemporalProperty(self.0.clone(), self.1.clone()),
            value,
        )))
    }

    fn __lt__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::lt(
            PropertyRef::TemporalProperty(self.0.clone(), self.1.clone()),
            value,
        )))
    }

    fn __le__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::le(
            PropertyRef::TemporalProperty(self.0.clone(), self.1.clone()),
            value,
        )))
    }

    fn __gt__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::gt(
            PropertyRef::TemporalProperty(self.0.clone(), self.1.clone()),
            value,
        )))
    }

    fn __ge__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::ge(
            PropertyRef::TemporalProperty(self.0.clone(), self.1.clone()),
            value,
        )))
    }

    fn includes(&self, values: Vec<Prop>) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::includes(
            PropertyRef::TemporalProperty(self.0.clone(), self.1.clone()),
            values,
        )))
    }

    fn excludes(&self, values: Vec<Prop>) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::excludes(
            PropertyRef::TemporalProperty(self.0.clone(), self.1.clone()),
            values,
        )))
    }

    fn is_none(&self) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::is_none(
            PropertyRef::TemporalProperty(self.0.clone(), self.1.clone()),
        )))
    }

    fn is_some(&self) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::is_some(
            PropertyRef::TemporalProperty(self.0.clone(), self.1.clone()),
        )))
    }

    fn fuzzy_search(
        &self,
        prop_value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::fuzzy_search(
            PropertyRef::TemporalProperty(self.0.clone(), self.1.clone()),
            prop_value,
            levenshtein_distance,
            prefix_match,
        )))
    }
}

#[pyclass(frozen, name = "TemporalPropertyFilter", module = "raphtory")]
#[derive(Clone)]
pub struct PyTemporalPropertyFilter(String);

#[pymethods]
impl PyTemporalPropertyFilter {
    #[new]
    fn new(name: String) -> Self {
        PyTemporalPropertyFilter(name)
    }

    fn any(&self) -> PyTemporalPropertyFilterOps {
        PyTemporalPropertyFilterOps::new(self.0.clone(), "any")
    }

    fn latest(&self) -> PyTemporalPropertyFilterOps {
        PyTemporalPropertyFilterOps::new(self.0.clone(), "latest")
    }
}

#[pyclass(frozen, name = "PropertyFilter", module = "raphtory")]
#[derive(Clone)]
pub struct PyPropertyFilter(String);

#[pymethods]
impl PyPropertyFilter {
    #[new]
    fn new(name: String) -> Self {
        PyPropertyFilter(name)
    }

    fn constant(&self) -> PyConstantPropertyFilterOps {
        PyConstantPropertyFilterOps(self.0.clone())
    }

    fn temporal(&self) -> PyTemporalPropertyFilter {
        PyTemporalPropertyFilter(self.0.clone())
    }

    fn __eq__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::eq(
            PropertyRef::Property(self.0.clone()),
            value,
        )))
    }

    fn __ne__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::ne(
            PropertyRef::Property(self.0.clone()),
            value,
        )))
    }

    fn __lt__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::lt(
            PropertyRef::Property(self.0.clone()),
            value,
        )))
    }

    fn __le__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::le(
            PropertyRef::Property(self.0.clone()),
            value,
        )))
    }

    fn __gt__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::gt(
            PropertyRef::Property(self.0.clone()),
            value,
        )))
    }

    fn __ge__(&self, value: Prop) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::ge(
            PropertyRef::Property(self.0.clone()),
            value,
        )))
    }

    fn includes(&self, values: Vec<Prop>) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::includes(
            PropertyRef::Property(self.0.clone()),
            values,
        )))
    }

    fn excludes(&self, values: Vec<Prop>) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::excludes(
            PropertyRef::Property(self.0.clone()),
            values,
        )))
    }

    fn is_none(&self) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::is_none(
            PropertyRef::Property(self.0.clone()),
        )))
    }

    fn is_some(&self) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::is_some(
            PropertyRef::Property(self.0.clone()),
        )))
    }

    fn fuzzy_search(
        &self,
        prop_value: String,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PyFilterExpr {
        PyFilterExpr(FilterExpr::Property(PropertyFilter::fuzzy_search(
            PropertyRef::Property(self.0.clone()),
            prop_value,
            levenshtein_distance,
            prefix_match,
        )))
    }
}

#[pyclass(frozen, name = "NodeFilterOp", module = "raphtory")]
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

#[pyclass(frozen, name = "Node", module = "raphtory")]
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
    fn property(name: String) -> PyPropertyFilter {
        PyPropertyFilter(name)
    }
}

#[pyclass(frozen, name = "EdgeFilterOp", module = "raphtory")]
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

#[pyclass(frozen, name = "Edge", module = "raphtory")]
#[derive(Clone)]
pub struct PyEdgeFilter;

#[pymethods]
impl PyEdgeFilter {
    #[staticmethod]
    fn src() -> PyEdgeFilterOp {
        PyEdgeFilterOp::new("from".to_string())
    }

    #[staticmethod]
    fn dst() -> PyEdgeFilterOp {
        PyEdgeFilterOp::new("to".to_string())
    }

    #[staticmethod]
    fn property(name: String) -> PyPropertyFilter {
        PyPropertyFilter(name)
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
    filter_module.add_class::<PyConstantPropertyFilterOps>()?;
    filter_module.add_class::<PyTemporalPropertyFilter>()?;
    filter_module.add_class::<PyTemporalPropertyFilterOps>()?;

    Ok(filter_module)
}
