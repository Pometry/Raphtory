use crate::{
    db::graph::views::filter::model::{
        InternalPropertyFilterOps, PropertyFilterBuilder, PropertyFilterOps,
        TemporalPropertyFilterBuilder,
    },
    python::{
        filter::filter_expr::{PyFilterExpr, PyInnerFilterExpr},
        types::iterable::FromIterable,
    },
};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyErr, Python};
use raphtory_api::core::entities::properties::prop::Prop;
use std::sync::Arc;

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

#[pyclass(frozen, name = "Property", module = "raphtory.filter", extends=PyPropertyFilterOps
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
