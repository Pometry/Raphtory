use crate::{
    algorithms::dynamics::temporal::epidemics::{
        Infected, IntoSeeds, Number, Probability, SeedError,
    },
    core::entities::VID,
    db::api::view::{DynamicGraph, StaticGraphViewOps},
    py_algorithm_result, py_algorithm_result_new_ord_hash_eq,
    python::{
        types::repr::{Repr, StructReprBuilder},
        utils::{errors::adapt_err_value, PyNodeRef},
    },
};
use pyo3::{
    prelude::*,
    types::{PyFloat, PyInt},
};
use rand::Rng;

impl Repr for Infected {
    fn repr(&self) -> String {
        StructReprBuilder::new("Infected")
            .add_field("infected", self.infected)
            .add_field("active", self.active)
            .add_field("recovered", self.recovered)
            .finish()
    }
}

#[pyclass]
pub struct PyInfected {
    inner: Infected,
}

#[pymethods]
impl PyInfected {
    #[getter]
    fn infected(&self) -> i64 {
        self.inner.infected
    }

    #[getter]
    fn active(&self) -> i64 {
        self.inner.active
    }

    #[getter]
    fn recovered(&self) -> i64 {
        self.inner.recovered
    }

    fn __repr__(&self) -> String {
        self.inner.repr()
    }
}

impl<'py> IntoPyObject<'py> for Infected {
    type Target = PyInfected;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyInfected { inner: self }.into_pyobject(py)
    }
}

pub enum PySeed {
    List(Vec<PyNodeRef>),
    Number(usize),
    Probability(f64),
}

impl<'source> FromPyObject<'source> for PySeed {
    fn extract_bound(ob: &Bound<'source, PyAny>) -> PyResult<Self> {
        let res = if ob.is_instance_of::<PyInt>() {
            Self::Number(ob.extract()?)
        } else if ob.is_instance_of::<PyFloat>() {
            Self::Probability(ob.extract()?)
        } else {
            Self::List(ob.extract()?)
        };
        Ok(res)
    }
}

impl IntoSeeds for PySeed {
    fn into_initial_list<G: StaticGraphViewOps, R: Rng + ?Sized>(
        self,
        graph: &G,
        rng: &mut R,
    ) -> Result<Vec<VID>, SeedError> {
        match self {
            PySeed::List(v) => v.into_initial_list(graph, rng),
            PySeed::Number(v) => Number(v).into_initial_list(graph, rng),
            PySeed::Probability(p) => Probability::try_from(p)?.into_initial_list(graph, rng),
        }
    }
}

py_algorithm_result!(AlgorithmResultSEIR, DynamicGraph, Infected, Infected);
py_algorithm_result_new_ord_hash_eq!(AlgorithmResultSEIR, DynamicGraph, Infected, Infected);

impl From<SeedError> for PyErr {
    fn from(value: SeedError) -> Self {
        adapt_err_value(&value)
    }
}
