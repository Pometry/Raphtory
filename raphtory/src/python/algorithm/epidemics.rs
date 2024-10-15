use crate::{
    algorithms::dynamics::temporal::epidemics::{
        Infected, IntoSeeds, Number, Probability, SeedError,
    },
    core::entities::{nodes::node_ref::NodeRef, VID},
    db::{
        api::view::{DynamicGraph, StaticGraphViewOps},
        graph::node::NodeView,
    },
    py_algorithm_result, py_algorithm_result_new_ord_hash_eq,
    python::{
        types::repr::{Repr, StructReprBuilder},
        utils::errors::adapt_err_value,
    },
};
use pyo3::{
    prelude::*,
    types::{PyFloat, PyLong},
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
struct PyInfected {
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

impl IntoPy<PyObject> for Infected {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyInfected { inner: self }.into_py(py)
    }
}

impl ToPyObject for Infected {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        self.clone().into_py(py)
    }
}

pub enum PySeed<'a> {
    List(Vec<NodeRef<'a>>),
    Number(usize),
    Probability(f64),
}

impl<'source> FromPyObject<'source> for PySeed<'source> {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let res = if ob.is_instance_of::<PyLong>() {
            Self::Number(ob.extract()?)
        } else if ob.is_instance_of::<PyFloat>() {
            Self::Probability(ob.extract()?)
        } else {
            Self::List(ob.extract()?)
        };
        Ok(res)
    }
}

impl<'a> IntoSeeds for PySeed<'a> {
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
