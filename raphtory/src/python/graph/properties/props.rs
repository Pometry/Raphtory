use crate::db::api::properties::internal::PropertiesOps;
use crate::db::api::properties::Properties;
use crate::db::api::view::internal::{DynamicGraph, Static};
use crate::python::graph::properties::DynProps;
use crate::python::types::repr::{iterator_dict_repr, Repr};
use pyo3::prelude::*;
use std::sync::Arc;

pub type DynProperties = Properties<Arc<dyn PropertiesOps + Send + Sync>>;

#[pyclass(name = "Properties")]
pub struct PyProperties {
    props: DynProperties,
}

impl<P: PropertiesOps + Clone + Send + Sync + Static + 'static> From<Properties<P>>
    for DynProperties
{
    fn from(value: Properties<P>) -> Self {
        Properties::new(Arc::new(value.props))
    }
}

impl From<Properties<DynamicGraph>> for DynProperties {
    fn from(value: Properties<DynamicGraph>) -> Self {
        let props: DynProps = Arc::new(value.props);
        Properties::new(props)
    }
}

impl<P: Into<DynProperties>> From<P> for PyProperties {
    fn from(value: P) -> Self {
        Self {
            props: value.into(),
        }
    }
}

impl<P: PropertiesOps + Clone + Send + Sync + 'static + Static> IntoPy<PyObject> for Properties<P> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyProperties::from(self).into_py(py)
    }
}

impl IntoPy<PyObject> for Properties<DynamicGraph> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyProperties::from(self).into_py(py)
    }
}

impl IntoPy<PyObject> for DynProperties {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyProperties::from(self).into_py(py)
    }
}

impl<P: PropertiesOps + Clone> Repr for Properties<P> {
    fn repr(&self) -> String {
        format!("Properties({{{}}})", iterator_dict_repr(self.iter()))
    }
}

impl Repr for PyProperties {
    fn repr(&self) -> String {
        self.props.repr()
    }
}
