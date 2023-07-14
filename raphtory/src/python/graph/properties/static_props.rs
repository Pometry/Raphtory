use crate::core::Prop;
use crate::db::api::properties::internal::PropertiesOps;
use crate::db::api::properties::StaticProperties;
use crate::db::api::view::internal::Static;
use crate::python::graph::properties::DynProps;
use crate::python::types::repr::Repr;
use crate::python::utils::PyGenericIterator;
use pyo3::exceptions::PyKeyError;
use pyo3::{pyclass, pymethods, PyResult};
use std::sync::Arc;

pub type DynStaticProperties = StaticProperties<DynProps>;

impl<P: PropertiesOps + Send + Sync + Static + 'static> From<StaticProperties<P>>
    for DynStaticProperties
{
    fn from(value: StaticProperties<P>) -> Self {
        StaticProperties {
            props: Arc::new(value.props),
        }
    }
}

#[pyclass(name = "MetaData")]
pub struct PyStaticProperties {
    props: DynStaticProperties,
}

#[pymethods]
impl PyStaticProperties {
    fn keys(&self) -> Vec<String> {
        self.props.keys()
    }
    fn values(&self) -> Vec<Prop> {
        self.props.values()
    }
    fn items(&self) -> Vec<(String, Prop)> {
        self.props.iter().collect()
    }

    fn __getitem__(&self, key: &str) -> PyResult<Prop> {
        self.props
            .get(key)
            .ok_or(PyKeyError::new_err("No such property"))
    }

    fn get(&self, key: &str) -> Option<Prop> {
        // Fixme: Add option to specify default?
        self.props.get(key)
    }

    fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    fn __contains__(&self, key: &str) -> bool {
        self.props.contains(key)
    }

    fn __len__(&self) -> usize {
        self.keys().len()
    }
}

impl<P: PropertiesOps + Send + Sync + 'static> From<StaticProperties<P>> for PyStaticProperties {
    fn from(value: StaticProperties<P>) -> Self {
        PyStaticProperties {
            props: StaticProperties::new(Arc::new(value.props)),
        }
    }
}

impl Repr for PyStaticProperties {
    fn repr(&self) -> String {
        self.props.repr()
    }
}
