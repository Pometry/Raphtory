use crate::core::Prop;
use crate::db::api::properties::internal::PropertiesOps;
use crate::db::api::properties::Properties;
use crate::db::api::view::internal::{DynamicGraph, Static};
use crate::python::graph::properties::{DynProps, DynStaticProperties, DynTemporalProperties};
use crate::python::types::repr::{iterator_dict_repr, Repr};
use crate::python::types::wrappers::iterators::{
    NestedOptionPropIterable, NestedStaticPropsIterable, NestedTemporalPropsIterable,
    OptionPropIterable, StaticPropsIterable, TemporalPropsIterable,
};
use crate::python::utils::{PyGenericIterator, PyNestedGenericIterator};
use itertools::Itertools;
use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

pub type DynProperties = Properties<Arc<dyn PropertiesOps + Send + Sync>>;

#[pyclass(name = "Properties")]
pub struct PyProperties {
    props: DynProperties,
}

#[pymethods]
impl PyProperties {
    /// Get property value.
    ///
    /// First searches temporal properties and returns latest value if it exists.
    /// If not, it falls back to static properties.
    pub fn get(&self, key: &str) -> Option<Prop> {
        self.props.get(key)
    }

    /// Check if property `key` exists.
    pub fn __contains__(&self, key: &str) -> bool {
        self.props.contains(key)
    }

    fn __getitem__(&self, key: &str) -> PyResult<Prop> {
        self.props
            .get(key)
            .ok_or(PyKeyError::new_err("No such property"))
    }

    fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    fn __len__(&self) -> usize {
        self.keys().len()
    }

    /// Get the names for all properties (includes temporal and static properties)
    pub fn keys(&self) -> Vec<String> {
        self.props.keys().map(|k| k.clone()).collect()
    }

    /// Get the values of the properties
    ///
    /// If a property exists as both temporal and static, temporal properties take priority with
    /// fallback to the static property if the temporal value does not exist.
    pub fn values(&self) -> Vec<Prop> {
        self.props.values().collect()
    }

    /// Get a list of key-value pairs
    pub fn items(&self) -> Vec<(String, Prop)> {
        self.props.as_vec()
    }

    /// Get a view of the temporal properties only.
    #[getter]
    pub fn temporal(&self) -> DynTemporalProperties {
        self.props.temporal()
    }

    /// Get a view of the static properties (meta-data) only.
    #[getter]
    pub fn meta(&self) -> DynStaticProperties {
        self.props.meta()
    }

    /// Convert properties view to a dict
    pub fn as_dict(&self) -> HashMap<String, Prop> {
        self.props.as_map()
    }
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

py_iterable!(
    PropsIterable,
    DynProperties,
    PyProperties,
    PyGenericIterator
);

#[pymethods]
impl PropsIterable {
    /// Get property value.
    ///
    /// First searches temporal properties and returns latest value if it exists.
    /// If not, it falls back to static properties.
    pub fn get(&self, key: &str) -> Option<OptionPropIterable> {
        self.__contains__(key).then(|| {
            let builder = self.builder.clone();
            let key = key.to_owned();
            (move || {
                let key = key.clone();
                builder().map(move |p| p.get(&key))
            })
            .into()
        })
    }

    /// Check if property `key` exists.
    pub fn __contains__(&self, key: &str) -> bool {
        self.iter().any(|p| p.contains(key))
    }

    fn __getitem__(&self, key: &str) -> PyResult<OptionPropIterable> {
        self.get(key).ok_or(PyKeyError::new_err("No such property"))
    }

    /// Get the names for all properties (includes temporal and static properties)
    pub fn keys(&self) -> Vec<String> {
        self.iter()
            // FIXME: Still have to clone all those strings which sucks
            .map(|p| p.keys().map(|k| k.clone()).collect_vec())
            .kmerge()
            .dedup()
            .collect()
    }

    /// Get the values of the properties
    ///
    /// If a property exists as both temporal and static, temporal properties take priority with
    /// fallback to the static property if the temporal value does not exist.
    pub fn values(&self) -> Vec<OptionPropIterable> {
        self.keys()
            .into_iter()
            .map(|key| self.get(&key).unwrap())
            .collect()
    }

    /// Get a list of key-value pairs
    pub fn items(&self) -> Vec<(String, OptionPropIterable)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    /// Get a view of the temporal properties only.
    #[getter]
    pub fn temporal(&self) -> TemporalPropsIterable {
        let builder = self.builder.clone();
        (move || builder().map(|p| p.temporal())).into()
    }

    /// Get a view of the static properties (meta-data) only.
    #[getter]
    pub fn meta(&self) -> StaticPropsIterable {
        let builder = self.builder.clone();
        (move || builder().map(|p| p.meta())).into()
    }

    /// Convert properties view to a dict
    pub fn as_dict(&self) -> HashMap<String, Vec<Option<Prop>>> {
        self.items()
            .into_iter()
            .map(|(k, v)| (k, v.collect()))
            .collect()
    }
}

py_nested_iterable!(
    NestedPropsIterable,
    DynProperties,
    PyProperties,
    PyNestedGenericIterator
);

#[pymethods]
impl NestedPropsIterable {
    /// Get property value.
    ///
    /// First searches temporal properties and returns latest value if it exists.
    /// If not, it falls back to static properties.
    pub fn get(&self, key: &str) -> Option<NestedOptionPropIterable> {
        self.__contains__(key).then(|| {
            let builder = self.builder.clone();
            let key = key.to_owned();
            (move || {
                let key = key.clone();
                builder().map(move |it| {
                    let key = key.clone();
                    it.map(move |p| p.get(&key))
                })
            })
            .into()
        })
    }

    /// Check if property `key` exists.
    pub fn __contains__(&self, key: &str) -> bool {
        self.iter().any(|mut it| it.any(|p| p.contains(key)))
    }

    fn __getitem__(&self, key: &str) -> Result<NestedOptionPropIterable, PyErr> {
        self.get(key).ok_or(PyKeyError::new_err("No such property"))
    }

    /// Get the names for all properties (includes temporal and static properties)
    pub fn keys(&self) -> Vec<String> {
        self.iter()
            // FIXME: Still have to clone all those strings which sucks
            .flat_map(|it| it.map(|p| p.keys().map(|k| k.clone()).collect_vec()))
            .kmerge()
            .dedup()
            .collect()
    }

    /// Get the values of the properties
    ///
    /// If a property exists as both temporal and static, temporal properties take priority with
    /// fallback to the static property if the temporal value does not exist.
    pub fn values(&self) -> Vec<NestedOptionPropIterable> {
        self.keys()
            .into_iter()
            .map(|key| self.get(&key).unwrap())
            .collect()
    }

    /// Get a list of key-value pairs
    pub fn items(&self) -> Vec<(String, NestedOptionPropIterable)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    /// Get a view of the temporal properties only.
    #[getter]
    pub fn temporal(&self) -> NestedTemporalPropsIterable {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.map(|p| p.temporal()))).into()
    }

    /// Get a view of the static properties (meta-data) only.
    #[getter]
    pub fn meta(&self) -> NestedStaticPropsIterable {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.map(|p| p.meta()))).into()
    }

    /// Convert properties view to a dict
    pub fn as_dict(&self) -> HashMap<String, Vec<Vec<Option<Prop>>>> {
        self.items()
            .into_iter()
            .map(|(k, v)| (k, v.collect()))
            .collect()
    }
}
