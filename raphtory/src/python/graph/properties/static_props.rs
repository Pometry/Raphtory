use crate::core::Prop;
use crate::db::api::properties::internal::PropertiesOps;
use crate::db::api::properties::StaticProperties;
use crate::db::api::view::internal::Static;
use crate::python::graph::properties::props::PropsComparable;
use crate::python::graph::properties::{
    DynProps, NestedOptionPropIterable, OptionPropIterable, PyNestedPropsIterableComparable,
    PyPropsIterableComparable,
};
use crate::python::types::iterable::{Iterable, NestedIterable};
use crate::python::types::repr::{iterator_dict_repr, Repr};
use crate::python::utils::PyGenericIterator;
use itertools::Itertools;
use pyo3::exceptions::{PyKeyError, PyTypeError};
use pyo3::prelude::*;
use pyo3::pyclass::CompareOp;
use std::collections::HashMap;
use std::ops::Deref;
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

impl<P: PropertiesOps + Send + Sync + 'static> IntoPy<PyObject> for StaticProperties<P> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyStaticProperties::from(self).into_py(py)
    }
}

impl<P: PropertiesOps> Repr for StaticProperties<P> {
    fn repr(&self) -> String {
        format!("StaticProperties({{{}}})", iterator_dict_repr(self.iter()))
    }
}

#[pyclass(name = "MetaData")]
pub struct PyStaticProperties {
    props: DynStaticProperties,
}

py_eq!(PyStaticProperties, PropsComparable);

#[pymethods]
impl PyStaticProperties {
    pub fn keys(&self) -> Vec<String> {
        self.props.keys()
    }
    pub fn values(&self) -> Vec<Prop> {
        self.props.values()
    }
    pub fn items(&self) -> Vec<(String, Prop)> {
        self.props.iter().collect()
    }

    pub fn __getitem__(&self, key: &str) -> PyResult<Prop> {
        self.props
            .get(key)
            .ok_or(PyKeyError::new_err("No such property"))
    }

    pub fn get(&self, key: &str) -> Option<Prop> {
        // Fixme: Add option to specify default?
        self.props.get(key)
    }

    pub fn as_dict(&self) -> HashMap<String, Prop> {
        self.props.as_map()
    }

    pub fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    pub fn __contains__(&self, key: &str) -> bool {
        self.props.contains(key)
    }

    pub fn __len__(&self) -> usize {
        self.keys().len()
    }

    pub fn __repr__(&self) -> String {
        self.repr()
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

#[pyclass(name = "StaticPropertiesIterable")]
pub struct PyStaticPropsIterable(Iterable<DynStaticProperties, PyStaticProperties>);

impl Deref for PyStaticPropsIterable {
    type Target = Iterable<DynStaticProperties, PyStaticProperties>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<F: Fn() -> It + Send + Sync + 'static, It: Iterator + Send + 'static> From<F>
    for PyStaticPropsIterable
where
    It::Item: Into<DynStaticProperties>,
{
    fn from(value: F) -> Self {
        Self(Iterable::new("StaticPropsIterable", value))
    }
}

py_eq!(PyStaticPropsIterable, PyPropsIterableComparable);

#[pymethods]
impl PyStaticPropsIterable {
    pub fn keys(&self) -> Vec<String> {
        self.iter().map(|p| p.keys()).kmerge().dedup().collect()
    }

    pub fn values(&self) -> Vec<OptionPropIterable> {
        self.keys()
            .into_iter()
            .map(|k| self.get(k).expect("key exists"))
            .collect()
    }
    pub fn items(&self) -> Vec<(String, OptionPropIterable)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    pub fn __getitem__(&self, key: String) -> PyResult<OptionPropIterable> {
        self.get(key).ok_or(PyKeyError::new_err("No such property"))
    }

    pub fn get(&self, key: String) -> Option<OptionPropIterable> {
        self.__contains__(&key).then(|| {
            let builder = self.builder.clone();
            let key = Arc::new(key);
            (move || {
                let key = key.clone();
                builder().map(move |p| p.get(key.as_ref()))
            })
            .into()
        })
    }

    pub fn __contains__(&self, key: &str) -> bool {
        self.iter().any(|p| p.contains(key))
    }

    pub fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    pub fn as_dict(&self) -> HashMap<String, Vec<Option<Prop>>> {
        self.items()
            .into_iter()
            .map(|(k, v)| (k, v.collect()))
            .collect()
    }
}

#[pyclass(name = "NestedStaticPropertiesIterable")]
pub struct NestedStaticPropsIterable(NestedIterable<DynStaticProperties, PyStaticProperties>);

impl Deref for NestedStaticPropsIterable {
    type Target = NestedIterable<DynStaticProperties, PyStaticProperties>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<F: Fn() -> It + Send + Sync + 'static, It: Iterator + Send + 'static> From<F>
    for NestedStaticPropsIterable
where
    It::Item: Iterator + Send,
    <It::Item as Iterator>::Item: Into<DynStaticProperties> + Send,
{
    fn from(value: F) -> Self {
        Self(NestedIterable::new("NestedPropsIterable", value))
    }
}

#[pymethods]
impl NestedStaticPropsIterable {
    pub fn keys(&self) -> Vec<String> {
        self.iter()
            .flat_map(|it| it.map(|p| p.keys()))
            .kmerge()
            .dedup()
            .collect()
    }

    pub fn values(&self) -> Vec<NestedOptionPropIterable> {
        self.keys()
            .into_iter()
            .map(|k| self.get(k).expect("key exists"))
            .collect()
    }
    pub fn items(&self) -> Vec<(String, NestedOptionPropIterable)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    pub fn __getitem__(&self, key: String) -> PyResult<NestedOptionPropIterable> {
        self.get(key).ok_or(PyKeyError::new_err("No such property"))
    }

    pub fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    pub fn get(&self, key: String) -> Option<NestedOptionPropIterable> {
        self.__contains__(&key).then(|| {
            let builder = self.builder.clone();
            let key = Arc::new(key);
            (move || {
                let key = key.clone();
                builder().map(move |it| {
                    let key = key.clone();
                    it.map(move |p| p.get(key.as_ref()))
                })
            })
            .into()
        })
    }

    pub fn __contains__(&self, key: &str) -> bool {
        self.iter().any(|mut it| it.any(|p| p.contains(key)))
    }

    pub fn as_dict(&self) -> HashMap<String, Vec<Vec<Option<Prop>>>> {
        self.items()
            .into_iter()
            .map(|(k, v)| (k, v.collect()))
            .collect()
    }
}

py_eq!(NestedStaticPropsIterable, PyNestedPropsIterableComparable);
