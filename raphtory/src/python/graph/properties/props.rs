use crate::core::Prop;
use crate::db::api::properties::internal::PropertiesOps;
use crate::db::api::properties::Properties;
use crate::db::api::view::internal::{DynamicGraph, Static};
use crate::python::graph::properties::{
    DynProps, DynStaticProperties, DynTemporalProperties, NestedStaticPropsIterable,
    NestedTemporalPropsIterable, PyStaticProperties, StaticPropsIterable, TemporalPropsIterable,
};
use crate::python::types::iterable::{Iterable, NestedIterable};
use crate::python::types::repr::{iterator_dict_repr, Repr};
use crate::python::types::wrappers::prop::PropValue;
use crate::python::utils::PyGenericIterator;
use itertools::Itertools;
use pyo3::basic::CompareOp;
use pyo3::exceptions::{PyKeyError, PyTypeError};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

pub type DynProperties = Properties<Arc<dyn PropertiesOps + Send + Sync>>;

#[derive(PartialEq, Clone)]
pub struct PropsComparable(HashMap<String, Prop>);

impl<'source> FromPyObject<'source> for PropsComparable {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(sp) = ob.extract::<PyRef<PyStaticProperties>>() {
            Ok(sp.deref().into())
        } else if let Ok(p) = ob.extract::<PyRef<PyProperties>>() {
            Ok(p.deref().into())
        } else if let Ok(m) = ob.extract::<HashMap<String, Prop>>() {
            Ok(PropsComparable(m))
        } else {
            Err(PyTypeError::new_err("not comparable with properties"))
        }
    }
}

impl From<&PyStaticProperties> for PropsComparable {
    fn from(value: &PyStaticProperties) -> Self {
        Self(value.as_dict())
    }
}

impl From<&PyProperties> for PropsComparable {
    fn from(value: &PyProperties) -> Self {
        Self(value.as_dict())
    }
}

impl From<DynStaticProperties> for PropsComparable {
    fn from(value: DynStaticProperties) -> Self {
        Self(value.as_map())
    }
}

impl From<DynProperties> for PropsComparable {
    fn from(value: DynProperties) -> Self {
        Self(value.as_map())
    }
}

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

    pub fn __richcmp__(&self, other: PropsComparable, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Lt => Err(PyTypeError::new_err("not ordered")),
            CompareOp::Le => Err(PyTypeError::new_err("not ordered")),
            CompareOp::Eq => Ok(PropsComparable::from(self) == other),
            CompareOp::Ne => Ok(PropsComparable::from(self) != other),
            CompareOp::Gt => Err(PyTypeError::new_err("not ordered")),
            CompareOp::Ge => Err(PyTypeError::new_err("not ordered")),
        }
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

#[derive(PartialEq, Clone)]
pub struct PyPropsIterableComparable(HashMap<String, OptionPropIterCmp>);

impl<'source> FromPyObject<'source> for PyPropsIterableComparable {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(sp) = ob.extract::<PyRef<StaticPropsIterable>>() {
            Ok(sp.deref().into())
        } else if let Ok(p) = ob.extract::<PyRef<PyPropsIterable>>() {
            Ok(p.deref().into())
        } else if let Ok(m) = ob.extract::<HashMap<String, OptionPropIterCmp>>() {
            Ok(Self(m))
        } else {
            Err(PyTypeError::new_err("not comparable with properties"))
        }
    }
}

impl From<&StaticPropsIterable> for PyPropsIterableComparable {
    fn from(value: &StaticPropsIterable) -> Self {
        Self(
            value
                .items()
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        )
    }
}

impl From<&PyPropsIterable> for PyPropsIterableComparable {
    fn from(value: &PyPropsIterable) -> Self {
        Self(
            value
                .items()
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        )
    }
}

#[pyclass(name = "PropertiesIterable")]
pub struct PyPropsIterable(Iterable<DynProperties, PyProperties>);

impl Deref for PyPropsIterable {
    type Target = Iterable<DynProperties, PyProperties>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<F: Fn() -> It + Send + Sync + 'static, It: Iterator + Send + 'static> From<F>
    for PyPropsIterable
where
    It::Item: Into<DynProperties>,
{
    fn from(value: F) -> Self {
        Self(Iterable::new("PropsIterable", value))
    }
}

#[pymethods]
impl PyPropsIterable {
    /// Get property value.
    ///
    /// First searches temporal properties and returns latest value if it exists.
    /// If not, it falls back to static properties.
    pub fn get(&self, key: &str) -> Option<OptionPropIterable> {
        self.__contains__(key).then(|| {
            let builder = self.builder.clone();
            let key = Arc::new(key.to_owned());
            (move || {
                let key = key.clone();
                builder().map(move |p| p.get(key.as_ref()))
            })
            .into()
        })
    }

    pub fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
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
    pub fn values(&self) -> NestedOptionPropIterable {
        let builder = self.builder.clone();
        let keys = Arc::new(self.keys());
        (move || {
            let builder = builder.clone();
            let keys = keys.clone();
            (0..keys.len()).map(move |index| {
                let builder = builder.clone();
                let keys = keys.clone();
                builder().map(move |p| {
                    let key = &keys[index];
                    p.get(key)
                })
            })
        })
        .into()
    }

    /// Get a list of key-value pairs
    pub fn items(&self) -> Vec<(String, OptionPropIterable)> {
        self.keys()
            .into_iter()
            .flat_map(|k| self.get(&k).map(|v| (k, v)))
            .collect()
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

    pub fn __richcmp__(&self, other: PyPropsIterableComparable, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Lt => Err(PyTypeError::new_err("not ordered")),
            CompareOp::Le => Err(PyTypeError::new_err("not ordered")),
            CompareOp::Eq => Ok(PyPropsIterableComparable::from(self) == other),
            CompareOp::Ne => Ok(PyPropsIterableComparable::from(self) != other),
            CompareOp::Gt => Err(PyTypeError::new_err("not ordered")),
            CompareOp::Ge => Err(PyTypeError::new_err("not ordered")),
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Properties({{{}}})",
            iterator_dict_repr(self.items().into_iter())
        )
    }
}

#[pyclass(name = "NestedPropertiesIterable")]
pub struct PyNestedPropsIterable(NestedIterable<DynProperties, PyProperties>);

impl Deref for PyNestedPropsIterable {
    type Target = NestedIterable<DynProperties, PyProperties>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<F: Fn() -> It + Send + Sync + 'static, It: Iterator + Send + 'static> From<F>
    for PyNestedPropsIterable
where
    It::Item: Iterator + Send,
    <It::Item as Iterator>::Item: Into<DynProperties> + Send,
{
    fn from(value: F) -> Self {
        Self(NestedIterable::new("NestedPropsIterable", value))
    }
}

#[derive(PartialEq, Clone)]
pub struct PyNestedPropsIterableComparable(HashMap<String, Vec<Vec<Option<Prop>>>>);

impl<'source> FromPyObject<'source> for PyNestedPropsIterableComparable {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(sp) = ob.extract::<PyRef<NestedStaticPropsIterable>>() {
            Ok(Self(sp.deref().as_dict()))
        } else if let Ok(p) = ob.extract::<PyRef<PyNestedPropsIterable>>() {
            Ok(Self(p.deref().as_dict()))
        } else if let Ok(m) = ob.extract::<HashMap<String, Vec<Vec<Option<Prop>>>>>() {
            Ok(Self(m))
        } else {
            Err(PyTypeError::new_err("not comparable with properties"))
        }
    }
}

#[pymethods]
impl PyNestedPropsIterable {
    /// Get property value.
    ///
    /// First searches temporal properties and returns latest value if it exists.
    /// If not, it falls back to static properties.
    pub fn get(&self, key: &str) -> Option<NestedOptionPropIterable> {
        self.__contains__(key).then(|| {
            let builder = self.builder.clone();
            let key = Arc::new(key.to_owned());
            (move || {
                let key = key.clone();
                builder().map(move |it| {
                    let key = key.clone();
                    it.map(move |p| p.get(key.clone().as_ref()))
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

    pub fn __iter(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    /// Get the values of the properties
    ///
    /// If a property exists as both temporal and static, temporal properties take priority with
    /// fallback to the static property if the temporal value does not exist.
    pub fn values(&self) -> Vec<NestedOptionPropIterable> {
        self.keys()
            .into_iter()
            .flat_map(|key| self.get(&key))
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

    pub fn __richcmp__(
        &self,
        other: PyNestedPropsIterableComparable,
        op: CompareOp,
    ) -> PyResult<bool> {
        match op {
            CompareOp::Lt => Err(PyTypeError::new_err("not ordered")),
            CompareOp::Le => Err(PyTypeError::new_err("not ordered")),
            CompareOp::Eq => Ok(PyNestedPropsIterableComparable(self.as_dict()) == other),
            CompareOp::Ne => Ok(PyNestedPropsIterableComparable(self.as_dict()) != other),
            CompareOp::Gt => Err(PyTypeError::new_err("not ordered")),
            CompareOp::Ge => Err(PyTypeError::new_err("not ordered")),
        }
    }
}

py_iterable!(OptionPropIterable, PropValue, PropValue);
py_iterable_comp!(OptionPropIterable, PropValue, OptionPropIterCmp);

py_nested_iterable!(NestedOptionPropIterable, PropValue, PropValue);
py_iterable_comp!(
    NestedOptionPropIterable,
    OptionPropIterCmp,
    NestedOptionPropIterCmp
);
