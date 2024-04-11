use crate::{
    core::{ArcStr, Prop},
    db::api::{
        properties::{
            dyn_props::{DynConstProperties, DynProperties, DynTemporalProperties},
            internal::PropertiesOps,
            Properties,
        },
        view::internal::{DynamicGraph, Static},
    },
    python::{
        graph::properties::{
            PyConstProperties, PyConstPropsList, PyConstPropsListList, PyTemporalPropsList,
            PyTemporalPropsListList,
        },
        types::{
            repr::{iterator_dict_repr, Repr},
            wrappers::prop::PropValue,
        },
        utils::PyGenericIterator,
    },
};
use itertools::Itertools;
use pyo3::{
    exceptions::{PyKeyError, PyTypeError},
    prelude::*,
};
use std::{collections::HashMap, ops::Deref, sync::Arc};

#[derive(PartialEq, Clone)]
pub struct PyPropsComp(HashMap<ArcStr, Prop>);

impl<'source> FromPyObject<'source> for PyPropsComp {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(sp) = ob.extract::<PyRef<PyConstProperties>>() {
            Ok(sp.deref().into())
        } else if let Ok(p) = ob.extract::<PyRef<PyProperties>>() {
            Ok(p.deref().into())
        } else if let Ok(m) = ob.extract::<HashMap<ArcStr, Prop>>() {
            Ok(PyPropsComp(m))
        } else {
            Err(PyTypeError::new_err("not comparable with properties"))
        }
    }
}

impl From<&PyConstProperties> for PyPropsComp {
    fn from(value: &PyConstProperties) -> Self {
        Self(value.as_dict())
    }
}

impl From<&PyProperties> for PyPropsComp {
    fn from(value: &PyProperties) -> Self {
        Self(value.as_dict())
    }
}

impl From<DynConstProperties> for PyPropsComp {
    fn from(value: DynConstProperties) -> Self {
        Self(value.as_map())
    }
}

impl From<DynProperties> for PyPropsComp {
    fn from(value: DynProperties) -> Self {
        Self(value.as_map())
    }
}

/// A view of the properties of an entity
#[pyclass(name = "Properties")]
pub struct PyProperties {
    props: DynProperties,
}

py_eq!(PyProperties, PyPropsComp);

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

    /// gets property value if it exists, otherwise raises `KeyError`
    fn __getitem__(&self, key: &str) -> PyResult<Prop> {
        self.props
            .get(key)
            .ok_or(PyKeyError::new_err("No such property"))
    }

    /// iterate over property keys
    fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    /// number of properties
    fn __len__(&self) -> usize {
        self.keys().len()
    }

    /// Get the names for all properties (includes temporal and static properties)
    pub fn keys(&self) -> Vec<ArcStr> {
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
    pub fn items(&self) -> Vec<(ArcStr, Prop)> {
        self.props.as_vec()
    }

    /// Get a view of the temporal properties only.
    #[getter]
    pub fn temporal(&self) -> DynTemporalProperties {
        self.props.temporal()
    }

    /// Get a view of the constant properties (meta-data) only.
    #[getter]
    pub fn constant(&self) -> DynConstProperties {
        self.props.constant()
    }

    /// Convert properties view to a dict
    pub fn as_dict(&self) -> HashMap<ArcStr, Prop> {
        self.props.as_map()
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
pub struct PyPropsListCmp(HashMap<ArcStr, PyPropValueListCmp>);

impl<'source> FromPyObject<'source> for PyPropsListCmp {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(sp) = ob.extract::<PyRef<PyConstPropsList>>() {
            Ok(sp.deref().into())
        } else if let Ok(p) = ob.extract::<PyRef<PyPropsList>>() {
            Ok(p.deref().into())
        } else if let Ok(m) = ob.extract::<HashMap<ArcStr, PyPropValueListCmp>>() {
            Ok(Self(m))
        } else {
            Err(PyTypeError::new_err("not comparable with properties"))
        }
    }
}

impl From<&PyConstPropsList> for PyPropsListCmp {
    fn from(value: &PyConstPropsList) -> Self {
        Self(
            value
                .items()
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        )
    }
}

impl From<&PyPropsList> for PyPropsListCmp {
    fn from(value: &PyPropsList) -> Self {
        Self(
            value
                .items()
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        )
    }
}

py_iterable_base!(PyPropsList, DynProperties, PyProperties);
py_eq!(PyPropsList, PyPropsListCmp);

#[pymethods]
impl PyPropsList {
    /// Get property value.
    ///
    /// First searches temporal properties and returns latest value if it exists.
    /// If not, it falls back to constant properties.
    pub fn get(&self, key: &str) -> Option<PyPropValueList> {
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

    fn __getitem__(&self, key: &str) -> PyResult<PyPropValueList> {
        self.get(key).ok_or(PyKeyError::new_err("No such property"))
    }

    /// Get the names for all properties (includes temporal and constant properties)
    pub fn keys(&self) -> Vec<ArcStr> {
        self.iter()
            // FIXME: Still have to clone all those strings which sucks
            .map(|p| p.keys().map(|k| k.clone()).sorted().collect_vec())
            .kmerge()
            .dedup()
            .collect()
    }

    /// Get the values of the properties
    ///
    /// If a property exists as both temporal and constant, temporal properties take priority with
    /// fallback to the constant property if the temporal value does not exist.
    pub fn values(&self) -> PyPropValueListList {
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
    pub fn items(&self) -> Vec<(ArcStr, PyPropValueList)> {
        self.keys()
            .into_iter()
            .flat_map(|k| self.get(&k).map(|v| (k, v)))
            .collect()
    }

    /// Get a view of the temporal properties only.
    #[getter]
    pub fn temporal(&self) -> PyTemporalPropsList {
        let builder = self.builder.clone();
        (move || builder().map(|p| p.temporal())).into()
    }

    /// Get a view of the constant properties (meta-data) only.
    #[getter]
    pub fn constant(&self) -> PyConstPropsList {
        let builder = self.builder.clone();
        (move || builder().map(|p| p.constant())).into()
    }

    /// Convert properties view to a dict
    pub fn as_dict(&self) -> HashMap<ArcStr, Vec<Option<Prop>>> {
        self.items()
            .into_iter()
            .map(|(k, v)| (k, v.collect()))
            .collect()
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Properties({{{}}})",
            iterator_dict_repr(self.items().into_iter())
        )
    }
}

py_nested_iterable_base!(PyNestedPropsIterable, DynProperties, PyProperties);
py_eq!(PyNestedPropsIterable, PyConstPropsListListCmp);

#[derive(PartialEq, Clone)]
pub struct PyConstPropsListListCmp(HashMap<ArcStr, PyPropValueListListCmp>);

impl<'source> FromPyObject<'source> for PyConstPropsListListCmp {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(sp) = ob.extract::<PyRef<PyConstPropsListList>>() {
            Ok(sp.deref().into())
        } else if let Ok(p) = ob.extract::<PyRef<PyNestedPropsIterable>>() {
            Ok(p.deref().into())
        } else if let Ok(m) = ob.extract::<HashMap<ArcStr, PyPropValueListListCmp>>() {
            Ok(Self(m))
        } else {
            Err(PyTypeError::new_err("not comparable with properties"))
        }
    }
}

impl From<&PyConstPropsListList> for PyConstPropsListListCmp {
    fn from(value: &PyConstPropsListList) -> Self {
        Self(
            value
                .items()
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        )
    }
}

impl From<&PyNestedPropsIterable> for PyConstPropsListListCmp {
    fn from(value: &PyNestedPropsIterable) -> Self {
        Self(
            value
                .items()
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        )
    }
}

#[pymethods]
impl PyNestedPropsIterable {
    /// Get property value.
    ///
    /// First searches temporal properties and returns latest value if it exists.
    /// If not, it falls back to constant properties.
    pub fn get(&self, key: &str) -> Option<PyPropValueListList> {
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

    fn __getitem__(&self, key: &str) -> Result<PyPropValueListList, PyErr> {
        self.get(key).ok_or(PyKeyError::new_err("No such property"))
    }

    /// Get the names for all properties (includes temporal and constant properties)
    pub fn keys(&self) -> Vec<ArcStr> {
        self.iter()
            // FIXME: Still have to clone all those strings which sucks
            .flat_map(|it| it.map(|p| p.keys().map(|k| k.clone()).collect_vec()))
            .kmerge()
            .dedup()
            .collect()
    }

    pub fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    /// Get the values of the properties
    ///
    /// If a property exists as both temporal and constant, temporal properties take priority with
    /// fallback to the constant property if the temporal value does not exist.
    pub fn values(&self) -> Vec<PyPropValueListList> {
        self.keys()
            .into_iter()
            .flat_map(|key| self.get(&key))
            .collect()
    }

    /// Get a list of key-value pairs
    pub fn items(&self) -> Vec<(ArcStr, PyPropValueListList)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    /// Get a view of the temporal properties only.
    #[getter]
    pub fn temporal(&self) -> PyTemporalPropsListList {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.map(|p| p.temporal()))).into()
    }

    /// Get a view of the constant properties (meta-data) only.
    #[getter]
    pub fn constant(&self) -> PyConstPropsListList {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.map(|p| p.constant()))).into()
    }

    /// Convert properties view to a dict
    pub fn as_dict(&self) -> HashMap<ArcStr, Vec<Vec<Option<Prop>>>> {
        self.items()
            .into_iter()
            .map(|(k, v)| (k, v.collect()))
            .collect()
    }
}

py_iterable!(PyPropValueList, PropValue, PropValue);
py_iterable_comp!(PyPropValueList, PropValue, PyPropValueListCmp);

py_nested_iterable!(PyPropValueListList, PropValue, PropValue);
py_iterable_comp!(
    PyPropValueListList,
    PyPropValueListCmp,
    PyPropValueListListCmp
);
