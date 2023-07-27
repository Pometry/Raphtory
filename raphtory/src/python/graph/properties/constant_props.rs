use crate::{
    core::Prop,
    db::api::{
        properties::{internal::PropertiesOps, ConstProperties},
        view::internal::Static,
    },
    python::{
        graph::properties::{
            props::PyPropsComp, DynProps, PyConstPropsListListCmp, PyPropValueList,
            PyPropValueListList, PyPropsListCmp,
        },
        types::repr::{iterator_dict_repr, Repr},
        utils::PyGenericIterator,
    },
};
use itertools::Itertools;
use pyo3::{
    exceptions::{PyKeyError, PyTypeError},
    prelude::*,
};
use std::{collections::HashMap, sync::Arc};

pub type DynConstProperties = ConstProperties<DynProps>;

impl<P: PropertiesOps + Send + Sync + Static + 'static> From<ConstProperties<P>>
    for DynConstProperties
{
    fn from(value: ConstProperties<P>) -> Self {
        ConstProperties {
            props: Arc::new(value.props),
        }
    }
}

impl<P: PropertiesOps + Send + Sync + 'static> IntoPy<PyObject> for ConstProperties<P> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyConstProperties::from(self).into_py(py)
    }
}

impl<P: PropertiesOps> Repr for ConstProperties<P> {
    fn repr(&self) -> String {
        format!("StaticProperties({{{}}})", iterator_dict_repr(self.iter()))
    }
}

/// A view of constant properties of an entity
#[pyclass(name = "ConstProperties")]
pub struct PyConstProperties {
    props: DynConstProperties,
}

py_eq!(PyConstProperties, PyPropsComp);

#[pymethods]
impl PyConstProperties {
    /// keys() -> list[str]
    ///
    /// lists the available property keys
    pub fn keys(&self) -> Vec<String> {
        self.props.keys()
    }

    /// values() -> list[Any]
    ///
    /// lists the property values
    pub fn values(&self) -> Vec<Prop> {
        self.props.values()
    }

    /// items() -> list[tuple[str, Any]]
    ///
    /// lists the property keys together with the corresponding value
    pub fn items(&self) -> Vec<(String, Prop)> {
        self.props.iter().collect()
    }

    /// __getitem__(key: str) -> Any
    ///
    /// get property value by key
    ///
    /// Raises:
    ///     KeyError: if property `key` does not exist
    pub fn __getitem__(&self, key: &str) -> PyResult<Prop> {
        self.props
            .get(key)
            .ok_or(PyKeyError::new_err("No such property"))
    }

    /// get(key: str) -> Any | None
    ///
    /// Arguments:
    ///     key: the name of the property
    ///
    /// get property value by key (returns `None` if key does not exist)
    pub fn get(&self, key: &str) -> Option<Prop> {
        // Fixme: Add option to specify default?
        self.props.get(key)
    }

    /// as_dict() -> dict[str, Any]
    ///
    /// convert the properties view to a python dict
    pub fn as_dict(&self) -> HashMap<String, Prop> {
        self.props.as_map()
    }

    /// __iter__() -> Iterator[str]
    ///
    /// iterate over property keys
    pub fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    /// __contains__(key: str) -> bool
    ///
    /// check if property `key` exists
    pub fn __contains__(&self, key: &str) -> bool {
        self.props.contains(key)
    }

    /// __len__() -> int
    ///
    /// the number of properties
    pub fn __len__(&self) -> usize {
        self.keys().len()
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl<P: PropertiesOps + Send + Sync + 'static> From<ConstProperties<P>> for PyConstProperties {
    fn from(value: ConstProperties<P>) -> Self {
        PyConstProperties {
            props: ConstProperties::new(Arc::new(value.props)),
        }
    }
}

impl Repr for PyConstProperties {
    fn repr(&self) -> String {
        self.props.repr()
    }
}

py_iterable_base!(PyConstPropsList, DynConstProperties, PyConstProperties);
py_eq!(PyConstPropsList, PyPropsListCmp);

#[pymethods]
impl PyConstPropsList {
    pub fn keys(&self) -> Vec<String> {
        self.iter().map(|p| p.keys()).kmerge().dedup().collect()
    }

    pub fn values(&self) -> Vec<PyPropValueList> {
        self.keys()
            .into_iter()
            .map(|k| self.get(k).expect("key exists"))
            .collect()
    }
    pub fn items(&self) -> Vec<(String, PyPropValueList)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    pub fn __getitem__(&self, key: String) -> PyResult<PyPropValueList> {
        self.get(key).ok_or(PyKeyError::new_err("No such property"))
    }

    pub fn get(&self, key: String) -> Option<PyPropValueList> {
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

py_nested_iterable_base!(PyConstPropsListList, DynConstProperties, PyConstProperties);
py_eq!(PyConstPropsListList, PyConstPropsListListCmp);

#[pymethods]
impl PyConstPropsListList {
    pub fn keys(&self) -> Vec<String> {
        self.iter()
            .flat_map(|it| it.map(|p| p.keys()))
            .kmerge()
            .dedup()
            .collect()
    }

    pub fn values(&self) -> Vec<PyPropValueListList> {
        self.keys()
            .into_iter()
            .map(|k| self.get(k).expect("key exists"))
            .collect()
    }
    pub fn items(&self) -> Vec<(String, PyPropValueListList)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    pub fn __getitem__(&self, key: String) -> PyResult<PyPropValueListList> {
        self.get(key).ok_or(PyKeyError::new_err("No such property"))
    }

    pub fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    pub fn get(&self, key: String) -> Option<PyPropValueListList> {
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
