use crate::{
    core::Prop,
    db::api::properties::{
        dyn_props::DynConstProperties, internal::PropertiesOps, ConstantProperties,
    },
    python::{
        graph::properties::{
            props::PyPropsComp, PyConstPropsListListCmp, PyPropValueList, PyPropValueListList,
            PyPropsListCmp,
        },
        types::repr::{iterator_dict_repr, Repr},
        utils::{NumpyArray, PyGenericIterator},
    },
};
use itertools::Itertools;
use pyo3::{
    exceptions::{PyKeyError, PyTypeError},
    prelude::*,
};
use raphtory_api::core::storage::arc_str::ArcStr;
use std::{collections::HashMap, sync::Arc};

impl<'py, P: PropertiesOps + Send + Sync + 'static> IntoPyObject<'py>
    for ConstantProperties<'static, P>
{
    type Target = PyConstantProperties;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyConstantProperties::from(self).into_pyobject(py)
    }
}

impl<'a, P: PropertiesOps> Repr for ConstantProperties<'a, P> {
    fn repr(&self) -> String {
        format!("StaticProperties({{{}}})", iterator_dict_repr(self.iter()))
    }
}

/// A view of constant properties of an entity
#[pyclass(name = "ConstantProperties", module = "raphtory", frozen)]
pub struct PyConstantProperties {
    props: DynConstProperties,
}

py_eq!(PyConstantProperties, PyPropsComp);

#[pymethods]
impl PyConstantProperties {
    /// lists the available property keys
    ///
    /// Returns:
    ///     list[str]: the property keys
    pub fn keys(&self) -> Vec<ArcStr> {
        self.props.keys().collect()
    }

    /// lists the property values
    ///
    /// Returns:
    ///     list | Array: the property values
    pub fn values(&self) -> NumpyArray {
        self.props.values().collect()
    }

    /// lists the property keys together with the corresponding value
    ///
    /// Returns:
    ///     list[Tuple[str, PropValue]]: the property keys with corresponding values
    pub fn items(&self) -> Vec<(ArcStr, Prop)> {
        self.props.iter().collect()
    }

    /// get property value by key
    ///
    /// Raises:
    ///     KeyError: if property `key` does not exist
    pub fn __getitem__(&self, key: &str) -> PyResult<Prop> {
        self.props
            .get(key)
            .ok_or(PyKeyError::new_err("No such property"))
    }

    /// get property value by key
    ///
    /// Arguments:
    ///     key (str): the name of the property
    ///
    /// Returns:
    ///     PropValue | None: the property value or `None` if value for `key` does not exist
    pub fn get(&self, key: &str) -> Option<Prop> {
        // Fixme: Add option to specify default?
        self.props.get(key)
    }

    /// as_dict() -> dict[str, Any]
    ///
    /// convert the properties view to a python dict
    ///
    /// Returns:
    ///     dict[str, PropValue]:
    pub fn as_dict(&self) -> HashMap<ArcStr, Prop> {
        self.props.as_map()
    }

    /// iterate over property keys
    ///
    /// Returns:
    ///     Iterator[str]: keys iterator
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

impl<P: PropertiesOps + Send + Sync + 'static> From<ConstantProperties<'static, P>>
    for PyConstantProperties
{
    fn from(value: ConstantProperties<P>) -> Self {
        PyConstantProperties {
            props: ConstantProperties::new(Arc::new(value.props)),
        }
    }
}

impl Repr for PyConstantProperties {
    fn repr(&self) -> String {
        self.props.repr()
    }
}

py_iterable_base!(PyConstPropsList, DynConstProperties, PyConstantProperties);
py_eq!(PyConstPropsList, PyPropsListCmp);

#[pymethods]
impl PyConstPropsList {
    pub fn keys(&self) -> Vec<ArcStr> {
        self.iter()
            .map(|p| p.keys().collect::<Vec<_>>())
            .kmerge()
            .dedup()
            .collect()
    }

    pub fn values(&self) -> Vec<PyPropValueList> {
        self.keys()
            .into_iter()
            .map(|k| self.get(k).expect("key exists"))
            .collect()
    }
    pub fn items(&self) -> Vec<(ArcStr, PyPropValueList)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    pub fn __getitem__(&self, key: ArcStr) -> PyResult<PyPropValueList> {
        self.get(key).ok_or(PyKeyError::new_err("No such property"))
    }

    pub fn get(&self, key: ArcStr) -> Option<PyPropValueList> {
        self.__contains__(&key).then(|| {
            let builder = self.builder.clone();
            let key = key.clone();
            (move || {
                let key = key.clone();
                builder().map(move |p| p.get(&key))
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

    pub fn as_dict(&self) -> HashMap<ArcStr, Vec<Option<Prop>>> {
        self.items()
            .into_iter()
            .map(|(k, v)| (k, v.collect()))
            .collect()
    }
}

py_nested_iterable_base!(
    PyConstPropsListList,
    DynConstProperties,
    PyConstantProperties
);
py_eq!(PyConstPropsListList, PyConstPropsListListCmp);

#[pymethods]
impl PyConstPropsListList {
    pub fn keys(&self) -> Vec<ArcStr> {
        self.iter()
            .flat_map(|it| it.map(|p| p.keys().collect::<Vec<_>>()))
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
    pub fn items(&self) -> Vec<(ArcStr, PyPropValueListList)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    pub fn __getitem__(&self, key: ArcStr) -> PyResult<PyPropValueListList> {
        self.get(key).ok_or(PyKeyError::new_err("No such property"))
    }

    pub fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    pub fn get(&self, key: ArcStr) -> Option<PyPropValueListList> {
        self.__contains__(&key).then(|| {
            let builder = self.builder.clone();
            let key = key.clone();
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

    pub fn __contains__(&self, key: &str) -> bool {
        self.iter().any(|mut it| it.any(|p| p.contains(key)))
    }

    pub fn as_dict(&self) -> HashMap<ArcStr, Vec<Vec<Option<Prop>>>> {
        self.items()
            .into_iter()
            .map(|(k, v)| (k, v.collect()))
            .collect()
    }
}
