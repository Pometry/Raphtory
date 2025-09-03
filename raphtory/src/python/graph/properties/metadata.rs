use crate::{
    db::api::properties::{dyn_props::DynMetadata, internal::InternalPropertiesOps, Metadata},
    prelude::PropertiesOps,
    python::{
        graph::properties::{
            props::PyPropsComp, PyMetadataListListCmp, PyPropValueList, PyPropValueListList,
            PyPropsListCmp,
        },
        types::repr::{iterator_dict_repr, Repr},
        utils::PyGenericIterator,
    },
};
use pyo3::{
    exceptions::{PyKeyError, PyTypeError},
    prelude::*,
};
use raphtory_api::core::{entities::properties::prop::Prop, storage::arc_str::ArcStr};
use std::{collections::HashMap, sync::Arc};

impl<'py, P: InternalPropertiesOps + Send + Sync + 'static> IntoPyObject<'py>
    for Metadata<'static, P>
{
    type Target = PyMetadata;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyMetadata::from(self).into_pyobject(py)
    }
}

impl<'a, P: InternalPropertiesOps> Repr for Metadata<'a, P> {
    fn repr(&self) -> String {
        format!("Metadata({{{}}})", iterator_dict_repr(self.iter_filtered()))
    }
}

/// A view of metadata of an entity
#[pyclass(name = "Metadata", module = "raphtory", frozen)]
pub struct PyMetadata {
    props: DynMetadata,
}

py_eq!(PyMetadata, PyPropsComp);

#[pymethods]
impl PyMetadata {
    /// lists the available property keys
    ///
    /// Returns:
    ///     list[str]: the property keys
    pub fn keys(&self) -> Vec<ArcStr> {
        self.props.iter_filtered().map(|(key, _)| key).collect()
    }

    /// lists the property values
    ///
    /// Returns:
    ///     list | Array: the property values
    pub fn values(&self) -> Vec<Prop> {
        self.props.iter_filtered().map(|(_, value)| value).collect()
    }

    /// lists the property keys together with the corresponding value
    ///
    /// Returns:
    ///     list[Tuple[str, PropValue]]: the property keys with corresponding values
    pub fn items(&self) -> Vec<(ArcStr, Prop)> {
        self.props.as_vec()
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
        self.props.get(key).is_some()
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

impl<P: InternalPropertiesOps + Send + Sync + 'static> From<Metadata<'static, P>> for PyMetadata {
    fn from(value: Metadata<P>) -> Self {
        PyMetadata {
            props: Metadata::new(Arc::new(value.props)),
        }
    }
}

impl Repr for PyMetadata {
    fn repr(&self) -> String {
        self.props.repr()
    }
}

py_iterable_base!(MetadataView, DynMetadata, PyMetadata);
py_eq!(MetadataView, PyPropsListCmp);

#[pymethods]
impl MetadataView {
    pub fn keys(&self) -> Vec<ArcStr> {
        self.iter()
            .next()
            .map(|p| p.keys().collect())
            .unwrap_or_default()
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
        self.iter().next().is_some_and(|p| p.contains(key))
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

py_nested_iterable_base!(MetadataListList, DynMetadata, PyMetadata);
py_eq!(MetadataListList, PyMetadataListListCmp);

#[pymethods]
impl MetadataListList {
    pub fn keys(&self) -> Vec<ArcStr> {
        self.iter()
            .flat_map(|mut it| it.next().map(|p| p.keys().collect()))
            .next()
            .unwrap_or_default()
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
        self.iter()
            .filter_map(|mut it| it.next())
            .next()
            .is_some_and(|p| p.contains(key))
    }

    pub fn as_dict(&self) -> HashMap<ArcStr, Vec<Vec<Option<Prop>>>> {
        self.items()
            .into_iter()
            .map(|(k, v)| (k, v.collect()))
            .collect()
    }
}
