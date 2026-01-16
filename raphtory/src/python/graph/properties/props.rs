use crate::{
    db::api::{
        properties::{
            dyn_props::{DynMetadata, DynProperties, DynTemporalProperties},
            internal::InternalPropertiesOps,
            Properties,
        },
        view::internal::{DynamicGraph, Static},
    },
    prelude::PropertiesOps,
    python::{
        graph::properties::{
            MetadataListList, MetadataView, PyMetadata, PyTemporalPropsList,
            PyTemporalPropsListList,
        },
        types::{
            repr::{iterator_dict_repr, Repr},
            wrappers::prop::PropValue,
        },
        utils::PyGenericIterator,
    },
};
use pyo3::{
    exceptions::{PyKeyError, PyTypeError},
    prelude::*,
};
use raphtory_api::core::{
    entities::properties::prop::{Prop, PropType},
    storage::arc_str::ArcStr,
};
use std::{collections::HashMap, ops::Deref, sync::Arc};

#[derive(Clone, Debug)]
pub struct PyPropsComp(HashMap<ArcStr, Prop>);

impl PartialEq for PyPropsComp {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<'source> FromPyObject<'source> for PyPropsComp {
    fn extract_bound(ob: &Bound<'source, PyAny>) -> PyResult<Self> {
        if let Ok(sp) = ob.extract::<PyRef<PyMetadata>>() {
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

impl From<&PyMetadata> for PyPropsComp {
    fn from(value: &PyMetadata) -> Self {
        Self(value.as_dict())
    }
}

impl From<&PyProperties> for PyPropsComp {
    fn from(value: &PyProperties) -> Self {
        Self(value.as_dict())
    }
}

impl From<DynMetadata> for PyPropsComp {
    fn from(value: DynMetadata) -> Self {
        Self(value.as_map())
    }
}

impl From<DynProperties> for PyPropsComp {
    fn from(value: DynProperties) -> Self {
        Self(value.as_map())
    }
}

/// A view of the properties of an entity
#[pyclass(name = "Properties", module = "raphtory", frozen)]
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
    ///
    /// Arguments:
    ///     key (str): the name of the property.
    ///
    /// Returns:
    ///     PropValue:
    pub fn get(&self, key: &str) -> Option<Prop> {
        self.props.get(key)
    }

    /// Get the PropType of a property. Specifically, returns the PropType of the latest value for this property if it exists.
    ///
    /// Arguments:
    ///     key (str): the name of the property.
    ///
    /// Returns:
    ///     PropType:
    pub fn get_dtype_of(&self, key: &str) -> Option<PropType> {
        self.props.get(key).map(|p| p.dtype())
    }

    /// Check if property `key` exists.
    ///
    /// Returns:
    ///     bool:
    pub fn __contains__(&self, key: &str) -> bool {
        self.props.get(key).is_some()
    }

    /// Gets property value if it exists, otherwise raises `KeyError`
    ///
    /// Returns:
    ///     dict[PropValue]:
    fn __getitem__(&self, key: &str) -> PyResult<Prop> {
        self.props
            .get(key)
            .ok_or(PyKeyError::new_err("No such property"))
    }

    /// Iterate over property keys
    ///
    /// Returns:
    ///     Iterator[str]:
    fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    /// Number of properties
    ///
    /// Returns:
    ///     int:
    fn __len__(&self) -> usize {
        self.keys().len()
    }

    /// Get the names for all properties
    ///
    /// Returns:
    ///     list[str]:
    pub fn keys(&self) -> Vec<ArcStr> {
        self.props.iter_filtered().map(|(key, _)| key).collect()
    }

    /// Get the values of the properties.
    ///
    /// Returns:
    ///     list[PropValue]:
    pub fn values(&self) -> Vec<Prop> {
        self.props.iter_filtered().map(|(_, value)| value).collect()
    }

    /// Get a list of key-value pairs
    ///
    /// Returns:
    ///     list[Tuple[str, PropValue]]:
    pub fn items(&self) -> Vec<(ArcStr, Prop)> {
        self.props.as_vec()
    }

    /// Get a view of the temporal properties only.
    ///
    /// Returns:
    ///     TemporalProp:
    #[getter]
    pub fn temporal(&self) -> DynTemporalProperties {
        self.props.temporal()
    }

    /// Convert properties view to a dict.
    ///
    /// Returns:
    ///     dict[str, PropValue]:
    pub fn as_dict(&self) -> HashMap<ArcStr, Prop> {
        self.props.as_map()
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Properties({{{}}})",
            iterator_dict_repr(self.items().into_iter())
        )
    }
}

impl<P: Into<DynProperties>> From<P> for PyProperties {
    fn from(value: P) -> Self {
        Self {
            props: value.into(),
        }
    }
}

impl<'py, P: InternalPropertiesOps + Clone + Send + Sync + 'static + Static> IntoPyObject<'py>
    for Properties<P>
{
    type Target = PyProperties;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyProperties::from(self).into_pyobject(py)
    }
}

impl<'py> IntoPyObject<'py> for Properties<DynamicGraph> {
    type Target = PyProperties;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyProperties::from(self).into_pyobject(py)
    }
}

impl<'py> IntoPyObject<'py> for DynProperties {
    type Target = PyProperties;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyProperties::from(self).into_pyobject(py)
    }
}

impl<P: InternalPropertiesOps + Clone> Repr for Properties<P> {
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
    fn extract_bound(ob: &Bound<'source, PyAny>) -> PyResult<Self> {
        if let Ok(sp) = ob.extract::<PyRef<MetadataView>>() {
            Ok(sp.deref().into())
        } else if let Ok(p) = ob.extract::<PyRef<PropertiesView>>() {
            Ok(p.deref().into())
        } else if let Ok(m) = ob.extract::<HashMap<ArcStr, PyPropValueListCmp>>() {
            Ok(Self(m))
        } else {
            Err(PyTypeError::new_err("not comparable with properties"))
        }
    }
}

impl From<&MetadataView> for PyPropsListCmp {
    fn from(value: &MetadataView) -> Self {
        Self(
            value
                .items()
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        )
    }
}

impl From<&PropertiesView> for PyPropsListCmp {
    fn from(value: &PropertiesView) -> Self {
        Self(
            value
                .items()
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        )
    }
}

py_iterable_base!(PropertiesView, DynProperties, PyProperties);
py_eq!(PropertiesView, PyPropsListCmp);

#[pymethods]
impl PropertiesView {
    /// Get property value.
    ///
    /// Arguments:
    ///     key (str): the name of the property.
    ///
    /// Returns:
    ///     PyPropValueList:
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
        self.iter().next().is_some_and(|p| p.contains(key))
    }

    fn __getitem__(&self, key: &str) -> PyResult<PyPropValueList> {
        self.get(key).ok_or(PyKeyError::new_err("No such property"))
    }

    /// Get the names for all properties.
    ///
    /// Returns:
    ///     list[str]:
    pub fn keys(&self) -> Vec<ArcStr> {
        self.iter()
            .next()
            .map(|p| p.keys().collect())
            .unwrap_or_default()
    }

    /// Get the values of the properties.
    ///
    /// Returns:
    ///     list[list[PropValue]]:
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

    /// Get a list of key-value pairs.
    ///
    /// Returns:
    ///     list[Tuple[str, List[PropValue]]]:
    pub fn items(&self) -> Vec<(ArcStr, PyPropValueList)> {
        self.keys()
            .into_iter()
            .flat_map(|k| self.get(&k).map(|v| (k, v)))
            .collect()
    }

    /// Get a view of the temporal properties only.
    ///
    /// Returns:
    ///     List[TemporalProp]:
    #[getter]
    pub fn temporal(&self) -> PyTemporalPropsList {
        let builder = self.builder.clone();
        (move || builder().map(|p| p.temporal())).into()
    }

    /// Convert properties view to a dict.
    ///
    /// Returns:
    ///     dict[str, List[PropValue]]:
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
py_eq!(PyNestedPropsIterable, PyMetadataListListCmp);

#[derive(PartialEq, Clone)]
pub struct PyMetadataListListCmp(HashMap<ArcStr, PyPropValueListListCmp>);

impl<'source> FromPyObject<'source> for PyMetadataListListCmp {
    fn extract_bound(ob: &Bound<'source, PyAny>) -> PyResult<Self> {
        if let Ok(sp) = ob.extract::<PyRef<MetadataListList>>() {
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

impl From<&MetadataListList> for PyMetadataListListCmp {
    fn from(value: &MetadataListList) -> Self {
        Self(
            value
                .items()
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        )
    }
}

impl From<&PyNestedPropsIterable> for PyMetadataListListCmp {
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
    /// Arguments:
    ///     key (str): the name of the property.
    ///
    /// Returns:
    ///     PyPropValueListList:
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
        self.iter()
            .filter_map(|mut it| it.next())
            .next()
            .is_some_and(|p| p.contains(key))
    }

    fn __getitem__(&self, key: &str) -> Result<PyPropValueListList, PyErr> {
        self.get(key).ok_or(PyKeyError::new_err("No such property"))
    }

    /// Get the names for all properties.
    ///
    /// Returns:
    ///     List[Str]:
    pub fn keys(&self) -> Vec<ArcStr> {
        self.iter()
            .filter_map(|mut it| it.next())
            .next()
            .map(|p| p.keys().collect())
            .unwrap_or_default()
    }

    pub fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    /// Get the values of the properties.
    ///
    ///
    /// Returns:
    ///     list[list[list[PropValue]]]:
    pub fn values(&self) -> Vec<PyPropValueListList> {
        self.keys()
            .into_iter()
            .flat_map(|key| self.get(&key))
            .collect()
    }

    /// Get a list of key-value pairs.
    ///
    /// Returns:
    ///     list[Tuple[str, List[PropValue]]]:
    pub fn items(&self) -> Vec<(ArcStr, PyPropValueListList)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    /// Get a view of the temporal properties only.
    ///
    /// Returns:
    ///     List[List[temporalprop]]:
    #[getter]
    pub fn temporal(&self) -> PyTemporalPropsListList {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.map(|p| p.temporal()))).into()
    }

    /// Convert properties view to a dict.
    ///
    /// Returns:
    ///     dict[str, List[List[PropValue]]]:
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
