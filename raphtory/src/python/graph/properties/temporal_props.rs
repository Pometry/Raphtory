use crate::{
    core::{utils::time::IntoTime, Prop},
    db::api::{
        properties::{internal::PropertiesOps, TemporalProperties, TemporalPropertyView},
        view::internal::{DynamicGraph, Static},
    },
    python::{
        graph::properties::{DynProps, PyPropValueList, PyPropValueListList},
        types::{
            repr::{iterator_dict_repr, iterator_repr, Repr},
            wrappers::prop::PropHistItems,
        },
        utils::{PyGenericIterator, PyTime},
    },
};
use itertools::Itertools;
use pyo3::{
    exceptions::{PyKeyError, PyTypeError},
    prelude::*,
};
use std::{collections::HashMap, ops::Deref, sync::Arc};

pub type DynTemporalProperties = TemporalProperties<DynProps>;
pub type DynTemporalProperty = TemporalPropertyView<DynProps>;

impl<P: PropertiesOps + Clone + Send + Sync + Static + 'static> From<TemporalProperties<P>>
    for DynTemporalProperties
{
    fn from(value: TemporalProperties<P>) -> Self {
        TemporalProperties::new(Arc::new(value.props))
    }
}

impl From<TemporalProperties<DynamicGraph>> for DynTemporalProperties {
    fn from(value: TemporalProperties<DynamicGraph>) -> Self {
        let props: Arc<dyn PropertiesOps + Send + Sync> = Arc::new(value.props);
        TemporalProperties::new(props)
    }
}

impl<P: Into<DynTemporalProperties>> From<P> for PyTemporalProperties {
    fn from(value: P) -> Self {
        Self {
            props: value.into(),
        }
    }
}

#[derive(PartialEq)]
pub struct PyTemporalPropsCmp(HashMap<String, PyTemporalPropCmp>);

impl From<HashMap<String, PyTemporalPropCmp>> for PyTemporalPropsCmp {
    fn from(value: HashMap<String, PyTemporalPropCmp>) -> Self {
        Self(value)
    }
}

impl From<&PyTemporalProperties> for PyTemporalPropsCmp {
    fn from(value: &PyTemporalProperties) -> Self {
        Self(
            value
                .histories()
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        )
    }
}

impl<'source> FromPyObject<'source> for PyTemporalPropsCmp {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(v) = ob.extract::<PyRef<PyTemporalProperties>>() {
            Ok(PyTemporalPropsCmp::from(v.deref()))
        } else if let Ok(v) = ob.extract::<HashMap<String, PyTemporalPropCmp>>() {
            Ok(PyTemporalPropsCmp::from(v))
        } else {
            Err(PyTypeError::new_err("cannot compare"))
        }
    }
}

#[pyclass(name = "TemporalProperties")]
pub struct PyTemporalProperties {
    props: DynTemporalProperties,
}

py_eq!(PyTemporalProperties, PyTemporalPropsCmp);

#[pymethods]
impl PyTemporalProperties {
    fn keys(&self) -> Vec<String> {
        self.props.keys().map(|k| k.clone()).collect()
    }
    fn values(&self) -> Vec<DynTemporalProperty> {
        self.props.values().collect()
    }
    fn items(&self) -> Vec<(String, DynTemporalProperty)> {
        self.props.iter().map(|(k, v)| (k.clone(), v)).collect()
    }

    fn latest(&self) -> HashMap<String, Prop> {
        self.props
            .iter_latest()
            .map(|(k, v)| (k.clone(), v))
            .collect()
    }
    fn histories(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.props
            .iter()
            .map(|(k, v)| (k.clone(), v.iter().collect()))
            .collect()
    }

    fn __getitem__(&self, key: &str) -> PyResult<Prop> {
        let v = self
            .props
            .get(key)
            .ok_or(PyKeyError::new_err("No such property"))?;
        Ok(v.latest().unwrap())
    }

    fn get(&self, key: &str) -> Option<DynTemporalProperty> {
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

#[pyclass(name = "Property")]
pub struct PyTemporalProp {
    prop: DynTemporalProperty,
}

#[derive(PartialEq, Clone)]
pub struct PyTemporalPropCmp(Vec<(i64, Prop)>);

impl<'source> FromPyObject<'source> for PyTemporalPropCmp {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(sp) = ob.extract::<PyRef<PyTemporalProp>>() {
            Ok(sp.deref().into())
        } else if let Ok(m) = ob.extract::<Vec<(i64, Prop)>>() {
            Ok(Self(m))
        } else {
            Err(PyTypeError::new_err("not comparable"))
        }
    }
}

impl From<&PyTemporalProp> for PyTemporalPropCmp {
    fn from(value: &PyTemporalProp) -> Self {
        Self(value.items())
    }
}

impl From<Vec<(i64, Prop)>> for PyTemporalPropCmp {
    fn from(value: Vec<(i64, Prop)>) -> Self {
        Self(value)
    }
}

impl From<DynTemporalProperty> for PyTemporalPropCmp {
    fn from(value: DynTemporalProperty) -> Self {
        PyTemporalPropCmp(value.iter().collect())
    }
}

py_eq!(PyTemporalProp, PyTemporalPropCmp);

#[pymethods]
impl PyTemporalProp {
    pub fn history(&self) -> Vec<i64> {
        self.prop.history()
    }
    pub fn values(&self) -> Vec<Prop> {
        self.prop.values()
    }
    pub fn items(&self) -> Vec<(i64, Prop)> {
        self.prop.iter().collect()
    }

    pub fn __iter__(&self) -> PyGenericIterator {
        self.prop.iter().into()
    }
    pub fn at(&self, t: PyTime) -> Option<Prop> {
        self.prop.at(t.into_time())
    }
    pub fn value(&self) -> Option<Prop> {
        self.prop.latest()
    }
}

impl<P: PropertiesOps + Send + Sync + 'static> From<TemporalPropertyView<P>> for PyTemporalProp {
    fn from(value: TemporalPropertyView<P>) -> Self {
        Self {
            prop: TemporalPropertyView {
                id: value.id,
                props: Arc::new(value.props),
            },
        }
    }
}

impl<P: PropertiesOps + Clone + Send + Sync + 'static + Static> IntoPy<PyObject>
    for TemporalProperties<P>
{
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyTemporalProperties::from(self).into_py(py)
    }
}

impl IntoPy<PyObject> for TemporalProperties<DynamicGraph> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyTemporalProperties::from(self).into_py(py)
    }
}

impl IntoPy<PyObject> for DynTemporalProperties {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyTemporalProperties::from(self).into_py(py)
    }
}

impl<P: PropertiesOps + Clone> Repr for TemporalProperties<P> {
    fn repr(&self) -> String {
        format!("Properties({{{}}})", iterator_dict_repr(self.iter()))
    }
}

impl<P: PropertiesOps> Repr for TemporalPropertyView<P> {
    fn repr(&self) -> String {
        format!("Property({})", iterator_repr(self.iter()))
    }
}

impl Repr for PyTemporalProp {
    fn repr(&self) -> String {
        self.prop.repr()
    }
}

impl Repr for PyTemporalProperties {
    fn repr(&self) -> String {
        self.props.repr()
    }
}

impl<P: PropertiesOps + Send + Sync + 'static> IntoPy<PyObject> for TemporalPropertyView<P> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyTemporalProp::from(self).into_py(py)
    }
}

py_iterable_base!(
    PyTemporalPropsList,
    DynTemporalProperties,
    PyTemporalProperties
);

#[derive(PartialEq)]
pub struct PyTemporalPropsListCmp(HashMap<String, PyTemporalPropListCmp>);

impl From<&PyTemporalPropsList> for PyTemporalPropsListCmp {
    fn from(value: &PyTemporalPropsList) -> Self {
        Self(
            value
                .items()
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        )
    }
}

impl From<HashMap<String, PyTemporalPropListCmp>> for PyTemporalPropsListCmp {
    fn from(value: HashMap<String, PyTemporalPropListCmp>) -> Self {
        Self(value)
    }
}

impl<'source> FromPyObject<'source> for PyTemporalPropsListCmp {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(v) = ob.extract::<PyRef<PyTemporalPropsList>>() {
            Ok(PyTemporalPropsListCmp::from(v.deref()))
        } else if let Ok(v) = ob.extract::<HashMap<String, PyTemporalPropListCmp>>() {
            Ok(PyTemporalPropsListCmp::from(v))
        } else {
            Err(PyTypeError::new_err("cannot compare"))
        }
    }
}

py_eq!(PyTemporalPropsList, PyTemporalPropsListCmp);

#[pymethods]
impl PyTemporalPropsList {
    fn keys(&self) -> Vec<String> {
        self.iter()
            // FIXME: Still have to clone all those strings which sucks
            .map(|p| p.keys().map(|k| k.clone()).collect_vec())
            .kmerge()
            .dedup()
            .collect()
    }
    fn values(&self) -> Vec<PyTemporalPropList> {
        self.keys()
            .into_iter()
            .map(|k| self.get(k).expect("key exists"))
            .collect()
    }
    fn items(&self) -> Vec<(String, PyTemporalPropList)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    fn latest(&self) -> HashMap<String, PyPropValueList> {
        let builder = self.builder.clone();
        self.keys()
            .into_iter()
            .map(move |k| {
                let builder = builder.clone();
                let nk = Arc::new(k.clone());
                (
                    k,
                    (move || {
                        let nk = nk.clone();
                        builder().map(move |p| p.get(nk.as_ref()).and_then(|v| v.latest()))
                    })
                    .into(),
                )
            })
            .collect()
    }

    fn histories(&self) -> HashMap<String, PropHistItemsList> {
        self.keys()
            .into_iter()
            .map(|k| {
                let kk = Arc::new(k.clone());
                let builder = self.builder.clone();
                let v = (move || {
                    let kk = kk.clone();
                    builder().map(move |p| {
                        p.get(kk.as_ref())
                            .map(|v| v.iter().collect::<Vec<_>>())
                            .unwrap_or_default()
                    })
                })
                .into();
                (k, v)
            })
            .collect()
    }

    fn __getitem__(&self, key: String) -> PyResult<PyTemporalPropList> {
        self.get(key).ok_or(PyKeyError::new_err("unknown property"))
    }

    fn __contains__(&self, key: &str) -> bool {
        self.iter().any(|p| p.contains(key))
    }

    fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    fn get(&self, key: String) -> Option<PyTemporalPropList> {
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
}

pub struct OptionPyTemporalProp(Option<PyTemporalProp>);

#[derive(PartialEq, FromPyObject, Clone)]
pub struct OptionPyTemporalPropCmp(Option<PyTemporalPropCmp>);

impl From<Option<DynTemporalProperty>> for OptionPyTemporalPropCmp {
    fn from(value: Option<DynTemporalProperty>) -> Self {
        OptionPyTemporalPropCmp(value.map(|v| v.into()))
    }
}

impl Repr for OptionPyTemporalProp {
    fn repr(&self) -> String {
        self.0.repr()
    }
}

impl IntoPy<PyObject> for OptionPyTemporalProp {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.into_py(py)
    }
}

impl From<Option<DynTemporalProperty>> for OptionPyTemporalProp {
    fn from(value: Option<DynTemporalProperty>) -> Self {
        Self(value.map(|v| v.into()))
    }
}

py_iterable!(
    PyTemporalPropList,
    Option<DynTemporalProperty>,
    OptionPyTemporalProp
);

py_iterable_comp!(
    PyTemporalPropList,
    OptionPyTemporalPropCmp,
    PyTemporalPropListCmp
);

#[pymethods]
impl PyTemporalPropList {
    pub fn history(&self) -> PropHistList {
        let builder = self.builder.clone();
        (move || builder().map(|p| p.map(|v| v.history()).unwrap_or_default())).into()
    }

    pub fn values(&self) -> PropHistValueList {
        let builder = self.builder.clone();
        (move || builder().map(|p| p.map(|v| v.values()).unwrap_or_default())).into()
    }

    pub fn items(&self) -> PropHistItemsList {
        let builder = self.builder.clone();
        (move || builder().map(|p| p.map(|v| v.iter().collect::<Vec<_>>()).unwrap_or_default()))
            .into()
    }

    pub fn at(&self, t: PyTime) -> PyPropValueList {
        let t = t.into_time();
        let builder = self.builder.clone();
        (move || builder().map(move |p| p.and_then(|v| v.at(t)))).into()
    }

    pub fn value(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || builder().map(|p| p.and_then(|v| v.latest()))).into()
    }
}

py_nested_iterable_base!(
    PyTemporalPropsListList,
    DynTemporalProperties,
    PyTemporalProperties
);

#[derive(PartialEq)]
pub struct PyTemporalPropsListListCmp(HashMap<String, PyTemporalPropListListCmp>);

impl From<&PyTemporalPropsListList> for PyTemporalPropsListListCmp {
    fn from(value: &PyTemporalPropsListList) -> Self {
        Self(
            value
                .items()
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        )
    }
}

impl From<HashMap<String, PyTemporalPropListListCmp>> for PyTemporalPropsListListCmp {
    fn from(value: HashMap<String, PyTemporalPropListListCmp>) -> Self {
        Self(value)
    }
}

impl<'source> FromPyObject<'source> for PyTemporalPropsListListCmp {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(v) = ob.extract::<PyRef<PyTemporalPropsListList>>() {
            Ok(Self::from(v.deref()))
        } else if let Ok(v) = ob.extract::<HashMap<String, PyTemporalPropListListCmp>>() {
            Ok(Self::from(v))
        } else {
            Err(PyTypeError::new_err("cannot compare"))
        }
    }
}

py_eq!(PyTemporalPropsListList, PyTemporalPropsListListCmp);

#[pymethods]
impl PyTemporalPropsListList {
    fn keys(&self) -> Vec<String> {
        self.iter()
            .flat_map(
                |it|             // FIXME: Still have to clone all those strings which sucks
            it.map(|p| p.keys().map(|k| k.clone()).collect_vec()),
            )
            .kmerge()
            .dedup()
            .collect()
    }
    fn values(&self) -> Vec<PyTemporalPropListList> {
        self.keys()
            .into_iter()
            .map(|k| self.get(k).expect("key exists"))
            .collect()
    }
    fn items(&self) -> Vec<(String, PyTemporalPropListList)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    fn latest(&self) -> HashMap<String, PyPropValueListList> {
        let builder = self.builder.clone();
        self.keys()
            .into_iter()
            .map(move |k| {
                let builder = builder.clone();
                let nk = Arc::new(k.clone());
                (
                    k,
                    (move || {
                        let nk = nk.clone();
                        builder().map(move |it| {
                            let nk = nk.clone();
                            it.map(move |p| p.get(nk.as_ref()).and_then(|v| v.latest()))
                        })
                    })
                    .into(),
                )
            })
            .collect()
    }

    fn histories(&self) -> HashMap<String, PyPropHistItemsListList> {
        let builder = self.builder.clone();
        self.keys()
            .into_iter()
            .map(move |k| {
                let builder = builder.clone();
                let kk = Arc::new(k.clone());
                let v = (move || {
                    let kk = kk.clone();
                    builder().map(move |it| {
                        let kk = kk.clone();
                        it.map(move |p| {
                            p.get(kk.as_ref())
                                .map(|v| v.iter().collect::<Vec<_>>())
                                .unwrap_or_default()
                        })
                    })
                })
                .into();
                (k, v)
            })
            .collect()
    }

    fn __getitem__(&self, key: String) -> PyResult<PyTemporalPropListList> {
        self.get(key).ok_or(PyKeyError::new_err("unknown property"))
    }

    fn __contains__(&self, key: &str) -> bool {
        self.iter().any(|mut it| it.any(|p| p.contains(key)))
    }

    fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    fn get(&self, key: String) -> Option<PyTemporalPropListList> {
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
}

py_nested_iterable!(
    PyTemporalPropListList,
    Option<DynTemporalProperty>,
    OptionPyTemporalProp
);

py_iterable_comp!(
    PyTemporalPropListList,
    PyTemporalPropListCmp,
    PyTemporalPropListListCmp
);

#[pymethods]
impl PyTemporalPropListList {
    pub fn history(&self) -> PropHistListList {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.map(|p| p.map(|v| v.history()).unwrap_or_default()))).into()
    }

    pub fn values(&self) -> PropHistValueListList {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.map(|p| p.map(|v| v.values()).unwrap_or_default()))).into()
    }

    pub fn items(&self) -> PyPropHistItemsListList {
        let builder = self.builder.clone();
        (move || {
            builder()
                .map(|it| it.map(|p| p.map(|v| v.iter().collect::<Vec<_>>()).unwrap_or_default()))
        })
        .into()
    }

    pub fn at(&self, t: PyTime) -> PyPropValueListList {
        let t = t.into_time();
        let builder = self.builder.clone();
        (move || builder().map(move |it| it.map(move |p| p.and_then(|v| v.at(t))))).into()
    }

    pub fn value(&self) -> PyPropValueListList {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.map(|p| p.and_then(|v| v.latest())))).into()
    }
}

py_iterable!(PropHistList, Vec<i64>);
py_iterable_comp!(PropHistList, Vec<i64>, PropHistListCmp);
py_nested_iterable!(PropHistListList, Vec<i64>);
py_iterable_comp!(PropHistListList, PropHistListCmp, PropHistListListCmp);

py_iterable!(PropHistValueList, Vec<Prop>);
py_iterable_comp!(PropHistValueList, Vec<Prop>, PropHistValueListCmp);
py_nested_iterable!(PropHistValueListList, Vec<Prop>);
py_iterable_comp!(
    PropHistValueListList,
    PropHistValueListCmp,
    PropHistValueListListCmp
);

py_iterable!(PropHistItemsList, PropHistItems);
py_iterable_comp!(PropHistItemsList, PropHistItems, PropHistItemsListCmp);
py_nested_iterable!(PyPropHistItemsListList, PropHistItems);
py_iterable_comp!(
    PyPropHistItemsListList,
    PropHistItemsListCmp,
    PropHistoryListListCmp
);
