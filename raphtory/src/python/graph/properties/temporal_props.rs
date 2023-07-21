use crate::core::utils::time::IntoTime;
use crate::core::Prop;
use crate::db::api::properties::internal::PropertiesOps;
use crate::db::api::properties::StaticProperties;
use crate::db::api::properties::{TemporalProperties, TemporalPropertyView};
use crate::db::api::view::internal::{DynamicGraph, Static};
use crate::python::graph::properties::static_props::PyStaticProperties;
use crate::python::graph::properties::{DynProps, NestedOptionPropIterable, OptionPropIterable};
use crate::python::types::repr::{iterator_dict_repr, iterator_repr, Repr};
use crate::python::utils::{PyGenericIterator, PyTime};
use itertools::Itertools;
use pyo3::exceptions::{PyKeyError, PyTypeError};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

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
pub struct TemporalPropsCmp(HashMap<String, PyTemporalPropertyViewCmp>);

impl From<&PyTemporalProperties> for TemporalPropsCmp {
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

impl FromPyObject for TemporalPropsCmp {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        todo!()
    }
}

#[pyclass(name = "TemporalProperties")]
pub struct PyTemporalProperties {
    props: DynTemporalProperties,
}

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
pub struct PyTemporalPropertyView {
    prop: DynTemporalProperty,
}

#[derive(PartialEq, Clone)]
pub struct PyTemporalPropertyViewCmp(Vec<(i64, Prop)>);

impl<'source> FromPyObject<'source> for PyTemporalPropertyViewCmp {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(sp) = ob.extract::<PyRef<PyTemporalPropertyView>>() {
            Ok(sp.deref().into())
        } else if let Ok(m) = ob.extract::<Vec<(i64, Prop)>>() {
            Ok(Self(m))
        } else {
            Err(PyTypeError::new_err("not comparable"))
        }
    }
}

impl From<&PyTemporalPropertyView> for PyTemporalPropertyViewCmp {
    fn from(value: &PyTemporalPropertyView) -> Self {
        Self(value.items())
    }
}

impl From<Vec<(i64, Prop)>> for PyTemporalPropertyViewCmp {
    fn from(value: Vec<(i64, Prop)>) -> Self {
        Self(value)
    }
}

py_eq!(PyTemporalPropertyView, PyTemporalPropertyViewCmp);

#[pymethods]
impl PyTemporalPropertyView {
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

impl<P: PropertiesOps + Send + Sync + 'static> From<TemporalPropertyView<P>>
    for PyTemporalPropertyView
{
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

impl Repr for PyTemporalPropertyView {
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
        PyTemporalPropertyView::from(self).into_py(py)
    }
}

py_iterable!(
    TemporalPropsIterable,
    DynTemporalProperties,
    PyTemporalProperties
);

#[pymethods]
impl TemporalPropsIterable {
    fn keys(&self) -> Vec<String> {
        self.iter()
            // FIXME: Still have to clone all those strings which sucks
            .map(|p| p.keys().map(|k| k.clone()).collect_vec())
            .kmerge()
            .dedup()
            .collect()
    }
    fn values(&self) -> Vec<TemporalPropertyIterable> {
        self.keys()
            .into_iter()
            .map(|k| self.get(k).expect("key exists"))
            .collect()
    }
    fn items(&self) -> Vec<(String, TemporalPropertyIterable)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    fn latest(&self) -> HashMap<String, OptionPropIterable> {
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

    fn histories(&self) -> HashMap<String, Vec<Vec<(i64, Prop)>>> {
        self.keys()
            .into_iter()
            .map(|k| {
                let v = self
                    .iter()
                    .map(|p| p.get(&k).map(|v| v.iter().collect()).unwrap_or_default())
                    .collect();
                (k, v)
            })
            .collect()
    }

    fn __getitem__(&self, key: String) -> PyResult<TemporalPropertyIterable> {
        self.get(key).ok_or(PyKeyError::new_err("unknown property"))
    }

    fn __contains__(&self, key: &str) -> bool {
        self.iter().any(|p| p.contains(key))
    }

    fn get(&self, key: String) -> Option<TemporalPropertyIterable> {
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

pub struct OptionPyTemporalPropertyView(Option<PyTemporalPropertyView>);

impl Repr for OptionPyTemporalPropertyView {
    fn repr(&self) -> String {
        self.0.repr()
    }
}

impl IntoPy<PyObject> for OptionPyTemporalPropertyView {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.into_py(py)
    }
}

impl From<Option<DynTemporalProperty>> for OptionPyTemporalPropertyView {
    fn from(value: Option<DynTemporalProperty>) -> Self {
        Self(value.map(|v| v.into()))
    }
}

py_iterable!(
    TemporalPropertyIterable,
    Option<DynTemporalProperty>,
    OptionPyTemporalPropertyView
);

#[pymethods]
impl TemporalPropertyIterable {
    pub fn history(&self) -> Vec<Vec<i64>> {
        self.iter()
            .map(|p| p.map(|v| v.history()).unwrap_or_default())
            .collect()
    }
    pub fn values(&self) -> Vec<Vec<Prop>> {
        self.iter()
            .map(|p| p.map(|v| v.values()).unwrap_or_default())
            .collect()
    }
    pub fn items(&self) -> Vec<Vec<(i64, Prop)>> {
        self.iter()
            .map(|p| p.map(|v| v.iter().collect()).unwrap_or_default())
            .collect()
    }

    pub fn at(&self, t: PyTime) -> Vec<Option<Prop>> {
        let t = t.into_time();
        self.iter().map(|p| p.and_then(|v| v.at(t))).collect()
    }
    pub fn value(&self) -> Vec<Option<Prop>> {
        self.iter().map(|p| p.and_then(|v| v.latest())).collect()
    }
}

py_nested_iterable!(
    NestedTemporalPropsIterable,
    DynTemporalProperties,
    PyTemporalProperties
);

#[pymethods]
impl NestedTemporalPropsIterable {
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
    fn values(&self) -> Vec<NestedTemporalPropertyIterable> {
        self.keys()
            .into_iter()
            .map(|k| self.get(k).expect("key exists"))
            .collect()
    }
    fn items(&self) -> Vec<(String, NestedTemporalPropertyIterable)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    fn latest(&self) -> HashMap<String, NestedOptionPropIterable> {
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

    fn histories(&self) -> HashMap<String, Vec<Vec<Vec<(i64, Prop)>>>> {
        self.keys()
            .into_iter()
            .map(|k| {
                let v = self
                    .iter()
                    .map(|it| {
                        it.map(|p| p.get(&k).map(|v| v.iter().collect()).unwrap_or_default())
                            .collect()
                    })
                    .collect();
                (k, v)
            })
            .collect()
    }

    fn __getitem__(&self, key: String) -> PyResult<NestedTemporalPropertyIterable> {
        self.get(key).ok_or(PyKeyError::new_err("unknown property"))
    }

    fn __contains__(&self, key: &str) -> bool {
        self.iter().any(|mut it| it.any(|p| p.contains(key)))
    }

    fn get(&self, key: String) -> Option<NestedTemporalPropertyIterable> {
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
    NestedTemporalPropertyIterable,
    Option<DynTemporalProperty>,
    OptionPyTemporalPropertyView
);

#[pymethods]
impl NestedTemporalPropertyIterable {
    pub fn history(&self) -> Vec<Vec<Vec<i64>>> {
        self.iter()
            .map(|it| {
                it.map(|p| p.map(|v| v.history()).unwrap_or_default())
                    .collect()
            })
            .collect()
    }
    pub fn values(&self) -> Vec<Vec<Vec<Prop>>> {
        self.iter()
            .map(|it| {
                it.map(|p| p.map(|v| v.values()).unwrap_or_default())
                    .collect()
            })
            .collect()
    }
    pub fn items(&self) -> Vec<Vec<Vec<(i64, Prop)>>> {
        self.iter()
            .map(|it| {
                it.map(|p| p.map(|v| v.iter().collect()).unwrap_or_default())
                    .collect()
            })
            .collect()
    }

    pub fn at(&self, t: PyTime) -> Vec<Vec<Option<Prop>>> {
        let t = t.into_time();
        self.iter()
            .map(|it| it.map(|p| p.and_then(|v| v.at(t))).collect())
            .collect()
    }
    pub fn value(&self) -> Vec<Vec<Option<Prop>>> {
        self.iter()
            .map(|it| it.map(|p| p.and_then(|v| v.latest())).collect())
            .collect()
    }
}
