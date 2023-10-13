use crate::{
    core::{utils::time::IntoTime, ArcStr, Prop},
    db::api::{
        properties::{internal::PropertiesOps, TemporalProperties, TemporalPropertyView},
        view::internal::{DynamicGraph, Static},
    },
    python::{
        graph::properties::{DynProps, PyPropValueList, PyPropValueListList},
        types::{
            repr::{iterator_dict_repr, iterator_repr, Repr},
            wrappers::{
                iterators::{
                    I64VecIterable, NestedI64VecIterable, NestedUsizeIterable, PropIterable,
                    UsizeIterable,
                },
                prop::{PropHistItems, PropValue},
            },
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
pub struct PyTemporalPropsCmp(HashMap<ArcStr, PyTemporalPropCmp>);

impl From<HashMap<ArcStr, PyTemporalPropCmp>> for PyTemporalPropsCmp {
    fn from(value: HashMap<ArcStr, PyTemporalPropCmp>) -> Self {
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
        } else if let Ok(v) = ob.extract::<HashMap<ArcStr, PyTemporalPropCmp>>() {
            Ok(PyTemporalPropsCmp::from(v))
        } else {
            Err(PyTypeError::new_err("cannot compare"))
        }
    }
}

/// A view of the temporal properties of an entity
#[pyclass(name = "TemporalProperties")]
pub struct PyTemporalProperties {
    props: DynTemporalProperties,
}

py_eq!(PyTemporalProperties, PyTemporalPropsCmp);

#[pymethods]
impl PyTemporalProperties {
    /// List the available property keys
    fn keys(&self) -> Vec<ArcStr> {
        self.props.keys().map(|k| k.clone()).collect()
    }

    /// List the values of the properties
    ///
    /// Returns:
    ///     list[TemporalProp]: the list of property views
    fn values(&self) -> Vec<DynTemporalProperty> {
        self.props.values().collect()
    }

    /// List the property keys together with the corresponding values
    fn items(&self) -> Vec<(ArcStr, DynTemporalProperty)> {
        self.props.iter().map(|(k, v)| (k.clone(), v)).collect()
    }

    /// Get the latest value of all properties
    ///
    /// Returns:
    ///     dict[str, Any]: the mapping of property keys to latest values
    fn latest(&self) -> HashMap<ArcStr, Prop> {
        self.props
            .iter_latest()
            .map(|(k, v)| (k.clone(), v))
            .collect()
    }

    /// Get the histories of all properties
    ///
    /// Returns:
    ///     dict[str, list[(int, Any)]]: the mapping of property keys to histories
    fn histories(&self) -> HashMap<ArcStr, Vec<(i64, Prop)>> {
        self.props
            .iter()
            .map(|(k, v)| (k.clone(), v.iter().collect()))
            .collect()
    }

    /// __getitem__(key: str) -> TemporalProp
    ///
    /// Get property value for `key`
    ///
    /// Returns:
    ///     the property view
    ///
    /// Raises:
    ///     KeyError: if property `key` does not exist
    fn __getitem__(&self, key: &str) -> PyResult<DynTemporalProperty> {
        self.get(key).ok_or(PyKeyError::new_err("No such property"))
    }

    /// get(key: str) -> Optional[TemporalProp]
    ///
    /// Get property value for `key` if it exists
    ///
    /// Returns:
    ///     the property view if it exists, otherwise `None`
    fn get(&self, key: &str) -> Option<DynTemporalProperty> {
        // Fixme: Add option to specify default?
        self.props.get(key)
    }

    /// Iterator over property keys
    fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    /// Check if property `key` exists
    fn __contains__(&self, key: &str) -> bool {
        self.props.contains(key)
    }

    /// The number of properties
    fn __len__(&self) -> usize {
        self.keys().len()
    }

    fn __repr__(&self) -> String {
        self.props.repr()
    }
}

/// A view of a temporal property
#[pyclass(name = "TemporalProp")]
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
    /// Get the timestamps at which the property was updated
    pub fn history(&self) -> Vec<i64> {
        self.prop.history()
    }

    /// Get the property values for each update
    pub fn values(&self) -> Vec<Prop> {
        self.prop.values()
    }

    /// List update timestamps and corresponding property values
    pub fn items(&self) -> Vec<(i64, Prop)> {
        self.prop.iter().collect()
    }

    /// Iterate over `items`
    pub fn __iter__(&self) -> PyGenericIterator {
        self.prop.iter().into()
    }
    /// Get the value of the property at time `t`
    pub fn at(&self, t: PyTime) -> Option<Prop> {
        self.prop.at(t.into_time())
    }
    /// Get the latest value of the property
    pub fn value(&self) -> Option<Prop> {
        self.prop.latest()
    }

    /// Compute the sum of all property values.
    ///
    /// Returns:
    ///     Prop: The sum of all property values.
    pub fn sum(&self) -> Prop {
        let mut it_iter = self.prop.iter();
        let first = it_iter.next().unwrap();
        it_iter.fold(first.1, |acc, elem| acc.add(elem.1).unwrap())
    }

    /// Find the minimum property value and its associated time.
    ///
    /// Returns:
    ///     (i64, Prop): A tuple containing the time and the minimum property value.
    pub fn min(&self) -> (i64, Prop) {
        let mut it_iter = self.prop.iter();
        let first = it_iter.next().unwrap();
        it_iter.fold(first, |acc, elem| if acc.1 <= elem.1 { acc } else { elem })
    }

    /// Find the maximum property value and its associated time.
    ///
    /// Returns:
    ///     (i64, Prop): A tuple containing the time and the maximum property value.
    pub fn max(&self) -> (i64, Prop) {
        let mut it_iter = self.prop.iter();
        let first = it_iter.next().unwrap();
        it_iter.fold(first, |acc, elem| if acc.1 >= elem.1 { acc } else { elem })
    }

    /// Count the number of properties.
    ///
    /// Returns:
    ///     int: The number of properties.
    pub fn count(&self) -> usize {
        self.prop.iter().count()
    }

    /// Compute the average of all property values. Alias for mean().
    ///
    /// Returns:
    ///     Prop: The average of each property values, or None if count is zero.
    pub fn average(&self) -> Option<Prop> {
        self.mean()
    }

    /// Compute the mean of all property values. Alias for mean().
    ///
    /// Returns:
    ///     Prop: The mean of each property values, or None if count is zero.
    pub fn mean(&self) -> Option<Prop> {
        let sum: Prop = self.sum();
        let count: usize = self.count();
        if count == 0 {
            return None;
        }
        match sum {
            Prop::I32(s) => Some(Prop::F32(s as f32 / count as f32)),
            Prop::I64(s) => Some(Prop::F64(s as f64 / count as f64)),
            Prop::U32(s) => Some(Prop::F32(s as f32 / count as f32)),
            Prop::U8(s) => Some(Prop::F64(s as f64 / count as f64)), // needs a test
            Prop::U16(s) => Some(Prop::F64(s as f64 / count as f64)), // needs a test
            Prop::U64(s) => Some(Prop::F64(s as f64 / count as f64)),
            Prop::F32(s) => Some(Prop::F32(s / count as f32)),
            Prop::F64(s) => Some(Prop::F64(s / count as f64)),
            _ => None,
        }
    }

    /// Compute the median of all property values.
    ///
    /// Returns:
    ///     (i64, Prop): A tuple containing the time and the median property value, or None if empty
    pub fn median(&self) -> Option<(i64, Prop)> {
        let it_iter = self.prop.iter();
        let mut vec: Vec<(i64, Prop)> = it_iter.collect_vec();
        // let mut vec: Vec<(i64, Prop)> = it_iter.map(|(t, v)| (t, v.clone())).collect();
        vec.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let len = vec.len();
        if len == 0 {
            return None;
        }
        if len % 2 == 0 {
            return Some(vec[len / 2 - 1].clone());
        }
        Some(vec[len / 2].clone())
    }

    pub fn __repr__(&self) -> String {
        self.prop.repr()
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
        format!(
            "TemporalProperties({{{}}})",
            iterator_dict_repr(self.iter())
        )
    }
}

impl<P: PropertiesOps> Repr for TemporalPropertyView<P> {
    fn repr(&self) -> String {
        format!("TemporalProp({})", iterator_repr(self.iter()))
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
pub struct PyTemporalPropsListCmp(HashMap<ArcStr, PyTemporalPropListCmp>);

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

impl From<HashMap<ArcStr, PyTemporalPropListCmp>> for PyTemporalPropsListCmp {
    fn from(value: HashMap<ArcStr, PyTemporalPropListCmp>) -> Self {
        Self(value)
    }
}

impl<'source> FromPyObject<'source> for PyTemporalPropsListCmp {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(v) = ob.extract::<PyRef<PyTemporalPropsList>>() {
            Ok(PyTemporalPropsListCmp::from(v.deref()))
        } else if let Ok(v) = ob.extract::<HashMap<ArcStr, PyTemporalPropListCmp>>() {
            Ok(PyTemporalPropsListCmp::from(v))
        } else {
            Err(PyTypeError::new_err("cannot compare"))
        }
    }
}

py_eq!(PyTemporalPropsList, PyTemporalPropsListCmp);

#[pymethods]
impl PyTemporalPropsList {
    fn keys(&self) -> Vec<ArcStr> {
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
    fn items(&self) -> Vec<(ArcStr, PyTemporalPropList)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    fn latest(&self) -> HashMap<ArcStr, PyPropValueList> {
        let builder = self.builder.clone();
        self.keys()
            .into_iter()
            .map(move |k| {
                let builder = builder.clone();
                let nk = k.clone();
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

    fn histories(&self) -> HashMap<ArcStr, PyPropHistItemsList> {
        self.keys()
            .into_iter()
            .map(|k| {
                let kk = k.clone();
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

    fn __getitem__(&self, key: ArcStr) -> PyResult<PyTemporalPropList> {
        self.get(key).ok_or(PyKeyError::new_err("unknown property"))
    }

    fn __contains__(&self, key: &str) -> bool {
        self.iter().any(|p| p.contains(key))
    }

    fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    fn get(&self, key: ArcStr) -> Option<PyTemporalPropList> {
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
    #[getter]
    pub fn history(&self) -> I64VecIterable {
        let builder = self.builder.clone();
        (move || builder().map(|p| p.map(|v| v.history()).unwrap_or_default())).into()
    }

    pub fn values(&self) -> PyPropHistValueList {
        let builder = self.builder.clone();
        (move || builder().map(|p| p.map(|v| v.values()).unwrap_or_default())).into()
    }

    pub fn items(&self) -> PyPropHistItemsList {
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
pub struct PyTemporalPropsListListCmp(HashMap<ArcStr, PyTemporalPropListListCmp>);

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

impl From<HashMap<ArcStr, PyTemporalPropListListCmp>> for PyTemporalPropsListListCmp {
    fn from(value: HashMap<ArcStr, PyTemporalPropListListCmp>) -> Self {
        Self(value)
    }
}

impl<'source> FromPyObject<'source> for PyTemporalPropsListListCmp {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(v) = ob.extract::<PyRef<PyTemporalPropsListList>>() {
            Ok(Self::from(v.deref()))
        } else if let Ok(v) = ob.extract::<HashMap<ArcStr, PyTemporalPropListListCmp>>() {
            Ok(Self::from(v))
        } else {
            Err(PyTypeError::new_err("cannot compare"))
        }
    }
}

py_eq!(PyTemporalPropsListList, PyTemporalPropsListListCmp);

#[pymethods]
impl PyTemporalPropsListList {
    fn keys(&self) -> Vec<ArcStr> {
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
    fn items(&self) -> Vec<(ArcStr, PyTemporalPropListList)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    fn latest(&self) -> HashMap<ArcStr, PyPropValueListList> {
        let builder = self.builder.clone();
        self.keys()
            .into_iter()
            .map(move |k| {
                let builder = builder.clone();
                let nk = k.clone();
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

    fn histories(&self) -> HashMap<ArcStr, PyPropHistItemsListList> {
        let builder = self.builder.clone();
        self.keys()
            .into_iter()
            .map(move |k| {
                let builder = builder.clone();
                let kk = k.clone();
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

    fn __getitem__(&self, key: ArcStr) -> PyResult<PyTemporalPropListList> {
        self.get(key).ok_or(PyKeyError::new_err("unknown property"))
    }

    fn __contains__(&self, key: &str) -> bool {
        self.iter().any(|mut it| it.any(|p| p.contains(key)))
    }

    fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    fn get(&self, key: ArcStr) -> Option<PyTemporalPropListList> {
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
    #[getter]
    pub fn history(&self) -> NestedI64VecIterable {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.map(|p| p.map(|v| v.history()).unwrap_or_default()))).into()
    }

    pub fn values(&self) -> PyPropHistValueListList {
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

    pub fn flatten(&self) -> PyTemporalPropList {
        let builder = self.builder.clone();
        (move || builder().flatten()).into()
    }
}

#[pymethods]
impl PyPropHistValueListList {
    pub fn flatten(&self) -> PyPropHistValueList {
        let builder = self.builder.clone();
        (move || builder().flatten()).into()
    }

    pub fn count(&self) -> NestedUsizeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.map(|itit| itit.len()))).into()
    }

    pub fn median(&self) -> PyPropValueListList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                it.map(|itit| {
                    let mut sorted: Vec<Prop> = itit.into_iter().collect();
                    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    let len = sorted.len();
                    match len {
                        0 => None,
                        1 => Some(sorted[0].clone()),
                        _ => {
                            let a = &sorted[len / 2];
                            Some(a.clone())
                        }
                    }
                })
            })
        })
        .into()
    }

    pub fn sum(&self) -> PyPropValueListList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                it.map(|itit| {
                    let mut itit_iter = itit.into_iter();
                    let first = itit_iter.next();
                    itit_iter.clone().fold(first, |acc, elem| match acc {
                        Some(a) => a.add(elem),
                        _ => None,
                    })
                })
            })
        })
        .into()
    }

    pub fn mean(&self) -> PyPropValueListList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                it.map(|itit| {
                    let mut itit_iter = itit.into_iter();
                    let first = itit_iter.next();
                    let sum = itit_iter.clone().fold(first, |acc, elem| match acc {
                        Some(a) => a.add(elem),
                        _ => Some(elem),
                    });
                    let count = itit_iter.count();
                    if count == 0 {
                        return None;
                    }
                    match sum {
                        Some(Prop::U8(s)) => Some(Prop::U8(s / count as u8)),
                        Some(Prop::U16(s)) => Some(Prop::U16(s / count as u16)),
                        Some(Prop::I32(s)) => Some(Prop::I32(s / count as i32)),
                        Some(Prop::I64(s)) => Some(Prop::I64(s / count as i64)),
                        Some(Prop::U32(s)) => Some(Prop::U32(s / count as u32)),
                        Some(Prop::U64(s)) => Some(Prop::U64(s / count as u64)),
                        Some(Prop::F32(s)) => Some(Prop::F32(s / count as f32)),
                        Some(Prop::F64(s)) => Some(Prop::F64(s / count as f64)),
                        _ => None,
                    }
                })
            })
        })
        .into()
    }
}

#[pymethods]
impl PropIterable {
    pub fn sum(&self) -> PropValue {
        let mut it_iter = self.iter();
        let first = it_iter.next();
        it_iter.fold(first, |acc, elem| acc.and_then(|val| val.add(elem)))
    }

    pub fn median(&self) -> PropValue {
        let mut sorted: Vec<Prop> = self.iter().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let len = sorted.len();
        match len {
            0 => None,
            1 => Some(sorted[0].clone()),
            _ => {
                let a = &sorted[len / 2];
                Some(a.clone())
            }
        }
    }

    pub fn count(&self) -> usize {
        self.iter().count()
    }

    pub fn min(&self) -> PropValue {
        let mut sorted: Vec<Prop> = self.iter().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let len = sorted.len();
        match len {
            0 => None,
            _ => {
                let a = &sorted[0];
                Some(a.clone())
            }
        }
    }

    pub fn max(&self) -> PropValue {
        let mut sorted: Vec<Prop> = self.iter().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let len = sorted.len();
        match len {
            0 => None,
            _ => {
                let a = &sorted[len - 1];
                Some(a.clone())
            }
        }
    }

    pub fn average(&self) -> PropValue {
        self.mean()
    }

    pub fn mean(&self) -> PropValue {
        let sum: PropValue = self.sum();
        let count: usize = self.iter().collect::<Vec<Prop>>().len();
        if count == 0 {
            return None;
        }
        match sum {
            Some(Prop::U8(s)) => Some(Prop::F64(s as f64 / count as f64)),
            Some(Prop::U16(s)) => Some(Prop::F64(s as f64 / count as f64)),
            Some(Prop::I32(s)) => Some(Prop::F32(s as f32 / count as f32)),
            Some(Prop::I64(s)) => Some(Prop::F64(s as f64 / count as f64)),
            Some(Prop::U32(s)) => Some(Prop::F32(s as f32 / count as f32)),
            Some(Prop::U64(s)) => Some(Prop::F64(s as f64 / count as f64)),
            Some(Prop::F32(s)) => Some(Prop::F32(s / count as f32)),
            Some(Prop::F64(s)) => Some(Prop::F64(s / count as f64)),
            _ => None,
        }
    }
}

#[pymethods]
impl PyPropHistValueList {
    pub fn sum(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                let mut it_iter = it.into_iter();
                let first = it_iter.next();
                it_iter.fold(first, |acc, elem| acc.and_then(|val| val.add(elem)))
            })
        })
        .into()
    }

    pub fn min(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                let mut it_iter = it.into_iter();
                let first = it_iter.next();
                it_iter.fold(first, |a, b| {
                    match PartialOrd::partial_cmp(&a, &Some(b.clone())) {
                        Some(std::cmp::Ordering::Less) => a,
                        _ => Some(b),
                    }
                })
            })
        })
        .into()
    }

    pub fn max(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                let mut it_iter = it.into_iter();
                let first = it_iter.next();
                it_iter.fold(first, |a, b| {
                    match PartialOrd::partial_cmp(&a, &Some(b.clone())) {
                        Some(std::cmp::Ordering::Greater) => a,
                        _ => Some(b),
                    }
                })
            })
        })
        .into()
    }

    pub fn median(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                let mut sorted: Vec<Prop> = it.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let len = sorted.len();
                match len {
                    0 => None,
                    1 => Some(sorted[0].clone()),
                    _ => {
                        let a = &sorted[len / 2];
                        Some(a.clone())
                    }
                }
            })
        })
        .into()
    }

    pub fn average(&self) -> PyPropValueList {
        self.mean()
    }

    pub fn mean(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                let mut it_iter = it.clone().into_iter();
                let first = it_iter.next();
                let sum = it_iter.fold(first, |acc, elem| acc.and_then(|val| val.add(elem)));
                let count = it.len();
                if count == 0 {
                    return None;
                }
                match sum {
                    Some(Prop::U8(s)) => Some(Prop::F64(s as f64 / count as f64)),
                    Some(Prop::U16(s)) => Some(Prop::F64(s as f64 / count as f64)),
                    Some(Prop::I32(s)) => Some(Prop::F32(s as f32 / count as f32)),
                    Some(Prop::I64(s)) => Some(Prop::F64(s as f64 / count as f64)),
                    Some(Prop::U32(s)) => Some(Prop::F32(s as f32 / count as f32)),
                    Some(Prop::U64(s)) => Some(Prop::F64(s as f64 / count as f64)),
                    Some(Prop::F32(s)) => Some(Prop::F32(s / count as f32)),
                    Some(Prop::F64(s)) => Some(Prop::F64(s / count as f64)),
                    _ => None,
                }
            })
        })
        .into()
    }

    pub fn count(&self) -> UsizeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.len())).into()
    }

    pub fn flatten(&self) -> PropIterable {
        let builder = self.builder.clone();
        (move || builder().flatten()).into()
    }
}

#[pymethods]
impl PyPropValueList {
    pub fn sum(&self) -> Option<Prop> {
        self.iter()
            .reduce(|acc, elem| match (acc, elem) {
                (Some(a), Some(b)) => a.add(b),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                _ => None,
            })
            .flatten()
    }

    pub fn count(&self) -> usize {
        self.iter().count()
    }

    pub fn min(&self) -> PropValue {
        let mut sorted: Vec<PropValue> = self.iter().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let len = sorted.len();
        match len {
            0 => None,
            _ => {
                let a = &sorted[0];
                a.clone()
            }
        }
    }

    pub fn max(&self) -> PropValue {
        let mut sorted: Vec<PropValue> = self.iter().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let len = sorted.len();
        match len {
            0 => None,
            _ => {
                let a = &sorted[len - 1];
                a.clone()
            }
        }
    }

    pub fn drop_none(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || builder().filter(|x| x.is_some())).into()
    }

    pub fn median(&self) -> PropValue {
        let mut sorted: Vec<PropValue> = self.iter().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let len = sorted.len();
        match len {
            0 => None,
            1 => sorted[0].clone(),
            _ => {
                let a = &sorted[len / 2];
                a.clone()
            }
        }
    }

    pub fn mean(&self) -> PropValue {
        let sum: PropValue = self.sum();
        let count: usize = self.iter().collect::<Vec<PropValue>>().len();
        if count == 0 {
            return None;
        }
        match sum {
            Some(Prop::U8(s)) => Some(Prop::F64(s as f64 / count as f64)),
            Some(Prop::U16(s)) => Some(Prop::F64(s as f64 / count as f64)),
            Some(Prop::I32(s)) => Some(Prop::F32(s as f32 / count as f32)),
            Some(Prop::I64(s)) => Some(Prop::F64(s as f64 / count as f64)),
            Some(Prop::U32(s)) => Some(Prop::F32(s as f32 / count as f32)),
            Some(Prop::U64(s)) => Some(Prop::F64(s as f64 / count as f64)),
            Some(Prop::F32(s)) => Some(Prop::F32(s / count as f32)),
            Some(Prop::F64(s)) => Some(Prop::F64(s / count as f64)),
            _ => None,
        }
    }

    pub fn average(&self) -> PropValue {
        self.mean()
    }
}

#[pymethods]
impl PyPropValueListList {
    pub fn sum(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                let mut it_iter = it.into_iter();
                let first = it_iter.next().flatten();
                it_iter.fold(first, |acc, elem| match (acc, elem) {
                    (Some(a), Some(b)) => a.add(b),
                    (Some(a), None) => Some(a),
                    (None, Some(b)) => Some(b),
                    _ => None,
                })
            })
        })
        .into()
    }

    pub fn min(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                let mut it_iter = it.into_iter();
                let first = it_iter.next().unwrap();
                it_iter.fold(first, |a, b| {
                    match PartialOrd::partial_cmp(&a, &Some(b.clone().unwrap())) {
                        Some(std::cmp::Ordering::Less) => a,
                        _ => Some(b.clone().unwrap()),
                    }
                })
            })
        })
        .into()
    }

    pub fn max(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                let mut it_iter = it.into_iter();
                let first = it_iter.next().unwrap();
                it_iter.fold(first, |a, b| {
                    match PartialOrd::partial_cmp(&a, &Some(b.clone().unwrap())) {
                        Some(std::cmp::Ordering::Greater) => a,
                        _ => Some(b.clone().unwrap()),
                    }
                })
            })
        })
        .into()
    }

    pub fn average(&self) -> PyPropValueList {
        self.mean()
    }

    pub fn mean(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|mut it| {
                let mut count: usize = 1;
                let first = it.next().flatten();
                let sum = it.fold(first, |acc, elem| {
                    count += 1;
                    match (acc, elem) {
                        (Some(a), Some(b)) => a.add(b),
                        (Some(a), None) => Some(a),
                        (None, Some(b)) => Some(b),
                        _ => None,
                    }
                });
                if count == 0 {
                    return None;
                }
                match sum {
                    Some(Prop::U8(s)) => Some(Prop::F64(s as f64 / count as f64)),
                    Some(Prop::U16(s)) => Some(Prop::F64(s as f64 / count as f64)),
                    Some(Prop::I32(s)) => Some(Prop::F32(s as f32 / count as f32)),
                    Some(Prop::I64(s)) => Some(Prop::F64(s as f64 / count as f64)),
                    Some(Prop::U32(s)) => Some(Prop::F32(s as f32 / count as f32)),
                    Some(Prop::U64(s)) => Some(Prop::F64(s as f64 / count as f64)),
                    Some(Prop::F32(s)) => Some(Prop::F32(s / count as f32)),
                    Some(Prop::F64(s)) => Some(Prop::F64(s / count as f64)),
                    _ => None,
                }
            })
        })
        .into()
    }

    pub fn median(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                let mut sorted: Vec<PropValue> = it.into_iter().collect();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let len = sorted.len();
                match len {
                    0 => None,
                    1 => sorted[0].clone(),
                    _ => {
                        let a = &sorted[len / 2];
                        a.clone()
                    }
                }
            })
        })
        .into()
    }

    pub fn flatten(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || builder().flatten()).into()
    }

    pub fn count(&self) -> UsizeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.count())).into()
    }

    pub fn drop_none(&self) -> PyPropValueListList {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.filter(|x| x.is_some()))).into()
    }
}

py_iterable!(PyPropHistValueList, Vec<Prop>);
py_iterable_comp!(PyPropHistValueList, Vec<Prop>, PyPropHistValueListCmp);
py_nested_iterable!(PyPropHistValueListList, Vec<Prop>);
py_iterable_comp!(
    PyPropHistValueListList,
    PyPropHistValueListCmp,
    PropHistValueListListCmp
);

py_iterable!(PyPropHistItemsList, PropHistItems);
py_iterable_comp!(PyPropHistItemsList, PropHistItems, PyPropHistItemsListCmp);
py_nested_iterable!(PyPropHistItemsListList, PropHistItems);
py_iterable_comp!(
    PyPropHistItemsListList,
    PyPropHistItemsListCmp,
    PyPropHistItemsListListCmp
);
