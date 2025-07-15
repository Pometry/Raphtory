use crate::{
    db::api::{
        properties::{
            dyn_props::{DynTemporalProperties, DynTemporalProperty},
            internal::PropertiesOps,
            TemporalProperties, TemporalPropertyView,
        },
        view::internal::{DynamicGraph, Static},
    },
    python::{
        graph::{
            history::PyHistory,
            properties::{PyPropValueList, PyPropValueListList},
        },
        types::{
            repr::{iterator_dict_repr, iterator_repr, Repr},
            wrappers::{
                iterables::{
                    I64VecIterable, NestedI64VecIterable, NestedUsizeIterable, PropIterable,
                    UsizeIterable,
                },
                iterators::PyBorrowingIterator,
                prop::{PropHistItems, PropValue},
            },
        },
        utils::{NumpyArray, PyGenericIterator, PyTime},
    },
};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use pyo3::{
    exceptions::{PyKeyError, PyTypeError},
    prelude::*,
};
use raphtory_api::core::{
    entities::properties::prop::{Prop, PropUnwrap},
    storage::{
        arc_str::ArcStr,
        timeindex::{AsTime, TimeError, TimeIndexEntry},
    },
};
use raphtory_core::utils::time::IntoTime;
use std::{collections::HashMap, ops::Deref, sync::Arc};

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
                .props
                .iter()
                .map(|(k, v)| (k.clone(), v.into()))
                .collect(),
        )
    }
}

impl<'source> FromPyObject<'source> for PyTemporalPropsCmp {
    fn extract_bound(ob: &Bound<'source, PyAny>) -> PyResult<Self> {
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
#[pyclass(name = "TemporalProperties", module = "raphtory", frozen)]
pub struct PyTemporalProperties {
    props: DynTemporalProperties,
}

py_eq!(PyTemporalProperties, PyTemporalPropsCmp);

#[pymethods]
impl PyTemporalProperties {
    /// List the available property keys
    fn keys(&self) -> Vec<ArcStr> {
        self.props.keys().collect()
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
#[pyclass(name = "TemporalProperty", module = "raphtory", frozen)]
pub struct PyTemporalProp {
    pub prop: DynTemporalProperty,
}

#[derive(Clone, PartialEq)]
pub struct PyTemporalPropCmp(Vec<(i64, Prop)>);

impl<'source> FromPyObject<'source> for PyTemporalPropCmp {
    fn extract_bound(ob: &Bound<'source, PyAny>) -> PyResult<Self> {
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
        Self(value.prop.iter().map(|(t, p)| (t.t(), p)).collect())
    }
}

impl From<Vec<(i64, Prop)>> for PyTemporalPropCmp {
    fn from(value: Vec<(i64, Prop)>) -> Self {
        Self(value)
    }
}

impl From<Vec<(TimeIndexEntry, Prop)>> for PyTemporalPropCmp {
    fn from(value: Vec<(TimeIndexEntry, Prop)>) -> Self {
        Self(value.into_iter().map(|(t, p)| (t.t(), p)).collect())
    }
}

impl From<DynTemporalProperty> for PyTemporalPropCmp {
    fn from(value: DynTemporalProperty) -> Self {
        PyTemporalPropCmp(value.iter().map(|(t, p)| (t.0, p)).collect())
    }
}

py_eq!(PyTemporalProp, PyTemporalPropCmp);

#[pymethods]
impl PyTemporalProp {
    /// Get a history object which contains time entries for when the property was updated
    pub fn history(&self) -> PyHistory {
        self.prop.history().to_owned().into()
    }

    /// Get the property values for each update
    pub fn values(&self) -> NumpyArray {
        self.prop.values().collect()
    }

    /// List update timestamps and corresponding property values
    pub fn items(&self) -> Vec<(TimeIndexEntry, Prop)> {
        self.prop.iter().collect()
    }

    // List of unique property values
    pub fn unique(&self) -> Vec<Prop> {
        self.prop.unique()
    }

    // List of ordered deduplicated property values
    pub fn ordered_dedupe(&self, latest_time: bool) -> Vec<(TimeIndexEntry, Prop)> {
        self.prop.ordered_dedupe(latest_time)
    }

    /// List update datetimes and corresponding property values
    pub fn items_date_time(&self) -> Result<Vec<(DateTime<Utc>, Prop)>, TimeError> {
        self.prop
            .iter()
            .map(|(t, p)| t.dt().map(|dt| (dt, p)))
            .collect::<Result<Vec<_>, TimeError>>()
    }

    /// Iterate over `items`
    pub fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(self.prop.clone(), DynTemporalProperty, |inner| inner.iter())
    }
    /// Get the value of the property at time `t`
    pub fn at(&self, t: PyTime) -> Option<Prop> {
        self.prop.at(t.into_time().t())
    }
    /// Get the latest value of the property
    pub fn value(&self) -> Option<Prop> {
        self.prop.latest()
    }

    /// Compute the sum of all property values.
    ///
    /// Returns:
    ///     Prop: The sum of all property values.
    pub fn sum(&self) -> Option<Prop> {
        compute_generalised_sum(self.prop.values(), |a, b| a.add(b), |d| d.dtype().has_add())
    }

    /// Find the minimum property value and its associated time.
    ///
    /// Returns:
    ///     (TimeIndexEntry, Prop): A tuple containing the time and the minimum property value.
    pub fn min(&self) -> Option<(TimeIndexEntry, Prop)> {
        compute_generalised_sum(
            self.prop.iter(),
            |a, b| {
                if a.1.partial_cmp(&b.1)?.is_le() {
                    Some(a)
                } else {
                    Some(b)
                }
            },
            |d| d.1.dtype().has_cmp(),
        )
    }

    /// Find the maximum property value and its associated time.
    ///
    /// Returns:
    ///     (TimeIndexEntry, Prop): A tuple containing the time and the maximum property value.
    pub fn max(&self) -> Option<(TimeIndexEntry, Prop)> {
        compute_generalised_sum(
            self.prop.iter(),
            |a, b| {
                if a.1.partial_cmp(&b.1)?.is_ge() {
                    Some(a)
                } else {
                    Some(b)
                }
            },
            |d| d.1.dtype().has_cmp(),
        )
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
        compute_mean(self.prop.values())
    }

    /// Compute the median of all property values.
    ///
    /// Returns:
    ///     (TimeIndexEntry, Prop): A tuple containing the time and the median property value, or None if empty
    pub fn median(&self) -> Option<(TimeIndexEntry, Prop)> {
        let mut sorted: Vec<(TimeIndexEntry, Prop)> = self.prop.iter().collect();
        if !sorted.first()?.1.dtype().has_cmp() {
            return None;
        }
        sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        let len = sorted.len();
        if len == 0 {
            None
        } else {
            Some(sorted[(len - 1) / 2].clone())
        }
    }

    pub fn __repr__(&self) -> String {
        self.prop.repr()
    }
}

impl<P: PropertiesOps + Send + Sync + Clone + 'static> From<TemporalPropertyView<P>>
    for PyTemporalProp
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

impl<'py, P: PropertiesOps + Clone + Send + Sync + 'static + Static> IntoPyObject<'py>
    for TemporalProperties<P>
{
    type Target = PyTemporalProperties;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyTemporalProperties::from(self).into_pyobject(py)
    }
}

impl<'py> IntoPyObject<'py> for TemporalProperties<DynamicGraph> {
    type Target = PyTemporalProperties;
    type Output = <Self::Target as IntoPyObject<'py>>::Output;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyTemporalProperties::from(self).into_pyobject(py)
    }
}

impl<'py> IntoPyObject<'py> for DynTemporalProperties {
    type Target = PyTemporalProperties;
    type Output = <Self::Target as IntoPyObject<'py>>::Output;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyTemporalProperties::from(self).into_pyobject(py)
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

impl<P: PropertiesOps + Clone> Repr for TemporalPropertyView<P> {
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

impl<'py, P: PropertiesOps + Send + Sync + Clone + 'static> IntoPyObject<'py>
    for TemporalPropertyView<P>
{
    type Target = PyTemporalProp;
    type Output = <Self::Target as IntoPyObject<'py>>::Output;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyTemporalProp::from(self).into_pyobject(py)
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
    fn extract_bound(ob: &Bound<'source, PyAny>) -> PyResult<Self> {
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
            .map(|p| p.keys().collect_vec())
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

impl<'py> IntoPyObject<'py> for OptionPyTemporalProp {
    type Target = <Option<PyTemporalProp> as IntoPyObject<'py>>::Target;
    type Output = <Option<PyTemporalProp> as IntoPyObject<'py>>::Output;
    type Error = <Option<PyTemporalProp> as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.0.into_pyobject(py)
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
        (move || builder().map(|p| p.map(|v| v.history().t().collect()).unwrap_or_default())).into()
    }

    pub fn values(&self) -> PyPropHistValueList {
        let builder = self.builder.clone();
        (move || builder().map(|p| p.map(|v| v.values().collect_vec()).unwrap_or_default())).into()
    }

    pub fn items(&self) -> PyPropHistItemsList {
        let builder = self.builder.clone();
        (move || builder().map(|p| p.map(|v| v.iter().collect_vec()).unwrap_or_default())).into()
    }

    pub fn at(&self, t: PyTime) -> PyPropValueList {
        let t = t.into_time().t();
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
    fn extract_bound(ob: &Bound<'source, PyAny>) -> PyResult<Self> {
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
            it.map(|p| p.keys().collect_vec()),
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
        (move || {
            builder().map(|it| it.map(|p| p.map(|v| v.history().t().collect()).unwrap_or_default()))
        })
        .into()
    }

    pub fn values(&self) -> PyPropHistValueListList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| it.map(|p| p.map(|v| v.values().collect_vec()).unwrap_or_default()))
        })
        .into()
    }

    pub fn items(&self) -> PyPropHistItemsListList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| it.map(|p| p.map(|v| v.iter().collect_vec()).unwrap_or_default()))
        })
        .into()
    }

    pub fn at(&self, t: PyTime) -> PyPropValueListList {
        let t = t.into_time().t();
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
        (move || builder().map(|it| it.map(compute_median))).into()
    }

    pub fn max(&self) -> PyPropValueListList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                it.map(|data| {
                    compute_generalised_sum(data, |a, b| a.max(b), |d| d.dtype().has_cmp())
                })
            })
        })
        .into()
    }

    pub fn min(&self) -> PyPropValueListList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                it.map(|data| {
                    compute_generalised_sum(data, |a, b| a.min(b), |d| d.dtype().has_cmp())
                })
            })
        })
        .into()
    }

    pub fn sum(&self) -> PyPropValueListList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                it.map(|data| {
                    compute_generalised_sum(data, |a, b| a.add(b), |d| d.dtype().has_add())
                })
            })
        })
        .into()
    }

    pub fn mean(&self) -> PyPropValueListList {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.map(compute_mean))).into()
    }
}

#[pymethods]
impl PropIterable {
    pub fn sum(&self) -> PropValue {
        compute_generalised_sum(self.iter(), |a, b| a.add(b), |d| d.dtype().has_add())
    }

    pub fn median(&self) -> PropValue {
        compute_median(self.iter().collect())
    }

    pub fn count(&self) -> usize {
        self.iter().count()
    }

    pub fn min(&self) -> PropValue {
        compute_generalised_sum(self.iter(), |a, b| a.min(b), |d| d.dtype().has_cmp())
    }

    pub fn max(&self) -> PropValue {
        compute_generalised_sum(self.iter(), |a, b| a.max(b), |d| d.dtype().has_cmp())
    }

    pub fn average(&self) -> PropValue {
        self.mean()
    }

    pub fn mean(&self) -> PropValue {
        compute_mean(self.iter())
    }
}

#[pymethods]
impl PyPropHistValueList {
    pub fn sum(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder()
                .map(|data| compute_generalised_sum(data, |a, b| a.add(b), |d| d.dtype().has_add()))
        })
        .into()
    }

    pub fn min(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder()
                .map(|data| compute_generalised_sum(data, |a, b| a.min(b), |d| d.dtype().has_cmp()))
        })
        .into()
    }

    pub fn max(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder()
                .map(|data| compute_generalised_sum(data, |a, b| a.max(b), |d| d.dtype().has_cmp()))
        })
        .into()
    }

    pub fn median(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || builder().map(compute_median)).into()
    }

    pub fn average(&self) -> PyPropValueList {
        self.mean()
    }

    pub fn mean(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || builder().map(compute_mean)).into()
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
        compute_generalised_sum(
            self.iter().flatten(),
            |a, b| a.add(b),
            |d| d.dtype().has_add(),
        )
    }

    pub fn count(&self) -> usize {
        self.iter().count()
    }

    pub fn min(&self) -> PropValue {
        compute_generalised_sum(
            self.iter().flatten(),
            |a, b| a.min(b),
            |d| d.dtype().has_cmp(),
        )
    }

    pub fn max(&self) -> PropValue {
        compute_generalised_sum(
            self.iter().flatten(),
            |a, b| a.max(b),
            |d| d.dtype().has_cmp(),
        )
    }

    pub fn drop_none(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || builder().flatten()).into()
    }

    pub fn median(&self) -> PropValue {
        compute_median(self.iter().flatten().collect())
    }

    pub fn mean(&self) -> PropValue {
        compute_mean(self.iter().flatten())
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
                compute_generalised_sum(it.flatten(), |a, b| a.add(b), |d| d.dtype().has_add())
            })
        })
        .into()
    }

    pub fn min(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                compute_generalised_sum(it.flatten(), |a, b| a.min(b), |d| d.dtype().has_cmp())
            })
        })
        .into()
    }

    pub fn max(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                compute_generalised_sum(it.flatten(), |a, b| a.max(b), |d| d.dtype().has_cmp())
            })
        })
        .into()
    }

    pub fn average(&self) -> PyPropValueList {
        self.mean()
    }

    pub fn mean(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || builder().map(|it| compute_mean(it.flatten()))).into()
    }

    pub fn median(&self) -> PyPropValueList {
        let builder = self.builder.clone();

        (move || builder().map(|it| compute_median(it.flatten().collect()))).into()
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

fn compute_median(mut data: Vec<Prop>) -> Option<Prop> {
    if data.is_empty() || !data[0].dtype().has_cmp() {
        return None;
    }
    data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Some(data[(data.len() - 1) / 2].clone())
}

fn compute_mean(data: impl IntoIterator<Item = Prop>) -> Option<Prop> {
    let mut iter = data.into_iter();
    let first_value = iter.next()?;
    let mut sum = first_value.as_f64()?;
    let mut count = 1usize;
    for value in iter {
        sum += value.as_f64()?;
        count += 1;
    }
    Some(Prop::F64(sum / count as f64))
}

fn compute_generalised_sum<V>(
    data: impl IntoIterator<Item = V>,
    op: impl Fn(V, V) -> Option<V>,
    check: impl Fn(&V) -> bool,
) -> Option<V> {
    let mut iter = data.into_iter();
    let first_value = iter.next()?;
    if !check(&first_value) {
        return None;
    }
    iter.try_fold(first_value, op)
}
