use crate::{
    db::api::{
        properties::{
            dyn_props::{DynTemporalProperties, DynTemporalProperty},
            internal::InternalPropertiesOps,
            TemporalProperties, TemporalPropertyView,
        },
        view::{
            history::History,
            internal::{DynamicGraph, Static},
        },
    },
    python::{
        graph::{
            history::{HistoryIterable, NestedHistoryIterable, PyHistory},
            properties::{PyPropValueList, PyPropValueListList},
        },
        types::{
            repr::{iterator_dict_repr, iterator_repr, Repr},
            wrappers::{
                iterables::{NestedUsizeIterable, PropIterable, UsizeIterable},
                iterators::PyBorrowingIterator,
                prop::{PropHistItems, PropValue},
            },
        },
        utils::{NumpyArray, PyGenericIterator},
    },
};
use itertools::Itertools;
use pyo3::{
    exceptions::{PyKeyError, PyTypeError},
    prelude::*,
    Borrowed,
};
use raphtory_api::core::{
    entities::properties::prop::{Prop, PropUnwrap},
    storage::{
        arc_str::ArcStr,
        timeindex::{AsTime, EventTime},
    },
};
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
                .iter_filtered()
                .map(|(k, v)| (k.clone(), v.into()))
                .collect(),
        )
    }
}

impl<'py> FromPyObject<'_, 'py> for PyTemporalPropsCmp {
    type Error = PyErr;
    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
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
    /// List the available property keys.
    ///
    /// Returns:
    ///     list[str]:
    fn keys(&self) -> Vec<ArcStr> {
        self.props.iter_filtered().map(|(key, _)| key).collect()
    }

    /// List the values of the properties
    ///
    /// Returns:
    ///     list[TemporalProperty]: the list of property views
    fn values(&self) -> Vec<DynTemporalProperty> {
        self.props.iter_filtered().map(|(_, value)| value).collect()
    }

    /// List the property keys together with the corresponding values
    ///
    /// Returns:
    ///     List[Tuple[str, TemporalProperty]]:
    fn items(&self) -> Vec<(ArcStr, DynTemporalProperty)> {
        self.props.iter_filtered().collect()
    }

    /// Get the latest value of all properties
    ///
    /// Returns:
    ///     dict[str, PropValue]: the mapping of property keys to latest values
    fn latest(&self) -> HashMap<ArcStr, Prop> {
        self.props.iter_latest().collect()
    }

    /// Get the histories of all properties
    ///
    /// Returns:
    ///     dict[str, list[Tuple[EventTime, PropValue]]]: the mapping of property keys to histories
    fn histories(&self) -> HashMap<ArcStr, Vec<(EventTime, Prop)>> {
        self.props
            .iter()
            .map(|(k, v)| (k.clone(), v.iter().collect()))
            .collect()
    }

    /// Get property value for `key`
    ///
    /// Returns:
    ///     TemporalProperty: the property view
    ///
    /// Raises:
    ///     KeyError: if property `key` does not exist
    fn __getitem__(&self, key: &str) -> PyResult<DynTemporalProperty> {
        self.get(key).ok_or(PyKeyError::new_err("No such property"))
    }

    /// Get property value for `key` if it exists.
    ///
    /// Arguments:
    ///     key (str): the name of the property.
    ///
    /// Returns:
    ///     TemporalProperty: the property view if it exists, otherwise `None`
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
        self.props.get(key).filter(|v| !v.is_empty()).is_some()
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

impl<'py> FromPyObject<'_, 'py> for PyTemporalPropCmp {
    type Error = PyErr;
    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
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

impl From<Vec<(EventTime, Prop)>> for PyTemporalPropCmp {
    fn from(value: Vec<(EventTime, Prop)>) -> Self {
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
    /// Returns a history object which contains time entries for when the property was updated.
    ///
    /// Returns:
    ///     History:
    #[getter]
    pub fn history(&self) -> PyHistory {
        self.prop.history().into()
    }

    /// Get the property values for each update.
    ///
    /// Returns:
    ///     NDArray: a numpy array of values, one per update.
    pub fn values(&self) -> NumpyArray {
        self.prop.values().collect()
    }

    /// List update times and corresponding property values.
    ///
    /// Returns:
    ///     List[Tuple[EventTime, PropValue]]:
    pub fn items(&self) -> Vec<(EventTime, Prop)> {
        self.prop.iter().collect()
    }

    /// List of unique property values.
    ///
    /// Returns:
    ///     List[PropValue]:
    pub fn unique(&self) -> Vec<Prop> {
        self.prop.unique()
    }

    /// List of ordered deduplicated property values.
    ///
    /// Arguments:
    ///     latest_time (bool): Enable to check the latest time only.
    ///
    /// Returns:
    ///     List[Tuple[EventTime, PropValue]]:
    pub fn ordered_dedupe(&self, latest_time: bool) -> Vec<(EventTime, Prop)> {
        self.prop.ordered_dedupe(latest_time)
    }

    /// Iterate over items.
    ///
    /// Returns:
    ///     PyBorrowingIterator:
    pub fn __iter__(&self) -> PyBorrowingIterator {
        py_borrowing_iter!(self.prop.clone(), DynTemporalProperty, |inner| inner.iter())
    }
    /// Get the value of the property at a specified time.
    ///
    /// Arguments:
    ///     t (TimeInput): time
    ///
    /// Returns:
    ///     Optional[PropValue]:
    pub fn at(&self, t: EventTime) -> Option<Prop> {
        self.prop.at(t)
    }
    /// Get the latest value of the property.
    ///
    /// Returns:
    ///     Optional[PropValue]:
    pub fn value(&self) -> Option<Prop> {
        self.prop.latest()
    }

    /// Compute the sum of all property values.
    ///
    /// Returns:
    ///     PropValue: The sum of all property values.
    pub fn sum(&self) -> Option<Prop> {
        self.prop.sum()
    }

    /// Find the minimum property value and its associated time.
    ///
    /// Returns:
    ///     Tuple[EventTime, PropValue]: A tuple containing the time and the minimum property value.
    pub fn min(&self) -> Option<(EventTime, Prop)> {
        self.prop.min()
    }

    /// Find the maximum property value and its associated time.
    ///
    /// Returns:
    ///     Tuple[EventTime, PropValue]: A tuple containing the time and the maximum property value.
    pub fn max(&self) -> Option<(EventTime, Prop)> {
        self.prop.max()
    }

    /// Count the number of properties.
    ///
    /// Returns:
    ///     int: The number of properties.
    pub fn count(&self) -> usize {
        self.prop.count()
    }

    /// Compute the average of all property values. Alias for mean().
    ///
    /// Returns:
    ///     PropValue: The average of each property values, or None if count is zero.
    pub fn average(&self) -> Option<Prop> {
        self.prop.average()
    }

    /// Compute the mean of all property values. Alias for mean().
    ///
    /// Returns:
    ///     PropValue: The mean of each property values, or None if count is zero.
    pub fn mean(&self) -> Option<Prop> {
        self.prop.mean()
    }

    /// Compute the median of all property values.
    ///
    /// Returns:
    ///     Tuple[EventTime, PropValue]: A tuple containing the time and the median property value, or None if empty
    pub fn median(&self) -> Option<(EventTime, Prop)> {
        self.prop.median()
    }

    pub fn __repr__(&self) -> String {
        self.prop.repr()
    }
}

impl<P: InternalPropertiesOps + Send + Sync + Clone + 'static> From<TemporalPropertyView<P>>
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

impl<'py, P: InternalPropertiesOps + Clone + Send + Sync + 'static + Static> IntoPyObject<'py>
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

impl<P: InternalPropertiesOps + Clone> Repr for TemporalProperties<P> {
    fn repr(&self) -> String {
        format!(
            "TemporalProperties({{{}}})",
            iterator_dict_repr(self.iter_filtered())
        )
    }
}

impl<P: InternalPropertiesOps + Clone> Repr for TemporalPropertyView<P> {
    fn repr(&self) -> String {
        format!("TemporalProperty({})", iterator_repr(self.iter()))
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

impl<'py, P: InternalPropertiesOps + Send + Sync + Clone + 'static> IntoPyObject<'py>
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

impl<'py> FromPyObject<'_, 'py> for PyTemporalPropsListCmp {
    type Error = PyErr;
    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
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
    /// Property keys present across the underlying entities.
    ///
    /// Returns:
    ///     list[str]:
    fn keys(&self) -> Vec<ArcStr> {
        self.iter()
            .next()
            .map(|p| p.keys().collect())
            .unwrap_or_default()
    }

    /// Per-key list of temporal property views.
    ///
    /// Returns:
    ///     list[PyTemporalPropList]:
    fn values(&self) -> Vec<PyTemporalPropList> {
        self.keys()
            .into_iter()
            .map(|k| self.get(k).expect("key exists"))
            .collect()
    }

    /// Pairs of `(key, temporal property list)` for every property key.
    ///
    /// Returns:
    ///     list[tuple[str, PyTemporalPropList]]:
    fn items(&self) -> Vec<(ArcStr, PyTemporalPropList)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    /// Latest value of each property across the underlying entities.
    ///
    /// Returns:
    ///     dict[str, PyPropValueList]:
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

    /// Full update history of each property across the underlying entities.
    ///
    /// Returns:
    ///     dict[str, PyPropHistItemsList]:
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
        self.iter().next().is_some_and(|p| p.contains(key))
    }

    fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    /// Look up a temporal property by key.
    ///
    /// Arguments:
    ///     key (str): property key.
    ///
    /// Returns:
    ///     Optional[PyTemporalPropList]:
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
    /// Update history (one history per underlying entity).
    ///
    /// Returns:
    ///     HistoryIterable:
    #[getter]
    pub fn history(&self) -> HistoryIterable {
        let builder = self.builder.clone();
        (move || {
            builder().map(|p| {
                p.map(|v| v.history().into_arc_dyn())
                    .unwrap_or(History::create_empty().into_arc_dyn())
            })
        })
        .into()
    }

    /// Per-entity list of property values across each entity's history.
    ///
    /// Returns:
    ///     PyPropHistValueList:
    pub fn values(&self) -> PyPropHistValueList {
        let builder = self.builder.clone();
        (move || builder().map(|p| p.map(|v| v.values().collect_vec()).unwrap_or_default())).into()
    }

    /// Per-entity list of `(time, value)` pairs across each entity's history.
    ///
    /// Returns:
    ///     PyPropHistItemsList:
    pub fn items(&self) -> PyPropHistItemsList {
        let builder = self.builder.clone();
        (move || builder().map(|p| p.map(|v| v.iter().collect_vec()).unwrap_or_default())).into()
    }

    /// Value of each entity's property at the given time (latest update at or before `t`).
    ///
    /// Arguments:
    ///     t (TimeInput): the time at which to evaluate the property.
    ///
    /// Returns:
    ///     PyPropValueList:
    pub fn at(&self, t: EventTime) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || builder().map(move |p| p.and_then(|v| v.at(t)))).into()
    }

    /// Latest value of each entity's property.
    ///
    /// Returns:
    ///     PyPropValueList:
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

impl<'py> FromPyObject<'_, 'py> for PyTemporalPropsListListCmp {
    type Error = PyErr;
    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
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
    /// Property keys present across the underlying entities.
    ///
    /// Returns:
    ///     list[str]:
    fn keys(&self) -> Vec<ArcStr> {
        self.iter()
            .flat_map(|it| it.map(|p| p.keys().collect_vec()))
            .next()
            .unwrap_or_default()
    }

    /// Per-key list of nested temporal property views.
    ///
    /// Returns:
    ///     list[PyTemporalPropListList]:
    fn values(&self) -> Vec<PyTemporalPropListList> {
        self.keys()
            .into_iter()
            .map(|k| self.get(k).expect("key exists"))
            .collect()
    }

    /// Pairs of `(key, nested temporal property list)` for every property key.
    ///
    /// Returns:
    ///     list[tuple[str, PyTemporalPropListList]]:
    fn items(&self) -> Vec<(ArcStr, PyTemporalPropListList)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    /// Latest value of each property across the nested entities.
    ///
    /// Returns:
    ///     dict[str, PyPropValueListList]:
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

    /// Full update history of each property across the nested entities.
    ///
    /// Returns:
    ///     dict[str, PyPropHistItemsListList]:
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
        self.iter()
            .filter_map(|mut it| it.next())
            .next()
            .is_some_and(|p| p.contains(key))
    }

    fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    /// Look up a nested temporal property by key.
    ///
    /// Arguments:
    ///     key (str): property key.
    ///
    /// Returns:
    ///     Optional[PyTemporalPropListList]:
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
    /// Update history (per outer entity, per inner entity).
    ///
    /// Returns:
    ///     NestedHistoryIterable:
    #[getter]
    pub fn history(&self) -> NestedHistoryIterable {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                it.map(|p| {
                    p.map(|v| v.history().into_arc_dyn())
                        .unwrap_or(History::create_empty().into_arc_dyn())
                })
            })
        })
        .into()
    }

    /// Nested list of property values across each inner entity's history.
    ///
    /// Returns:
    ///     PyPropHistValueListList:
    pub fn values(&self) -> PyPropHistValueListList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| it.map(|p| p.map(|v| v.values().collect_vec()).unwrap_or_default()))
        })
        .into()
    }

    /// Nested list of `(time, value)` pairs across each inner entity's history.
    ///
    /// Returns:
    ///     PyPropHistItemsListList:
    pub fn items(&self) -> PyPropHistItemsListList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| it.map(|p| p.map(|v| v.iter().collect_vec()).unwrap_or_default()))
        })
        .into()
    }

    /// Value of each inner entity's property at the given time.
    ///
    /// Arguments:
    ///     t (TimeInput): the time at which to evaluate the property.
    ///
    /// Returns:
    ///     PyPropValueListList:
    pub fn at(&self, t: EventTime) -> PyPropValueListList {
        let builder = self.builder.clone();
        (move || builder().map(move |it| it.map(move |p| p.and_then(|v| v.at(t))))).into()
    }

    /// Latest value of each inner entity's property.
    ///
    /// Returns:
    ///     PyPropValueListList:
    pub fn value(&self) -> PyPropValueListList {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.map(|p| p.and_then(|v| v.latest())))).into()
    }

    /// Flatten the nested temporal property list to a single list of temporal properties.
    ///
    /// Returns:
    ///     PyTemporalPropList:
    pub fn flatten(&self) -> PyTemporalPropList {
        let builder = self.builder.clone();
        (move || builder().flatten()).into()
    }
}

#[pymethods]
impl PyPropHistValueListList {
    /// Flatten the nested history-values list to a single history-values list.
    ///
    /// Returns:
    ///     PyPropHistValueList:
    pub fn flatten(&self) -> PyPropHistValueList {
        let builder = self.builder.clone();
        (move || builder().flatten()).into()
    }

    /// Number of properties (or rows of properties).
    ///
    /// Returns:
    ///     NestedUsizeIterable:
    pub fn count(&self) -> NestedUsizeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.map(|itit| itit.len()))).into()
    }

    /// Median
    ///
    ///  Returns:
    ///     list[list[PropValue]]:
    pub fn median(&self) -> PyPropValueListList {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.map(compute_median))).into()
    }

    /// Find the maximum property value and its associated time.
    ///
    /// Returns:
    ///     list[list[PropValue]]:
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

    /// Min property value.
    ///
    /// Returns:
    ///     list[list[PropValue]]:
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

    /// Sum of property values.
    ///
    /// Returns:
    ///     list[list[PropValue]]:
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

    /// Mean property value across each row.
    ///
    /// Returns:
    ///     PyPropValueListList:
    pub fn mean(&self) -> PyPropValueListList {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.map(compute_mean))).into()
    }
}

#[pymethods]
impl PropIterable {
    /// Sum of property values.
    ///
    /// Returns:
    ///     PropValue:
    pub fn sum(&self) -> PropValue {
        compute_generalised_sum(self.iter(), |a, b| a.add(b), |d| d.dtype().has_add())
    }

    /// Median property values.
    ///
    /// Returns:
    ///     PropValue:
    pub fn median(&self) -> PropValue {
        compute_median(self.iter().collect())
    }

    /// Number of properties (or rows of properties).
    ///
    /// Returns:
    ///     int:
    pub fn count(&self) -> usize {
        self.iter().count()
    }

    /// Min property value.
    ///
    /// Returns:
    ///     PropValue:
    pub fn min(&self) -> PropValue {
        compute_generalised_sum(self.iter(), |a, b| a.min(b), |d| d.dtype().has_cmp())
    }

    /// Find the maximum property value and its associated time.
    ///
    /// Returns:
    ///     PropValue:
    pub fn max(&self) -> PropValue {
        compute_generalised_sum(self.iter(), |a, b| a.max(b), |d| d.dtype().has_cmp())
    }

    /// Compute the average of all property values. Alias for mean().
    ///
    /// Returns:
    ///     PropValue: The average of each property values, or None if count is zero.
    pub fn average(&self) -> PropValue {
        self.mean()
    }

    /// Compute the mean of all property values.
    ///
    /// Returns:
    ///     PropValue: The mean of each property values, or None if count is zero.
    pub fn mean(&self) -> PropValue {
        compute_mean(self.iter())
    }
}

#[pymethods]
impl PyPropHistValueList {
    /// Sum of property values.
    ///
    /// Returns:
    ///     list[PropValue]:
    pub fn sum(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder()
                .map(|data| compute_generalised_sum(data, |a, b| a.add(b), |d| d.dtype().has_add()))
        })
        .into()
    }

    /// Min property value.
    ///
    /// Returns:
    ///     list[PropValue]:
    pub fn min(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder()
                .map(|data| compute_generalised_sum(data, |a, b| a.min(b), |d| d.dtype().has_cmp()))
        })
        .into()
    }

    /// Find the maximum property value and its associated time.
    ///
    /// Returns:
    ///     list[PropValue]:
    pub fn max(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder()
                .map(|data| compute_generalised_sum(data, |a, b| a.max(b), |d| d.dtype().has_cmp()))
        })
        .into()
    }

    /// Median property value of each row.
    ///
    /// Returns:
    ///     PyPropValueList:
    pub fn median(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || builder().map(compute_median)).into()
    }

    /// Compute the average of all property values. Alias for mean().
    ///
    /// Returns:
    ///     list[PropValue]:
    pub fn average(&self) -> PyPropValueList {
        self.mean()
    }

    /// Compute the mean of all property values.
    ///
    /// Returns:
    ///     list[PropValue]: The mean of each property values, or None if count is zero.
    pub fn mean(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || builder().map(compute_mean)).into()
    }

    /// Number of properties (or rows of properties).
    ///
    /// Returns:
    ///     UsizeIterable:
    pub fn count(&self) -> UsizeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.len())).into()
    }

    /// Flatten the per-row history values into a single iterable of values.
    ///
    /// Returns:
    ///     PropIterable:
    pub fn flatten(&self) -> PropIterable {
        let builder = self.builder.clone();
        (move || builder().flatten()).into()
    }
}

#[pymethods]
impl PyPropValueList {
    /// Sum of property values.
    ///
    /// Returns:
    ///     PropValue:
    pub fn sum(&self) -> Option<Prop> {
        compute_generalised_sum(
            self.iter().flatten(),
            |a, b| a.add(b),
            |d| d.dtype().has_add(),
        )
    }

    /// Number of properties (or rows of properties).
    ///
    /// Returns:
    ///     int:
    pub fn count(&self) -> usize {
        self.iter().count()
    }

    /// Min property value.
    ///
    /// Returns:
    ///     PropValue:
    pub fn min(&self) -> PropValue {
        compute_generalised_sum(
            self.iter().flatten(),
            |a, b| a.min(b),
            |d| d.dtype().has_cmp(),
        )
    }

    /// Find the maximum property value and its associated time.
    ///
    /// Returns:
    ///     PropValue:
    pub fn max(&self) -> PropValue {
        compute_generalised_sum(
            self.iter().flatten(),
            |a, b| a.max(b),
            |d| d.dtype().has_cmp(),
        )
    }

    /// Drop none.
    ///
    /// Returns:
    ///     list[PropValue]:
    pub fn drop_none(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || builder().flatten()).into()
    }

    /// Compute the median of all property values.
    ///
    /// Returns:
    ///     PropValue:
    pub fn median(&self) -> PropValue {
        compute_median(self.iter().flatten().collect())
    }

    /// Compute the mean of all property values.
    ///
    /// Returns:
    ///     PropValue: The mean of each property values, or None if count is zero.
    pub fn mean(&self) -> PropValue {
        compute_mean(self.iter().flatten())
    }

    /// Compute the average of all property values. Alias for mean().
    ///
    /// Returns:
    ///     PropValue: The average of each property values, or None if count is zero.
    pub fn average(&self) -> PropValue {
        self.mean()
    }
}

#[pymethods]
impl PyPropValueListList {
    /// Sum of property values.
    ///
    /// Returns:
    ///     list[PropValue]:
    pub fn sum(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                compute_generalised_sum(it.flatten(), |a, b| a.add(b), |d| d.dtype().has_add())
            })
        })
        .into()
    }

    /// Min property value.
    ///
    /// Returns:
    ///     list[PropValue]:
    pub fn min(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                compute_generalised_sum(it.flatten(), |a, b| a.min(b), |d| d.dtype().has_cmp())
            })
        })
        .into()
    }

    /// Find the maximum property value and its associated time.
    ///
    /// Returns:
    ///     list[PropValue]:
    pub fn max(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || {
            builder().map(|it| {
                compute_generalised_sum(it.flatten(), |a, b| a.max(b), |d| d.dtype().has_cmp())
            })
        })
        .into()
    }

    /// Compute the average of all property values. Alias for mean().
    ///
    /// Returns:
    ///     list[PropValue]:
    pub fn average(&self) -> PyPropValueList {
        self.mean()
    }

    /// Mean property value across each row.
    ///
    /// Returns:
    ///     PyPropValueList:
    pub fn mean(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || builder().map(|it| compute_mean(it.flatten()))).into()
    }

    /// Median property value across each row.
    ///
    /// Returns:
    ///     PyPropValueList:
    pub fn median(&self) -> PyPropValueList {
        let builder = self.builder.clone();

        (move || builder().map(|it| compute_median(it.flatten().collect()))).into()
    }

    /// Flatten the nested iterable into a single list of values.
    ///
    /// Returns:
    ///     PyPropValueList:
    pub fn flatten(&self) -> PyPropValueList {
        let builder = self.builder.clone();
        (move || builder().flatten()).into()
    }

    /// Number of properties (or rows of properties).
    ///
    /// Returns:
    ///     UsizeIterable:
    pub fn count(&self) -> UsizeIterable {
        let builder = self.builder.clone();
        (move || builder().map(|it| it.count())).into()
    }

    /// Drop missing entries from each row.
    ///
    /// Returns:
    ///     PyPropValueListList:
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
