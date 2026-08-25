use crate::{
    client::{
        remote_metadata::{
            RemoteMetadata, RemoteProperties, RemotePropertyTuple, RemoteTemporalProperties,
            RemoteTemporalProperty,
        },
        ClientError,
    },
    python::client::remote_history::PyRemoteHistory,
};
use pyo3::{
    exceptions::PyKeyError,
    prelude::*,
    types::{PyDict, PyIterator, PyList},
    Py, PyAny,
};
use raphtory::python::utils::execute_async_task;
use raphtory_api::core::{
    entities::properties::prop::{Prop, PropType},
    storage::timeindex::{AsTime, EventTime},
};
use std::sync::Arc;

/// A handle to the metadata container of a remote graph, node, or edge —
/// the non-temporal properties whose values don't change over the graph's
/// lifetime.
///
/// Returned by [RemoteGraph.metadata][raphtory.graphql.RemoteGraph.metadata],
/// [RemoteNode.metadata][raphtory.graphql.RemoteNode.metadata], and
/// [RemoteEdge.metadata][raphtory.graphql.RemoteEdge.metadata].
#[derive(Clone)]
#[pyclass(name = "RemoteMetadata", module = "raphtory.graphql", from_py_object)]
pub struct PyRemoteMetadata {
    pub(crate) inner: Arc<RemoteMetadata>,
}

impl PyRemoteMetadata {
    pub(crate) fn new(inner: RemoteMetadata) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[pymethods]
impl PyRemoteMetadata {
    /// Fetch a single metadata value by key. Returns `None` if the key
    /// isn't present. Fires one RPC.
    ///
    /// Arguments:
    ///     key (str): the metadata name to look up.
    ///
    /// Returns:
    ///     Optional[PropValue]: the metadata value as a native Python object,
    ///         or `None`.
    pub fn get(&self, key: String) -> Result<Option<Prop>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.get(key).await })
    }

    /// All metadata keys present on this entity. Fires one RPC.
    ///
    /// Returns:
    ///     list[str]: the metadata keys.
    pub fn keys(&self) -> Result<Vec<String>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.keys().await })
    }

    /// All metadata values as native Python objects. If `keys` is provided,
    /// only entries with those names are returned. Fires one RPC.
    ///
    /// Arguments:
    ///     keys (list[str], optional): restrict the result to these metadata names.
    ///
    /// Returns:
    ///     list[PropValue]: the metadata values.
    #[pyo3(signature = (keys = None))]
    pub fn values(&self, keys: Option<Vec<String>>) -> Result<Vec<Prop>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.values(keys).await })
    }

    /// All `(key, value)` metadata entries, values as native Python objects.
    /// Fires one RPC.
    ///
    /// Returns:
    ///     list[tuple[str, PropValue]]: the `(key, value)` metadata entries.
    pub fn items(&self) -> Result<Vec<(String, Prop)>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.items(None).await })
    }

    /// `md[key]` — the metadata value, or raises `KeyError` if absent. Fires
    /// one RPC. Contrast with `.get(key)`, which returns `None`.
    fn __getitem__(&self, key: String) -> PyResult<Prop> {
        let inner = Arc::clone(&self.inner);
        let lookup = key.clone();
        let result = execute_async_task(move || async move { inner.get(lookup).await })?;
        result.ok_or_else(|| PyKeyError::new_err(key))
    }

    /// `key in md` — whether a metadata entry with this key exists. Fires one RPC.
    fn __contains__(&self, key: String) -> Result<bool, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.contains(key).await })
    }

    /// `len(md)` — number of metadata keys. Fires one RPC.
    fn __len__(&self) -> Result<usize, ClientError> {
        let inner = Arc::clone(&self.inner);
        Ok(execute_async_task(move || async move { inner.keys().await })?.len())
    }

    /// `for k in md` — iterate metadata keys. Fires one RPC (fetches all keys).
    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner = Arc::clone(&self.inner);
        let keys = execute_async_task(move || async move { inner.keys().await })?;
        Ok(PyList::new(py, keys)?.try_iter()?.into_any().unbind())
    }

    /// All `(key, value)` entries as a native Python `dict`. Fires one RPC.
    ///
    /// Returns:
    ///     dict[str, PropValue]: the metadata as a `dict`.
    fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner = Arc::clone(&self.inner);
        let items = execute_async_task(move || async move { inner.items(None).await })?;
        let dict = PyDict::new(py);
        for (key, value) in items {
            dict.set_item(key, value)?;
        }
        Ok(dict.into_any().unbind())
    }
}

/// A handle to the full properties container of a remote graph, node, or
/// edge — includes both non-temporal metadata and temporal properties.
///
/// Same terminal shape as `RemoteMetadata` (`get`/`contains`/`keys`/`values`).
/// For temporal properties, `.get(key)` and `.values()` yield the property's
/// most recent value under the current view; drill into a property's timeline
/// via `.temporal()`.
///
/// Returned by [RemoteGraph.properties][raphtory.graphql.RemoteGraph.properties],
/// [RemoteNode.properties][raphtory.graphql.RemoteNode.properties], and
/// [RemoteEdge.properties][raphtory.graphql.RemoteEdge.properties].
#[derive(Clone)]
#[pyclass(name = "RemoteProperties", module = "raphtory.graphql", from_py_object)]
pub struct PyRemoteProperties {
    pub(crate) inner: Arc<RemoteProperties>,
}

impl PyRemoteProperties {
    pub(crate) fn new(inner: RemoteProperties) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[pymethods]
impl PyRemoteProperties {
    /// Fetch a single property value by key. Returns `None` if the key
    /// isn't present. For a temporal property, yields its most recent value
    /// under the current view. Fires one RPC.
    ///
    /// Arguments:
    ///     key (str): the property name to look up.
    ///
    /// Returns:
    ///     Optional[PropValue]: the property value, or `None` if absent.
    pub fn get(&self, key: String) -> Result<Option<Prop>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.get(key).await })
    }

    /// All property keys in the current view. Fires one RPC.
    ///
    /// Returns:
    ///     list[str]: the property keys.
    pub fn keys(&self) -> Result<Vec<String>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.keys().await })
    }

    /// The data-type of the property's latest value by key, as a `PropType`.
    /// Returns `None`
    /// when the key isn't present. Mirrors the local `Properties.get_dtype_of`.
    /// Fires one RPC.
    ///
    /// Arguments:
    ///     key (str): the name of the property.
    ///
    /// Returns:
    ///     Optional[PropType]: the property's data-type, or None if absent.
    pub fn get_dtype_of(&self, key: String) -> Result<Option<PropType>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.get_dtype_of(key).await })
    }

    /// All property values as native Python objects (temporal properties yield
    /// their most recent value). If `keys` is provided, only those names are
    /// returned. Fires one RPC.
    ///
    /// Arguments:
    ///     keys (list[str], optional): restrict the result to these property names.
    ///
    /// Returns:
    ///     list[PropValue]: the property values.
    #[pyo3(signature = (keys = None))]
    pub fn values(&self, keys: Option<Vec<String>>) -> Result<Vec<Prop>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.values(keys).await })
    }

    /// All `(key, value)` property entries, values as native Python objects.
    /// Fires one RPC.
    ///
    /// Returns:
    ///     list[tuple[str, PropValue]]: the `(key, value)` property entries.
    pub fn items(&self) -> Result<Vec<(String, Prop)>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.items(None).await })
    }

    /// The temporal-only sub-container — excludes metadata and provides
    /// per-key timeline accessors. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteTemporalProperties: the temporal-only sub-container.
    #[getter]
    pub fn temporal(&self) -> PyRemoteTemporalProperties {
        PyRemoteTemporalProperties {
            inner: Arc::new(self.inner.temporal()),
        }
    }

    /// `props[key]` — the property value, or raises `KeyError` if absent.
    /// Fires one RPC. Contrast with `.get(key)`, which returns `None`.
    fn __getitem__(&self, key: String) -> PyResult<Prop> {
        let inner = Arc::clone(&self.inner);
        let lookup = key.clone();
        let result = execute_async_task(move || async move { inner.get(lookup).await })?;
        result.ok_or_else(|| PyKeyError::new_err(key))
    }

    /// `key in props` — whether a property with this key exists. Fires one RPC.
    fn __contains__(&self, key: String) -> Result<bool, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.contains(key).await })
    }

    /// `len(props)` — number of property keys. Fires one RPC.
    fn __len__(&self) -> Result<usize, ClientError> {
        let inner = Arc::clone(&self.inner);
        Ok(execute_async_task(move || async move { inner.keys().await })?.len())
    }

    /// `for k in props` — iterate property keys. Fires one RPC (fetches all keys).
    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner = Arc::clone(&self.inner);
        let keys = execute_async_task(move || async move { inner.keys().await })?;
        Ok(PyList::new(py, keys)?.try_iter()?.into_any().unbind())
    }

    /// All `(key, value)` entries as a native Python `dict` (temporal
    /// properties yield their most recent value). Fires one RPC.
    ///
    /// Returns:
    ///     dict[str, PropValue]: the properties as a `dict`.
    fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner = Arc::clone(&self.inner);
        let items = execute_async_task(move || async move { inner.items(None).await })?;
        let dict = PyDict::new(py);
        for (key, value) in items {
            dict.set_item(key, value)?;
        }
        Ok(dict.into_any().unbind())
    }
}

/// A handle to the temporal-only view of a properties container. Each
/// property has a full history over time.
///
/// Returned by `PyRemoteProperties.temporal`.
#[derive(Clone)]
#[pyclass(
    name = "RemoteTemporalProperties",
    module = "raphtory.graphql",
    from_py_object
)]
pub struct PyRemoteTemporalProperties {
    pub(crate) inner: Arc<RemoteTemporalProperties>,
}

#[pymethods]
impl PyRemoteTemporalProperties {
    /// Fetch a temporal property by key. Returns `None` if the key isn't
    /// present. Fires one RPC (existence check).
    ///
    /// Arguments:
    ///     key (str): the temporal property name to look up.
    ///
    /// Returns:
    ///     Optional[RemoteTemporalProperty]: the temporal property handle, or `None` if
    ///         absent.
    pub fn get(&self, key: String) -> Result<Option<PyRemoteTemporalProperty>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let result = execute_async_task(move || async move { inner.get(key).await })?;
        Ok(result.map(|tp| PyRemoteTemporalProperty {
            inner: Arc::new(tp),
        }))
    }

    /// All temporal property keys. Fires one RPC.
    ///
    /// Returns:
    ///     list[str]: the temporal property keys.
    pub fn keys(&self) -> Result<Vec<String>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.keys().await })
    }

    /// All temporal properties as handles. If `keys` is provided, only
    /// entries with those names are returned. Fires one RPC (fetches key
    /// list); each returned handle fires its own RPCs on subsequent calls.
    ///
    /// Arguments:
    ///     keys (list[str], optional): restrict the result to these property names.
    ///
    /// Returns:
    ///     list[RemoteTemporalProperty]: the temporal property handles.
    #[pyo3(signature = (keys = None))]
    pub fn values(
        &self,
        keys: Option<Vec<String>>,
    ) -> Result<Vec<PyRemoteTemporalProperty>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let result = execute_async_task(move || async move { inner.values(keys).await })?;
        Ok(result
            .into_iter()
            .map(|tp| PyRemoteTemporalProperty {
                inner: Arc::new(tp),
            })
            .collect())
    }

    /// All `(key, temporal-property handle)` entries. Fires one RPC (fetches
    /// the key list); each returned handle fires its own RPCs on subsequent
    /// method calls.
    ///
    /// Returns:
    ///     list[tuple[str, RemoteTemporalProperty]]: the `(key, temporal-property handle)`
    ///         entries.
    pub fn items(&self) -> Result<Vec<(String, PyRemoteTemporalProperty)>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let result = execute_async_task(move || async move { inner.values(None).await })?;
        Ok(result
            .into_iter()
            .map(|tp| {
                (
                    tp.key.clone(),
                    PyRemoteTemporalProperty {
                        inner: Arc::new(tp),
                    },
                )
            })
            .collect())
    }

    /// Every temporal property's full history, as
    /// `{key: [(EventTime, value), ...]}` — mirrors the local
    /// `TemporalProperties.histories`. Composed from `items()` + each
    /// property's `items()`; fires 1 RPC for the property list plus 2 per
    /// property (its history + values), so it is heavy for wide containers —
    /// prefer `.get(key).items()` when you only need one property.
    ///
    /// Returns:
    ///     dict[str, list[tuple[EventTime, PropValue]]]: every property's full history,
    ///         keyed by property name.
    pub fn histories<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (key, tp) in self.items()? {
            dict.set_item(key, tp.items()?)?;
        }
        Ok(dict)
    }

    /// The latest value of every temporal property, as `{key: value}` —
    /// mirrors the local `TemporalProperties.latest()`. Composed from
    /// `items()` + each property's `value()`; fires 1 RPC for the property
    /// list plus 1 per property. Keys whose property has no update in view
    /// are omitted (their latest is `None`), matching the local behaviour.
    ///
    /// Returns:
    ///     dict[str, PropValue]: the latest value of every property, keyed by property
    ///         name; keys with no update in view are omitted.
    pub fn latest<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (key, tp) in self.items()? {
            if let Some(value) = tp.value()? {
                dict.set_item(key, value)?;
            }
        }
        Ok(dict)
    }

    /// `td[key]` — the temporal property handle, or raises `KeyError` if
    /// absent. Fires one RPC (existence check). Contrast with `.get(key)`,
    /// which returns `None`.
    fn __getitem__(&self, key: String) -> PyResult<PyRemoteTemporalProperty> {
        match self.get(key.clone())? {
            Some(tp) => Ok(tp),
            None => Err(PyKeyError::new_err(key)),
        }
    }

    /// `key in td` — whether a temporal property with this key exists.
    /// Fires one RPC.
    fn __contains__(&self, key: String) -> Result<bool, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.contains(key).await })
    }

    /// `len(td)` — number of temporal property keys. Fires one RPC.
    fn __len__(&self) -> Result<usize, ClientError> {
        Ok(self.keys()?.len())
    }

    /// `for k in td` — iterate temporal property keys. Fires one RPC.
    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyIterator>> {
        let keys = self.keys()?;
        PyList::new(py, keys)?.try_iter()
    }
}

/// A handle to a single temporal property — one key with its full history
/// of updates, plus statistical summaries and time-indexed accessors.
///
/// Returned by [PyRemoteTemporalProperties.get][raphtory.graphql.RemoteTemporalProperties.get]
/// and [PyRemoteTemporalProperties.values][raphtory.graphql.RemoteTemporalProperties.values].
#[derive(Clone)]
#[pyclass(
    name = "RemoteTemporalProperty",
    module = "raphtory.graphql",
    from_py_object
)]
pub struct PyRemoteTemporalProperty {
    pub(crate) inner: Arc<RemoteTemporalProperty>,
}

#[pymethods]
impl PyRemoteTemporalProperty {
    /// The property name — cached on the handle, no RPC needed.
    ///
    /// Returns:
    ///     str: the property name.
    #[getter]
    pub fn key(&self) -> String {
        self.inner.key.clone()
    }

    /// The event history of this property. Lazy — no RPC.
    ///
    /// Returns:
    ///     RemoteHistory: the property's event history.
    #[getter]
    pub fn history(&self) -> PyRemoteHistory {
        PyRemoteHistory::new(self.inner.history())
    }

    /// All values this property has ever taken, in temporal order.
    /// Fires one RPC. Returns a list of native Python values.
    ///
    /// Returns:
    ///     list[PropValue]: every value the property has taken, in temporal order.
    pub fn values(&self) -> Result<Vec<Prop>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.values().await })
    }

    /// Value at or before time `t`, as a native Python object. Returns
    /// `None` if no update exists on or before `t`. Fires one RPC.
    ///
    /// Arguments:
    ///     t (EventTime): the time to read the value at.
    ///
    /// Returns:
    ///     Optional[PropValue]: the value at or before `t`, or `None` if there is no such
    ///         update.
    pub fn at(&self, t: EventTime) -> Result<Option<Prop>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let t = t.t();
        execute_async_task(move || async move { inner.at(t).await })
    }

    /// The most recent value, or `None` if the property has no updates in
    /// view — matching the local `TemporalProperty.value`. Fires one RPC.
    ///
    /// Returns:
    ///     Optional[PropValue]: the most recent value, or `None` if the property has no
    ///         updates in view.
    pub fn value(&self) -> Result<Option<Prop>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.latest().await })
    }

    /// Number of updates recorded for this property in the current view.
    /// Fires one RPC.
    ///
    /// Returns:
    ///     int: the number of updates in the current view.
    pub fn count(&self) -> Result<i64, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.count().await })
    }

    /// Distinct values this property has ever taken (order not guaranteed).
    /// Fires one RPC.
    ///
    /// Returns:
    ///     list[PropValue]: the distinct values the property has taken (order not
    ///         guaranteed).
    pub fn unique(&self) -> Result<Vec<Prop>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.unique().await })
    }

    /// Collapse consecutive-equal updates into single `(time, value)` pairs.
    /// `latest_time = True` picks the last timestamp of each run; `False`
    /// picks the first. Fires one RPC.
    ///
    /// Arguments:
    ///     latest_time (bool): pick the last timestamp of each run of equal values rather
    ///         than the first.
    ///
    /// Returns:
    ///     list[tuple[EventTime, PropValue]]: one `(time, value)` pair per run of
    ///         consecutive-equal updates.
    pub fn ordered_dedupe(&self, latest_time: bool) -> Result<Vec<(EventTime, Prop)>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let tuples =
            execute_async_task(move || async move { inner.ordered_dedupe(latest_time).await })?;
        Ok(tuples.into_iter().map(tuple_to_py).collect())
    }

    /// Sum of all updates. `None` if not additive. Fires one RPC.
    ///
    /// Returns:
    ///     Optional[PropValue]: the sum of all updates, or `None` if not additive.
    pub fn sum(&self) -> Result<Option<Prop>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.sum().await })
    }

    /// Mean of all updates. `None` if not numeric or empty. Fires one RPC.
    ///
    /// Returns:
    ///     Optional[PropValue]: the mean of all updates, or `None` if not numeric or empty.
    pub fn mean(&self) -> Result<Option<Prop>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.mean().await })
    }

    /// Alias for `mean`. Fires one RPC.
    ///
    /// Returns:
    ///     Optional[PropValue]: the mean of all updates, or `None` if not numeric or empty.
    pub fn average(&self) -> Result<Option<Prop>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.average().await })
    }

    /// Minimum `(time, value)` pair. `None` if not comparable or empty.
    /// Fires one RPC.
    ///
    /// Returns:
    ///     Optional[tuple[EventTime, PropValue]]: the minimum `(time, value)` pair, or
    ///         `None` if not comparable or empty.
    pub fn min(&self) -> Result<Option<(EventTime, Prop)>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let val = execute_async_task(move || async move { inner.min().await })?;
        Ok(val.map(tuple_to_py))
    }

    /// Maximum `(time, value)` pair. `None` if not comparable or empty.
    /// Fires one RPC.
    ///
    /// Returns:
    ///     Optional[tuple[EventTime, PropValue]]: the maximum `(time, value)` pair, or
    ///         `None` if not comparable or empty.
    pub fn max(&self) -> Result<Option<(EventTime, Prop)>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let val = execute_async_task(move || async move { inner.max().await })?;
        Ok(val.map(tuple_to_py))
    }

    /// Median `(time, value)` pair. `None` if not comparable or empty.
    /// Fires one RPC.
    ///
    /// Returns:
    ///     Optional[tuple[EventTime, PropValue]]: the median `(time, value)` pair, or
    ///         `None` if not comparable or empty.
    pub fn median(&self) -> Result<Option<(EventTime, Prop)>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let val = execute_async_task(move || async move { inner.median().await })?;
        Ok(val.map(tuple_to_py))
    }

    /// All `(time, value)` pairs this property has taken, in temporal order.
    /// Mirrors the local `TemporalProperty.items()`. Fires two RPCs — one for
    /// the history (event times) and one for the values — then pairs them
    /// element-wise.
    ///
    /// Returns:
    ///   list[Tuple[EventTime, PropValue]]: one pair per update.
    pub fn items(&self) -> Result<Vec<(EventTime, Prop)>, ClientError> {
        let history = self.inner.history();
        let times = execute_async_task(move || async move { history.collect().await })?;
        let inner = Arc::clone(&self.inner);
        let vals = execute_async_task(move || async move { inner.values().await })?;
        Ok(times.into_iter().zip(vals).collect())
    }

    /// `for (time, value) in temporal_property:` — iterate the `(time, value)`
    /// pairs in temporal order. Fires two RPCs (see `items()`), then yields
    /// each pair locally.
    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let items = self.items()?;
        Ok(PyList::new(py, items)?.try_iter()?.into_any().unbind())
    }
}

/// A remote `(time, value)` pair as the native tuple the local API returns.
fn tuple_to_py(t: RemotePropertyTuple) -> (EventTime, Prop) {
    (t.time, t.value)
}
