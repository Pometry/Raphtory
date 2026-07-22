use crate::{
    client::{
        remote_metadata::{
            RemoteMetadata, RemoteProperties, RemotePropertyTuple, RemoteTemporalProperties,
            RemoteTemporalProperty,
        },
        ClientError,
    },
    python::client::remote_history::{PyRemoteEventTime, PyRemoteHistory},
};
use pyo3::{
    exceptions::PyKeyError,
    prelude::*,
    types::{PyDict, PyList},
    IntoPyObject, Py, PyAny,
};
use raphtory::python::utils::execute_async_task;
use raphtory_api::core::entities::properties::prop::Prop;
use std::sync::Arc;

/// Convert a `Prop` value into a native Python object — the raw value a local
/// `Properties`/`Metadata` `.get()`/`.values()` returns (drop-in parity; no
/// `RemoteProperty` wrapper). Used by the non-temporal containers.
fn prop_to_py(py: Python<'_>, value: Prop) -> Result<Py<PyAny>, ClientError> {
    Ok(value
        .into_pyobject(py)
        .map_err(|e| ClientError::InvalidResponse(e.to_string()))?
        .unbind())
}

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
    ///     the metadata value as a native Python object, or `None`.
    pub fn get(&self, py: Python<'_>, key: String) -> Result<Option<Py<PyAny>>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let result = execute_async_task(move || async move { inner.get(key).await })?;
        result.map(|p| prop_to_py(py, p.value)).transpose()
    }

    /// Whether a metadata entry with this key exists. Fires one RPC.
    pub fn contains(&self, key: String) -> Result<bool, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.contains(key).await })
    }

    /// All metadata keys present on this entity. Fires one RPC.
    pub fn keys(&self) -> Result<Vec<String>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.keys().await })
    }

    /// All metadata values as native Python objects. If `keys` is provided,
    /// only entries with those names are returned. Fires one RPC.
    #[pyo3(signature = (keys = None))]
    pub fn values(
        &self,
        py: Python<'_>,
        keys: Option<Vec<String>>,
    ) -> Result<Vec<Py<PyAny>>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let result = execute_async_task(move || async move { inner.values(keys).await })?;
        result
            .into_iter()
            .map(|p| prop_to_py(py, p.value))
            .collect()
    }

    /// All `(key, value)` metadata entries, values as native Python objects.
    /// Fires one RPC.
    pub fn items(&self, py: Python<'_>) -> Result<Vec<(String, Py<PyAny>)>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let result = execute_async_task(move || async move { inner.values(None).await })?;
        result
            .into_iter()
            .map(|p| Ok((p.key, prop_to_py(py, p.value)?)))
            .collect()
    }

    /// `md[key]` — the metadata value, or raises `KeyError` if absent. Fires
    /// one RPC. Contrast with `.get(key)`, which returns `None`.
    fn __getitem__(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        let inner = Arc::clone(&self.inner);
        let lookup = key.clone();
        let result = execute_async_task(move || async move { inner.get(lookup).await })?;
        match result {
            Some(p) => Ok(prop_to_py(py, p.value)?),
            None => Err(PyKeyError::new_err(key)),
        }
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
    fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner = Arc::clone(&self.inner);
        let items = execute_async_task(move || async move { inner.values(None).await })?;
        let dict = PyDict::new(py);
        for p in items {
            dict.set_item(p.key, prop_to_py(py, p.value)?)?;
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
/// via `.temporal()` (shipped in a follow-up batch).
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
    pub fn get(&self, py: Python<'_>, key: String) -> Result<Option<Py<PyAny>>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let result = execute_async_task(move || async move { inner.get(key).await })?;
        result.map(|p| prop_to_py(py, p.value)).transpose()
    }

    /// Whether a property with this key exists. Fires one RPC.
    pub fn contains(&self, key: String) -> Result<bool, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.contains(key).await })
    }

    /// All property keys in the current view. Fires one RPC.
    pub fn keys(&self) -> Result<Vec<String>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.keys().await })
    }

    /// All property values as native Python objects (temporal properties yield
    /// their most recent value). If `keys` is provided, only those names are
    /// returned. Fires one RPC.
    #[pyo3(signature = (keys = None))]
    pub fn values(
        &self,
        py: Python<'_>,
        keys: Option<Vec<String>>,
    ) -> Result<Vec<Py<PyAny>>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let result = execute_async_task(move || async move { inner.values(keys).await })?;
        result
            .into_iter()
            .map(|p| prop_to_py(py, p.value))
            .collect()
    }

    /// All `(key, value)` property entries, values as native Python objects.
    /// Fires one RPC.
    pub fn items(&self, py: Python<'_>) -> Result<Vec<(String, Py<PyAny>)>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let result = execute_async_task(move || async move { inner.values(None).await })?;
        result
            .into_iter()
            .map(|p| Ok((p.key, prop_to_py(py, p.value)?)))
            .collect()
    }

    /// The temporal-only sub-container — excludes metadata and provides
    /// per-key timeline accessors. Lazy — no RPC.
    #[getter]
    pub fn temporal(&self) -> PyRemoteTemporalProperties {
        PyRemoteTemporalProperties {
            inner: Arc::new(self.inner.temporal()),
        }
    }

    /// `props[key]` — the property value, or raises `KeyError` if absent.
    /// Fires one RPC. Contrast with `.get(key)`, which returns `None`.
    fn __getitem__(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        let inner = Arc::clone(&self.inner);
        let lookup = key.clone();
        let result = execute_async_task(move || async move { inner.get(lookup).await })?;
        match result {
            Some(p) => Ok(prop_to_py(py, p.value)?),
            None => Err(PyKeyError::new_err(key)),
        }
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
    fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner = Arc::clone(&self.inner);
        let items = execute_async_task(move || async move { inner.values(None).await })?;
        let dict = PyDict::new(py);
        for p in items {
            dict.set_item(p.key, prop_to_py(py, p.value)?)?;
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
    pub fn get(&self, key: String) -> Result<Option<PyRemoteTemporalProperty>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let result = execute_async_task(move || async move { inner.get(key).await })?;
        Ok(result.map(|tp| PyRemoteTemporalProperty {
            inner: Arc::new(tp),
        }))
    }

    /// Whether a temporal property with this key exists. Fires one RPC.
    pub fn contains(&self, key: String) -> Result<bool, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.contains(key).await })
    }

    /// All temporal property keys. Fires one RPC.
    pub fn keys(&self) -> Result<Vec<String>, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.keys().await })
    }

    /// All temporal properties as handles. If `keys` is provided, only
    /// entries with those names are returned. Fires one RPC (fetches key
    /// list); each returned handle fires its own RPCs on subsequent calls.
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
    #[getter]
    pub fn key(&self) -> String {
        self.inner.key.clone()
    }

    /// The event history of this property. Lazy — no RPC.
    #[getter]
    pub fn history(&self) -> PyRemoteHistory {
        PyRemoteHistory::new(self.inner.history())
    }

    /// All values this property has ever taken, in temporal order.
    /// Fires one RPC. Returns a list of native Python values.
    pub fn values(&self, py: Python<'_>) -> Result<Py<PyAny>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let vals = execute_async_task(move || async move { inner.values().await })?;
        Ok(vals
            .into_pyobject(py)
            .map_err(|e| ClientError::InvalidResponse(e.to_string()))?
            .unbind())
    }

    /// Value at or before time `t`, as a native Python object. Returns
    /// `None` if no update exists on or before `t`. Fires one RPC.
    pub fn at(&self, py: Python<'_>, t: i64) -> Result<Option<Py<PyAny>>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let val = execute_async_task(move || async move { inner.at(t).await })?;
        val.map(|p| {
            p.into_pyobject(py)
                .map(|b| b.unbind())
                .map_err(|e| ClientError::InvalidResponse(e.to_string()))
        })
        .transpose()
    }

    /// The most recent value, or `None` if the property has no updates
    /// in view. Fires one RPC.
    pub fn latest(&self, py: Python<'_>) -> Result<Option<Py<PyAny>>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let val = execute_async_task(move || async move { inner.latest().await })?;
        val.map(|p| {
            p.into_pyobject(py)
                .map(|b| b.unbind())
                .map_err(|e| ClientError::InvalidResponse(e.to_string()))
        })
        .transpose()
    }

    /// Number of updates recorded for this property in the current view.
    /// Fires one RPC.
    pub fn count(&self) -> Result<i64, ClientError> {
        let inner = Arc::clone(&self.inner);
        execute_async_task(move || async move { inner.count().await })
    }

    /// Distinct values this property has ever taken (order not guaranteed).
    /// Fires one RPC.
    pub fn unique(&self, py: Python<'_>) -> Result<Py<PyAny>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let vals = execute_async_task(move || async move { inner.unique().await })?;
        Ok(vals
            .into_pyobject(py)
            .map_err(|e| ClientError::InvalidResponse(e.to_string()))?
            .unbind())
    }

    /// Collapse consecutive-equal updates into single `(time, value)` pairs.
    /// `latest_time = True` picks the last timestamp of each run; `False`
    /// picks the first. Fires one RPC.
    pub fn ordered_dedupe(
        &self,
        latest_time: bool,
    ) -> Result<Vec<PyRemotePropertyTuple>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let tuples =
            execute_async_task(move || async move { inner.ordered_dedupe(latest_time).await })?;
        Ok(tuples
            .into_iter()
            .map(PyRemotePropertyTuple::from)
            .collect())
    }

    /// Sum of all updates. `None` if not additive. Fires one RPC.
    pub fn sum(&self, py: Python<'_>) -> Result<Option<Py<PyAny>>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let val = execute_async_task(move || async move { inner.sum().await })?;
        val.map(|p| {
            p.into_pyobject(py)
                .map(|b| b.unbind())
                .map_err(|e| ClientError::InvalidResponse(e.to_string()))
        })
        .transpose()
    }

    /// Mean of all updates. `None` if not numeric or empty. Fires one RPC.
    pub fn mean(&self, py: Python<'_>) -> Result<Option<Py<PyAny>>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let val = execute_async_task(move || async move { inner.mean().await })?;
        val.map(|p| {
            p.into_pyobject(py)
                .map(|b| b.unbind())
                .map_err(|e| ClientError::InvalidResponse(e.to_string()))
        })
        .transpose()
    }

    /// Alias for `mean`. Fires one RPC.
    pub fn average(&self, py: Python<'_>) -> Result<Option<Py<PyAny>>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let val = execute_async_task(move || async move { inner.average().await })?;
        val.map(|p| {
            p.into_pyobject(py)
                .map(|b| b.unbind())
                .map_err(|e| ClientError::InvalidResponse(e.to_string()))
        })
        .transpose()
    }

    /// Minimum `(time, value)` pair. `None` if not comparable or empty.
    /// Fires one RPC.
    pub fn min(&self) -> Result<Option<PyRemotePropertyTuple>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let val = execute_async_task(move || async move { inner.min().await })?;
        Ok(val.map(PyRemotePropertyTuple::from))
    }

    /// Maximum `(time, value)` pair. `None` if not comparable or empty.
    /// Fires one RPC.
    pub fn max(&self) -> Result<Option<PyRemotePropertyTuple>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let val = execute_async_task(move || async move { inner.max().await })?;
        Ok(val.map(PyRemotePropertyTuple::from))
    }

    /// Median `(time, value)` pair. `None` if not comparable or empty.
    /// Fires one RPC.
    pub fn median(&self) -> Result<Option<PyRemotePropertyTuple>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let val = execute_async_task(move || async move { inner.median().await })?;
        Ok(val.map(PyRemotePropertyTuple::from))
    }
}

/// A `(time, value)` snapshot inside a temporal property. Returned by
/// `min` / `max` / `median` (a single pair) and each entry of
/// `ordered_dedupe` (a list of pairs).
#[derive(Clone)]
#[pyclass(name = "RemotePropertyTuple", module = "raphtory.graphql")]
pub struct PyRemotePropertyTuple {
    inner: RemotePropertyTuple,
}

impl From<RemotePropertyTuple> for PyRemotePropertyTuple {
    fn from(inner: RemotePropertyTuple) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyRemotePropertyTuple {
    /// The event time at which this value was observed.
    #[getter]
    pub fn time(&self) -> PyRemoteEventTime {
        self.inner.time.clone().into()
    }

    /// The property value at that time, as a native Python object.
    #[getter]
    pub fn value(&self, py: Python<'_>) -> Result<Py<PyAny>, ClientError> {
        Ok(self
            .inner
            .value
            .clone()
            .into_pyobject(py)
            .map_err(|e| ClientError::InvalidResponse(e.to_string()))?
            .unbind())
    }
}
