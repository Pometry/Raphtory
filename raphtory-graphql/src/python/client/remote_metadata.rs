use crate::{
    client::{
        remote_metadata::{
            RemoteMetadata, RemoteProperties, RemoteProperty, RemotePropertyTuple,
            RemoteTemporalProperties, RemoteTemporalProperty,
        },
        ClientError,
    },
    python::client::remote_history::{PyRemoteEventTime, PyRemoteHistory},
};
use pyo3::{prelude::*, IntoPyObject, Py, PyAny};
use raphtory::python::utils::execute_async_task;
use std::sync::Arc;

/// A single `(key, value)` property reading. The value is exposed as a
/// native Python object (int, str, float, bool, list, dict, datetime, ...)
/// via raphtory's existing `Prop` → Python conversion.
///
/// Returned by [RemoteMetadata.get][raphtory.graphql.RemoteMetadata.get] and
/// by [RemoteMetadata.values][raphtory.graphql.RemoteMetadata.values].
#[derive(Clone)]
#[pyclass(name = "RemoteProperty", module = "raphtory.graphql")]
pub struct PyRemoteProperty {
    inner: RemoteProperty,
}

impl PyRemoteProperty {
    pub(crate) fn new(inner: RemoteProperty) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyRemoteProperty {
    /// The property name.
    #[getter]
    pub fn key(&self) -> String {
        self.inner.key.clone()
    }

    /// The property value, converted to a native Python object.
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

    fn __repr__(&self) -> String {
        format!("RemoteProperty(key={:?}, value=...)", self.inner.key)
    }
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
    ///     Optional[RemoteProperty]: the `(key, value)` pair, or `None`.
    pub fn get(&self, key: String) -> Result<Option<PyRemoteProperty>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let result = execute_async_task(move || async move { inner.get(key).await })?;
        Ok(result.map(PyRemoteProperty::new))
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

    /// All `(key, value)` metadata entries. If `keys` is provided, only
    /// entries with those names are returned. Fires one RPC.
    ///
    /// Arguments:
    ///     keys (list[str], optional): whitelist of names to return.
    ///
    /// Returns:
    ///     list[RemoteProperty]: one entry per metadata key.
    #[pyo3(signature = (keys = None))]
    pub fn values(&self, keys: Option<Vec<String>>) -> Result<Vec<PyRemoteProperty>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let result = execute_async_task(move || async move { inner.values(keys).await })?;
        Ok(result.into_iter().map(PyRemoteProperty::new).collect())
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
    pub fn get(&self, key: String) -> Result<Option<PyRemoteProperty>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let result = execute_async_task(move || async move { inner.get(key).await })?;
        Ok(result.map(PyRemoteProperty::new))
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

    /// All `(key, value)` property entries. If `keys` is provided, only
    /// entries with those names are returned. Fires one RPC.
    #[pyo3(signature = (keys = None))]
    pub fn values(&self, keys: Option<Vec<String>>) -> Result<Vec<PyRemoteProperty>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let result = execute_async_task(move || async move { inner.values(keys).await })?;
        Ok(result.into_iter().map(PyRemoteProperty::new).collect())
    }

    /// The temporal-only sub-container — excludes metadata and provides
    /// per-key timeline accessors. Lazy — no RPC.
    #[getter]
    pub fn temporal(&self) -> PyRemoteTemporalProperties {
        PyRemoteTemporalProperties {
            inner: Arc::new(self.inner.temporal()),
        }
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
