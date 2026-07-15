use crate::client::{
    remote_metadata::{RemoteMetadata, RemoteProperty},
    ClientError,
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
    pub fn values(
        &self,
        keys: Option<Vec<String>>,
    ) -> Result<Vec<PyRemoteProperty>, ClientError> {
        let inner = Arc::clone(&self.inner);
        let result = execute_async_task(move || async move { inner.values(keys).await })?;
        Ok(result.into_iter().map(PyRemoteProperty::new).collect())
    }
}
