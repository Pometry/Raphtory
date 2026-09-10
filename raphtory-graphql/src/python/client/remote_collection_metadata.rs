//! Python bindings for the collection-level (columnar) metadata / properties
//! views.
//!
//! These mirror the local `MetadataView` / `PropertiesView` (flat collections)
//! and their nested `*ListList` counterparts (`PathFromGraph` / `NestedEdges`).
//! Every accessor returns a *column*: one value per collection member, with
//! `None` where a member lacks the key. On nested collections each column is
//! itself nested (one inner list per source node).
//!
//! This layer is conversion-only — each method delegates to the client view
//! (which shapes the RPC to the question: key lookups for `keys`/`contains`,
//! a single-column fetch for `get`, one full fetch for the all-columns reads)
//! and converts the result to Python.
//!
//! Deferred: the local `PropertiesView.temporal` getter is not yet implemented
//! here — the columnar temporal timeline view is out of scope for now.

use crate::client::{
    remote::remote_collection_metadata::{Column, RemoteMetadataView, RemotePropertiesView},
    ClientError,
};
use pyo3::{
    exceptions::PyKeyError,
    prelude::*,
    types::{PyDict, PyList},
};
use raphtory::python::utils::execute_async_task;
use std::sync::Arc;

// A column converts to `list` (flat) or `list[list]` (nested); pyo3 handles
// the `Prop` values.
impl<'py> IntoPyObject<'py> for Column {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Column::Flat(col) => Ok(col.into_pyobject(py)?.into_any()),
            Column::Nested(col) => Ok(col.into_pyobject(py)?.into_any()),
        }
    }
}

// The two view types share an identical method surface; the macro keeps the
// two `#[pymethods]` blocks in lockstep without duplication.
macro_rules! columnar_view_methods {
    ($ty:ident, $entity:literal) => {
        #[pymethods]
        impl $ty {
            #[doc = concat!("All ", $entity, " keys, read from the first collection member's registry (matching the local view). Fires one key-lookup RPC — no property values travel.")]
            #[doc = ""]
            #[doc = "Returns:"]
            #[doc = "    list[str]: the keys."]
            pub fn keys(&self) -> Result<Vec<String>, ClientError> {
                let inner = Arc::clone(&self.inner);
                execute_async_task(move || async move { inner.keys().await })
            }

            #[doc = concat!("The column of values for `key` — one entry per ", $entity, " member (nested per source for nested collections), `None` where a member lacks the key. Returns `None` if the key is not registered. Fires one single-column RPC.")]
            #[doc = ""]
            #[doc = "Arguments:"]
            #[doc = concat!("    key (str): the ", $entity, " name to look up.")]
            #[doc = ""]
            #[doc = "Returns:"]
            #[doc = "    Optional[list]: the column of values, or `None` if the key is not registered."]
            pub fn get(&self, key: String) -> Result<Option<Column>, ClientError> {
                let inner = Arc::clone(&self.inner);
                execute_async_task(move || async move { inner.get(&key).await })
            }

            #[doc = "One column per key, in key order. Fires one RPC."]
            #[doc = ""]
            #[doc = "Returns:"]
            #[doc = "    list: one column per key, in key order."]
            pub fn values(&self) -> Result<Vec<Column>, ClientError> {
                let inner = Arc::clone(&self.inner);
                let cols = execute_async_task(move || async move { inner.fetch_all().await })?;
                Ok(cols.into_iter().map(|(_, col)| col).collect())
            }

            #[doc = "All `(key, column)` entries, in key order. Fires one RPC."]
            #[doc = ""]
            #[doc = "Returns:"]
            #[doc = "    list[tuple[str, list]]: the `(key, column)` entries, in key order."]
            pub fn items(&self) -> Result<Vec<(String, Column)>, ClientError> {
                let inner = Arc::clone(&self.inner);
                execute_async_task(move || async move { inner.fetch_all().await })
            }

            #[doc = "All `(key, column)` entries as a native Python `dict`. Fires one RPC."]
            #[doc = ""]
            #[doc = "Returns:"]
            #[doc = "    dict[str, list]: the columns, keyed by key."]
            pub fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
                let inner = Arc::clone(&self.inner);
                let cols = execute_async_task(move || async move { inner.fetch_all().await })?;
                let dict = PyDict::new(py);
                for (key, col) in cols {
                    dict.set_item(key, col)?;
                }
                Ok(dict.unbind())
            }

            #[doc = concat!("`key in view` — whether a ", $entity, " with this key exists (matching the local view). Fires one key-lookup RPC.")]
            fn __contains__(&self, key: String) -> Result<bool, ClientError> {
                let inner = Arc::clone(&self.inner);
                execute_async_task(move || async move { inner.contains(&key).await })
            }

            #[doc = concat!("`view[key]` — the column of values for `key`, raising `KeyError` if the key is not registered (matching the local view). Contrast with `.get(key)`, which returns `None`. Fires one single-column RPC.")]
            fn __getitem__(&self, key: String) -> PyResult<Column> {
                self.get(key)?
                    .ok_or_else(|| PyKeyError::new_err("No such property"))
            }

            #[doc = "`for k in view` — iterate the keys (matching the local view). Fires one key-lookup RPC."]
            fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
                let keys = self.keys()?;
                Ok(PyList::new(py, keys)?.try_iter()?.into_any().unbind())
            }
        }
    };
}

/// A columnar view over the non-temporal metadata of a remote node/edge
/// collection. Every accessor returns one value per member (nested per source
/// for nested collections).
///
/// Returned by the `metadata` getter on the remote collection handles.
#[derive(Clone)]
#[pyclass(
    name = "RemoteMetadataView",
    module = "raphtory.graphql",
    from_py_object
)]
pub struct PyRemoteMetadataView {
    pub(crate) inner: Arc<RemoteMetadataView>,
}

impl PyRemoteMetadataView {
    pub(crate) fn new(inner: RemoteMetadataView) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

columnar_view_methods!(PyRemoteMetadataView, "metadata");

/// A columnar view over the properties of a remote node/edge collection
/// (temporal properties yield their most recent value under the current view).
///
/// Returned by the `properties` getter on the remote collection handles.
#[derive(Clone)]
#[pyclass(
    name = "RemotePropertiesView",
    module = "raphtory.graphql",
    from_py_object
)]
pub struct PyRemotePropertiesView {
    pub(crate) inner: Arc<RemotePropertiesView>,
}

impl PyRemotePropertiesView {
    pub(crate) fn new(inner: RemotePropertiesView) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

columnar_view_methods!(PyRemotePropertiesView, "property");
