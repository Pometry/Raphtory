//! Python bindings for the collection-level (columnar) metadata / properties
//! views.
//!
//! These mirror the local `MetadataView` / `PropertiesView` (flat collections)
//! and their nested `*ListList` counterparts (`PathFromGraph` / `NestedEdges`).
//! Every accessor returns a *column*: one value per collection member, with
//! `None` where a member lacks the key. On nested collections each column is
//! itself nested (one inner list per source node).
//!
//! Deferred: the local `PropertiesView.temporal` getter is not yet implemented
//! here — the columnar temporal timeline view is out of scope for now.

use crate::client::{
    remote_collection_metadata::{ColumnarProps, RemoteMetadataView, RemotePropertiesView},
    ClientError,
};
use pyo3::{
    prelude::*,
    types::{PyDict, PyList},
    Py, PyAny,
};
use raphtory::python::utils::execute_async_task;
use raphtory_api::core::entities::properties::prop::Prop;
use std::sync::Arc;

/// Convert a single `Prop` value into a native Python object.
fn prop_to_py(py: Python<'_>, value: Prop) -> PyResult<Py<PyAny>> {
    Ok(value
        .into_pyobject(py)
        .map_err(|e| ClientError::InvalidResponse(e.to_string()))?
        .unbind())
}

/// Look up `key` in one member's `(key, value)` entries, returning its value
/// as a Python object, or Python `None` when the member lacks the key.
fn member_value(py: Python<'_>, entries: &[(String, Prop)], key: &str) -> PyResult<Py<PyAny>> {
    match entries.iter().find(|(k, _)| k == key) {
        Some((_, v)) => prop_to_py(py, v.clone()),
        None => Ok(py.None()),
    }
}

/// Build the column for `key` — flat (`list`) or nested (`list[list]`) — or
/// `None` (Python `None`) when the key is absent from every member, matching
/// the local view's `get()` behaviour.
fn build_column(py: Python<'_>, data: &ColumnarProps, key: &str) -> PyResult<Option<Py<PyAny>>> {
    if !data.contains_key(key) {
        return Ok(None);
    }
    let column: Py<PyAny> = match data {
        ColumnarProps::Flat(members) => {
            let items: Vec<Py<PyAny>> = members
                .iter()
                .map(|m| member_value(py, m, key))
                .collect::<PyResult<_>>()?;
            PyList::new(py, items)?.into_any().unbind()
        }
        ColumnarProps::Nested(sources) => {
            let rows: Vec<Py<PyAny>> = sources
                .iter()
                .map(|source| {
                    let inner: Vec<Py<PyAny>> = source
                        .iter()
                        .map(|m| member_value(py, m, key))
                        .collect::<PyResult<_>>()?;
                    Ok(PyList::new(py, inner)?.into_any().unbind())
                })
                .collect::<PyResult<_>>()?;
            PyList::new(py, rows)?.into_any().unbind()
        }
    };
    Ok(Some(column))
}

// The two view types share an identical method surface and pivot logic; the
// only difference is which client handle (and thus which container) they wrap.
// A macro keeps the two `#[pymethods]` blocks in lockstep without duplication.
macro_rules! columnar_view_methods {
    ($ty:ident, $entity:literal) => {
        #[pymethods]
        impl $ty {
            #[doc = concat!("All keys present across the ", $entity, " collection, in first-seen order. Fires one RPC.")]
            pub fn keys(&self) -> Result<Vec<String>, ClientError> {
                let inner = Arc::clone(&self.inner);
                let data = execute_async_task(move || async move { inner.fetch().await })?;
                Ok(data.keys())
            }

            #[doc = concat!("The column of values for `key` — one entry per ", $entity, " member (nested per source for nested collections), `None` where a member lacks the key. Returns `None` if no member has the key. Fires one RPC.")]
            pub fn get(&self, py: Python<'_>, key: String) -> PyResult<Option<Py<PyAny>>> {
                let inner = Arc::clone(&self.inner);
                let data = execute_async_task(move || async move { inner.fetch().await })?;
                build_column(py, &data, &key)
            }

            #[doc = "One column per key, in key order. Fires one RPC."]
            pub fn values(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
                let inner = Arc::clone(&self.inner);
                let data = execute_async_task(move || async move { inner.fetch().await })?;
                let mut cols: Vec<Py<PyAny>> = Vec::new();
                for key in data.keys() {
                    if let Some(col) = build_column(py, &data, &key)? {
                        cols.push(col);
                    }
                }
                Ok(PyList::new(py, cols)?.into_any().unbind())
            }

            #[doc = "All `(key, column)` entries, in key order. Fires one RPC."]
            pub fn items(&self, py: Python<'_>) -> PyResult<Vec<(String, Py<PyAny>)>> {
                let inner = Arc::clone(&self.inner);
                let data = execute_async_task(move || async move { inner.fetch().await })?;
                let mut out: Vec<(String, Py<PyAny>)> = Vec::new();
                for key in data.keys() {
                    if let Some(col) = build_column(py, &data, &key)? {
                        out.push((key, col));
                    }
                }
                Ok(out)
            }

            #[doc = "All `(key, column)` entries as a native Python `dict`. Fires one RPC."]
            pub fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
                let inner = Arc::clone(&self.inner);
                let data = execute_async_task(move || async move { inner.fetch().await })?;
                let dict = PyDict::new(py);
                for key in data.keys() {
                    if let Some(col) = build_column(py, &data, &key)? {
                        dict.set_item(key, col)?;
                    }
                }
                Ok(dict.into_any().unbind())
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
