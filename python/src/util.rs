//! Helper functions for the Python bindings.
//!
//! This module contains helper functions for the Python bindings.
//! These functions are not part of the public API and are not exported to the Python module.
use crate::perspective::{PyPerspective, PyPerspectiveSet};
use crate::vertex::PyVertex;
use docbrown::core::tgraph::VertexRef;
use docbrown::db::graph_window::WindowSet;
use docbrown::db::perspective::Perspective;
use docbrown::db::view_api::TimeOps;
use pyo3::exceptions::{PyException, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyIterator;
use std::error::Error;

/// Extract a `VertexRef` from a Python object.
/// The object can be a `str`, `u64` or `PyVertex`.
/// If the object is a `PyVertex`, the `VertexRef` is extracted from the `PyVertex`.
/// If the object is a `str`, the `VertexRef` is created from the `str`.
/// If the object is a `int`, the `VertexRef` is created from the `int`.
///
/// Arguments
///     vref: The Python object to extract the `VertexRef` from.
///
/// Returns
///    A `VertexRef` extracted from the Python object.
pub(crate) fn extract_vertex_ref(vref: &PyAny) -> PyResult<VertexRef> {
    if let Ok(s) = vref.extract::<String>() {
        Ok(s.into())
    } else if let Ok(gid) = vref.extract::<u64>() {
        Ok(gid.into())
    } else if let Ok(v) = vref.extract::<PyVertex>() {
        Ok(v.into())
    } else {
        Err(PyTypeError::new_err("Not a valid vertex"))
    }
}

pub(crate) fn window_impl<T: TimeOps + Sized + Clone>(
    slf: &T,
    t_start: Option<i64>,
    t_end: Option<i64>,
) -> T::WindowedViewType {
    slf.window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
}

pub(crate) fn through_impl<T: TimeOps + Sized + Clone>(
    slf: &T,
    perspectives: &PyAny,
) -> Result<WindowSet<T>, PyErr> {
    struct PyPerspectiveIterator {
        pub iter: Py<PyIterator>,
    }
    unsafe impl Send for PyPerspectiveIterator {} // iter is used by holding the GIL
    impl Iterator for PyPerspectiveIterator {
        type Item = Perspective;
        fn next(&mut self) -> Option<Self::Item> {
            Python::with_gil(|py| {
                let item = self.iter.as_ref(py).next()?.ok()?;
                Some(item.extract::<PyPerspective>().ok()?.into())
            })
        }
    }

    let result = match perspectives.extract::<PyPerspectiveSet>() {
        Ok(perspective_set) => slf.through_perspectives(perspective_set.ps),
        Err(_) => {
            let iter = PyPerspectiveIterator {
                iter: Py::from(perspectives.iter()?),
            };
            slf.through_iter(Box::new(iter))
        }
    };
    Ok(result)
}

pub(crate) fn adapt_err_value<E>(err: &E) -> PyErr
where
    E: Error + ?Sized,
{
    let error_log = display_error_chain::DisplayErrorChain::new(err).to_string();
    PyException::new_err(error_log)
}

pub fn adapt_result<U, E>(result: Result<U, E>) -> PyResult<U>
where
    E: Error,
{
    result.map_err(|e| adapt_err_value(&e))
}
