use crate::vertex::PyVertex;
use crate::wrappers::{PyPerspective, PyPerspectiveSet};
use docbrown_core::tgraph::VertexRef;
use docbrown_db::graph_window::WindowSet;
use docbrown_db::perspective::Perspective;
use docbrown_db::view_api::TimeOps;
use pyo3::exceptions::{PyException, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyIterator;
use std::error::Error;

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
