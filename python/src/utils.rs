//! Helper functions for the Python bindings.
//!
//! This module contains helper functions for the Python bindings.
//! These functions are not part of the public API and are not exported to the Python module.
use crate::vertex::PyVertex;
use pyo3::exceptions::{PyException, PyTypeError};
use pyo3::prelude::*;
use raphtory::core::tgraph::VertexRef;
use raphtory::core::time::error::ParseTimeError;
use raphtory::core::time::Interval;
use raphtory::db::view_api::time::WindowSet;
use raphtory::db::view_api::TimeOps;
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

pub(crate) fn adapt_err_value<E>(err: &E) -> PyErr
where
    E: Error + ?Sized,
{
    let error_log = display_error_chain::DisplayErrorChain::new(err).to_string();
    PyException::new_err(error_log)
}

pub fn adapt_result<U, E>(result: Result<U, E>) -> PyResult<U>
// TODO: make this private
where
    E: Error,
{
    result.map_err(|e| adapt_err_value(&e))
}

pub(crate) fn expanding_impl<T, O>(slf: &T, step: &PyAny) -> PyResult<O>
where
    T: TimeOps + Clone + 'static,
    O: From<WindowSet<T>>,
{
    let step = extract_interval(step)?;
    adapt_result(slf.expanding(step)).map(|iter| iter.into())
}

pub(crate) fn rolling_impl<T, O>(slf: &T, window: &PyAny, step: Option<&PyAny>) -> PyResult<O>
where
    T: TimeOps + Clone + 'static,
    O: From<WindowSet<T>>,
{
    let window = extract_interval(window)?;
    let step = step.map(|step| extract_interval(step)).transpose()?;
    adapt_result(slf.rolling(window, step)).map(|iter| iter.into())
}

pub(crate) fn extract_interval(interval: &PyAny) -> PyResult<IntervalBox> {
    let string = interval.extract::<String>();
    let result = string.map(|string| IntervalBox::new(string.as_str()));

    let result = result.or_else(|_| {
        let number = interval.extract::<u64>();
        number.map(|number| IntervalBox::new(number))
    });

    result.map_err(|_| {
        let message = format!("interval '{interval}' must be a str or an unsigned number");
        PyTypeError::new_err(message)
    })
}

pub(crate) struct IntervalBox {
    interval: Result<Interval, ParseTimeError>,
}

impl IntervalBox {
    fn new<I>(interval: I) -> Self
    where
        I: TryInto<Interval, Error = ParseTimeError>,
    {
        Self {
            interval: interval.try_into(),
        }
    }
}

impl TryFrom<IntervalBox> for Interval {
    type Error = ParseTimeError;
    fn try_from(value: IntervalBox) -> Result<Self, Self::Error> {
        value.interval
    }
}
