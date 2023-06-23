//! Helper functions for the Python bindings.
//!
//! This module contains helper functions for the Python bindings.
//! These functions are not part of the public API and are not exported to the Python module.
use crate::core::time::error::ParseTimeError;
use crate::core::time::{Interval, TryIntoTime};
use crate::core::vertex::InputVertex;
use crate::core::vertex_ref::VertexRef;
use crate::db::view_api::*;
use crate::python::vertex::PyVertex;
use chrono::NaiveDateTime;
use pyo3::exceptions::{PyException, PyTypeError};
use pyo3::prelude::*;
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
    t_start: Option<&PyAny>,
    t_end: Option<&PyAny>,
) -> PyResult<T::WindowedViewType> {
    let t_start = t_start.map(|t| extract_time(t)).transpose()?;
    let t_end = t_end.map(|t| extract_time(t)).transpose()?;
    Ok(slf.window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX)))
}

pub(crate) fn at_impl<T: TimeOps + Sized + Clone>(
    slf: &T,
    end: &PyAny,
) -> PyResult<T::WindowedViewType> {
    let end = extract_time(end)?;
    Ok(slf.at(end))
}

pub fn adapt_err_value<E>(err: &E) -> PyErr
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

pub(crate) fn expanding_impl<T>(slf: &T, step: &PyAny) -> PyResult<WindowSet<T>>
where
    T: TimeOps + Clone + Sync + Send + 'static,
    T::WindowedViewType: IntoPy<PyObject> + Send + Sync,
{
    let step = extract_interval(step)?;
    adapt_result(slf.expanding(step))
}

pub(crate) fn rolling_impl<T>(
    slf: &T,
    window: &PyAny,
    step: Option<&PyAny>,
) -> PyResult<PyWindowSet>
where
    T: TimeOps + Clone + Sync + Send + 'static,
    T::WindowedViewType: IntoPy<PyObject> + Send + Sync,
{
    let window = extract_interval(window)?;
    let step = step.map(extract_interval).transpose()?;
    let window_set: WindowSet<T> = adapt_result(slf.rolling(window, step))?;
    Ok(window_set.into())
}

fn parse_email_timestamp(timestamp: &str) -> PyResult<i64> {
    Python::with_gil(|py| {
        let email_utils = PyModule::import(py, "email.utils")?;
        let datetime = email_utils.call_method1("parsedate_to_datetime", (timestamp,))?;
        let py_seconds = datetime.call_method1("timestamp", ())?;
        let seconds = py_seconds.extract::<f64>()?;
        Ok(seconds as i64 * 1000)
    })
}

pub(crate) fn extract_time(time: &PyAny) -> PyResult<i64> {
    let from_number = time.extract::<i64>().map(Ok);
    let from_str = time.extract::<&str>().map(|str| {
        str.try_into_time()
            .or_else(|e| parse_email_timestamp(str).map_err(|_| e))
    });

    let mut extract_results = vec![from_number, from_str].into_iter();
    let first_valid_extraction = extract_results
        .find_map(|result| match result {
            Ok(val) => Some(Ok(val)),
            Err(_) => None,
        })
        .unwrap_or_else(|| {
            let message = format!("time '{time}' must be a str or an int");
            Err(PyTypeError::new_err(message))
        })?;

    adapt_result(first_valid_extraction)
}

pub(crate) fn extract_into_time(time: &PyAny) -> PyResult<TimeBox> {
    let string = time.extract::<String>();
    let result = string.map(|string| {
        let timestamp = string.as_str();
        let parsing_result = timestamp
            .try_into_time()
            .or_else(|e| parse_email_timestamp(timestamp).map_err(|_| e));
        TimeBox::new(parsing_result)
    });

    let result = result.or_else(|_| {
        let number = time.extract::<i64>();
        number.map(|number| TimeBox::new(number.try_into_time()))
    });

    let result = result.or_else(|_| {
        let parsed_datetime = time.extract::<NaiveDateTime>();
        parsed_datetime.map(|parsed_datetime| TimeBox::new(parsed_datetime.try_into_time()))
    });

    result.map_err(|_| {
        let message = format!("time '{time}' must be a str, dt or an integer");
        PyTypeError::new_err(message)
    })
}

pub(crate) struct TimeBox {
    parsing_result: Result<i64, ParseTimeError>,
}

impl TimeBox {
    fn new(parsing_result: Result<i64, ParseTimeError>) -> Self {
        Self { parsing_result }
    }
}

impl TryIntoTime for TimeBox {
    fn try_into_time(self) -> Result<i64, ParseTimeError> {
        self.parsing_result
    }
}

pub(crate) fn extract_interval(interval: &PyAny) -> PyResult<IntervalBox> {
    let string = interval.extract::<String>();
    let result = string.map(|string| IntervalBox::new(string.as_str()));

    let result = result.or_else(|_| {
        let number = interval.extract::<u64>();
        number.map(IntervalBox::new)
    });

    result.map_err(|_| {
        let message = format!("interval '{interval}' must be a str or an unsigned integer");
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

impl<'source> FromPyObject<'source> for IntervalBox {
    fn extract(interval: &'source PyAny) -> PyResult<Self> {
        let string = interval.extract::<String>();
        let result = string.map(|string| IntervalBox::new(string.as_str()));

        let result = result.or_else(|_| {
            let number = interval.extract::<u64>();
            number.map(IntervalBox::new)
        });

        result.map_err(|_| {
            let message = format!("interval '{interval}' must be a str or an unsigned integer");
            PyTypeError::new_err(message)
        })
    }
}

impl TryFrom<IntervalBox> for Interval {
    type Error = ParseTimeError;
    fn try_from(value: IntervalBox) -> Result<Self, Self::Error> {
        value.interval
    }
}

/// A trait for vertices that can be used as input for the graph.
/// This allows us to add vertices with different types of ids, either strings or ints.
#[derive(Clone, Debug)]
pub struct InputVertexBox {
    id: u64,
    name: Option<String>,
}

/// Implementation for vertices that can be used as input for the graph.
/// This allows us to add vertices with different types of ids, either strings or ints.
impl InputVertexBox {
    pub(crate) fn new<T>(vertex: T) -> InputVertexBox
    where
        T: InputVertex,
    {
        InputVertexBox {
            id: vertex.id(),
            name: vertex.id_str().map(|s| s.into()),
        }
    }
}

/// Implementation for vertices that can be used as input for the graph.
/// This allows us to add vertices with different types of ids, either strings or ints.
impl InputVertex for InputVertexBox {
    /// Returns the id of the vertex.
    fn id(&self) -> u64 {
        self.id
    }

    /// Returns the name property of the vertex.
    fn id_str(&self) -> Option<&str> {
        match &self.name {
            Some(n) => Some(n),
            None => None,
        }
    }
}

pub(crate) fn extract_input_vertex(id: &PyAny) -> PyResult<InputVertexBox> {
    match id.extract::<String>() {
        Ok(string) => Ok(InputVertexBox::new(string)),
        Err(_) => {
            let msg = "IDs need to be strings or an unsigned integers";
            let number = id.extract::<u64>().map_err(|_| PyTypeError::new_err(msg))?;
            Ok(InputVertexBox::new(number))
        }
    }
}

pub trait WindowSetOps {
    fn build_iter(&self) -> PyGenericIterator;
    fn time_index(&self, center: bool) -> PyGenericIterable;
}

impl<T> WindowSetOps for WindowSet<T>
where
    T: TimeOps + Clone + Sync + 'static + Send,
    T::WindowedViewType: IntoPy<PyObject> + Send,
{
    fn build_iter(&self) -> PyGenericIterator {
        self.clone().into()
    }

    fn time_index(&self, center: bool) -> PyGenericIterable {
        let window_set = self.clone();

        if window_set.temporal() {
            let iterable = move || {
                let iter: Box<dyn Iterator<Item = NaiveDateTime> + Send> = Box::new(
                    window_set
                        .clone()
                        .time_index(center)
                        .map(|epoch| NaiveDateTime::from_timestamp_millis(epoch).unwrap()),
                );
                iter
            };
            iterable.into()
        } else {
            (move || {
                let iter: Box<dyn Iterator<Item = i64> + Send> =
                    Box::new(window_set.time_index(center));
                iter
            })
            .into()
        }
    }
}

#[pyclass(name = "WindowSet")]
pub struct PyWindowSet {
    window_set: Box<dyn WindowSetOps + Send>,
}

impl<T> From<WindowSet<T>> for PyWindowSet
where
    T: TimeOps + Clone + Sync + Send + 'static,
    T::WindowedViewType: IntoPy<PyObject> + Send + Sync,
{
    fn from(value: WindowSet<T>) -> Self {
        Self {
            window_set: Box::new(value),
        }
    }
}

impl<T> IntoPy<PyObject> for WindowSet<T>
where
    T: TimeOps + Clone + Sync + Send + 'static,
    T::WindowedViewType: IntoPy<PyObject> + Send + Sync,
{
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyWindowSet::from(self).into_py(py)
    }
}

#[pymethods]
impl PyWindowSet {
    fn __iter__(&self) -> PyGenericIterator {
        self.window_set.build_iter()
    }

    /// Returns the time index of this window set
    ///
    /// It uses the last time of each window as the reference or the center of each if `center` is
    /// set to `True`
    ///
    /// Arguments:
    ///     center (bool): if True time indexes are centered. Defaults to False
    ///
    /// Returns:
    ///     Iterable: the time index"
    #[pyo3(signature = (center=false))]
    fn time_index(&self, center: bool) -> PyGenericIterable {
        self.window_set.time_index(center)
    }
}

#[pyclass(name = "Iterable")]
pub struct PyGenericIterable {
    build_iter: Box<dyn Fn() -> Box<dyn Iterator<Item = PyObject> + Send> + Send>,
}

impl<F, I, T> From<F> for PyGenericIterable
where
    F: (Fn() -> I) + Send + Sync + 'static,
    I: Iterator<Item = T> + Send + 'static,
    T: IntoPy<PyObject> + 'static,
{
    fn from(value: F) -> Self {
        let build_py_iter: Box<dyn Fn() -> Box<dyn Iterator<Item = PyObject> + Send> + Send> =
            Box::new(move || Box::new(value().map(|item| Python::with_gil(|py| item.into_py(py)))));
        Self {
            build_iter: build_py_iter,
        }
    }
}

#[pymethods]
impl PyGenericIterable {
    fn __iter__(&self) -> PyGenericIterator {
        (self.build_iter)().into()
    }
}

#[pyclass(name = "Iterator")]
pub struct PyGenericIterator {
    iter: Box<dyn Iterator<Item = PyObject> + Send>,
}

impl<I, T> From<I> for PyGenericIterator
where
    I: Iterator<Item = T> + Send + 'static,
    T: IntoPy<PyObject> + 'static,
{
    fn from(value: I) -> Self {
        let py_iter = Box::new(value.map(|item| Python::with_gil(|py| item.into_py(py))));
        Self { iter: py_iter }
    }
}

#[pymethods]
impl PyGenericIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&mut self) -> Option<PyObject> {
        self.iter.next()
    }
}

pub(crate) trait IntoPyObject {
    fn into_py_object(self) -> PyObject;
}
