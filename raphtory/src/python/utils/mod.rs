//! Helper functions for the Python bindings.
//!
//! This module contains helper functions for the Python bindings.
//! These functions are not part of the public API and are not exported to the Python module.
use crate::{
    core::{
        entities::vertices::{input_vertex::InputVertex, vertex_ref::VertexRef},
        utils::time::{error::ParseTimeError, Interval, IntoTime, TryIntoTime},
    },
    db::api::view::*,
    python::graph::vertex::PyVertex,
};
use chrono::NaiveDateTime;
use pyo3::{exceptions::PyTypeError, prelude::*};

pub mod errors;

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
impl<'source> FromPyObject<'source> for VertexRef {
    fn extract(vref: &'source PyAny) -> PyResult<Self> {
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

pub struct PyTime {
    parsing_result: i64,
}

impl<'source> FromPyObject<'source> for PyTime {
    fn extract(time: &'source PyAny) -> PyResult<Self> {
        if let Ok(string) = time.extract::<String>() {
            let timestamp = string.as_str();
            let parsing_result = timestamp
                .try_into_time()
                .or_else(|e| parse_email_timestamp(timestamp).map_err(|_| e))?;
            return Ok(PyTime::new(parsing_result));
        }
        if let Ok(number) = time.extract::<i64>() {
            return Ok(PyTime::new(number.try_into_time()?));
        }
        if let Ok(parsed_datetime) = time.extract::<NaiveDateTime>() {
            return Ok(PyTime::new(parsed_datetime.try_into_time()?));
        }
        let message = format!("time '{time}' must be a str, dt or an integer");
        Err(PyTypeError::new_err(message))
    }
}

impl PyTime {
    fn new(parsing_result: i64) -> Self {
        Self { parsing_result }
    }
    pub const MIN: PyTime = PyTime {
        parsing_result: i64::MIN,
    };
    pub const MAX: PyTime = PyTime {
        parsing_result: i64::MAX,
    };
}

impl IntoTime for PyTime {
    fn into_time(self) -> i64 {
        self.parsing_result
    }
}

pub(crate) struct PyInterval {
    interval: Result<Interval, ParseTimeError>,
}

impl PyInterval {
    fn new<I>(interval: I) -> Self
    where
        I: TryInto<Interval, Error = ParseTimeError>,
    {
        Self {
            interval: interval.try_into(),
        }
    }
}

impl<'source> FromPyObject<'source> for PyInterval {
    fn extract(interval: &'source PyAny) -> PyResult<Self> {
        let string = interval.extract::<String>();
        let result = string.map(|string| PyInterval::new(string.as_str()));

        let result = result.or_else(|_| {
            let number = interval.extract::<u64>();
            number.map(PyInterval::new)
        });

        result.map_err(|_| {
            let message = format!("interval '{interval}' must be a str or an unsigned integer");
            PyTypeError::new_err(message)
        })
    }
}

impl TryFrom<PyInterval> for Interval {
    type Error = ParseTimeError;
    fn try_from(value: PyInterval) -> Result<Self, Self::Error> {
        value.interval
    }
}

/// A trait for vertices that can be used as input for the graph.
/// This allows us to add vertices with different types of ids, either strings or ints.
#[derive(Clone, Debug)]
pub struct PyInputVertex {
    id: u64,
    name: Option<String>,
}

impl<'source> FromPyObject<'source> for PyInputVertex {
    fn extract(id: &'source PyAny) -> PyResult<Self> {
        match id.extract::<String>() {
            Ok(string) => Ok(PyInputVertex::new(string)),
            Err(_) => {
                let msg = "IDs need to be strings or an unsigned integers";
                let number = id.extract::<u64>().map_err(|_| PyTypeError::new_err(msg))?;
                Ok(PyInputVertex::new(number))
            }
        }
    }
}

/// Implementation for vertices that can be used as input for the graph.
/// This allows us to add vertices with different types of ids, either strings or ints.
impl PyInputVertex {
    pub(crate) fn new<T>(vertex: T) -> PyInputVertex
    where
        T: InputVertex,
    {
        PyInputVertex {
            id: vertex.id(),
            name: vertex.id_str().map(|s| s.into()),
        }
    }
}

/// Implementation for vertices that can be used as input for the graph.
/// This allows us to add vertices with different types of ids, either strings or ints.
impl InputVertex for PyInputVertex {
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
