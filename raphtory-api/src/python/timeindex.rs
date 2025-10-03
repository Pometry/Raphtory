use crate::core::{
    storage::timeindex::{AsTime, EventTime, TimeError},
    utils::time::{
        InputTime, IntoTime, ParseTimeError, TryIntoInputTime, TryIntoTime, TryIntoTimeNeedsEventId,
    },
};
use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
use pyo3::{
    basic::CompareOp,
    exceptions::{PyException, PyRuntimeError, PyTypeError},
    prelude::*,
    types::{PyDateTime, PyList, PyTuple},
};
use serde::Serialize;
use std::hash::{DefaultHasher, Hash, Hasher};

impl<'py> IntoPyObject<'py> for EventTime {
    type Target = PyEventTime;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyEventTime::from(self).into_pyobject(py)
    }
}

impl<'source> FromPyObject<'source> for EventTime {
    fn extract_bound(time: &Bound<'source, PyAny>) -> PyResult<Self> {
        InputTime::extract_bound(time).map(|input_time| input_time.as_time())
    }
}

/// Components that can make an EventTime. They can be used as the event id as well.
/// Extract them from Python as individual components so we can support tuples for EventTime.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct EventTimeComponent {
    component: i64,
}

impl IntoTime for EventTimeComponent {
    fn into_time(self) -> EventTime {
        EventTime::from(self.component)
    }
}

impl TryIntoTimeNeedsEventId for EventTimeComponent {}

impl EventTimeComponent {
    pub fn new(component: i64) -> Self {
        Self { component }
    }

    pub fn t(&self) -> i64 {
        self.component
    }
}

impl<'source> FromPyObject<'source> for EventTimeComponent {
    fn extract_bound(component: &Bound<'source, PyAny>) -> PyResult<Self> {
        extract_time_index_component(component).map_err(|e| match e {
            ParsingError::Matched(err) => err,
            ParsingError::Unmatched => {
                let message = format!(
                    "Time component '{component}' must be a str, datetime, float, or an integer."
                );
                PyTypeError::new_err(message)
            }
        })
    }
}
enum ParsingError {
    Matched(PyErr),
    Unmatched,
}

fn extract_time_index_component<'source>(
    component: &Bound<'source, PyAny>,
) -> Result<EventTimeComponent, ParsingError> {
    if let Ok(string) = component.extract::<String>() {
        let timestamp = string.as_str();
        let parsing_result = timestamp.try_into_time().or_else(|e| {
            parse_email_timestamp(timestamp).map_err(|_| ParsingError::Matched(e.into()))
        })?;
        return Ok(EventTimeComponent::new(parsing_result.0));
    }
    if let Ok(number) = component.extract::<i64>() {
        return Ok(EventTimeComponent::new(number));
    }
    if let Ok(float_time) = component.extract::<f64>() {
        // seconds since Unix epoch as returned by python `timestamp`
        let float_ms = float_time * 1000.0;
        let float_ms_trunc = float_ms.round();
        let rel_err = (float_ms - float_ms_trunc).abs() / (float_ms.abs() + f64::EPSILON);
        if rel_err > 4.0 * f64::EPSILON {
            return Err(ParsingError::Matched(PyRuntimeError::new_err(
                "Float timestamps with more than millisecond precision are not supported.",
            )));
        }
        return Ok(EventTimeComponent::new(float_ms_trunc as i64));
    }
    if let Ok(parsed_datetime) = component.extract::<DateTime<FixedOffset>>() {
        return Ok(EventTimeComponent::new(parsed_datetime.timestamp_millis()));
    }
    if let Ok(parsed_datetime) = component.extract::<NaiveDateTime>() {
        // Important, this is needed to prevent inconsistencies by ensuring that naive DateTime objects are always treated as UTC and not local time.
        // TimeIndexEntry and History objects use UTC so everything should be extracted as UTC.
        return Ok(EventTimeComponent::new(
            parsed_datetime.and_utc().timestamp_millis(),
        ));
    }
    if let Ok(py_datetime) = component.downcast::<PyDateTime>() {
        let time = (py_datetime
            .call_method0("timestamp")
            .map_err(ParsingError::Matched)?
            .extract::<f64>()
            .map_err(ParsingError::Matched)?
            * 1000.0) as i64;
        return Ok(EventTimeComponent::new(time));
    }
    Err(ParsingError::Unmatched)
}

fn parse_email_timestamp(timestamp: &str) -> PyResult<EventTime> {
    Python::with_gil(|py| {
        let email_utils = PyModule::import(py, "email.utils")?;
        let datetime = email_utils.call_method1("parsedate_to_datetime", (timestamp,))?;
        let py_seconds = datetime.call_method1("timestamp", ())?;
        let seconds = py_seconds.extract::<f64>()?;
        Ok(EventTime::from(seconds as i64 * 1000))
    })
}

/// Raphtory’s EventTime.
/// Represents a unique timepoint in the graph’s history as (epoch, event_id).
///
/// - epoch: timestamp in milliseconds since the Unix epoch.
/// - event_id: id used for ordering between equal timestamps.
///
/// Unless specified manually, the event ids are generated automatically by Raphtory to
/// maintain a unique ordering of events.
/// EventTime can be converted into a Unix Epoch timestamp or a Python datetime, and compared
/// either by timestamp (against ints/floats/datetimes/strings), by tuple of (epoch, event_id),
/// or against another EventTime.
#[pyclass(name = "EventTime", module = "raphtory", frozen)]
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Ord, PartialOrd, Eq)]
pub struct PyEventTime {
    time: EventTime,
}

impl PyEventTime {
    pub fn inner(&self) -> EventTime {
        self.time
    }

    pub fn new(time: EventTime) -> Self {
        Self { time }
    }

    pub const MIN: PyEventTime = PyEventTime {
        time: EventTime::MIN,
    };
    pub const MAX: PyEventTime = PyEventTime {
        time: EventTime::MAX,
    };
}

#[pymethods]
impl PyEventTime {
    /// Returns the UTC datetime representation of this EventTime's timestamp.
    ///
    /// Returns:
    ///     datetime: The UTC datetime.
    ///
    /// Raises:
    ///     TimeError: Returns TimeError on timestamp conversion errors (e.g. out-of-range timestamp).
    #[getter]
    pub fn dt(&self) -> Result<DateTime<Utc>, TimeError> {
        self.time.dt()
    }

    /// Returns the event id used to order events within the same timestamp.
    ///
    /// Returns:
    ///     int: The event id.
    #[getter]
    pub fn event_id(&self) -> usize {
        self.time.i()
    }

    /// Returns the timestamp in milliseconds since the Unix epoch.
    ///
    /// Returns:
    ///     int: Milliseconds since the Unix epoch.
    #[getter]
    pub fn t(&self) -> i64 {
        self.time.t()
    }

    /// Return this entry as a tuple of (epoch, event_id), where the epoch is in milliseconds.
    ///
    /// Returns:
    ///     tuple[int,int]: (epoch, event_id).
    #[getter]
    pub fn as_tuple(&self) -> (i64, usize) {
        self.time.as_tuple()
    }

    pub fn __richcmp__(&self, other: &Bound<PyAny>, op: CompareOp) -> PyResult<bool> {
        // extract TimeIndexComponent first. If we're dealing with a single i64 (or something that can be converted to an i64), we only compare timestamps
        if let Ok(component) = other.extract::<EventTimeComponent>() {
            match op {
                CompareOp::Eq => Ok(self.t() == component.t()),
                CompareOp::Ne => Ok(self.t() != component.t()),
                CompareOp::Gt => Ok(self.t() > component.t()),
                CompareOp::Lt => Ok(self.t() < component.t()),
                CompareOp::Ge => Ok(self.t() >= component.t()),
                CompareOp::Le => Ok(self.t() <= component.t()),
            }
        // If an EventTime was passed, we then compare the event id
        } else if let Ok(time_index) = other.extract::<EventTime>() {
            match op {
                CompareOp::Eq => Ok(self.time == time_index),
                CompareOp::Ne => Ok(self.time != time_index),
                CompareOp::Gt => Ok(self.time > time_index),
                CompareOp::Lt => Ok(self.time < time_index),
                CompareOp::Ge => Ok(self.time >= time_index),
                CompareOp::Le => Ok(self.time <= time_index),
            }
        } else {
            Err(PyTypeError::new_err("Unsupported comparison: EventTime can only be compared with a str, datetime, float, integer, a tuple/list of two of those types, or another EventTime."))
        }
    }

    pub fn __repr__(&self) -> String {
        format!("EventTime({}, {})", self.time.0, self.time.1)
    }

    pub fn __hash__(&self) -> isize {
        let mut hasher = DefaultHasher::new();
        self.time.hash(&mut hasher);
        hasher.finish() as isize
    }

    pub fn __int__(&self) -> i64 {
        self.t()
    }

    /// Creates a new EventTime.
    ///
    /// Arguments:
    ///     timestamp (int | float | datetime | str): A time input convertible to an EventTime.
    ///     event_id (int | float | datetime | str | None): Optionally, specify the event id. Defaults to 0.
    ///
    /// Returns:
    ///     EventTime:
    #[new]
    #[pyo3(signature = (timestamp, event_id=None))]
    pub fn py_new(timestamp: EventTimeComponent, event_id: Option<EventTimeComponent>) -> Self {
        let event_id = event_id.map(|t| t.t() as usize).unwrap_or(0);
        Self {
            time: EventTime::new(timestamp.t(), event_id),
        }
    }
}

impl IntoTime for PyEventTime {
    fn into_time(self) -> EventTime {
        self.time
    }
}

impl TryIntoInputTime for PyEventTime {
    fn try_into_input_time(self) -> Result<InputTime, ParseTimeError> {
        Ok(InputTime::Indexed(self.t(), self.event_id()))
    }
}

impl From<EventTime> for PyEventTime {
    fn from(time: EventTime) -> Self {
        Self { time }
    }
}

impl From<PyEventTime> for EventTime {
    fn from(value: PyEventTime) -> Self {
        value.inner()
    }
}

impl<'source> FromPyObject<'source> for InputTime {
    fn extract_bound(input: &Bound<'source, PyAny>) -> PyResult<Self> {
        if let Ok(py_time) = input.downcast::<PyEventTime>() {
            return Ok(py_time.get().try_into_input_time()?);
        }
        // Handle list/tuple case: [timestamp, event_id]
        if input.downcast::<PyTuple>().is_ok() || input.downcast::<PyList>().is_ok() {
            let py = input.py();
            if let Ok(items) = input.extract::<Vec<PyObject>>() {
                let len = items.len();
                if len != 2 {
                    return Err(PyTypeError::new_err(format!(
                        "List/tuple for time input must have exactly 2 elements [timestamp, event_id], got {} elements.",
                        len
                    )));
                }
                let first = items[0].bind(py);
                let second = items[1].bind(py);
                let first_entry = extract_time_index_component(first).map_err(|e| match e {
                    ParsingError::Matched(err) => err,
                    ParsingError::Unmatched => {
                        let message = format!("Time component '{first}' must be a str, datetime, float, or an integer.");
                        PyTypeError::new_err(message)
                    }
                })?;
                let second_entry = extract_time_index_component(second).map_err(|e| match e {
                    ParsingError::Matched(err) => err,
                    ParsingError::Unmatched => {
                        let message = format!("Time component '{second}' must be a str, datetime, float, or an integer.");
                        PyTypeError::new_err(message)
                    }
                })?;
                return Ok(InputTime::Indexed(
                    first_entry.t(),
                    second_entry.t() as usize,
                ));
            }
        }
        // allow errors from EventTimeComponent extraction to pass through (except if no type has matched at all)
        match extract_time_index_component(input) {
            Ok(component) => Ok(InputTime::Simple(component.t())),
            Err(ParsingError::Matched(err)) => Err(err),
            Err(ParsingError::Unmatched) => {
                let message = format!("Time '{input}' must be a str, datetime, float, integer, or a tuple/list of two of those types.");
                Err(PyTypeError::new_err(message))
            }
        }
    }
}

impl From<TimeError> for PyErr {
    fn from(err: TimeError) -> Self {
        PyException::new_err(err.to_string())
    }
}
