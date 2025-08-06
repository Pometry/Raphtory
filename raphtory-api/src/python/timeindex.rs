use crate::core::{
    storage::timeindex::{AsTime, TimeError, TimeIndexEntry},
    utils::time::{IntoTime, TryIntoTime, TryIntoTimeNeedsSecondaryIndex},
};
use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
use pyo3::{
    exceptions::{PyException, PyRuntimeError, PyTypeError},
    prelude::*,
    types::PyDateTime,
};
use serde::Serialize;

impl<'py> IntoPyObject<'py> for TimeIndexEntry {
    type Target = PyTime;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyTime::from(self).into_pyobject(py)
    }
}

impl<'source> FromPyObject<'source> for TimeIndexEntry {
    fn extract_bound(time: &Bound<'source, PyAny>) -> PyResult<Self> {
        if let Ok(string) = time.extract::<String>() {
            let timestamp = string.as_str();
            let parsing_result = timestamp
                .try_into_time()
                .or_else(|e| parse_email_timestamp(timestamp).map_err(|_| e))?;
            return Ok(parsing_result);
        }
        if let Ok(number) = time.extract::<i64>() {
            return Ok(TimeIndexEntry::from(number));
        }
        if let Ok(float_time) = time.extract::<f64>() {
            // seconds since Unix epoch as returned by python `timestamp`
            let float_ms = float_time * 1000.0;
            let float_ms_trunc = float_ms.round();
            let rel_err = (float_ms - float_ms_trunc).abs() / (float_ms.abs() + f64::EPSILON);
            if rel_err > 4.0 * f64::EPSILON {
                return Err(PyRuntimeError::new_err(
                    "Float timestamps with more than millisecond precision are not supported.",
                ));
            }
            return Ok(TimeIndexEntry::from(float_ms_trunc as i64));
        }
        // Handle list/tuple case: [timestamp, secondary_index]
        if let Ok(seq) = time.extract::<Vec<i64>>() {
            if seq.len() == 2 {
                return Ok(TimeIndexEntry::new(seq[0], seq[1] as usize));
            } else {
                return Err(PyTypeError::new_err(format!(
                    "List/tuple for TimeIndexEntry must have exactly 2 elements [timestamp, secondary_index], got {} elements",
                    seq.len()
                )));
            }
        }
        if let Ok(parsed_datetime) = time.extract::<DateTime<FixedOffset>>() {
            return Ok(TimeIndexEntry::from(parsed_datetime.timestamp_millis()));
        }
        if let Ok(parsed_datetime) = time.extract::<NaiveDateTime>() {
            // Important, this is needed to ensure that naive DateTime objects are treated as UTC and not local time
            return Ok(TimeIndexEntry::from(
                parsed_datetime.and_utc().timestamp_millis(),
            ));
        }
        if let Ok(py_datetime) = time.downcast::<PyDateTime>() {
            let time = (py_datetime.call_method0("timestamp")?.extract::<f64>()? * 1000.0) as i64;
            return Ok(TimeIndexEntry::from(time));
        }
        if let Ok(py_time) = time.downcast::<PyTime>() {
            return Ok(py_time.get().inner());
        }
        let message = format!("time '{time}' must be a str, datetime, float, or an integer");
        Err(PyTypeError::new_err(message))
    }
}

fn parse_email_timestamp(timestamp: &str) -> PyResult<TimeIndexEntry> {
    Python::with_gil(|py| {
        let email_utils = PyModule::import(py, "email.utils")?;
        let datetime = email_utils.call_method1("parsedate_to_datetime", (timestamp,))?;
        let py_seconds = datetime.call_method1("timestamp", ())?;
        let seconds = py_seconds.extract::<f64>()?;
        Ok(TimeIndexEntry::from(seconds as i64 * 1000))
    })
}

#[pyclass(name = "RaphtoryTime", module = "raphtory", frozen, eq, ord)]
#[derive(Debug, Clone, Serialize, PartialEq, Ord, PartialOrd, Eq)]
pub struct PyTime {
    time: TimeIndexEntry,
}

impl PyTime {
    pub fn new(time: TimeIndexEntry) -> Self {
        Self { time }
    }

    pub fn inner(&self) -> TimeIndexEntry {
        self.time
    }

    pub const MIN: PyTime = PyTime {
        time: TimeIndexEntry::MIN,
    };
    pub const MAX: PyTime = PyTime {
        time: TimeIndexEntry::MAX,
    };
}

#[pymethods]
impl PyTime {
    /// Get the datetime representation of the time
    #[getter]
    pub fn dt(&self) -> Result<DateTime<Utc>, TimeError> {
        self.time.dt()
    }

    #[getter]
    pub fn secondary_index(&self) -> usize {
        self.time.i()
    }

    /// Get the epoch timestamp of the time
    #[getter]
    pub fn t(&self) -> i64 {
        self.time.t()
    }

    pub fn __repr__(&self) -> String {
        format!("TimeIndexEntry[{}, {}]", self.time.0, self.time.1)
    }

    // TODO: Might wanna remove this later
    #[staticmethod]
    pub fn create(t: i64, s: usize) -> Self {
        Self {
            time: TimeIndexEntry::new(t, s),
        }
    }
}

impl IntoTime for PyTime {
    fn into_time(self) -> TimeIndexEntry {
        self.time
    }
}

impl TryIntoTimeNeedsSecondaryIndex for PyTime {}

impl From<TimeIndexEntry> for PyTime {
    fn from(time: TimeIndexEntry) -> Self {
        Self { time }
    }
}

impl From<PyTime> for TimeIndexEntry {
    fn from(value: PyTime) -> Self {
        value.inner()
    }
}

impl From<TimeError> for PyErr {
    fn from(err: TimeError) -> Self {
        PyException::new_err(err.to_string())
    }
}
