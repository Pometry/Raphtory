use crate::core::{
    storage::timeindex::{AsTime, TimeError, TimeIndexEntry},
    utils::time::{IntoTime, TryIntoTimeNeedsSecondaryIndex},
};
use chrono::{DateTime, Utc};
use pyo3::{exceptions::PyException, prelude::*};
use serde::Serialize;

impl<'py> IntoPyObject<'py> for TimeIndexEntry {
    type Target = PyTime;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyTime::from(self).into_pyobject(py)
    }
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
    pub fn dt(&self) -> Result<DateTime<Utc>, TimeError> {
        self.time.dt()
    }

    pub fn secondary_index(&self) -> usize {
        self.time.i()
    }

    /// Get the epoch timestamp of the time
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
