use crate::core::storage::timeindex::{AsTime, TimeError, TimeIndexEntry};
use chrono::{DateTime, Utc};
use pyo3::{exceptions::PyException, prelude::*};

impl<'py> IntoPyObject<'py> for TimeIndexEntry {
    type Target = PyRaphtoryTime;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyRaphtoryTime::from(self).into_pyobject(py)
    }
}

impl<'source> FromPyObject<'source> for TimeIndexEntry {
    fn extract_bound(ob: &Bound<'source, PyAny>) -> PyResult<Self> {
        let py_time = ob.downcast::<PyRaphtoryTime>()?;
        Ok(py_time.get().inner())
    }
}

#[pyclass(name = "RaphtoryTime", module = "raphtory", frozen, eq, ord)]
#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq)]
pub struct PyRaphtoryTime {
    time: TimeIndexEntry,
}

impl PyRaphtoryTime {
    /// Get the internal TimeIndexEntry
    pub fn inner(&self) -> TimeIndexEntry {
        self.time
    }
}

#[pymethods]
impl PyRaphtoryTime {
    /// Get the datetime representation of the time
    pub fn dt(&self) -> Result<DateTime<Utc>, TimeError> {
        self.time.dt()
    }

    pub fn secondary_index(&self) -> usize {
        self.time.i()
    }

    /// Get the epoch timestamp of the time
    pub fn epoch(&self) -> i64 {
        self.time.t()
    }

    pub fn __repr__(&self) -> String {
        format!("TimeIndexEntry[{}, {}]", self.time.0, self.time.1)
    }

    // TODO: Might wanna remove this later
    #[staticmethod]
    pub fn new(t: i64, s: usize) -> Self {
        Self {
            time: TimeIndexEntry::new(t, s),
        }
    }
}

impl From<TimeIndexEntry> for PyRaphtoryTime {
    fn from(time: TimeIndexEntry) -> Self {
        Self { time }
    }
}

impl From<TimeError> for PyErr {
    fn from(err: TimeError) -> Self {
        PyException::new_err(err.to_string())
    }
}
