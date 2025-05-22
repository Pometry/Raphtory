use crate::core::storage::timeindex::{AsTime, TimeIndexEntry};
use chrono::{DateTime, Utc};
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, PyErr, Python};

impl<'py> IntoPyObject<'py> for TimeIndexEntry {
    type Target = PyRaphtoryTime;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyRaphtoryTime::from(self).into_pyobject(py)
    }
}

#[pyclass(name = "RaphtoryTime", module = "raphtory", frozen)]
pub struct PyRaphtoryTime {
    time: TimeIndexEntry,
}

#[pymethods]
impl PyRaphtoryTime {
    /// Get the datetime representation of the time
    pub fn dt(&self) -> Option<DateTime<Utc>> {
        self.time.dt()
    }

    /// Get the epoch timestamp of the time
    pub fn epoch(&self) -> i64 {
        self.time.t()
    }

    pub fn __repr__(&self) -> String {
        format!("TimeIndexEntry[{}, {}]", self.time.0, self.time.1)
    }
}

impl From<TimeIndexEntry> for PyRaphtoryTime {
    fn from(time: TimeIndexEntry) -> Self {
        Self { time }
    }
}
