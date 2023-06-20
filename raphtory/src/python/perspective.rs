//! This module defines the `PyPerspective`  struct and the `PyPerspectiveSet` iterator.
//!
//! `PyPerspective` is a simple struct representing a time range from `start` to `end`.
//! The start time is inclusive and the end time is exclusive.
//!
//! `PyPerspectiveSet` is an iterator over a range of time periods (`Perspective`s).
//! It can be used to generate rolling or expanding perspectives based on a `step` size and an optional `window` size.
//!
//! These perpectives are used when querying the graph to determine the time bounds.
use pyo3::{pyclass, pymethods};
use raphtory::db::perspective;
use raphtory::db::perspective::PerspectiveSet;
use std::i64;

/// A struct representing a time range from `start` to `end`.
///
/// The start time is inclusive and the end time is exclusive.
#[derive(Clone)]
#[pyclass(name = "Perspective")]
pub struct PyPerspective {
    pub start: Option<i64>,
    pub end: Option<i64>,
}

/// Representing a time range from `start` to `end` for a graph
#[pymethods]
impl PyPerspective {
    /// Creates a new `Perspective` with the given `start` and `end` times.
    /// Arguments:
    ///    start (int): The start time of the perspective. If None, the perspective will start at the beginning of the graph.
    ///     end (int): The end time of the perspective. If None, the perspective will end at the end of the graph.    
    ///
    /// Returns:
    ///    Perspective: A new perspective with the given start and end times.
    #[new]
    #[pyo3(signature = (start=None, end=None))]
    fn new(start: Option<i64>, end: Option<i64>) -> Self {
        PyPerspective { start, end }
    }

    /// Creates an `PyPerspectiveSet` with the given `step` size and optional `start` and `end` times,
    /// using an expanding window.
    ///
    /// An expanding window is a window that grows by `step` size at each iteration.
    ///
    /// Arguments:
    ///    step (int): The size of the step to take at each iteration.
    ///    start (int): The start time of the perspective. If None, the perspective will start at the beginning of the graph. (optional)
    ///    end (int): The end time of the perspective. If None, the perspective will end at the end of the graph. (optional)
    ///
    /// Returns:
    ///    PyPerspectiveSet: An iterator over a range of time periods (`Perspective`s).
    #[staticmethod]
    #[pyo3(signature = (step, start=None, end=None))]
    fn expanding(step: u64, start: Option<i64>, end: Option<i64>) -> PyPerspectiveSet {
        PyPerspectiveSet {
            ps: perspective::Perspective::expanding(step, start, end),
        }
    }

    /// Creates an `PerspectiveSet` with the given `window` size and optional `step`, `start` and `end` times,
    /// using a rolling window.
    ///
    /// A rolling window is a window that moves forward by `step` size at each iteration.
    /// If `step` is not provided, it defaults to the `window` size.
    ///
    /// Arguments:
    ///   window (int): The size of the window to use at each iteration.
    ///   step (int): The size of the step to take at each iteration. (optional)
    ///   start (int): The start time of the perspective. If None, the perspective will start at the beginning of the graph. (optional)
    ///   end (int): The end time of the perspective. If None, the perspective will end at the end of the graph. (optional)
    ///
    /// Returns:
    ///   PyPerspectiveSet: An iterator over a range of time periods (`Perspective`s).
    #[staticmethod]
    #[pyo3(signature = (window, step=None, start=None, end=None))]
    fn rolling(
        window: u64,
        step: Option<u64>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyPerspectiveSet {
        PyPerspectiveSet {
            ps: perspective::Perspective::rolling(window, step, start, end),
        }
    }
}

impl From<perspective::Perspective> for PyPerspective {
    fn from(value: perspective::Perspective) -> Self {
        PyPerspective {
            start: value.start,
            end: value.end,
        }
    }
}

impl From<PyPerspective> for perspective::Perspective {
    fn from(value: PyPerspective) -> Self {
        perspective::Perspective {
            start: value.start,
            end: value.end,
        }
    }
}

/// A PerspectiveSet represents a set of windows on a timeline,
/// defined by a start, end, step, and window size.
#[pyclass(name = "PerspectiveSet")]
#[derive(Clone)]
pub struct PyPerspectiveSet {
    pub(crate) ps: PerspectiveSet,
}
