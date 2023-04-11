//! This module defines the `Perspective` struct and the `PerspectiveSet` iterator.
//!
//! `Perspective` is a simple struct representing a time range from `start` to `end`.
//! The start time is inclusive and the end time is exclusive.
//!
//! `PerspectiveSet` is an iterator over a range of time periods (`Perspective`s).
//! It can be used to generate rolling or expanding perspectives based on a `step` size and an optional `window` size.
//!
//! These perpectives are used when querying the graph to determine the time bounds.
//!
//! # Examples
//! ```rust
//! use docbrown_db::algorithms::degree::average_degree;
//! use docbrown_db::graph::Graph;
//! use docbrown_db::perspective::Perspective;
//! use docbrown_db::view_api::*;
//!
//! let graph = Graph::new(1);
//! graph.add_edge(0, 1, 2, &vec![], None);
//! graph.add_edge(0, 1, 3, &vec![], None);
//! graph.add_edge(1, 2, 3, &vec![], None);
//! graph.add_edge(2, 2, 4, &vec![], None);
//! graph.add_edge(3, 2, 1, &vec![], None);
//!
//! let start = graph.start();
//! let end = graph.end();
//! let perspectives = Perspective::expanding(1, start, end);
//!
//! // A rolling perspective with a window size of 2 and a step size of 1
//! let view_persp = graph.through_perspectives(perspectives);
//!
//! for window in view_persp {
//!   println!("Degree: {:?}", average_degree(&window.graph));
//! }
//!
//! ```
use std::ops::Range;

/// A struct representing a time range from `start` to `end`.
///
/// The start time is inclusive and the end time is exclusive.
#[derive(Debug, PartialEq)]
pub struct Perspective {
    pub start: Option<i64>, // inclusive
    pub end: Option<i64>,   // exclusive
}

/// Representing a time range from `start` to `end` for a graph
impl Perspective {
    /// Creates a new `Perspective` with the given `start` and `end` times.
    pub fn new(start: Option<i64>, end: Option<i64>) -> Perspective {
        Perspective { start, end }
    }

    /// Creates a new `Perspective` with a backward-facing window of size `window`
    /// that ends inclusively at `inclusive_end`.
    pub(crate) fn new_backward(window: Option<i64>, inclusive_end: i64) -> Perspective {
        Perspective {
            start: window.map(|w| inclusive_end + 1 - w),
            end: Some(inclusive_end + 1),
        }
    }

    /// Creates an `PerspectiveSet` with the given `step` size and optional `start` and `end` times,
    /// using an expanding window.
    ///
    /// An expanding window is a window that grows by `step` size at each iteration.
    pub fn expanding(step: u64, start: Option<i64>, end: Option<i64>) -> PerspectiveSet {
        PerspectiveSet {
            start,
            end,
            step: step as i64,
            window: None,
        }
    }

    /// Creates an `PerspectiveSet` with the given `window` size and optional `step`, `start` and `end` times,
    /// using a rolling window.
    ///
    /// A rolling window is a window that moves forward by `step` size at each iteration.
    pub fn rolling(
        window: u64,
        step: Option<u64>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PerspectiveSet {
        PerspectiveSet {
            start,
            end,
            step: step.unwrap_or(window) as i64,
            window: Some(window as i64),
        }
    }
    // TODO pub fn weeks(n), days(n), hours(n), minutes(n), seconds(n), millis(n)
}

/// A PerspectiveSet represents a set of windows on a timeline,
/// defined by a start, end, step, and window size.
#[derive(Clone)]
pub struct PerspectiveSet {
    start: Option<i64>,
    end: Option<i64>,
    step: i64,
    window: Option<i64>,
}

/// A PerspectiveSet represents a set of windows on a timeline,
/// defined by a start, end, step, and window size.
impl PerspectiveSet {
    /// Given a timeline, build an iterator over this PerspectiveSet.
    /// If a start is specified, use it. Otherwise, start the iterator just before the timeline's start.
    /// If an end is specified, use it. Otherwise, end the iterator at the timeline's end.
    /// If a window is specified, use it. Otherwise, use a window size of 1.
    /// If the cursor of the iterator is more than or equal to end, return None.
    pub(crate) fn build_iter(&self, timeline: Range<i64>) -> PerspectiveIterator {
        // TODO: alignment with the epoch for start?
        // if the user sets a start, we just use it, but if we need to decide where to put the
        // first window, it doesn't make any sense that it only includes 1 point if the the step is
        // large. Instead we put the first window such that the previous one ends just before the
        // start of the timeline, i.e. the previous cursor position = timeline.start - 1
        let start = self.start.unwrap_or(timeline.start - 1 + self.step);
        let end = self.end.unwrap_or(match self.window {
            None => timeline.end,
            Some(window) => timeline.end + window - self.step,
            // the iterator returns None when cursor - step >= end and we want to do so when:
            // perspective.start > timeline.end <==> perspective.start >= timeline.end + 1
            // as: cursor = perspective.end - 1
            // and: perspective.start = perspective.end - window
            // then: cursor - step >= timeline.end + window - step = end
        });
        PerspectiveIterator {
            cursor: start,
            end,
            step: self.step,
            window: self.window,
        }
    }
}

/// An iterator over a PerspectiveSet. Yields Perspectives over a timeline.
pub(crate) struct PerspectiveIterator {
    cursor: i64, // last point to be included in the next perspective
    end: i64,    // if cursor - step >= end, this iterator returns None
    step: i64,
    window: Option<i64>,
}

/// An iterator over a PerspectiveSet. Yields Perspectives over a timeline.
impl PerspectiveIterator {
    /// Create an empty PerspectiveIterator. Used when the PerspectiveSet has no windows.
    pub(crate) fn empty() -> PerspectiveIterator {
        PerspectiveIterator {
            cursor: i64::MAX,
            end: i64::MIN,
            step: 1,
            window: None,
        }
    }
}

/// An iterator over a PerspectiveSet.
impl Iterator for PerspectiveIterator {
    type Item = Perspective;

    /// Yield the next Perspective in the iterator.
    /// If the cursor of the iterator is more than or equal to end, return None.
    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor - self.step >= self.end {
            None
        } else {
            let current_cursor = self.cursor;
            self.cursor += self.step;
            Some(Perspective::new_backward(self.window, current_cursor))
        }
    }
}

#[cfg(test)]
mod perspective_tests {
    use crate::perspective::Perspective;
    use itertools::Itertools;

    fn gen_rolling(tuples: Vec<(i64, i64)>) -> Vec<Perspective> {
        tuples
            .iter()
            .map(|(start, end)| Perspective::new(Some(*start), Some(*end)))
            .collect()
    }

    fn gen_expanding(tuples: Vec<i64>) -> Vec<Perspective> {
        tuples
            .iter()
            .map(|point| Perspective::new(None, Some(*point)))
            .collect()
    }

    #[test]
    fn rolling_with_all_none() {
        let windows = Perspective::rolling(2, None, None, None);
        let expected = gen_rolling(vec![(1, 3), (3, 5)]);
        assert_eq!(windows.build_iter(1..3).collect_vec(), expected);
    }

    #[test]
    fn rolling_with_start_and_end() {
        let windows = Perspective::rolling(3, Some(2), Some(-5), Some(-1));
        let expected = gen_rolling(vec![(-7, -4), (-5, -2), (-3, 0)]);
        assert_eq!(windows.build_iter(0..0).collect_vec(), expected);

        let windows = Perspective::rolling(3, Some(2), Some(-5), Some(0));
        let expected = gen_rolling(vec![(-7, -4), (-5, -2), (-3, 0), (-1, 2)]);
        assert_eq!(windows.build_iter(0..0).collect_vec(), expected);
    }

    #[test]
    fn rolling_with_end() {
        let windows = Perspective::rolling(3, Some(2), None, Some(4));
        let expected = gen_rolling(vec![(-1, 2), (1, 4), (3, 6)]);
        assert_eq!(windows.build_iter(0..2).collect_vec(), expected);
    }

    #[test]
    fn rolling_with_start() {
        let windows = Perspective::rolling(3, Some(2), Some(2), None);
        let expected = gen_rolling(vec![(0, 3), (2, 5), (4, 7)]);
        assert_eq!(windows.build_iter(0..4).collect_vec(), expected);
    }

    #[test]
    fn expanding_with_all_none() {
        let windows = Perspective::expanding(2, None, None);
        let expected = gen_expanding(vec![2, 4, 6]);
        assert_eq!(windows.build_iter(0..4).collect_vec(), expected);
    }

    #[test]
    fn expanding_with_start_and_end() {
        let windows = Perspective::expanding(2, Some(3), Some(6));
        let expected = gen_expanding(vec![4, 6, 8]);
        assert_eq!(windows.build_iter(0..0).collect_vec(), expected);
    }

    #[test]
    fn expanding_with_start() {
        let windows = Perspective::expanding(2, Some(3), None);
        let expected = gen_expanding(vec![4, 6, 8]);
        assert_eq!(windows.build_iter(0..6).collect_vec(), expected);
    }

    #[test]
    fn expanding_with_end() {
        let windows = Perspective::expanding(2, None, Some(5));
        let expected = gen_expanding(vec![2, 4, 6]);
        assert_eq!(windows.build_iter(0..4).collect_vec(), expected);
    }
}
