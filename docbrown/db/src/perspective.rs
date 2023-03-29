//! Defines the `Perspective` struct, which represents a view of the graph at a particular time.

use std::ops::Range;

#[derive(Debug, PartialEq)]
pub struct Perspective {
    pub start: Option<i64>, // inclusive
    pub end: Option<i64>,   // exclusive
}

impl Perspective {
    pub fn new(start: Option<i64>, end: Option<i64>) -> Perspective {
        Perspective { start, end }
    }
    pub(crate) fn new_backward(window: Option<i64>, inclusive_end: i64) -> Perspective {
        Perspective {
            start: window.map(|w| inclusive_end + 1 - w),
            end: Some(inclusive_end + 1),
        }
    }
    pub fn expanding(step: u64, start: Option<i64>, end: Option<i64>) -> PerspectiveSet {
        PerspectiveSet {
            start: start,
            end,
            step: step as i64,
            window: None,
        }
    }
    pub fn rolling(
        window: u64,
        step: Option<u64>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PerspectiveSet {
        PerspectiveSet {
            start: start,
            end: end,
            step: step.unwrap_or(window) as i64,
            window: Some(window as i64),
        }
    }
    // TODO pub fn weeks(n), days(n), hours(n), minutes(n), seconds(n), millis(n)
}

#[derive(Clone)]
pub struct PerspectiveSet {
    start: Option<i64>,
    end: Option<i64>,
    step: i64,
    window: Option<i64>,
}

impl PerspectiveSet {
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
            end: end,
            step: self.step,
            window: self.window,
        }
    }
}

pub(crate) struct PerspectiveIterator {
    cursor: i64, // last point to be included in the next perspective
    end: i64,    // if cursor - step >= end, this iterator returns None
    step: i64,
    window: Option<i64>,
}

impl PerspectiveIterator {
    pub(crate) fn empty() -> PerspectiveIterator {
        PerspectiveIterator {
            cursor: i64::MAX,
            end: i64::MIN,
            step: 1,
            window: None,
        }
    }
}

impl Iterator for PerspectiveIterator {
    type Item = Perspective;
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
            .map(|(start, end)| Perspective::new(Some(start.clone()), Some(end.clone())))
            .collect()
    }

    fn gen_expanding(tuples: Vec<i64>) -> Vec<Perspective> {
        tuples
            .iter()
            .map(|point| Perspective::new(None, Some(point.clone())))
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
