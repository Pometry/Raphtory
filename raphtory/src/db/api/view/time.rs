use crate::{
    core::{
        storage::timeindex::AsTime,
        utils::time::{error::ParseTimeError, Interval, IntoOptTime, IntoTime},
    },
    db::{
        api::view::internal::{OneHopFilter, TimeSemantics},
        graph::views::window_graph::WindowedGraph,
    },
    prelude::GraphViewOps,
};
use chrono::{DateTime, Utc};
use std::{
    cmp::{max, min},
    marker::PhantomData,
};

/// Trait defining time query operations
pub trait TimeOps<'graph> {
    type WindowedViewType: TimeOps<'graph> + 'graph;

    /// Return the timestamp of the start of the view or None if the view start is unbounded.
    fn start(&self) -> Option<i64>;

    fn start_date_time(&self) -> Option<DateTime<Utc>> {
        self.start()?.dt()
    }

    /// Return the timestamp of the of the view or None if the view end is unbounded.
    fn end(&self) -> Option<i64>;

    fn end_date_time(&self) -> Option<DateTime<Utc>> {
        self.end()?.dt()
    }

    /// Return the start timestamp for WindowSets or None if the timeline is empty
    fn timeline_start(&self) -> Option<i64>;

    /// Return the end timestamp for WindowSets or None if the timeline is empty
    fn timeline_end(&self) -> Option<i64>;

    /// set the start of the window to the larger of `start` and `self.start()`
    fn shrink_start<T: IntoTime>(&self, start: T) -> Self::WindowedViewType {
        let start = max(start.into_time(), self.start().unwrap_or(i64::MIN));
        let end = self.end().unwrap_or(start);
        self.window(start, end)
    }

    /// set the end of the window to the smaller of `end` and `self.end()`
    fn shrink_end<T: IntoTime>(&self, end: T) -> Self::WindowedViewType {
        let end = min(end.into_time(), self.end().unwrap_or(i64::MAX));
        let start = self.start().unwrap_or(end);
        self.window(start, end)
    }

    /// shrink both the start and end of the window (same as calling `shrink_start` followed by `shrink_end` but more efficient)
    fn shrink_window<T: IntoTime>(&self, start: T, end: T) -> Self::WindowedViewType {
        let start = max(start.into_time(), self.start().unwrap_or(i64::MIN));
        let end = min(end.into_time(), self.end().unwrap_or(i64::MAX));
        self.window(start, end)
    }

    /// Return the size of the window covered by this view or None if the window is unbounded
    fn window_size(&self) -> Option<u64> {
        match (self.start(), self.end()) {
            (Some(start), Some(end)) => Some((end - start) as u64),
            _ => None,
        }
    }

    /// Create a view including all events between `start` (inclusive) and `end` (exclusive)
    fn window<T1: IntoOptTime, T2: IntoOptTime>(
        &self,
        start: T1,
        end: T2,
    ) -> Self::WindowedViewType;

    /// Create a view that only includes events at `time`
    fn at<T: IntoTime>(&self, time: T) -> Self::WindowedViewType {
        let start = time.into_time();
        self.window(start, start.saturating_add(1))
    }

    /// Create a view that only includes events after `start` (exclusive)
    fn after<T: IntoTime>(&self, start: T) -> Self::WindowedViewType {
        let start = start.into_time().saturating_add(1);
        self.window(start, None)
    }

    /// Create a view that only includes events before `end` (exclusive)
    fn before<T: IntoTime>(&self, end: T) -> Self::WindowedViewType {
        let end = end.into_time();
        self.window(None, end)
    }

    /// Creates a `WindowSet` with the given `step` size    
    /// using an expanding window. The last window may fall partially outside the range of the data/view.
    ///
    /// An expanding window is a window that grows by `step` size at each iteration.
    fn expanding<I>(&self, step: I) -> Result<WindowSet<'graph, Self>, ParseTimeError>
    where
        Self: Sized + Clone + 'static,
        I: TryInto<Interval, Error = ParseTimeError>,
    {
        let parent = self.clone();
        match (self.timeline_start(), self.timeline_end()) {
            (Some(start), Some(end)) => {
                let step: Interval = step.try_into()?;

                Ok(WindowSet::new(parent, start, end, step, None))
            }
            _ => Ok(WindowSet::empty(parent)),
        }
    }

    /// Creates a `WindowSet` with the given `window` size and optional `step`
    /// using a rolling window. The last window may fall partially outside the range of the data/view.
    ///
    /// A rolling window is a window that moves forward by `step` size at each iteration.
    fn rolling<I>(
        &self,
        window: I,
        step: Option<I>,
    ) -> Result<WindowSet<'graph, Self>, ParseTimeError>
    where
        Self: Sized + Clone + 'static,
        I: TryInto<Interval, Error = ParseTimeError>,
    {
        let parent = self.clone();
        match (self.timeline_start(), self.timeline_end()) {
            (Some(start), Some(end)) => {
                let window: Interval = window.try_into()?;
                let step: Interval = match step {
                    Some(step) => step.try_into()?,
                    None => window,
                };
                Ok(WindowSet::new(parent, start, end, step, Some(window)))
            }
            _ => Ok(WindowSet::empty(parent)),
        }
    }
}

impl<'graph, V: OneHopFilter<'graph> + 'graph> TimeOps<'graph> for V {
    type WindowedViewType = V::Filtered<WindowedGraph<V::FilteredGraph>>;

    fn start(&self) -> Option<i64> {
        self.current_filter().view_start()
    }

    fn end(&self) -> Option<i64> {
        self.current_filter().view_end()
    }

    fn timeline_start(&self) -> Option<i64> {
        self.start()
            .or_else(|| self.current_filter().earliest_time())
    }

    fn timeline_end(&self) -> Option<i64> {
        self.end().or_else(|| {
            self.current_filter()
                .latest_time()
                .map(|v| v.saturating_add(1))
        })
    }

    fn window<T1: IntoOptTime, T2: IntoOptTime>(
        &self,
        start: T1,
        end: T2,
    ) -> Self::WindowedViewType {
        let base_start = self.base_graph().start();
        let base_end = self.base_graph().end();
        let actual_start = match (base_start, start.into_opt_time()) {
            (Some(base), Some(start)) => Some(max(base, start)),
            (None, v) => v,
            (v, None) => v,
        };
        let actual_end = match (base_end, end.into_opt_time()) {
            (Some(base), Some(end)) => Some(min(base, end)),
            (None, v) => v,
            (v, None) => v,
        };
        let actual_end = match (actual_end, actual_start) {
            (Some(end), Some(start)) => Some(max(end, start)),
            _ => actual_end,
        };
        self.one_hop_filtered(WindowedGraph::new(
            self.current_filter().clone(),
            actual_start,
            actual_end,
        ))
    }
}

#[derive(Clone)]
pub struct WindowSet<'graph, T> {
    view: T,
    cursor: i64,
    end: i64,
    step: Interval,
    window: Option<Interval>,
    _marker: PhantomData<&'graph T>,
}

impl<'graph, T: TimeOps<'graph> + Clone + 'graph> WindowSet<'graph, T> {
    fn new(view: T, start: i64, end: i64, step: Interval, window: Option<Interval>) -> Self {
        let cursor_start = start + step;
        Self {
            view,
            cursor: cursor_start,
            end,
            step,
            window,
            _marker: PhantomData,
        }
    }

    fn empty(view: T) -> Self {
        // timeline_start is greater than end, so no windows to return, even with end inclusive
        WindowSet::new(view, 1, 0, Default::default(), None)
    }

    // TODO: make this optionally public only for the development feature flag
    pub fn temporal(&self) -> bool {
        self.step.epoch_alignment
            || match self.window {
                Some(window) => window.epoch_alignment,
                None => false,
            }
    }

    /// Returns the time index of this window set
    pub fn time_index(&self, center: bool) -> TimeIndex<'graph, T> {
        TimeIndex {
            windowset: self.clone(),
            center,
        }
    }
}

pub struct TimeIndex<'graph, T> {
    windowset: WindowSet<'graph, T>,
    center: bool,
}

impl<'graph, T: TimeOps<'graph> + Clone + 'graph> Iterator for TimeIndex<'graph, T> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        let center = self.center;
        self.windowset.next().map(move |view| {
            if center {
                view.start().unwrap() + ((view.end().unwrap() - view.start().unwrap()) / 2)
            } else {
                view.end().unwrap() - 1
            }
        })
    }
}

impl<'graph, T: TimeOps<'graph> + Clone + 'graph> Iterator for WindowSet<'graph, T> {
    type Item = T::WindowedViewType;
    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor < self.end + self.step {
            let window_end = self.cursor;
            let window_start = self.window.map(|w| window_end - w);
            let window = self.view.window(window_start, window_end);
            self.cursor = self.cursor + self.step;
            Some(window)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod time_tests {
    use crate::{
        core::utils::time::TryIntoTime,
        db::{
            api::{
                mutation::AdditionOps,
                view::{time::WindowSet, TimeOps},
            },
            graph::graph::Graph,
        },
        prelude::{GraphViewOps, NO_PROPS},
    };
    use itertools::Itertools;

    // start inclusive, end exclusive
    fn graph_with_timeline(start: i64, end: i64) -> Graph {
        let g = Graph::new();
        g.add_node(start, 0, NO_PROPS).unwrap();
        g.add_node(end - 1, 0, NO_PROPS).unwrap();
        assert_eq!(g.timeline_start().unwrap(), start);
        assert_eq!(g.timeline_end().unwrap(), end);
        g
    }

    fn assert_bounds<'graph, G>(
        windows: WindowSet<'graph, G>,
        expected: Vec<(Option<i64>, Option<i64>)>,
    ) where
        G: GraphViewOps<'graph>,
    {
        let window_bounds = windows.map(|w| (w.start(), w.end())).collect_vec();
        assert_eq!(window_bounds, expected)
    }

    #[test]
    fn rolling() {
        let g = graph_with_timeline(1, 7);
        let windows = g.rolling(2, None).unwrap();
        let expected = vec![(Some(1), Some(3)), (Some(3), Some(5)), (Some(5), Some(7))];
        assert_bounds(windows, expected);

        let g = graph_with_timeline(1, 6);
        let windows = g.rolling(3, Some(2)).unwrap();
        let expected = vec![(Some(0), Some(3)), (Some(2), Some(5)), (Some(4), Some(7))];
        assert_bounds(windows, expected.clone());

        let g = graph_with_timeline(0, 9).window(1, 6);
        let windows = g.rolling(3, Some(2)).unwrap();
        assert_bounds(
            windows,
            vec![(Some(1), Some(3)), (Some(2), Some(5)), (Some(4), Some(6))],
        );
    }

    #[test]
    fn expanding() {
        let g = graph_with_timeline(1, 7);
        let windows = g.expanding(2).unwrap();
        let expected = vec![(None, Some(3)), (None, Some(5)), (None, Some(7))];
        assert_bounds(windows, expected);

        let g = graph_with_timeline(1, 6);
        let windows = g.expanding(2).unwrap();
        let expected = vec![(None, Some(3)), (None, Some(5)), (None, Some(7))];
        assert_bounds(windows, expected.clone());

        let g = graph_with_timeline(0, 9).window(1, 6);
        let windows = g.expanding(2).unwrap();
        assert_bounds(
            windows,
            vec![(Some(1), Some(3)), (Some(1), Some(5)), (Some(1), Some(6))],
        );
    }

    #[test]
    fn rolling_dates() {
        let start = "2020-06-06 00:00:00".try_into_time().unwrap();
        let end = "2020-06-07 23:59:59.999".try_into_time().unwrap();
        let g = graph_with_timeline(start, end);
        let windows = g.rolling("1 day", None).unwrap();
        let expected = vec![
            (
                "2020-06-06 00:00:00".try_into_time().ok(), // entire 2020-06-06
                "2020-06-07 00:00:00".try_into_time().ok(),
            ),
            (
                "2020-06-07 00:00:00".try_into_time().ok(), // entire 2020-06-06
                "2020-06-08 00:00:00".try_into_time().ok(),
            ),
        ];
        assert_bounds(windows, expected);

        let start = "2020-06-06 00:00:00".try_into_time().unwrap();
        let end = "2020-06-08 00:00:00".try_into_time().unwrap();
        let g = graph_with_timeline(start, end);
        let windows = g.rolling("1 day", None).unwrap();
        let expected = vec![
            (
                "2020-06-06 00:00:00".try_into_time().ok(), // entire 2020-06-06
                "2020-06-07 00:00:00".try_into_time().ok(),
            ),
            (
                "2020-06-07 00:00:00".try_into_time().ok(), // entire 2020-06-07
                "2020-06-08 00:00:00".try_into_time().ok(),
            ),
        ];
        assert_bounds(windows, expected);

        // TODO: turn this back on if we bring bach epoch alignment for unwindowed graphs
        // let start = "2020-06-05 23:59:59.999".into_time().unwrap();
        // let end = "2020-06-07 00:00:00.000".into_time().unwrap();
        // let g = graph_with_timeline(start, end);
        // let windows = g.rolling("1 day", None).unwrap();
        // let expected = vec![
        //     (
        //         "2020-06-05 00:00:00".into_time().unwrap(), // entire 2020-06-06
        //         "2020-06-06 00:00:00".into_time().unwrap(),
        //     ),
        //     (
        //         "2020-06-06 00:00:00".into_time().unwrap(), // entire 2020-06-07
        //         "2020-06-07 00:00:00".into_time().unwrap(),
        //     ),
        // ];
        // assert_bounds(windows, expected);
    }

    #[test]
    fn expanding_dates() {
        let start = "2020-06-06 00:00:00".try_into_time().unwrap();
        let end = "2020-06-07 23:59:59.999".try_into_time().unwrap();
        let g = graph_with_timeline(start, end);
        let windows = g.expanding("1 day").unwrap();
        let expected = vec![
            (None, "2020-06-07 00:00:00".try_into_time().ok()),
            (None, "2020-06-08 00:00:00".try_into_time().ok()),
        ];
        assert_bounds(windows, expected);

        let start = "2020-06-06 00:00:00".try_into_time().unwrap();
        let end = "2020-06-08 00:00:00".try_into_time().unwrap();
        let g = graph_with_timeline(start, end);
        let windows = g.expanding("1 day").unwrap();
        let expected = vec![
            (None, "2020-06-07 00:00:00".try_into_time().ok()),
            (None, "2020-06-08 00:00:00".try_into_time().ok()),
        ];
        assert_bounds(windows, expected);
    }
}
