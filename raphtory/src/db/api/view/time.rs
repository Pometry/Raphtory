use crate::{
    core::{storage::timeindex::AsTime, utils::time::Interval},
    db::api::view::{
        internal::{GraphTimeSemanticsOps, InternalFilter, InternalMaterialize},
        time::internal::InternalTimeOps,
    },
};
use raphtory_api::{
    core::{
        storage::timeindex::EventTime,
        utils::time::{IntoTime, ParseTimeError},
    },
    GraphType,
};
use raphtory_core::utils::time::{AlignmentUnit, IntervalSize};
use std::{
    cmp::{max, min},
    marker::PhantomData,
};

pub(crate) mod internal {
    use crate::{
        db::{api::view::internal::InternalFilter, graph::views::window_graph::WindowedGraph},
        prelude::{GraphViewOps, TimeOps},
    };
    use raphtory_api::core::storage::timeindex::{AsTime, EventTime};
    use raphtory_storage::core_ops::CoreGraphOps;
    use std::cmp::{max, min};

    pub trait InternalTimeOps<'graph> {
        type InternalWindowedView: TimeOps<'graph> + 'graph;
        fn timeline_start(&self) -> Option<EventTime>;
        fn timeline_end(&self) -> Option<EventTime>;
        fn latest_t(&self) -> Option<i64>;
        fn internal_window(
            &self,
            start: Option<EventTime>,
            end: Option<EventTime>,
        ) -> Self::InternalWindowedView;
    }
    impl<'graph, E: InternalFilter<'graph> + 'graph> InternalTimeOps<'graph> for E {
        type InternalWindowedView = E::Filtered<WindowedGraph<E::Graph>>;

        fn timeline_start(&self) -> Option<EventTime> {
            self.start()
                .or_else(|| self.base_graph().core_graph().earliest_time())
        }

        fn timeline_end(&self) -> Option<EventTime> {
            self.end().or_else(|| {
                self.base_graph()
                    .core_graph()
                    .latest_time()
                    .map(|v| EventTime::from(v.0.saturating_add(1)))
            })
        }

        fn latest_t(&self) -> Option<i64> {
            self.base_graph().latest_time().map(|t| t.t())
        }

        fn internal_window(
            &self,
            start: Option<EventTime>,
            end: Option<EventTime>,
        ) -> Self::InternalWindowedView {
            let base_start = self.base_graph().start();
            let base_end = self.base_graph().end();
            let actual_start = match (base_start, start) {
                (Some(base), Some(start)) => Some(max(base, start)),
                (None, v) => v,
                (v, None) => v,
            };
            let actual_end = match (base_end, end) {
                (Some(base), Some(end)) => Some(min(base, end)),
                (None, v) => v,
                (v, None) => v,
            };
            let actual_end = match (actual_end, actual_start) {
                (Some(end), Some(start)) => Some(max(end, start)),
                _ => actual_end,
            };
            self.apply_filter(WindowedGraph::new(
                self.base_graph().clone(),
                actual_start,
                actual_end,
            ))
        }
    }
}

/// Trait defining time query operations
pub trait TimeOps<'graph>:
    InternalTimeOps<'graph, InternalWindowedView = Self::WindowedViewType>
{
    type WindowedViewType: TimeOps<'graph> + 'graph;
    /// Return the time entry of the start of the view or None if the view start is unbounded.
    fn start(&self) -> Option<EventTime>;

    /// Return the time entry of the view or None if the view end is unbounded.
    fn end(&self) -> Option<EventTime>;

    /// set the start of the window to the larger of `start` and `self.start()`
    fn shrink_start<T: IntoTime>(&self, start: T) -> Self::WindowedViewType;

    /// set the end of the window to the smaller of `end` and `self.end()`
    fn shrink_end<T: IntoTime>(&self, end: T) -> Self::WindowedViewType;

    /// shrink both the start and end of the window (same as calling `shrink_start` followed by `shrink_end` but more efficient)
    fn shrink_window<T: IntoTime>(&self, start: T, end: T) -> Self::WindowedViewType;

    /// Return the size of the window covered by this view or None if the window is unbounded
    fn window_size(&self) -> Option<u64>;

    /// Create a view including all events between `start` (inclusive) and `end` (exclusive)
    fn window<T1: IntoTime, T2: IntoTime>(&self, start: T1, end: T2) -> Self::WindowedViewType;

    /// Create a view that only includes events at `time`
    fn at<T: IntoTime>(&self, time: T) -> Self::WindowedViewType;

    /// Create a view that only includes events at the latest time
    fn latest(&self) -> Self::WindowedViewType;

    /// Create a view including all events that have not been explicitly deleted at `time`
    ///
    /// This is equivalent to `before(time + 1)` for `EventGraph`s and `at(time)` for `PersitentGraph`s
    fn snapshot_at<T: IntoTime>(&self, time: T) -> Self::WindowedViewType;

    /// Create a view including all events that have not been explicitly deleted at the latest time
    ///
    /// This is equivalent to a no-op for `EventGraph`s and `latest()` for `PersitentGraph`s
    fn snapshot_latest(&self) -> Self::WindowedViewType;

    /// Create a view that only includes events after `start` (exclusive)
    fn after<T: IntoTime>(&self, start: T) -> Self::WindowedViewType;

    /// Create a view that only includes events before `end` (exclusive)
    fn before<T: IntoTime>(&self, end: T) -> Self::WindowedViewType;

    /// Creates a `WindowSet` with the given `step` size
    /// using an expanding window. The last window may fall partially outside the range of the data/view.
    ///
    /// An expanding window is a window that grows by `step` size at each iteration.
    ///
    /// The window will be aligned with the smallest unit of time passed. For example, if the interval
    /// is "1 month and 1 day", the first window will begin at the start of the day of the first time event.
    fn expanding<I>(&self, step: I) -> Result<WindowSet<'graph, Self>, ParseTimeError>
    where
        Self: Sized + Clone + 'graph,
        I: TryInto<Interval> + Clone,
        ParseTimeError: From<<I as TryInto<Interval>>::Error>;

    /// Creates a `WindowSet` with the given `step` size using an expanding window, where the windows are aligned
    /// with the `alignment_unit` passed. The last window may fall partially outside the range of the data/view.
    ///
    /// An expanding window is a window that grows by `step` size at each iteration.
    ///
    /// Note that `alignment_unit = AlignmentUnit::Unaligned` achieves unaligned behaviour.
    fn expanding_aligned<I>(
        &self,
        step: I,
        alignment_unit: AlignmentUnit,
    ) -> Result<WindowSet<'graph, Self>, ParseTimeError>
    where
        Self: Sized + Clone + 'graph,
        I: TryInto<Interval>,
        ParseTimeError: From<<I as TryInto<Interval>>::Error>;

    /// Creates a `WindowSet` with the given `window` size and optional `step`
    /// using a rolling window. The last window may fall partially outside the range of the data/view.
    /// Note that passing a `step` larger than `window` can lead to some entries appearing before
    /// the start of the first window and/or after the end of the last window (i.e. not included in any window)
    ///
    /// A rolling window is a window that moves forward by `step` size at each iteration.
    ///
    /// The window will be aligned with the smallest unit of time passed. For example, if the interval
    /// is "1 month and 1 day", the first window will begin at the start of the day of the first time event.
    fn rolling<I>(
        &self,
        window: I,
        step: Option<I>,
    ) -> Result<WindowSet<'graph, Self>, ParseTimeError>
    where
        Self: Sized + Clone + 'graph,
        I: TryInto<Interval> + Clone,
        ParseTimeError: From<<I as TryInto<Interval>>::Error>;

    /// Creates a `WindowSet` with the given `window` size and optional `step` using a rolling window, where the windows
    /// are aligned with the `alignment_unit` passed. The last window may fall partially outside the range of the data/view.
    /// Note that, depending on the `alignment_unit`, passing a `step` larger than `window` can lead to some entries
    /// appearing before the start of the first window and/or after the end of the last window (i.e. not included in any window)
    ///
    /// A rolling window is a window that moves forward by `step` size at each iteration.
    ///
    /// Note that `alignment_unit = AlignmentUnit::Unaligned` achieves unaligned behaviour.
    fn rolling_aligned<I>(
        &self,
        window: I,
        step: Option<I>,
        alignment_unit: AlignmentUnit,
    ) -> Result<WindowSet<'graph, Self>, ParseTimeError>
    where
        Self: Sized + Clone + 'graph,
        I: TryInto<Interval>,
        ParseTimeError: From<<I as TryInto<Interval>>::Error>;
}

impl<'graph, V: InternalFilter<'graph> + 'graph + InternalTimeOps<'graph>> TimeOps<'graph> for V {
    type WindowedViewType = V::InternalWindowedView;

    fn start(&self) -> Option<EventTime> {
        self.base_graph().view_start()
    }

    fn end(&self) -> Option<EventTime> {
        self.base_graph().view_end()
    }

    fn shrink_start<T: IntoTime>(&self, start: T) -> Self::WindowedViewType {
        let start = Some(max(
            start.into_time(),
            self.start().unwrap_or(EventTime::MIN),
        ));
        self.internal_window(start, self.end())
    }

    fn shrink_end<T: IntoTime>(&self, end: T) -> Self::WindowedViewType {
        let end = Some(min(end.into_time(), self.end().unwrap_or(EventTime::MAX)));
        self.internal_window(self.start(), end)
    }

    fn shrink_window<T: IntoTime>(&self, start: T, end: T) -> Self::WindowedViewType {
        let start = max(start.into_time(), self.start().unwrap_or(EventTime::MIN));
        let end = min(end.into_time(), self.end().unwrap_or(EventTime::MAX));
        self.internal_window(Some(start), Some(end))
    }

    fn window_size(&self) -> Option<u64> {
        match (self.start(), self.end()) {
            (Some(start), Some(end)) => Some((end.t() - start.t()) as u64),
            _ => None,
        }
    }

    fn window<T1: IntoTime, T2: IntoTime>(&self, start: T1, end: T2) -> Self::WindowedViewType {
        self.internal_window(Some(start.into_time()), Some(end.into_time()))
    }

    fn at<T: IntoTime>(&self, time: T) -> Self::WindowedViewType {
        let start = time.into_time();
        self.internal_window(
            Some(EventTime::start(start.t())),
            Some(EventTime::start(start.t().saturating_add(1))),
        )
    }

    fn latest(&self) -> Self::WindowedViewType {
        let time = self.latest_t();
        self.internal_window(
            time.map(EventTime::start),
            time.map(|t| EventTime::start(t.saturating_add(1))),
        )
    }

    fn snapshot_at<T: IntoTime>(&self, time: T) -> Self::WindowedViewType {
        match self.base_graph().graph_type() {
            GraphType::EventGraph => self.before(time.into_time().t().saturating_add(1)),
            GraphType::PersistentGraph => self.at(time),
        }
    }

    fn snapshot_latest(&self) -> Self::WindowedViewType {
        match self.latest_t() {
            Some(latest) => self.snapshot_at(latest),
            None => self.snapshot_at(i64::MIN),
        }
    }

    fn after<T: IntoTime>(&self, start: T) -> Self::WindowedViewType {
        let start_time = start.into_time();
        let start = EventTime::start(start_time.t().saturating_add(1));
        self.internal_window(Some(start), None)
    }

    fn before<T: IntoTime>(&self, end: T) -> Self::WindowedViewType {
        let end = EventTime::start(end.into_time().t());
        self.internal_window(None, Some(end))
    }

    fn expanding<I>(&self, step: I) -> Result<WindowSet<'graph, Self>, ParseTimeError>
    where
        Self: Sized + Clone + 'graph,
        I: TryInto<Interval> + Clone,
        ParseTimeError: From<<I as TryInto<Interval>>::Error>,
    {
        // step is usually a number or a small string so performance impact of cloning should be minimal
        let alignment_unit = step
            .clone()
            .try_into()?
            .alignment_unit
            .unwrap_or(AlignmentUnit::Unaligned);
        // Align the timestamp to the smallest unit.
        // If there is None (the Interval is discrete), no alignment is done
        self.expanding_aligned(step, alignment_unit)
    }

    fn expanding_aligned<I>(
        &self,
        step: I,
        alignment_unit: AlignmentUnit,
    ) -> Result<WindowSet<'graph, Self>, ParseTimeError>
    where
        Self: Sized + Clone + 'graph,
        I: TryInto<Interval>,
        ParseTimeError: From<<I as TryInto<Interval>>::Error>,
    {
        let parent = self.clone();
        match (self.timeline_start(), self.timeline_end()) {
            (Some(start), Some(end)) => {
                let step: Interval = step.try_into()?;
                let start_time = alignment_unit.align_timestamp(start.t());
                WindowSet::new(parent, start_time, end.t(), step, None)
            }
            _ => WindowSet::empty(parent),
        }
    }

    fn rolling<I>(
        &self,
        window: I,
        step: Option<I>,
    ) -> Result<WindowSet<'graph, Self>, ParseTimeError>
    where
        Self: Sized + Clone + 'graph,
        I: TryInto<Interval> + Clone,
        ParseTimeError: From<<I as TryInto<Interval>>::Error>,
    {
        // step and window are usually numbers or small strings so performance impact of cloning should be minimal
        let alignment_unit = match &step {
            Some(s) => s
                .clone()
                .try_into()?
                .alignment_unit
                .unwrap_or(AlignmentUnit::Unaligned),
            None => window
                .clone()
                .try_into()?
                .alignment_unit
                .unwrap_or(AlignmentUnit::Unaligned),
        };
        // Align the timestamp to the smallest unit in step.
        // If there is None (i.e. the Interval is discrete), no alignment is done.
        self.rolling_aligned(window, step, alignment_unit)
    }

    fn rolling_aligned<I>(
        &self,
        window: I,
        step: Option<I>,
        alignment_unit: AlignmentUnit,
    ) -> Result<WindowSet<'graph, Self>, ParseTimeError>
    where
        Self: Sized + Clone + 'graph,
        I: TryInto<Interval>,
        ParseTimeError: From<<I as TryInto<Interval>>::Error>,
    {
        let parent = self.clone();
        match (self.timeline_start(), self.timeline_end()) {
            (Some(start), Some(end)) => {
                let window: Interval = window.try_into()?;
                let step: Interval = match step {
                    Some(step) => step.try_into()?,
                    None => window,
                };
                let start_time = alignment_unit.align_timestamp(start.t());
                WindowSet::new(parent, start_time, end.t(), step, Some(window))
            }
            _ => WindowSet::empty(parent),
        }
    }
}

#[derive(Clone)]
pub struct WindowSet<'graph, T> {
    view: T,
    start: i64,
    counter: u32, // u32 because months from Temporal intervals are u32 (due to chrono months being u32)
    end: i64,
    step: Interval,
    window: Option<Interval>,
    _marker: PhantomData<&'graph T>,
}

impl<'graph, T: TimeOps<'graph> + Clone + 'graph> WindowSet<'graph, T> {
    fn new(
        view: T,
        start: i64,
        end: i64,
        step: Interval,
        window: Option<Interval>,
    ) -> Result<Self, ParseTimeError> {
        match step.size {
            IntervalSize::Discrete(v) => {
                if v == 0 {
                    return Err(ParseTimeError::ZeroSizeStep);
                }
            }
            IntervalSize::Temporal { millis, months } => {
                if millis == 0 && months == 0 {
                    return Err(ParseTimeError::ZeroSizeStep);
                }
            }
        };
        Ok(Self {
            view,
            start,
            counter: 1,
            end,
            step,
            window,
            _marker: PhantomData,
        })
    }

    fn empty(view: T) -> Result<Self, ParseTimeError> {
        // timeline_start is greater than end, so no windows to return, even with end inclusive
        WindowSet::new(view, 1, 0, Default::default(), None)
    }

    // TODO: make this optionally public only for the development feature flag
    pub fn temporal(&self) -> bool {
        self.step.alignment_unit.is_some()
            || match self.window {
                Some(window) => window.alignment_unit.is_some(),
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
                view.start().unwrap().t()
                    + ((view.end().unwrap().t() - view.start().unwrap().t()) / 2)
            } else {
                view.end().unwrap().t() - 1
            }
        })
    }
}

impl<'graph, T: TimeOps<'graph> + Clone + 'graph> Iterator for WindowSet<'graph, T> {
    type Item = T::WindowedViewType;
    fn next(&mut self) -> Option<Self::Item> {
        let window_end = self.start + (self.counter * self.step);

        if window_end < self.end + self.step {
            let window_start = self.window.map(|w| window_end - w);
            if let Some(start) = window_start {
                // this is required because if we have steps > window size you can end up overstepping
                // the end by so much in the final window that there is no data inside
                if start >= self.end {
                    // this is >= because the end passed through is already +1
                    return None;
                }
            }
            let window = self.view.internal_window(
                window_start.map(EventTime::start),
                Some(EventTime::start(window_end)),
            );
            self.counter += 1;
            Some(window)
        } else {
            None
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}
impl<'graph, T: TimeOps<'graph> + Clone + 'graph> ExactSizeIterator for WindowSet<'graph, T> {
    // unfortunately because Interval can change size, there is no nice divide option
    fn len(&self) -> usize {
        let mut window_end = self.start + (self.counter * self.step);
        let mut count = 0;
        while window_end < self.end + self.step {
            let window_start = self.window.map(|w| window_end - w);
            if let Some(start) = window_start {
                if start >= self.end {
                    break;
                }
            }
            count += 1;
            window_end = window_end + self.step;
        }
        count
    }
}
