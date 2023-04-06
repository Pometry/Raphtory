use crate::graph_window::WindowSet;
use crate::perspective::{Perspective, PerspectiveIterator, PerspectiveSet};
use std::cmp::{max, min};
use std::iter;

/// Trait defining time query operations
pub trait TimeOps {
    type WindowedViewType;

    /// Return the timestamp of the default start for perspectives of the view (if any).
    fn start(&self) -> Option<i64>;

    /// Return the timestamp of the default for perspectives of the view (if any).
    fn end(&self) -> Option<i64>;

    /// the larger of `t_start` and `self.start()` (useful for creating nested windows)
    fn actual_start(&self, t_start: i64) -> i64 {
        match self.start() {
            None => t_start,
            Some(start) => max(t_start, start),
        }
    }

    /// the smaller of `t_end` and `self.end()` (useful for creating nested windows)
    fn actual_end(&self, t_end: i64) -> i64 {
        match self.end() {
            None => t_end,
            Some(end) => min(t_end, end),
        }
    }

    /// Create a view including all events between `t_start` (inclusive) and `t_end` (exclusive)
    fn window(&self, t_start: i64, t_end: i64) -> Self::WindowedViewType;

    /// Create a view including all events until `end` (inclusive)
    fn at(&self, end: i64) -> Self::WindowedViewType {
        self.window(i64::MIN, end.saturating_add(1))
    }

    /// Creates a `WindowSet` with the given `step` size and optional `start` and `end` times,    
    /// using an expanding window.
    ///
    /// An expanding window is a window that grows by `step` size at each iteration.
    fn expanding(&self, step: u64, start: Option<i64>, end: Option<i64>) -> WindowSet<Self>
    where
        Self: Sized + Clone,
    {
        self.through_perspectives(Perspective::expanding(step, start, end))
    }

    /// Creates a `WindowSet` with the given `window` size and optional `step`, `start` and `end` times,
    /// using a rolling window.
    ///
    /// A rolling window is a window that moves forward by `step` size at each iteration.
    fn rolling(
        &self,
        window: u64,
        step: Option<u64>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> WindowSet<Self>
    where
        Self: Sized + Clone,
    {
        self.through_perspectives(Perspective::rolling(window, step, start, end))
    }

    /// Creates a `WindowSet` from a set of perspectives
    fn through_perspectives(&self, perspectives: PerspectiveSet) -> WindowSet<Self>
    where
        Self: Sized + Clone,
    {
        let iter = match (self.start(), self.end()) {
            (Some(start), Some(end)) => perspectives.build_iter(start..end),
            _ => PerspectiveIterator::empty(),
        };
        WindowSet::new(self.clone(), Box::new(iter))
    }

    /// Creates a `WindowSet` from an iterator over perspectives
    fn through_iter(
        &self,
        perspectives: Box<dyn Iterator<Item = Perspective> + Send>,
    ) -> WindowSet<Self>
    where
        Self: Sized + Clone,
    {
        let iter = if self.start().is_some() && self.end().is_some() {
            perspectives
        } else {
            Box::new(iter::empty::<Perspective>())
        };
        WindowSet::new(self.clone(), iter)
    }
}
