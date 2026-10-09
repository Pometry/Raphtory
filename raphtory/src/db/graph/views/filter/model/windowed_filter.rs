use crate::{
    db::{
        api::view::internal::{GraphView, Static},
        graph::views::{filter::model::CreateView, window_graph::WindowedGraph},
    },
    errors::GraphError,
    prelude::TimeOps,
};
use raphtory_api::core::{
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
};
use std::{fmt, fmt::Display};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Windowed<M> {
    pub start: EventTime,
    pub end: EventTime,
    pub inner: M,
}

impl<M> Static for Windowed<M> {}

impl<M: Display> Display for Windowed<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WINDOW[{}..{}]({})",
            self.start.t(),
            self.end.t(),
            self.inner
        )
    }
}

impl<M> Windowed<M> {
    #[inline]
    pub fn new(start: EventTime, end: EventTime, entity: M) -> Self {
        Self {
            start,
            end,
            inner: entity,
        }
    }

    #[inline]
    pub fn from_times<S: IntoTime, E: IntoTime>(start: S, end: E, entity: M) -> Self {
        let s = start.into_time();
        let e = end.into_time();
        Self::new(s, e, entity)
    }
}

// ── expr-layer view construction ──

impl<T: CreateView> CreateView for Windowed<T> {
    type View<'graph, G: GraphView + 'graph> = WindowedGraph<<T as CreateView>::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.window(self.start, self.end))
    }
}

/// The window's start moved to `start` when that is later, as `shrink_start`
/// gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShrinkStart<M> {
    pub start: EventTime,
    pub inner: M,
}

impl<M> Static for ShrinkStart<M> {}

impl<M: Display> Display for ShrinkStart<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SHRINK_START[{}]({})", self.start.t(), self.inner)
    }
}

impl<M> ShrinkStart<M> {
    #[inline]
    pub fn new<T: IntoTime>(start: T, entity: M) -> Self {
        Self {
            start: start.into_time(),
            inner: entity,
        }
    }
}

/// The window's end moved to `end` when that is earlier, as `shrink_end`
/// gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShrinkEnd<M> {
    pub end: EventTime,
    pub inner: M,
}

impl<M> Static for ShrinkEnd<M> {}

impl<M: Display> Display for ShrinkEnd<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SHRINK_END[{}]({})", self.end.t(), self.inner)
    }
}

impl<M> ShrinkEnd<M> {
    #[inline]
    pub fn new<T: IntoTime>(end: T, entity: M) -> Self {
        Self {
            end: end.into_time(),
            inner: entity,
        }
    }
}

impl<T: CreateView> CreateView for ShrinkStart<T> {
    type View<'graph, G: GraphView + 'graph> = WindowedGraph<<T as CreateView>::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.shrink_start(self.start))
    }
}

impl<T: CreateView> CreateView for ShrinkEnd<T> {
    type View<'graph, G: GraphView + 'graph> = WindowedGraph<<T as CreateView>::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.shrink_end(self.end))
    }
}
