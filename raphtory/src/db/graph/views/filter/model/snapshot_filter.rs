use crate::{
    db::{
        api::view::{
            internal::{GraphView, Static},
            time::TimeOps,
        },
        graph::views::{filter::model::CreateView, window_graph::WindowedGraph},
    },
    errors::GraphError,
};
use raphtory_api::core::{storage::timeindex::EventTime, utils::time::IntoTime};
use std::{fmt, fmt::Display};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotAt<M> {
    pub time: EventTime,
    pub inner: M,
}

impl<M> Static for SnapshotAt<M> {}

impl<M> SnapshotAt<M> {
    #[inline]
    pub fn new<T: IntoTime>(time: T, inner: M) -> Self {
        Self {
            time: time.into_time(),
            inner,
        }
    }
}

impl<M: Display> Display for SnapshotAt<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SNAPSHOT_AT[{}]({})", self.time, self.inner)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotLatest<M> {
    pub inner: M,
}

impl<M> Static for SnapshotLatest<M> {}

impl<M> SnapshotLatest<M> {
    #[inline]
    pub fn new(inner: M) -> Self {
        Self { inner }
    }
}

impl<M: Display> Display for SnapshotLatest<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SNAPSHOT_LATEST({})", self.inner)
    }
}

// ── expr-layer view construction ──

impl<T: CreateView> CreateView for SnapshotAt<T> {
    type View<'graph, G: GraphView + 'graph> = WindowedGraph<<T as CreateView>::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.snapshot_at(self.time))
    }
}

impl<T: CreateView> CreateView for SnapshotLatest<T> {
    type View<'graph, G: GraphView + 'graph> = WindowedGraph<<T as CreateView>::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.snapshot_latest())
    }
}
