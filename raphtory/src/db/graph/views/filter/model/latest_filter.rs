use crate::{
    db::{
        api::view::internal::{GraphView, Static},
        graph::views::{filter::model::CreateView, window_graph::WindowedGraph},
    },
    errors::GraphError,
    prelude::TimeOps,
};
use std::{fmt, fmt::Display};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Latest<M> {
    pub inner: M,
}

impl<M> Static for Latest<M> {}

impl<M> Latest<M> {
    #[inline]
    pub fn new(inner: M) -> Self {
        Self { inner }
    }
}

impl<M: Display> Display for Latest<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LATEST({})", self.inner)
    }
}

// ── expr-layer view construction ──

impl<T: CreateView> CreateView for Latest<T> {
    type View<'graph, G: GraphView + 'graph> = WindowedGraph<<T as CreateView>::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.latest())
    }
}
