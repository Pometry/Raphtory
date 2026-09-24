use crate::{
    db::{
        api::{
            state::ops::filter::NodeExistsOp,
            view::internal::{GraphView, Static},
        },
        graph::views::{
            filter::{
                model::{
                    edge_expr::ops::EdgeExistsOp, graph_filter::GraphFilterOps, ComposableFilter,
                    CreateView, InternalViewWrapOps,
                },
                CreateFilter,
            },
            window_graph::WindowedGraph,
        },
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

impl<T: InternalViewWrapOps> InternalViewWrapOps for Windowed<T> {
    type Window = T::Window;

    fn bounds(&self) -> (EventTime, EventTime) {
        (self.start, self.end)
    }

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        self.inner.build_window(start, end)
    }
}

/// A view wrapper applied as a filter: the inner filter's view is applied to the
/// graph and this view on top of it, in the order the chain was written. The nodes
/// and edges it selects are the ones that exist in the resulting view.
impl<T: GraphFilterOps> CreateFilter for Windowed<T> {
    type FilteredGraph<'graph, G>
        = WindowedGraph<T::FilteredGraph<'graph, G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = NodeExistsOp<WindowedGraph<T::FilteredGraph<'graph, G>>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = EdgeExistsOp<WindowedGraph<T::FilteredGraph<'graph, G>>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(self
            .inner
            .create_graph_filter(graph)?
            .window(self.start, self.end))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(NodeExistsOp::new(self.create_graph_filter(graph)?))
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        Ok(EdgeExistsOp::new(self.create_graph_filter(graph)?))
    }
}

impl<T: ComposableFilter> ComposableFilter for Windowed<T> {}

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

// ── expr layer: the windowed view scopes any inner expression (per-expression view) ──
// Nesting order of chained views is pinned by the view-semantics tests.
