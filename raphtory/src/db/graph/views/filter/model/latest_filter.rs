use crate::{
    db::{
        api::{state::ops::filter::NodeExistsOp, view::internal::GraphView},
        graph::views::{
            filter::{
                model::{
                    edge_expr::ops::EdgeExistsOp, graph_filter::GraphFilterOps,
                    windowed_filter::Windowed, ComposableFilter, CreateView, InternalViewWrapOps,
                },
                CreateFilter,
            },
            window_graph::WindowedGraph,
        },
    },
    errors::GraphError,
    prelude::TimeOps,
};
use raphtory_api::core::storage::timeindex::EventTime;
use std::{fmt, fmt::Display};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Latest<M> {
    pub inner: M,
}

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

impl<T: InternalViewWrapOps> InternalViewWrapOps for Latest<T> {
    type Window = Windowed<Latest<T>>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Windowed::from_times(start, end, self)
    }
}

/// A view wrapper applied as a filter: the inner filter's view is applied to the
/// graph and this view on top of it, in the order the chain was written. The nodes
/// and edges it selects are the ones that exist in the resulting view.
impl<T: GraphFilterOps> CreateFilter for Latest<T> {
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
        Ok(self.inner.create_graph_filter(graph)?.latest())
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

impl<T: ComposableFilter> ComposableFilter for Latest<T> {}

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

// ── expr layer: the latest view scopes any inner expression (per-expression view) ──
// Nesting order of chained views is pinned by the view-semantics tests.
