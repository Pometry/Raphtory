use crate::{
    db::{
        api::{state::NodeOp, view::internal::GraphView},
        graph::views::{
            filter::{
                model::{
                    edge_expr::EdgeOp, is_active_edge_filter::IsActiveEdge,
                    is_active_node_filter::IsActiveNode, is_deleted_filter::IsDeletedEdge,
                    is_self_loop_filter::IsSelfLoopEdge, is_valid_filter::IsValidEdge,
                    node_expr::CreateOp, CombinedFilter, ComposableFilter, CreateView,
                    EdgeViewFilterOps, InternalViewWrapOps, NodeViewFilterOps, Wrap,
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
    entities::properties::prop::Prop,
    storage::timeindex::{AsTime, EventTime},
    utils::time::IntoTime,
};
use std::{fmt, fmt::Display, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Windowed<M> {
    pub start: EventTime,
    pub end: EventTime,
    pub inner: M,
}

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

impl<T: CreateFilter + Clone + Send + Sync + 'static> CreateFilter for Windowed<T> {
    type EntityFiltered<'graph, G, F>
        = T::EntityFiltered<'graph, G, F>
    where
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type NodeFilter<'graph, G, F>
        = T::NodeFilter<'graph, G, F>
    where
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type FilteredGraph<'graph, G>
        = WindowedGraph<T::FilteredGraph<'graph, G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G, F>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError>
    where
        G: GraphView + 'graph,
        F: GraphView + 'graph,
    {
        self.inner.create_filter(graph, filtered)
    }

    fn create_node_filter<'graph, G, F>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError>
    where
        G: GraphView + 'graph,
        F: GraphView + 'graph,
    {
        self.inner.create_node_filter(graph, filtered)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(self
            .inner
            .filter_graph_view(graph)?
            .window(self.start.t(), self.end.t()))
    }
}

impl<T: ComposableFilter> ComposableFilter for Windowed<T> {}

impl<M> Wrap for Windowed<M> {
    type Wrapped<T> = Windowed<T>;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        Windowed::new(self.start, self.end, value)
    }
}

impl<U: NodeViewFilterOps> NodeViewFilterOps for Windowed<U> {
    type Output<T: CombinedFilter> = Windowed<U::Output<T>>;

    fn is_active(&self) -> Self::Output<IsActiveNode> {
        self.wrap(self.inner.is_active())
    }
}

impl<U: EdgeViewFilterOps> EdgeViewFilterOps for Windowed<U> {
    type Output<T: CombinedFilter> = Windowed<U::Output<T>>;

    fn is_active(&self) -> Self::Output<IsActiveEdge> {
        self.wrap(self.inner.is_active())
    }

    fn is_valid(&self) -> Self::Output<IsValidEdge> {
        self.wrap(self.inner.is_valid())
    }

    fn is_deleted(&self) -> Self::Output<IsDeletedEdge> {
        self.wrap(self.inner.is_deleted())
    }

    fn is_self_loop(&self) -> Self::Output<IsSelfLoopEdge> {
        self.wrap(self.inner.is_self_loop())
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
        Ok(inner.window(self.start.t(), self.end.t()))
    }
}

// ── expr layer: the windowed view scopes any inner expression (per-expression view) ──
// Nesting order of chained views is pinned by the view-semantics tests.

impl<T: CreateOp> CreateOp for Windowed<T> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner
            .create_node_op(graph.window(self.start.t(), self.end.t()))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner
            .create_edge_op(graph.window(self.start.t(), self.end.t()))
    }
}
