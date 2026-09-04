use crate::{
    db::{
        api::{state::NodeOp, view::internal::GraphView},
        graph::views::{
            filter::{
                model::{
                    edge_expr::EdgeOp, is_active_edge_filter::IsActiveEdge,
                    is_active_node_filter::IsActiveNode, is_deleted_filter::IsDeletedEdge,
                    is_self_loop_filter::IsSelfLoopEdge, is_valid_filter::IsValidEdge,
                    node_expr::CreateOp, windowed_filter::Windowed, CombinedFilter,
                    ComposableFilter, CreateView, EdgeViewFilterOps, InternalViewWrapOps,
                    NodeViewFilterOps, Wrap,
                },
                CreateFilter,
            },
            window_graph::WindowedGraph,
        },
    },
    errors::GraphError,
    prelude::TimeOps,
};
use raphtory_api::core::{entities::properties::prop::Prop, storage::timeindex::EventTime};
use std::{fmt, fmt::Display, sync::Arc};

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

impl<T: CreateFilter + Clone + Send + Sync + 'static> CreateFilter for Latest<T> {
    type EntityFiltered<'graph, G, F>
        = T::EntityFiltered<'graph, G, F>
    where
        G: GraphView + TimeOps<'graph> + 'graph,
        F: GraphView + TimeOps<'graph> + 'graph;

    type NodeFilter<'graph, G, F>
        = T::NodeFilter<'graph, G, F>
    where
        G: GraphView + TimeOps<'graph> + 'graph,
        F: GraphView + TimeOps<'graph> + 'graph;

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
        Ok(self.inner.filter_graph_view(graph)?.latest())
    }
}

impl<T: ComposableFilter> ComposableFilter for Latest<T> {}

impl<M> Wrap for Latest<M> {
    type Wrapped<T> = Latest<T>;
    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        Latest::new(value)
    }
}

impl<U: NodeViewFilterOps> NodeViewFilterOps for Latest<U> {
    type Output<T: CombinedFilter> = Latest<U::Output<T>>;

    fn is_active(&self) -> Self::Output<IsActiveNode> {
        self.wrap(self.inner.is_active())
    }
}

impl<U: EdgeViewFilterOps> EdgeViewFilterOps for Latest<U> {
    type Output<T: CombinedFilter> = Latest<U::Output<T>>;

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

impl<T: CreateOp> CreateOp for Latest<T> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_node_op(graph.latest())
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_edge_op(graph.latest())
    }
}
