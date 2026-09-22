use crate::{
    db::{
        api::{
            state::NodeOp,
            view::{internal::GraphView, time::TimeOps},
        },
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
};
use raphtory_api::core::{
    entities::properties::prop::Prop, storage::timeindex::EventTime, utils::time::IntoTime,
};
use std::{fmt, fmt::Display, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotAt<M> {
    pub time: EventTime,
    pub inner: M,
}

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

impl<T: InternalViewWrapOps> InternalViewWrapOps for SnapshotAt<T> {
    type Window = Windowed<SnapshotAt<T>>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Windowed::from_times(start, end, self)
    }
}

impl<T: CreateFilter + Clone + Send + Sync + 'static> CreateFilter for SnapshotAt<T> {
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
        Ok(self.inner.filter_graph_view(graph)?.snapshot_at(self.time))
    }
}

impl<T: ComposableFilter> ComposableFilter for SnapshotAt<T> {}

impl<M> Wrap for SnapshotAt<M> {
    type Wrapped<T> = SnapshotAt<T>;
    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        SnapshotAt {
            time: self.time,
            inner: value,
        }
    }
}

impl<U: NodeViewFilterOps> NodeViewFilterOps for SnapshotAt<U> {
    type Output<T: CombinedFilter> = SnapshotAt<U::Output<T>>;

    fn is_active(&self) -> Self::Output<IsActiveNode> {
        self.wrap(self.inner.is_active())
    }
}

impl<U: EdgeViewFilterOps> EdgeViewFilterOps for SnapshotAt<U> {
    type Output<T: CombinedFilter> = SnapshotAt<U::Output<T>>;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotLatest<M> {
    pub inner: M,
}

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

impl<T: InternalViewWrapOps> InternalViewWrapOps for SnapshotLatest<T> {
    type Window = Windowed<SnapshotLatest<T>>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Windowed::from_times(start, end, self)
    }
}

impl<T: CreateFilter + Clone + Send + Sync + 'static> CreateFilter for SnapshotLatest<T> {
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
        Ok(self.inner.filter_graph_view(graph)?.snapshot_latest())
    }
}

impl<T: ComposableFilter> ComposableFilter for SnapshotLatest<T> {}

impl<M> Wrap for SnapshotLatest<M> {
    type Wrapped<T> = SnapshotLatest<T>;
    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        SnapshotLatest::new(value)
    }
}

impl<U: NodeViewFilterOps> NodeViewFilterOps for SnapshotLatest<U> {
    type Output<T: CombinedFilter> = SnapshotLatest<U::Output<T>>;

    fn is_active(&self) -> Self::Output<IsActiveNode> {
        self.wrap(self.inner.is_active())
    }
}

impl<U: EdgeViewFilterOps> EdgeViewFilterOps for SnapshotLatest<U> {
    type Output<T: CombinedFilter> = SnapshotLatest<U::Output<T>>;

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

// ── expr layer: the snapshot-at view scopes any inner expression (per-expression view) ──
// Nesting order of chained views is pinned by the view-semantics tests.

impl<T: CreateOp> CreateOp for SnapshotAt<T> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_node_op(graph.snapshot_at(self.time))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_edge_op(graph.snapshot_at(self.time))
    }
}

// ── expr layer: the snapshot-latest view scopes any inner expression (per-expression view) ──
// Nesting order of chained views is pinned by the view-semantics tests.

impl<T: CreateOp> CreateOp for SnapshotLatest<T> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_node_op(graph.snapshot_latest())
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_edge_op(graph.snapshot_latest())
    }
}
