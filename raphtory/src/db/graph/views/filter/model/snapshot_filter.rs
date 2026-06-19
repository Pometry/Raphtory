use crate::{
    db::{
        api::view::internal::GraphView,
        graph::views::{
            filter::{
                model::{
                    edge_filter::CompositeEdgeFilter,
                    is_active_edge_filter::IsActiveEdge,
                    is_deleted_filter::IsDeletedEdge,
                    is_self_loop_filter::IsSelfLoopEdge,
                    is_valid_filter::IsValidEdge,
                    windowed_filter::Windowed,
                    CombinedFilter, ComposableFilter, CompositeExplodedEdgeFilter,
                    CompositeNodeFilter, CreateView, EdgeViewFilterOps, InternalViewWrapOps,
                    TryAsCompositeFilter, Wrap,
                },
                CreateFilter,
            },
            window_graph::WindowedGraph,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, TimeOps},
};
use raphtory_api::core::{storage::timeindex::EventTime, utils::time::IntoTime};
use std::{fmt, fmt::Display};

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

impl<T: TryAsCompositeFilter> TryAsCompositeFilter for SnapshotAt<T> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::SnapshotAt(Box::new(SnapshotAt {
            time: self.time,
            inner: self.inner.try_as_composite_node_filter()?,
        })))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::SnapshotAt(Box::new(SnapshotAt::new(
            self.time,
            self.inner.try_as_composite_edge_filter()?,
        ))))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::SnapshotAt(Box::new(
            SnapshotAt::new(
                self.time,
                self.inner.try_as_composite_exploded_edge_filter()?,
            ),
        )))
    }
}

impl<T: CreateFilter + Clone + Send + Sync + 'static> CreateFilter for SnapshotAt<T> {
    type EntityFiltered<'graph, G>
        = T::EntityFiltered<'graph, WindowedGraph<G>>
    where
        G: GraphViewOps<'graph> + TimeOps<'graph, WindowedViewType = WindowedGraph<G>>;

    type NodeFilter<'graph, G>
        = T::NodeFilter<'graph, WindowedGraph<G>>
    where
        G: GraphView + TimeOps<'graph, WindowedViewType = WindowedGraph<G>> + 'graph;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError>
    where
        G: GraphViewOps<'graph> + TimeOps<'graph, WindowedViewType = WindowedGraph<G>>,
    {
        self.inner.create_filter(graph.snapshot_at(self.time))
    }

    fn create_node_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError>
    where
        G: GraphView + TimeOps<'graph, WindowedViewType = WindowedGraph<G>> + 'graph,
    {
        self.inner.create_node_filter(graph.snapshot_at(self.time))
    }
}

impl<T: ComposableFilter> ComposableFilter for SnapshotAt<T> {}

impl<T: CreateView> CreateView for SnapshotAt<T> {
    type View<'graph, G: GraphView + 'graph> = WindowedGraph<T::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.snapshot_at(self.time))
    }
}

impl<M> Wrap for SnapshotAt<M> {
    type Wrapped<T> = SnapshotAt<T>;
    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        SnapshotAt {
            time: self.time,
            inner: value,
        }
    }
}

impl<T: EdgeViewFilterOps> EdgeViewFilterOps for SnapshotAt<T> {
    type Output<F: CombinedFilter> = SnapshotAt<T::Output<F>>;

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

impl<T: TryAsCompositeFilter> TryAsCompositeFilter for SnapshotLatest<T> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::SnapshotLatest(Box::new(
            SnapshotLatest::new(self.inner.try_as_composite_node_filter()?),
        )))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::SnapshotLatest(Box::new(
            SnapshotLatest::new(self.inner.try_as_composite_edge_filter()?),
        )))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::SnapshotLatest(Box::new(
            SnapshotLatest::new(self.inner.try_as_composite_exploded_edge_filter()?),
        )))
    }
}

impl<T: CreateFilter + Clone + Send + Sync + 'static> CreateFilter for SnapshotLatest<T> {
    type EntityFiltered<'graph, G>
        = T::EntityFiltered<'graph, WindowedGraph<G>>
    where
        G: GraphViewOps<'graph> + TimeOps<'graph, WindowedViewType = WindowedGraph<G>>;

    type NodeFilter<'graph, G>
        = T::NodeFilter<'graph, WindowedGraph<G>>
    where
        G: GraphView + TimeOps<'graph, WindowedViewType = WindowedGraph<G>> + 'graph;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError>
    where
        G: GraphViewOps<'graph> + TimeOps<'graph, WindowedViewType = WindowedGraph<G>>,
    {
        self.inner.create_filter(graph.snapshot_latest())
    }

    fn create_node_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError>
    where
        G: GraphView + TimeOps<'graph, WindowedViewType = WindowedGraph<G>> + 'graph,
    {
        self.inner.create_node_filter(graph.snapshot_latest())
    }
}

impl<T: ComposableFilter> ComposableFilter for SnapshotLatest<T> {}

impl<T: CreateView> CreateView for SnapshotLatest<T> {
    type View<'graph, G: GraphView + 'graph> = WindowedGraph<T::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.snapshot_latest())
    }
}

impl<M> Wrap for SnapshotLatest<M> {
    type Wrapped<T> = SnapshotLatest<T>;
    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        SnapshotLatest::new(value)
    }
}

impl<T: EdgeViewFilterOps> EdgeViewFilterOps for SnapshotLatest<T> {
    type Output<F: CombinedFilter> = SnapshotLatest<T::Output<F>>;

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
