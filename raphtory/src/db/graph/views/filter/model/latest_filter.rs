use crate::{
    db::{
        api::view::internal::GraphView,
        graph::views::{
            filter::{
                model::{
                    edge_filter::CompositeEdgeFilter, is_active_edge_filter::IsActiveEdge,
                    is_deleted_filter::IsDeletedEdge, is_self_loop_filter::IsSelfLoopEdge,
                    is_valid_filter::IsValidEdge, windowed_filter::Windowed, CombinedFilter,
                    ComposableFilter, CompositeExplodedEdgeFilter, CompositeNodeFilter, CreateView,
                    EdgeViewFilterOps, InternalViewWrapOps, TryAsCompositeFilter, Wrap,
                },
                CreateFilter,
            },
            window_graph::WindowedGraph,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, TimeOps},
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

impl<T: TryAsCompositeFilter> TryAsCompositeFilter for Latest<T> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Latest(Box::new(Latest::new(
            self.inner.try_as_composite_node_filter()?,
        ))))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::Latest(Box::new(Latest::new(
            self.inner.try_as_composite_edge_filter()?,
        ))))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::Latest(Box::new(Latest::new(
            self.inner.try_as_composite_exploded_edge_filter()?,
        ))))
    }
}

impl<T: CreateFilter + Clone + Send + Sync + 'static> CreateFilter for Latest<T> {
    type EntityFiltered<'graph, G>
        = T::EntityFiltered<'graph, WindowedGraph<G>>
    where
        G: GraphViewOps<'graph> + TimeOps<'graph, WindowedViewType = WindowedGraph<G>> + Clone;

    type NodeFilter<'graph, G>
        = T::NodeFilter<'graph, WindowedGraph<G>>
    where
        G: GraphView + TimeOps<'graph, WindowedViewType = WindowedGraph<G>> + Clone + 'graph;

    fn create_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError>
    where
        G: GraphViewOps<'graph> + TimeOps<'graph, WindowedViewType = WindowedGraph<G>> + Clone,
    {
        self.inner.create_filter(graph.latest())
    }

    fn create_node_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError>
    where
        G: GraphView + TimeOps<'graph, WindowedViewType = WindowedGraph<G>> + Clone + 'graph,
    {
        self.inner.create_node_filter(graph.latest())
    }
}

impl<T: ComposableFilter> ComposableFilter for Latest<T> {}

impl<T: CreateView> CreateView for Latest<T> {
    type View<'graph, G: GraphView + 'graph> = WindowedGraph<T::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.latest())
    }
}

impl<M> Wrap for Latest<M> {
    type Wrapped<T> = Latest<T>;
    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        Latest::new(value)
    }
}

impl<T: EdgeViewFilterOps> EdgeViewFilterOps for Latest<T> {
    type Output<F: CombinedFilter> = Latest<T::Output<F>>;

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
