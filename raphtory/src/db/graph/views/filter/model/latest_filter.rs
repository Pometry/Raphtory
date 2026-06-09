use crate::{
    db::{
        api::view::internal::GraphView,
        graph::views::{
            filter::{
                model::{
                    edge_filter::CompositeEdgeFilter,
                    windowed_filter::Windowed,
                    ComposableFilter, CompositeExplodedEdgeFilter,
                    CompositeNodeFilter, InternalViewWrapOps,
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
        = T::EntityFiltered<'graph, G>
    where
        G: GraphViewOps<'graph> + TimeOps<'graph> + Clone;

    type NodeFilter<'graph, G>
        = T::NodeFilter<'graph, G>
    where
        G: GraphView + TimeOps<'graph> + Clone + 'graph;

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
        G: GraphViewOps<'graph> + TimeOps<'graph, WindowedViewType = WindowedGraph<G>> + Clone,
    {
        self.inner.create_filter(graph)
    }

    fn create_node_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError>
    where
        G: GraphView + TimeOps<'graph, WindowedViewType = WindowedGraph<G>> + Clone + 'graph,
    {
        self.inner.create_node_filter(graph)
    }
}

impl<T: ComposableFilter> ComposableFilter for Latest<T> {}

impl<M> Wrap for Latest<M> {
    type Wrapped<T> = Latest<T>;
    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        Latest::new(value)
    }
}
