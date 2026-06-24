use crate::{
    db::{
        api::view::internal::{GraphView, InternalFilter},
        graph::views::{
            filter::{
                model::{
                    edge_filter::CompositeEdgeFilter, is_active_edge_filter::IsActiveEdge,
                    is_deleted_filter::IsDeletedEdge, is_self_loop_filter::IsSelfLoopEdge,
                    is_valid_filter::IsValidEdge, CombinedFilter, ComposableFilter,
                    CompositeExplodedEdgeFilter, CompositeNodeFilter, EdgeViewFilterOps,
                    InternalViewWrapOps, TryAsCompositeFilter, Wrap,
                },
                CreateFilter,
            },
            layer_graph::LayeredGraph,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, LayerOps},
};
use raphtory_api::core::{entities::Layer, storage::timeindex::EventTime};
use std::{fmt, fmt::Display};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layered<M> {
    pub layer: Layer,
    pub inner: M,
}

impl<M: Display> Display for Layered<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LAYER[{:?}]({})", self.layer, self.inner)
    }
}

impl<M> Layered<M> {
    #[inline]
    pub fn new(layer: Layer, entity: M) -> Self {
        Self {
            layer,
            inner: entity,
        }
    }

    #[inline]
    pub fn from_layers<L: Into<Layer>>(layer: L, entity: M) -> Self {
        Self::new(layer.into(), entity)
    }
}

impl<T: InternalViewWrapOps> InternalViewWrapOps for Layered<T> {
    type Window = Layered<T::Window>;

    fn bounds(&self) -> (EventTime, EventTime) {
        self.inner.bounds()
    }

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Layered::new(self.layer, self.inner.build_window(start, end))
    }
}

impl<T: TryAsCompositeFilter> TryAsCompositeFilter for Layered<T> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        let filter = self.inner.try_as_composite_node_filter()?;
        let filter = CompositeNodeFilter::Layered(Box::new(self.wrap(filter)));
        Ok(filter)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        let filter = self.inner.try_as_composite_edge_filter()?;
        let filter = CompositeEdgeFilter::Layered(Box::new(self.wrap(filter)));
        Ok(filter)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        let filter = self.inner.try_as_composite_exploded_edge_filter()?;
        let filter = CompositeExplodedEdgeFilter::Layered(Box::new(self.wrap(filter)));
        Ok(filter)
    }
}

impl<T: CreateFilter + Clone + Send + Sync + 'static> CreateFilter for Layered<T> {
    type EntityFiltered<'graph, G>
        = T::EntityFiltered<'graph, <G as LayerOps<'graph>>::LayeredViewType>
    where
        G: GraphViewOps<'graph> + InternalFilter<'graph>,
        <G as LayerOps<'graph>>::LayeredViewType: GraphViewOps<'graph>;

    type NodeFilter<'graph, G>
        = T::NodeFilter<'graph, <G as LayerOps<'graph>>::LayeredViewType>
    where
        G: GraphView + InternalFilter<'graph> + 'graph,
        <G as LayerOps<'graph>>::LayeredViewType: GraphView + 'graph;

    fn create_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError>
    where
        G: GraphViewOps<'graph> + InternalFilter<'graph>,
        <G as LayerOps<'graph>>::LayeredViewType: GraphViewOps<'graph>,
    {
        self.inner.create_filter(graph.layers(self.layer)?)
    }

    fn create_node_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError>
    where
        G: GraphView + InternalFilter<'graph> + 'graph,
        <G as LayerOps<'graph>>::LayeredViewType: GraphView + 'graph,
    {
        self.inner.create_node_filter(graph.layers(self.layer)?)
    }
}

impl<T: ComposableFilter> ComposableFilter for Layered<T> {}

impl<M> Wrap for Layered<M> {
    type Wrapped<T> = Layered<T>;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        Layered::new(self.layer.clone(), value)
    }
}

impl<T: EdgeViewFilterOps> EdgeViewFilterOps for Layered<T> {
    type Output<F: CombinedFilter> = Layered<T::Output<F>>;

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
