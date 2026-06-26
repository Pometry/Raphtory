use crate::{
    db::{
        api::view::internal::{GraphView, InternalFilter},
        graph::views::filter::{
            model::{ComposableFilter, InternalViewWrapOps, Wrap},
            CreateFilter,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, LayerOps},
};
use raphtory_api::core::{entities::Layer, storage::timeindex::EventTime};
use std::{fmt, fmt::Display};
use crate::db::graph::views::filter::model::EdgeViewFilterOps;

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

impl<T: EdgeViewFilterOps> EdgeViewFilterOps for Layered<T> {}
