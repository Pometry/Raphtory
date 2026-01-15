use crate::{
    db::{
        api::view::internal::GraphView,
        graph::views::{
            filter::{
                model::{
                    edge_filter::CompositeEdgeFilter,
                    node_filter::builders::{
                        InternalNodeFilterBuilder, InternalNodeIdFilterBuilder,
                    },
                    property_filter::builders::{
                        MetadataFilterBuilder, PropertyExprBuilder, PropertyFilterBuilder,
                    },
                    windowed_filter::Windowed,
                    ComposableFilter, CompositeExplodedEdgeFilter, CompositeNodeFilter,
                    InternalPropertyFilterBuilder, InternalPropertyFilterFactory,
                    InternalViewWrapOps, Op, PropertyRef, TemporalPropertyFilterFactory,
                    TryAsCompositeFilter, ViewWrapOps, Wrap,
                },
                CreateFilter,
            },
            layer_graph::LayeredGraph,
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, LayerOps, PropertyFilter},
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

impl<T: InternalNodeFilterBuilder> InternalNodeFilterBuilder for Layered<T> {
    type FilterType = T::FilterType;

    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: InternalNodeIdFilterBuilder> InternalNodeIdFilterBuilder for Layered<T> {
    fn field_name(&self) -> &'static str {
        self.inner.field_name()
    }
}

impl<T: InternalPropertyFilterBuilder> InternalPropertyFilterBuilder for Layered<T> {
    type Filter = Layered<T::Filter>;
    type ExprBuilder = Layered<T::ExprBuilder>;
    type Marker = T::Marker;

    fn property_ref(&self) -> PropertyRef {
        self.inner.property_ref()
    }

    fn ops(&self) -> &[Op] {
        self.inner.ops()
    }

    fn entity(&self) -> Self::Marker {
        self.inner.entity()
    }

    fn filter(&self, filter: PropertyFilter<Self::Marker>) -> Self::Filter {
        self.wrap(self.inner.filter(filter))
    }

    fn into_expr_builder(&self, builder: PropertyExprBuilder<Self::Marker>) -> Self::ExprBuilder {
        self.wrap(self.inner.into_expr_builder(builder))
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
        = T::EntityFiltered<'graph, LayeredGraph<G>>
    where
        G: GraphViewOps<'graph>;

    type NodeFilter<'graph, G>
        = T::NodeFilter<'graph, LayeredGraph<G>>
    where
        G: GraphView + 'graph;

    fn create_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError>
    where
        G: GraphViewOps<'graph>,
    {
        self.inner.create_filter(graph.layers(self.layer)?)
    }

    fn create_node_filter<'graph, G>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError>
    where
        G: GraphView + 'graph,
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

impl<T: InternalPropertyFilterFactory> InternalPropertyFilterFactory for Layered<T> {
    type Entity = T::Entity;
    type PropertyBuilder = Layered<T::PropertyBuilder>;
    type MetadataBuilder = Layered<T::MetadataBuilder>;

    fn entity(&self) -> Self::Entity {
        self.inner.entity()
    }

    fn property_builder(
        &self,
        builder: PropertyFilterBuilder<Self::Entity>,
    ) -> Self::PropertyBuilder {
        self.wrap(self.inner.property_builder(builder))
    }

    fn metadata_builder(
        &self,
        builder: MetadataFilterBuilder<Self::Entity>,
    ) -> Self::MetadataBuilder {
        self.wrap(self.inner.metadata_builder(builder))
    }
}

impl<T: TemporalPropertyFilterFactory> TemporalPropertyFilterFactory for Layered<T> {}
