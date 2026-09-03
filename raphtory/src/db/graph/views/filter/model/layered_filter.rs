use crate::{
    db::{
        api::{state::NodeOp, view::internal::GraphView},
        graph::views::{
            filter::{
                model::{
                    edge_expr::EdgeOp,
                    edge_filter::CompositeEdgeFilter,
                    is_active_edge_filter::IsActiveEdge,
                    is_active_node_filter::IsActiveNode,
                    is_deleted_filter::IsDeletedEdge,
                    is_self_loop_filter::IsSelfLoopEdge,
                    is_valid_filter::IsValidEdge,
                    node_expr::CreateOp,
                    node_filter::builders::{
                        InternalNodeFilterBuilder, InternalNodeIdFilterBuilder,
                    },
                    property_filter::{builders::PropertyExprBuilderInput, PropertyFilterInput},
                    CombinedFilter, ComposableFilter, CompositeExplodedEdgeFilter,
                    CompositeNodeFilter, EdgeViewFilterOps, FilterTree, GraphViewOp,
                    InternalPropertyFilterBuilder, InternalPropertyFilterFactory,
                    InternalViewWrapOps, NodeViewFilterOps, Op, PropertyRef,
                    TemporalPropertyFilterFactory, Wrap,
                },
                CreateFilter,
            },
            layer_graph::LayeredGraph,
        },
    },
    errors::GraphError,
    prelude::LayerOps,
};
use raphtory_api::core::{
    entities::{properties::prop::Prop, Layer},
    storage::timeindex::EventTime,
};
use std::{fmt, fmt::Display, sync::Arc};

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

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        self.wrap(self.inner.filter(filter))
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        self.wrap(self.inner.with_expr_builder(builder))
    }
}

impl<T: CreateFilter + Clone + Send + Sync + 'static> CreateFilter for Layered<T> {
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
        = LayeredGraph<T::FilteredGraph<'graph, G>>
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
        self.inner
            .filter_graph_view(graph)?
            .layers(self.layer.clone())
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

    fn property_builder(&self, property: String) -> Self::PropertyBuilder {
        self.wrap(self.inner.property_builder(property))
    }

    fn metadata_builder(&self, property: String) -> Self::MetadataBuilder {
        self.wrap(self.inner.metadata_builder(property))
    }
}

impl<T: TemporalPropertyFilterFactory> TemporalPropertyFilterFactory for Layered<T> {}

impl<U: NodeViewFilterOps> NodeViewFilterOps for Layered<U> {
    type Output<T: CombinedFilter> = Layered<U::Output<T>>;

    fn is_active(&self) -> Self::Output<IsActiveNode> {
        self.wrap(self.inner.is_active())
    }
}

impl<U: EdgeViewFilterOps> EdgeViewFilterOps for Layered<U> {
    type Output<T: CombinedFilter> = Layered<U::Output<T>>;

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

// ── expr layer: the layer view scopes any inner expression (per-expression view) ──
// Nesting order of chained views is pinned by the Phase-3 semantics tests.

impl<T: CreateOp> CreateOp for Layered<T> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_node_op(graph.layers(self.layer.clone())?)
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_edge_op(graph.layers(self.layer.clone())?)
    }
}
