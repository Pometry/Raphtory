use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                EdgeTimeSemanticsOps, Immutable, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
                InheritExplodedEdgeFilterOps, InheritLayerOps, InheritListOps, InheritMaterialize,
                InheritNodeFilterOps, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalEdgeLayerFilterOps, Static,
            },
        },
        graph::views::layer_graph::LayeredGraph,
    },
    prelude::GraphViewOps,
};
use raphtory_api::{core::entities::LayerIds, inherit::Base};
use raphtory_api::core::entities::LayerId;
use raphtory_storage::{core_ops::InheritCoreGraphOps, graph::edges::edge_ref::EdgeEntryRef};

#[derive(Copy, Clone, Debug)]
pub struct IsActiveGraph<G> {
    graph: G,
}

impl<'graph, G: GraphViewOps<'graph>> Base for IsActiveGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> IsActiveGraph<G> {
    pub fn new(graph: G) -> Self {
        Self { graph }
    }
}

impl<G> Static for IsActiveGraph<G> {}
impl<G> Immutable for IsActiveGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for IsActiveGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for IsActiveGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for IsActiveGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for IsActiveGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for IsActiveGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for IsActiveGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for IsActiveGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for IsActiveGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps for IsActiveGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for IsActiveGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeFilterOps for IsActiveGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritExplodedEdgeFilterOps for IsActiveGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeLayerFilterOps for IsActiveGraph<G> {
    fn internal_edge_layer_filtered(&self) -> bool {
        true
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        let time_semantics = self.graph.edge_time_semantics();
        time_semantics.edge_is_active(edge, LayeredGraph::new(&self.graph, LayerIds::One(layer.0)))
            && self.graph.internal_filter_edge_layer(edge, layer)
    }
}
