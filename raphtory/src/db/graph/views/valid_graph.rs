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
use raphtory_api::{
    core::entities::{LayerId, LayerIds},
    inherit::Base,
};
use raphtory_storage::{core_ops::InheritCoreGraphOps, graph::edges::edge_ref::EdgeEntryRef};

#[derive(Copy, Clone, Debug)]
pub struct ValidGraph<G> {
    graph: G,
}

impl<'graph, G: GraphViewOps<'graph>> Base for ValidGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> ValidGraph<G> {
    pub fn new(graph: G) -> Self {
        Self { graph }
    }
}

impl<G> Static for ValidGraph<G> {}
impl<G> Immutable for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for ValidGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps for ValidGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for ValidGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeFilterOps for ValidGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritExplodedEdgeFilterOps for ValidGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeLayerFilterOps for ValidGraph<G> {
    fn internal_edge_layer_filtered(&self) -> bool {
        true
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        let time_semantics = self.graph.edge_time_semantics();
        time_semantics.edge_is_valid(edge, LayeredGraph::new(&self.graph, LayerIds::One(layer.0)))
            && self.graph.internal_filter_edge_layer(edge, layer)
    }
}
