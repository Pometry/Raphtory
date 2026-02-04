use crate::{
    db::api::{
        properties::internal::InheritPropertiesOps,
        view::internal::{
            Immutable, InheritEdgeHistoryFilter, InheritEdgeLayerFilterOps,
            InheritExplodedEdgeFilterOps, InheritLayerOps, InheritListOps, InheritMaterialize,
            InheritNodeFilterOps, InheritNodeHistoryFilter, InheritStorageOps,
            InheritTimeSemantics, InternalEdgeFilterOps, Static,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::{core::entities::LayerIds, inherit::Base};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
};

#[derive(Copy, Clone, Debug)]
pub struct IsSelfLoopGraph<G> {
    graph: G,
}

impl<'graph, G: GraphViewOps<'graph>> Base for IsSelfLoopGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> IsSelfLoopGraph<G> {
    pub fn new(graph: G) -> Self {
        Self { graph }
    }
}

impl<G> Static for IsSelfLoopGraph<G> {}
impl<G> Immutable for IsSelfLoopGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for IsSelfLoopGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for IsSelfLoopGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for IsSelfLoopGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for IsSelfLoopGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for IsSelfLoopGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for IsSelfLoopGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for IsSelfLoopGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for IsSelfLoopGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps for IsSelfLoopGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for IsSelfLoopGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeFilterOps for IsSelfLoopGraph<G> {
    fn internal_edge_filtered(&self) -> bool {
        true
    }

    fn internal_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        edge.src() == edge.dst() && self.graph.internal_filter_edge(edge, layer_ids)
    }
}

impl<'graph, G: GraphViewOps<'graph>> InheritExplodedEdgeFilterOps for IsSelfLoopGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeLayerFilterOps for IsSelfLoopGraph<G> {}
