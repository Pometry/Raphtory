use crate::{
    db::{
        api::{
            properties::internal::{
                InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
            },
            view::internal::{
                EdgeTimeSemanticsOps, Immutable, InheritEdgeHistoryFilter,
                InheritEdgeLayerFilterOps, InheritExplodedEdgeFilterOps, InheritLayerOps,
                InheritListOps, InheritMaterialize, InheritNodeFilterOps, InheritNodeHistoryFilter,
                InheritStorageOps, InheritTimeSemantics, InternalEdgeFilterOps, Static,
            },
        },
        graph::views::layer_graph::LayeredGraph,
    },
    prelude::GraphViewOps,
};
use raphtory_api::{core::entities::LayerIds, inherit::Base};
use raphtory_storage::{core_ops::InheritCoreGraphOps, graph::edges::edge_ref::EdgeEntryRef};

#[derive(Copy, Clone, Debug)]
pub struct IsDeletedGraph<G> {
    graph: G,
}

impl<'graph, G: GraphViewOps<'graph>> Base for IsDeletedGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> IsDeletedGraph<G> {
    pub fn new(graph: G) -> Self {
        Self { graph }
    }
}

impl<G> Static for IsDeletedGraph<G> {}
impl<G> Immutable for IsDeletedGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for IsDeletedGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for IsDeletedGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for IsDeletedGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for IsDeletedGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for IsDeletedGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for IsDeletedGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for IsDeletedGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for IsDeletedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritNodePropertySchemaOps for IsDeletedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgePropertySchemaOps for IsDeletedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps for IsDeletedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for IsDeletedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritExplodedEdgeFilterOps for IsDeletedGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeLayerFilterOps for IsDeletedGraph<G> {}

/// An edge is deleted only when *no* layer of the current view still holds it
/// alive, which is what `EdgeView::is_deleted` reports.
///
/// This has to be the whole-edge filter rather than the per-layer one: an edge
/// passes a layer filter when *any* of its layers passes, so testing layers
/// individually would answer "some layer has a deletion" instead. The two
/// readings diverge as soon as an edge's layers disagree — a deletion recorded
/// on a layer the edge was never added to (which `delete_edge` does by default,
/// tombstoning `_default`) would then report an edge as deleted while it is
/// still alive on another layer, and while `is_deleted()` says it is not.
impl<'graph, G: GraphViewOps<'graph>> InternalEdgeFilterOps for IsDeletedGraph<G> {
    fn internal_edge_filtered(&self) -> bool {
        true
    }

    fn internal_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        let time_semantics = self.graph.edge_time_semantics();
        time_semantics.edge_is_deleted(edge, LayeredGraph::new(&self.graph, layer_ids.clone()))
            && self.graph.internal_filter_edge(edge, layer_ids)
    }
}
