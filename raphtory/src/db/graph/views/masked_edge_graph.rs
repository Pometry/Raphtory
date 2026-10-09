use crate::db::api::{
    properties::internal::{
        InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
    },
    state::ops::GraphView,
    view::internal::{
        Immutable, InheritEdgeHistoryFilter, InheritEdgeLayerFilterOps,
        InheritExplodedEdgeFilterOps, InheritListOps, InheritMaterialize, InheritNodeFilterOps,
        InheritNodeHistoryFilter, InheritStorageOps, InheritTimeSemantics, InternalEdgeFilterOps,
        Static,
    },
};
use raphtory_api::{
    core::entities::{LayerIds, EID},
    inherit::Base,
    roaring::RoaringEdgeMap,
};
use raphtory_storage::{core_ops::InheritCoreGraphOps, layer_ops::InheritLayerOps};
use std::{fmt::Debug, sync::Arc};
use storage::{api::edges::EdgeRefOps, EdgeEntryRef};

#[derive(Clone, Debug)]
pub struct MaskedEdgeGraph<G> {
    graph: G,
    mask: Arc<RoaringEdgeMap>,
}

impl<G> Static for MaskedEdgeGraph<G> {}

impl<G> Base for MaskedEdgeGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G: GraphView> Immutable for MaskedEdgeGraph<G> {}

impl<G: GraphView> InheritCoreGraphOps for MaskedEdgeGraph<G> {}
impl<G: GraphView> InheritStorageOps for MaskedEdgeGraph<G> {}
impl<G: GraphView> InheritTimeSemantics for MaskedEdgeGraph<G> {}
impl<G: GraphView> InheritPropertiesOps for MaskedEdgeGraph<G> {}
impl<G: GraphView> InheritNodePropertySchemaOps for MaskedEdgeGraph<G> {}
impl<G: GraphView> InheritEdgePropertySchemaOps for MaskedEdgeGraph<G> {}
impl<G: GraphView> InheritMaterialize for MaskedEdgeGraph<G> {}
impl<G: GraphView> InheritLayerOps for MaskedEdgeGraph<G> {}
impl<G: GraphView> InheritNodeHistoryFilter for MaskedEdgeGraph<G> {}
impl<G: GraphView> InheritEdgeHistoryFilter for MaskedEdgeGraph<G> {}
impl<G: GraphView> InheritListOps for MaskedEdgeGraph<G> {}
impl<G: GraphView> InheritExplodedEdgeFilterOps for MaskedEdgeGraph<G> {}
impl<G: GraphView> InheritEdgeLayerFilterOps for MaskedEdgeGraph<G> {}
impl<G: GraphView> InheritNodeFilterOps for MaskedEdgeGraph<G> {}

impl<G: GraphView> MaskedEdgeGraph<G> {
    pub fn new(graph: G, nodes: impl IntoIterator<Item = EID>) -> Self {
        let mask = nodes.into_iter().collect();
        MaskedEdgeGraph {
            graph,
            mask: Arc::new(mask),
        }
    }
}

impl<G: GraphView> InternalEdgeFilterOps for MaskedEdgeGraph<G> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        false
    }

    fn node_filter_includes_edge_filter(&self) -> bool {
        self.graph.node_filter_includes_edge_filter()
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        !self.mask.contains(edge.edge_id()) && self.graph.internal_filter_edge(edge, layer_ids)
    }
}
