use crate::db::api::{
    properties::internal::{
        InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
    },
    state::ops::GraphView,
    view::internal::{
        FilterOps, Immutable, InheritEdgeHistoryFilter, InheritEdgeLayerFilterOps,
        InheritExplodedEdgeFilterOps, InheritListOps, InheritMaterialize, InheritNodeFilterOps,
        InheritNodeHistoryFilter, InheritStorageOps, InheritTimeSemantics, InternalEdgeFilterOps,
        InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps, Static,
    },
};
use raphtory_api::{
    core::{
        entities::{LayerId, LayerIds, ELID},
        storage::timeindex::EventTime,
    },
    inherit::Base,
};
use raphtory_storage::{core_ops::InheritCoreGraphOps, layer_ops::InheritLayerOps};
use storage::EdgeEntryRef;

#[derive(Debug, Clone)]
pub struct EdgeFilteredGraph<G, F> {
    base: G,
    filter: F,
}

impl<G, F> EdgeFilteredGraph<G, F> {
    pub fn new(base: G, filter: F) -> Self {
        Self { base, filter }
    }
}

impl<G, F> Base for EdgeFilteredGraph<G, F> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.base
    }
}

impl<G, F> Static for EdgeFilteredGraph<G, F> {}
impl<G, F> Immutable for EdgeFilteredGraph<G, F> {}

impl<G: GraphView, F: GraphView> InheritCoreGraphOps for EdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritStorageOps for EdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritLayerOps for EdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritListOps for EdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritMaterialize for EdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritNodeFilterOps for EdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritPropertiesOps for EdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritNodePropertySchemaOps for EdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritEdgePropertySchemaOps for EdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritTimeSemantics for EdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritNodeHistoryFilter for EdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritEdgeHistoryFilter for EdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InternalEdgeLayerFilterOps for EdgeFilteredGraph<G, F> {
    fn internal_edge_layer_filtered(&self) -> bool {
        self.base.internal_edge_layer_filtered() || self.filter.internal_edge_layer_filtered()
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.base.internal_filter_edge_layer(edge, layer)
            && self.filter.filter_edge_layer(edge, layer)
    }
}
impl<G: GraphView, F: GraphView> InternalExplodedEdgeFilterOps for EdgeFilteredGraph<G, F> {
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.base.internal_exploded_edge_filtered() || self.filter.internal_exploded_edge_filtered()
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_exploded_edge(&self, eid: ELID, t: EventTime, layer_ids: &LayerIds) -> bool {
        self.base.internal_filter_exploded_edge(eid, t, layer_ids)
            && self.filter.filter_exploded_edge(eid, t)
    }
}

impl<G: GraphView, F: GraphView> InternalEdgeFilterOps for EdgeFilteredGraph<G, F> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.filter.filtered()
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        self.base.internal_filter_edge(edge, layer_ids) && self.filter.filter_edge(edge)
    }
}
