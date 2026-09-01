use crate::db::api::{
    properties::internal::{
        InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
    },
    state::ops::GraphView,
    view::internal::{
        FilterOps, Immutable, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
        InheritEdgeLayerFilterOps, InheritListOps, InheritMaterialize, InheritNodeFilterOps,
        InheritNodeHistoryFilter, InheritStorageOps, InheritTimeSemantics,
        InternalExplodedEdgeFilterOps, Static,
    },
};
use raphtory_api::{
    core::{
        entities::{LayerIds, ELID},
        storage::timeindex::EventTime,
    },
    inherit::Base,
};
use raphtory_storage::{core_ops::InheritCoreGraphOps, layer_ops::InheritLayerOps};

#[derive(Debug, Clone)]
pub struct ExplodedEdgeFilteredGraph<G, F> {
    base: G,
    filter: F,
}

impl<G, F> ExplodedEdgeFilteredGraph<G, F> {
    pub fn new(base: G, filter: F) -> Self {
        Self { base, filter }
    }
}

impl<G, F> Base for ExplodedEdgeFilteredGraph<G, F> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.base
    }
}

impl<G, F> Static for ExplodedEdgeFilteredGraph<G, F> {}
impl<G, F> Immutable for ExplodedEdgeFilteredGraph<G, F> {}

impl<G: GraphView, F: GraphView> InheritCoreGraphOps for ExplodedEdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritStorageOps for ExplodedEdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritLayerOps for ExplodedEdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritListOps for ExplodedEdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritMaterialize for ExplodedEdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritNodeFilterOps for ExplodedEdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritPropertiesOps for ExplodedEdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritNodePropertySchemaOps for ExplodedEdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritEdgePropertySchemaOps for ExplodedEdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritTimeSemantics for ExplodedEdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritNodeHistoryFilter for ExplodedEdgeFilteredGraph<G, F> {}
impl<G: GraphView, F: GraphView> InheritEdgeHistoryFilter for ExplodedEdgeFilteredGraph<G, F> {}

impl<G: GraphView, F: GraphView> InheritEdgeLayerFilterOps for ExplodedEdgeFilteredGraph<G, F> {}

impl<G: GraphView, F: GraphView> InheritEdgeFilterOps for ExplodedEdgeFilteredGraph<G, F> {}

impl<G: GraphView, F: GraphView> InternalExplodedEdgeFilterOps for ExplodedEdgeFilteredGraph<G, F> {
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.base.internal_exploded_edge_filtered() || self.filter.filtered()
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_exploded_edge(&self, eid: ELID, t: EventTime, layer_ids: &LayerIds) -> bool {
        self.base.internal_filter_exploded_edge(eid, t, layer_ids)
            && self.filter.filter_exploded_edge(eid, t)
    }
}
