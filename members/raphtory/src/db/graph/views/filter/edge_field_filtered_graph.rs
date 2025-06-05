use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                EdgeFilterOps, Immutable, InheritEdgeHistoryFilter, InheritLayerOps,
                InheritListOps, InheritMaterialize, InheritNodeFilterOps, InheritNodeHistoryFilter,
                InheritStorageOps, InheritTimeSemantics, Static,
            },
        },
        graph::views::filter::{internal::CreateEdgeFilter, model::Filter, EdgeFieldFilter},
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{
        entities::{LayerIds, ELID},
        storage::timeindex::TimeIndexEntry,
    },
    inherit::Base,
};
use raphtory_storage::{
    core_ops::{CoreGraphOps, InheritCoreGraphOps},
    graph::edges::edge_ref::EdgeStorageRef,
};

#[derive(Debug, Clone)]
pub struct EdgeFieldFilteredGraph<G> {
    graph: G,
    filter: Filter,
}

impl<G> EdgeFieldFilteredGraph<G> {
    pub(crate) fn new(graph: G, filter: Filter) -> Self {
        Self { graph, filter }
    }
}

impl CreateEdgeFilter for EdgeFieldFilter {
    type EdgeFiltered<'graph, G: GraphViewOps<'graph>> = EdgeFieldFilteredGraph<G>;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError> {
        Ok(EdgeFieldFilteredGraph::new(graph, self.0))
    }
}

impl<G> Base for EdgeFieldFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for EdgeFieldFilteredGraph<G> {}
impl<G> Immutable for EdgeFieldFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for EdgeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for EdgeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for EdgeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for EdgeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for EdgeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps for EdgeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for EdgeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for EdgeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for EdgeFieldFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for EdgeFieldFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for EdgeFieldFilteredGraph<G> {
    #[inline]
    fn edges_filtered(&self) -> bool {
        true
    }

    fn edge_history_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn edge_list_trusted(&self) -> bool {
        false
    }

    fn filter_edge_history(&self, eid: ELID, t: TimeIndexEntry, layer_ids: &LayerIds) -> bool {
        self.graph.filter_edge_history(eid, t, layer_ids) && {
            let edge = self.core_edge(eid.edge);
            self.filter.matches_edge(&self.graph, edge.as_ref())
        }
    }

    #[inline]
    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        if self.graph.filter_edge(edge, layer_ids) {
            self.filter.matches_edge(&self.graph, edge)
        } else {
            false
        }
    }
}
