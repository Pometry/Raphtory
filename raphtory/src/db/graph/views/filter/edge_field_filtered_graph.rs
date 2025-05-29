use crate::{
    core::utils::errors::GraphError,
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            storage::graph::edges::edge_ref::EdgeStorageRef,
            view::{
                internal::{
                    EdgeFilterOps, Immutable, InheritCoreOps, InheritEdgeHistoryFilter,
                    InheritLayerOps, InheritListOps, InheritMaterialize, InheritNodeFilterOps,
                    InheritNodeHistoryFilter, InheritStorageOps, InheritTimeSemantics, Static,
                },
                Base,
            },
        },
        graph::views::filter::{internal::InternalEdgeFilterOps, model::Filter, EdgeFieldFilter},
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::LayerIds;

#[derive(Debug, Clone)]
pub struct EdgeFieldFilteredGraph<G> {
    graph: G,
    filter: Filter,
}

impl<'graph, G> EdgeFieldFilteredGraph<G> {
    pub(crate) fn new(graph: G, filter: Filter) -> Self {
        Self { graph, filter }
    }
}

impl InternalEdgeFilterOps for EdgeFieldFilter {
    type EdgeFiltered<'graph, G: GraphViewOps<'graph>> = EdgeFieldFilteredGraph<G>;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError> {
        Ok(EdgeFieldFilteredGraph::new(graph, self.0))
    }
}

impl<'graph, G> Base for EdgeFieldFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for EdgeFieldFilteredGraph<G> {}
impl<G> Immutable for EdgeFieldFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for EdgeFieldFilteredGraph<G> {}
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

    #[inline]
    fn edge_list_trusted(&self) -> bool {
        false
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
