use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                Immutable, InheritEdgeHistoryFilter, InheritEdgeLayerFilterOps,
                InheritExplodedEdgeFilterOps, InheritLayerOps, InheritListOps, InheritMaterialize,
                InheritNodeFilterOps, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalEdgeFilterOps, Static,
            },
        },
        graph::views::filter::{internal::CreateEdgeFilter, model::Filter, EdgeFieldFilter},
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::{core::entities::LayerIds, inherit::Base};
use raphtory_storage::{core_ops::InheritCoreGraphOps, graph::edges::edge_ref::EdgeStorageRef};

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

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeLayerFilterOps for EdgeFieldFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritExplodedEdgeFilterOps for EdgeFieldFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeFilterOps for EdgeFieldFilteredGraph<G> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        if self.graph.internal_filter_edge(edge, layer_ids) {
            self.filter.matches_edge(&self.graph, edge)
        } else {
            false
        }
    }
}
