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
        graph::views::filter::model::{edge_filter::Endpoint, node_filter::CompositeNodeFilter},
    },
    prelude::GraphViewOps,
};
use raphtory_api::{core::entities::LayerIds, inherit::Base};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
};

#[derive(Debug, Clone)]
pub struct EdgeNodeFilteredGraph<G> {
    graph: G,
    endpoint: Endpoint,
    filter: CompositeNodeFilter,
}

impl<G> EdgeNodeFilteredGraph<G> {
    #[inline]
    pub fn new(graph: G, endpoint: Endpoint, node_cf: CompositeNodeFilter) -> Self {
        Self {
            graph,
            endpoint,
            filter: node_cf,
        }
    }
}

impl<G> Base for EdgeNodeFilteredGraph<G> {
    type Base = G;
    #[inline]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for EdgeNodeFilteredGraph<G> {}
impl<G> Immutable for EdgeNodeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for EdgeNodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for EdgeNodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for EdgeNodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for EdgeNodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for EdgeNodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps for EdgeNodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for EdgeNodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for EdgeNodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for EdgeNodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for EdgeNodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeLayerFilterOps for EdgeNodeFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritExplodedEdgeFilterOps for EdgeNodeFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeFilterOps for EdgeNodeFilteredGraph<G> {
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
        if !self.graph.internal_filter_edge(edge, layer_ids) {
            return false;
        }

        // Fetch the endpoint node and delegate to the node composite filter.
        let ok = match self.endpoint {
            Endpoint::Src => self
                .filter
                .matches_node(&self.graph, self.graph.core_node(edge.src()).as_ref()),
            Endpoint::Dst => self
                .filter
                .matches_node(&self.graph, self.graph.core_node(edge.dst()).as_ref()),
        };
        ok
    }
}
