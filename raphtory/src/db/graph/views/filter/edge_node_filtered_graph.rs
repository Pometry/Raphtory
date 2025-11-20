use crate::db::api::state::ops::NodeFilterOp;
use crate::db::api::view::internal::{GraphView};
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
        graph::views::filter::model::edge_filter::Endpoint,
    },
    prelude::GraphViewOps,
};
use raphtory_api::{core::entities::LayerIds, inherit::Base};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
};

#[derive(Debug, Clone)]
pub struct EdgeNodeFilteredGraph<G, F> {
    graph: G,
    endpoint: Endpoint,
    filter: F,
}

impl<G, F> EdgeNodeFilteredGraph<G, F> {
    #[inline]
    pub fn new(graph: G, endpoint: Endpoint, filter: F) -> Self {
        Self {
            graph,
            endpoint,
            filter,
        }
    }
}

impl<G, F> Base for EdgeNodeFilteredGraph<G, F> {
    type Base = G;
    #[inline]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, F> Static for EdgeNodeFilteredGraph<G, F> {}
impl<G, F> Immutable for EdgeNodeFilteredGraph<G, F> {}

impl<G: GraphView, F: NodeFilterOp> InheritCoreGraphOps for EdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritStorageOps for EdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritLayerOps for EdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritListOps for EdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritMaterialize for EdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritNodeFilterOps for EdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritPropertiesOps for EdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritTimeSemantics for EdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritNodeHistoryFilter for EdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritEdgeHistoryFilter for EdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritEdgeLayerFilterOps for EdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritExplodedEdgeFilterOps for EdgeNodeFilteredGraph<G, F> {}

impl<G: GraphView, F: NodeFilterOp> InternalEdgeFilterOps for EdgeNodeFilteredGraph<G, F> {
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

        let vid = match self.endpoint {
            Endpoint::Src => edge.src(),
            Endpoint::Dst => edge.dst(),
        };

        self.filter.apply(self.graph.core_graph(), vid)
    }
}
