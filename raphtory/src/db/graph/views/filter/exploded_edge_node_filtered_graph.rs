use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            state::ops::NodeFilterOp,
            view::internal::{
                GraphView, Immutable, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
                InheritEdgeLayerFilterOps, InheritExplodedEdgeFilterOps, InheritLayerOps,
                InheritListOps, InheritMaterialize, InheritNodeFilterOps, InheritNodeHistoryFilter,
                InheritStorageOps, InheritTimeSemantics, InternalEdgeFilterOps,
                InternalExplodedEdgeFilterOps, Static,
            },
        },
        graph::views::filter::model::edge_filter::Endpoint,
    },
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
    core_ops::InheritCoreGraphOps,
    graph::edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
};

#[derive(Debug, Clone)]
pub struct ExplodedEdgeNodeFilteredGraph<G, F> {
    graph: G,
    endpoint: Endpoint,
    filter: F,
}

impl<G, F> ExplodedEdgeNodeFilteredGraph<G, F> {
    #[inline]
    pub fn new(graph: G, endpoint: Endpoint, filter: F) -> Self {
        Self {
            graph,
            endpoint,
            filter,
        }
    }
}

impl<G, F> Base for ExplodedEdgeNodeFilteredGraph<G, F> {
    type Base = G;
    #[inline]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, F> Static for ExplodedEdgeNodeFilteredGraph<G, F> {}
impl<G, F> Immutable for ExplodedEdgeNodeFilteredGraph<G, F> {}

impl<G: GraphView, F: NodeFilterOp> InheritCoreGraphOps for ExplodedEdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritStorageOps for ExplodedEdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritLayerOps for ExplodedEdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritListOps for ExplodedEdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritMaterialize for ExplodedEdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritNodeFilterOps for ExplodedEdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritPropertiesOps for ExplodedEdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritTimeSemantics for ExplodedEdgeNodeFilteredGraph<G, F> {}
impl<G: GraphView, F: NodeFilterOp> InheritNodeHistoryFilter
    for ExplodedEdgeNodeFilteredGraph<G, F>
{
}
impl<G: GraphView, F: NodeFilterOp> InheritEdgeHistoryFilter
    for ExplodedEdgeNodeFilteredGraph<G, F>
{
}
impl<G: GraphView, F: NodeFilterOp> InheritEdgeLayerFilterOps
    for ExplodedEdgeNodeFilteredGraph<G, F>
{
}
impl<G: GraphView, F: NodeFilterOp> InheritEdgeFilterOps for ExplodedEdgeNodeFilteredGraph<G, F> {}

impl<G: GraphView, F: NodeFilterOp> InternalExplodedEdgeFilterOps
    for ExplodedEdgeNodeFilteredGraph<G, F>
{
    #[inline]
    fn internal_exploded_edge_filtered(&self) -> bool {
        true
    }

    #[inline]
    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> bool {
        if !self.graph.internal_filter_exploded_edge(eid, t, layer_ids) {
            return false;
        }

        let edge = self.graph.core_edge(eid.edge);

        let vid = match self.endpoint {
            Endpoint::Src => edge.src(),
            Endpoint::Dst => edge.dst(),
        };

        // TODO: Fix me! Apply needs to take a graph view as input
        self.filter.apply(self.graph.core_graph(), vid)
    }
}
