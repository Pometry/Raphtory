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
        graph::views::filter::{internal::InternalEdgeFilterOps, model::NotFilter},
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::{
    entities::{LayerIds, ELID},
    storage::timeindex::TimeIndexEntry,
};

#[derive(Debug, Clone)]
pub struct EdgeNotFilteredGraph<G, T> {
    graph: G,
    filter: T,
}

impl<T: InternalEdgeFilterOps> InternalEdgeFilterOps for NotFilter<T> {
    type EdgeFiltered<'graph, G: GraphViewOps<'graph>>
        = EdgeNotFilteredGraph<G, T::EdgeFiltered<'graph, G>>
    where
        Self: 'graph;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError> {
        let filter = self.0.create_edge_filter(graph.clone())?;
        Ok(EdgeNotFilteredGraph { graph, filter })
    }
}

impl<G, T> Base for EdgeNotFilteredGraph<G, T> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, T> Static for EdgeNotFilteredGraph<G, T> {}
impl<G, T> Immutable for EdgeNotFilteredGraph<G, T> {}

impl<'graph, G: GraphViewOps<'graph>, T> InheritCoreOps for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritStorageOps for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritLayerOps for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritListOps for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritMaterialize for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritNodeFilterOps for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritPropertiesOps for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritTimeSemantics for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritNodeHistoryFilter for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritEdgeHistoryFilter for EdgeNotFilteredGraph<G, T> {}

impl<'graph, G: GraphViewOps<'graph>, T: EdgeFilterOps> EdgeFilterOps
    for EdgeNotFilteredGraph<G, T>
{
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
        self.graph.filter_edge_history(eid, t, layer_ids)
            && !self.filter.filter_edge_history(eid, t, layer_ids)
    }

    #[inline]
    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.filter_edge(edge, layer_ids) && !self.filter.filter_edge(edge.clone(), layer_ids)
    }
}
