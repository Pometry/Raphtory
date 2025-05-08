use crate::{
    core::utils::errors::GraphError,
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            storage::graph::edges::edge_ref::EdgeStorageRef,
            view::{
                internal::{
                    EdgeFilterOps, Immutable, InheritCoreOps, InheritEdgeFilterOps,
                    InheritEdgeHistoryFilter, InheritLayerOps, InheritListOps, InheritMaterialize,
                    InheritNodeFilterOps, InheritNodeHistoryFilter, InheritStorageOps,
                    InheritTimeSemantics, Static,
                },
                Base,
            },
        },
        graph::views::filter::{
            edge_field_filtered_graph::EdgeFieldFilteredGraph, internal::InternalEdgeFilterOps,
            model::NotFilter,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::LayerIds;

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

    #[inline]
    fn edge_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        !self.filter.filter_edge(edge.clone(), layer_ids)
    }
}
