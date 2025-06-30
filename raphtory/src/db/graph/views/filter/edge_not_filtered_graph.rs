use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                Immutable, InheritEdgeHistoryFilter, InheritLayerOps, InheritListOps,
                InheritMaterialize, InheritNodeFilterOps, InheritNodeHistoryFilter,
                InheritStorageOps, InheritTimeSemantics, InternalEdgeFilterOps,
                InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps, Static,
            },
        },
        graph::views::filter::{internal::CreateEdgeFilter, model::NotFilter},
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
use raphtory_storage::{core_ops::InheritCoreGraphOps, graph::edges::edge_ref::EdgeStorageRef};

#[derive(Debug, Clone)]
pub struct EdgeNotFilteredGraph<G, T> {
    graph: G,
    filter: T,
}

impl<T: CreateEdgeFilter> CreateEdgeFilter for NotFilter<T> {
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

impl<'graph, G: GraphViewOps<'graph>, T> InheritCoreGraphOps for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritStorageOps for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritLayerOps for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritListOps for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritMaterialize for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritNodeFilterOps for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritPropertiesOps for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritTimeSemantics for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritNodeHistoryFilter for EdgeNotFilteredGraph<G, T> {}
impl<'graph, G: GraphViewOps<'graph>, T> InheritEdgeHistoryFilter for EdgeNotFilteredGraph<G, T> {}

impl<'graph, G: GraphViewOps<'graph>, T: InternalEdgeLayerFilterOps> InternalEdgeLayerFilterOps
    for EdgeNotFilteredGraph<G, T>
{
    fn internal_edge_layer_filtered(&self) -> bool {
        true
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_edge_layer(&self, edge: EdgeStorageRef, layer: usize) -> bool {
        self.graph.internal_filter_edge_layer(edge, layer)
            && !self.filter.internal_filter_edge_layer(edge, layer)
    }
}

impl<'graph, G: GraphViewOps<'graph>, T: InternalExplodedEdgeFilterOps>
    InternalExplodedEdgeFilterOps for EdgeNotFilteredGraph<G, T>
{
    fn internal_exploded_edge_filtered(&self) -> bool {
        true
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> bool {
        self.graph.internal_filter_exploded_edge(eid, t, layer_ids)
            && !self.filter.internal_filter_exploded_edge(eid, t, layer_ids)
    }
}

impl<'graph, G: GraphViewOps<'graph>, T: InternalEdgeFilterOps> InternalEdgeFilterOps
    for EdgeNotFilteredGraph<G, T>
{
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
        self.graph.internal_filter_edge(edge, layer_ids)
            && !self.filter.internal_filter_edge(edge, layer_ids)
    }
}
