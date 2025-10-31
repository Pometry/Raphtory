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
pub struct EdgeNodeFilteredGraph<G, F> {
    graph: G,
    endpoint: Endpoint,
    filtered_graph: F,
}

impl<G, F> EdgeNodeFilteredGraph<G, F> {
    #[inline]
    pub fn new(graph: G, endpoint: Endpoint, filtered_graph: F) -> Self {
        Self {
            graph,
            endpoint,
            filtered_graph,
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

impl<'graph, G: GraphViewOps<'graph>, F: GraphViewOps<'graph>> InheritCoreGraphOps
    for EdgeNodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: GraphViewOps<'graph>> InheritStorageOps
    for EdgeNodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: GraphViewOps<'graph>> InheritLayerOps
    for EdgeNodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: GraphViewOps<'graph>> InheritListOps
    for EdgeNodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: GraphViewOps<'graph>> InheritMaterialize
    for EdgeNodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: GraphViewOps<'graph>> InheritNodeFilterOps
    for EdgeNodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: GraphViewOps<'graph>> InheritPropertiesOps
    for EdgeNodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: GraphViewOps<'graph>> InheritTimeSemantics
    for EdgeNodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: GraphViewOps<'graph>> InheritNodeHistoryFilter
    for EdgeNodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: GraphViewOps<'graph>> InheritEdgeHistoryFilter
    for EdgeNodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: GraphViewOps<'graph>> InheritEdgeLayerFilterOps
    for EdgeNodeFilteredGraph<G, F>
{
}
impl<'graph, G: GraphViewOps<'graph>, F: GraphViewOps<'graph>> InheritExplodedEdgeFilterOps
    for EdgeNodeFilteredGraph<G, F>
{
}

impl<'graph, G: GraphViewOps<'graph>, F: GraphViewOps<'graph>> InternalEdgeFilterOps
    for EdgeNodeFilteredGraph<G, F>
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
        if !self.graph.internal_filter_edge(edge, layer_ids) {
            return false;
        }

        let src_binding = self.graph.core_node(edge.src());
        let dst_binding = self.graph.core_node(edge.dst());
        let node_ref = match self.endpoint {
            Endpoint::Src => src_binding.as_ref(),
            Endpoint::Dst => dst_binding.as_ref(),
        };

        self.filtered_graph
            .internal_filter_node(node_ref, layer_ids)
    }
}
