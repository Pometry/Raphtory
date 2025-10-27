use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                Immutable, InheritEdgeHistoryFilter, InheritEdgeLayerFilterOps,
                InheritExplodedEdgeFilterOps, InheritListOps, InheritMaterialize,
                InheritNodeFilterOps, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalEdgeFilterOps, Static,
            },
        },
        graph::views::filter::{
            internal::CreateFilter,
            model::edge_filter::{EdgeDstEndpoint, EdgeSrcEndpoint},
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, PropertyFilter},
};
use raphtory_api::{core::entities::LayerIds, inherit::Base};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
    layer_ops::InheritLayerOps,
};

#[derive(Debug, Clone)]
pub struct EdgeSrcEndpointPropertyFilteredGraph<G> {
    graph: G,
    prop_id: usize,
    filter: PropertyFilter<EdgeSrcEndpoint>,
}

#[derive(Debug, Clone)]
pub struct EdgeDstEndpointPropertyFilteredGraph<G> {
    graph: G,
    prop_id: usize,
    filter: PropertyFilter<EdgeDstEndpoint>,
}

impl<G> EdgeSrcEndpointPropertyFilteredGraph<G> {
    fn new(graph: G, prop_id: usize, filter: PropertyFilter<EdgeSrcEndpoint>) -> Self {
        Self {
            graph,
            prop_id,
            filter,
        }
    }
}

impl<G> EdgeDstEndpointPropertyFilteredGraph<G> {
    fn new(graph: G, prop_id: usize, filter: PropertyFilter<EdgeDstEndpoint>) -> Self {
        Self {
            graph,
            prop_id,
            filter,
        }
    }
}

impl CreateFilter for PropertyFilter<EdgeSrcEndpoint> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = EdgeSrcEndpointPropertyFilteredGraph<G>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let prop_id = self.resolve_prop_id(graph.node_meta(), false)?;
        Ok(EdgeSrcEndpointPropertyFilteredGraph::new(
            graph, prop_id, self,
        ))
    }
}

impl CreateFilter for PropertyFilter<EdgeDstEndpoint> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = EdgeDstEndpointPropertyFilteredGraph<G>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let prop_id = self.resolve_prop_id(graph.node_meta(), false)?;
        Ok(EdgeDstEndpointPropertyFilteredGraph::new(
            graph, prop_id, self,
        ))
    }
}

// EdgeSrcEndpointPropertyFilteredGraph
impl<G> Base for EdgeSrcEndpointPropertyFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for EdgeSrcEndpointPropertyFilteredGraph<G> {}
impl<G> Immutable for EdgeSrcEndpointPropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps
    for EdgeSrcEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps
    for EdgeSrcEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for EdgeSrcEndpointPropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for EdgeSrcEndpointPropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize
    for EdgeSrcEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps
    for EdgeSrcEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps
    for EdgeSrcEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics
    for EdgeSrcEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter
    for EdgeSrcEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter
    for EdgeSrcEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritExplodedEdgeFilterOps
    for EdgeSrcEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeLayerFilterOps
    for EdgeSrcEndpointPropertyFilteredGraph<G>
{
}

// EdgeDstEndpointPropertyFilteredGraph
impl<G> Base for EdgeDstEndpointPropertyFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for EdgeDstEndpointPropertyFilteredGraph<G> {}
impl<G> Immutable for EdgeDstEndpointPropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps
    for EdgeDstEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps
    for EdgeDstEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for EdgeDstEndpointPropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for EdgeDstEndpointPropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize
    for EdgeDstEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps
    for EdgeDstEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps
    for EdgeDstEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics
    for EdgeDstEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter
    for EdgeDstEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter
    for EdgeDstEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritExplodedEdgeFilterOps
    for EdgeDstEndpointPropertyFilteredGraph<G>
{
}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeLayerFilterOps
    for EdgeDstEndpointPropertyFilteredGraph<G>
{
}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeFilterOps
    for EdgeSrcEndpointPropertyFilteredGraph<G>
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
            && self.filter.matches_node(
                &self.graph,
                self.prop_id,
                self.graph.core_node(edge.src()).as_ref(),
            )
    }
}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeFilterOps
    for EdgeDstEndpointPropertyFilteredGraph<G>
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
            && self.filter.matches_node(
                &self.graph,
                self.prop_id,
                self.graph.core_node(edge.dst()).as_ref(),
            )
    }
}
