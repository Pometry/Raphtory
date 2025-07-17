use crate::{
    core::entities::LayerIds,
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
        graph::views::filter::{internal::CreateEdgeFilter, PropertyFilter},
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::inherit::Base;
use raphtory_storage::{core_ops::InheritCoreGraphOps, graph::edges::edge_ref::EdgeStorageRef};

#[derive(Debug, Clone)]
pub struct EdgePropertyFilteredGraph<G> {
    graph: G,
    t_prop_id: Option<usize>,
    c_prop_id: Option<usize>,
    filter: PropertyFilter,
}

impl<G> EdgePropertyFilteredGraph<G> {
    pub(crate) fn new(
        graph: G,
        t_prop_id: Option<usize>,
        c_prop_id: Option<usize>,
        filter: PropertyFilter,
    ) -> Self {
        Self {
            graph,
            t_prop_id,
            c_prop_id,
            filter,
        }
    }
}

impl CreateEdgeFilter for PropertyFilter {
    type EdgeFiltered<'graph, G: GraphViewOps<'graph>> = EdgePropertyFilteredGraph<G>;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError> {
        let t_prop_id = self.resolve_temporal_prop_id(graph.edge_meta())?;
        let c_prop_id = self.resolve_constant_prop_id(graph.edge_meta())?;
        Ok(EdgePropertyFilteredGraph::new(
            graph, t_prop_id, c_prop_id, self,
        ))
    }
}

impl<G> Base for EdgePropertyFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G> Static for EdgePropertyFilteredGraph<G> {}
impl<G> Immutable for EdgePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for EdgePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritExplodedEdgeFilterOps
    for EdgePropertyFilteredGraph<G>
{
}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeLayerFilterOps for EdgePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeFilterOps for EdgePropertyFilteredGraph<G> {
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
            self.filter
                .matches_edge(&self.graph, self.t_prop_id, self.c_prop_id, edge)
        } else {
            false
        }
    }
}
