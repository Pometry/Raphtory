use crate::{
    db::api::{
        properties::internal::InheritPropertiesOps,
        view::internal::{
            EdgeList, Immutable, InheritMaterialize, InheritStorageOps, InheritTimeSemantics,
            InternalEdgeFilterOps, InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps,
            InternalLayerOps, InternalNodeFilterOps, ListOps, NodeList, Static,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{
        entities::{LayerIds, ELID},
        storage::timeindex::EventTime,
    },
    inherit::Base,
};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::{edges::edge_ref::EdgeEntryRef, nodes::node_ref::NodeStorageRef},
};

#[derive(Debug, Clone)]
pub struct AndFilteredGraph<G, L, R> {
    pub(crate) graph: G,
    pub(crate) left: L,
    pub(crate) right: R,
    pub(crate) layer_ids: LayerIds,
}

impl<G, L, R> Base for AndFilteredGraph<G, L, R> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G, L, R> Static for AndFilteredGraph<G, L, R> {}
impl<G, L, R> Immutable for AndFilteredGraph<G, L, R> {}

impl<G, L, R> InheritCoreGraphOps for AndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritStorageOps for AndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritMaterialize for AndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritPropertiesOps for AndFilteredGraph<G, L, R> {}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritTimeSemantics for AndFilteredGraph<G, L, R> {}

impl<G, L: Send + Sync, R: Send + Sync> InternalLayerOps for AndFilteredGraph<G, L, R>
where
    G: InternalLayerOps,
{
    fn layer_ids(&self) -> &LayerIds {
        &self.layer_ids
    }
}

impl<G, L, R> ListOps for AndFilteredGraph<G, L, R>
where
    L: ListOps,
    R: ListOps,
{
    fn node_list(&self) -> NodeList {
        let left = self.left.node_list();
        let right = self.right.node_list();
        left.intersection(&right)
    }

    fn edge_list(&self) -> EdgeList {
        let left = self.left.edge_list();
        let right = self.right.edge_list();
        left.intersection(&right)
    }
}

impl<G, L: InternalNodeFilterOps, R: InternalNodeFilterOps> InternalNodeFilterOps
    for AndFilteredGraph<G, L, R>
{
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        self.left.internal_nodes_filtered() || self.right.internal_nodes_filtered()
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        self.left.internal_node_list_trusted() && self.right.internal_node_list_trusted()
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.left.internal_filter_node(node, layer_ids)
            && self.right.internal_filter_node(node, layer_ids)
    }
}

impl<G, L: InternalEdgeLayerFilterOps, R: InternalEdgeLayerFilterOps> InternalEdgeLayerFilterOps
    for AndFilteredGraph<G, L, R>
{
    fn internal_edge_layer_filtered(&self) -> bool {
        self.left.internal_edge_layer_filtered() || self.right.internal_edge_layer_filtered()
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        self.left.internal_layer_filter_edge_list_trusted()
            && self.right.internal_layer_filter_edge_list_trusted()
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: usize) -> bool {
        self.left.internal_filter_edge_layer(edge, layer)
            && self.right.internal_filter_edge_layer(edge, layer)
    }
}

impl<G, L: InternalExplodedEdgeFilterOps, R: InternalExplodedEdgeFilterOps>
    InternalExplodedEdgeFilterOps for AndFilteredGraph<G, L, R>
{
    fn internal_exploded_edge_filtered(&self) -> bool {
        self.left.internal_exploded_edge_filtered() || self.right.internal_exploded_edge_filtered()
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        self.left.internal_exploded_filter_edge_list_trusted()
            && self.right.internal_exploded_filter_edge_list_trusted()
    }

    fn internal_filter_exploded_edge(&self, eid: ELID, t: EventTime, layer_ids: &LayerIds) -> bool {
        self.left.internal_filter_exploded_edge(eid, t, layer_ids)
            && self.right.internal_filter_exploded_edge(eid, t, layer_ids)
    }
}

impl<G, L: InternalEdgeFilterOps, R: InternalEdgeFilterOps> InternalEdgeFilterOps
    for AndFilteredGraph<G, L, R>
{
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.left.internal_edge_filtered() || self.right.internal_edge_filtered()
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        self.left.internal_edge_list_trusted() && self.right.internal_edge_list_trusted()
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        self.left.internal_filter_edge(edge, layer_ids)
            && self.right.internal_filter_edge(edge, layer_ids)
    }
}
