use crate::{
    db::{
        api::{
            properties::internal::{
                InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
            },
            view::internal::{
                EdgeList, GraphView, Immutable, InheritMaterialize, InheritStorageOps,
                InheritTimeSemantics, InternalEdgeFilterOps, InternalEdgeLayerFilterOps,
                InternalExplodedEdgeFilterOps, InternalLayerOps, InternalNodeFilterOps, ListOps,
                NodeList, Static,
            },
        },
        graph::views::filter::leg::{
            keeps_edge, keeps_edge_layer, keeps_exploded_edge, keeps_node, restricts_edge_layers,
            restricts_edges, restricts_exploded_edges, restricts_nodes, restricts_view,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::{
        entities::{LayerId, LayerIds, ELID},
        storage::timeindex::EventTime,
    },
    inherit::Base,
};
use raphtory_storage::{
    core_ops::{CoreGraphOps, InheritCoreGraphOps},
    graph::{edges::edge_ref::EdgeEntryRef, nodes::node_ref::NodeStorageRef},
};

#[derive(Debug, Clone)]
pub struct AndFilteredGraph<G, L, R> {
    pub(crate) graph: G,
    pub(crate) left: L,
    pub(crate) right: R,
    pub(crate) layer_ids: LayerIds,
}

impl<G: GraphView, L: GraphView, R: GraphView> AndFilteredGraph<G, L, R> {
    pub fn new(graph: G, left: L, right: R) -> Self {
        let layer_ids = left.layer_ids().intersect(right.layer_ids());
        Self {
            graph,
            left,
            right,
            layer_ids,
        }
    }
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
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritNodePropertySchemaOps
    for AndFilteredGraph<G, L, R>
{
}
impl<'graph, G: GraphViewOps<'graph>, L, R> InheritEdgePropertySchemaOps
    for AndFilteredGraph<G, L, R>
{
}
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
    G: CoreGraphOps,
    L: ListOps,
    R: ListOps,
{
    fn node_list(&self) -> NodeList {
        let left = self.left.node_list();
        let right = self.right.node_list();
        left.intersection(&right, self.graph.core_graph())
    }

    fn edge_list(&self) -> EdgeList {
        let left = self.left.edge_list();
        let right = self.right.edge_list();
        left.intersection(&right, self.graph.core_graph())
    }
}

impl<G, L: GraphView, R: GraphView> InternalNodeFilterOps for AndFilteredGraph<G, L, R> {
    #[inline]
    fn internal_nodes_filtered(&self) -> bool {
        restricts_nodes(&self.left) || restricts_nodes(&self.right)
    }

    #[inline]
    fn internal_node_list_trusted(&self) -> bool {
        self.left.internal_node_list_trusted()
            && self.right.internal_node_list_trusted()
            && !restricts_view(&self.left)
            && !restricts_view(&self.right)
    }

    #[inline]
    fn internal_filter_node(&self, node: NodeStorageRef, _layer_ids: &LayerIds) -> bool {
        keeps_node(&self.left, node) && keeps_node(&self.right, node)
    }
}

impl<G, L: GraphView, R: GraphView> InternalEdgeLayerFilterOps for AndFilteredGraph<G, L, R> {
    fn internal_edge_layer_filtered(&self) -> bool {
        restricts_edge_layers(&self.left) || restricts_edge_layers(&self.right)
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        self.left.internal_layer_filter_edge_list_trusted()
            && self.right.internal_layer_filter_edge_list_trusted()
            && !restricts_view(&self.left)
            && !restricts_view(&self.right)
    }

    fn internal_filter_edge_layer(&self, edge: EdgeEntryRef, layer: LayerId) -> bool {
        keeps_edge_layer(&self.left, edge, layer) && keeps_edge_layer(&self.right, edge, layer)
    }
}

impl<G, L: GraphView, R: GraphView> InternalExplodedEdgeFilterOps for AndFilteredGraph<G, L, R> {
    fn internal_exploded_edge_filtered(&self) -> bool {
        restricts_exploded_edges(&self.left) || restricts_exploded_edges(&self.right)
    }

    fn internal_exploded_filter_edge_list_trusted(&self) -> bool {
        self.left.internal_exploded_filter_edge_list_trusted()
            && self.right.internal_exploded_filter_edge_list_trusted()
            && !restricts_view(&self.left)
            && !restricts_view(&self.right)
    }

    fn internal_filter_exploded_edge(
        &self,
        eid: ELID,
        t: EventTime,
        _layer_ids: &LayerIds,
    ) -> bool {
        keeps_exploded_edge(&self.left, eid, t) && keeps_exploded_edge(&self.right, eid, t)
    }
}

impl<G, L: GraphView, R: GraphView> InternalEdgeFilterOps for AndFilteredGraph<G, L, R> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        restricts_edges(&self.left) || restricts_edges(&self.right)
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        self.left.internal_edge_list_trusted()
            && self.right.internal_edge_list_trusted()
            && !restricts_view(&self.left)
            && !restricts_view(&self.right)
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, _layer_ids: &LayerIds) -> bool {
        keeps_edge(&self.left, edge) && keeps_edge(&self.right, edge)
    }
}
